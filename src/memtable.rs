use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::logql::{LabelMatcher, LineFilter};
use crate::part::MetadataWindow;
use crate::tenant::TenantId;

pub type Labels = BTreeMap<String, String>;

/// One tenant's streams. The MemTable keeps a map of these rather than one
/// flat `(tenant, labels)` map so that every read path has to name a tenant
/// before it can reach any entry.
pub type TenantStreams = HashMap<Labels, Vec<LogEntry>>;
pub type MemTableSnapshot = HashMap<TenantId, TenantStreams>;

#[derive(Clone)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: Vec<(String, String)>,
}

pub struct StreamResult {
    pub labels: Labels,
    pub entries: Vec<LogEntry>,
}

pub struct QueryResult {
    pub results: Vec<StreamResult>,
    pub scanned_rows: usize,
    pub scanned_bytes: u64,
}

pub struct MemTable {
    inner: RwLock<MemTableSnapshot>,
    flushing: RwLock<Option<MemTableSnapshot>>,
    /// Live byte totals for the two buffers, maintained by every mutation.
    ///
    /// Walking the entries instead would be O(rows) under a read lock, and the
    /// callers are the flush loop's twice-a-second size check and the ingest
    /// backpressure gate — both of which get slower exactly when the memtable
    /// is growing, which is when they most need to be cheap.
    inner_bytes: AtomicU64,
    flushing_bytes: AtomicU64,
}

/// What a stream's own identity contributes, counted once per stream rather
/// than per entry. High-cardinality label sets are the case the memtable
/// ceiling most needs to catch, so this is counted rather than ignored.
fn stream_overhead_bytes(tenant: &TenantId, labels: &Labels) -> u64 {
    labels_overhead_bytes(tenant.as_str().len(), labels)
}

fn labels_overhead_bytes(tenant_bytes: usize, labels: &Labels) -> u64 {
    let labels_bytes: usize = labels
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum();
    (tenant_bytes + labels_bytes) as u64
}

fn entries_bytes(entries: &[LogEntry]) -> u64 {
    entries
        .iter()
        .map(|entry| {
            let metadata_bytes: usize = entry
                .structured_metadata
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum();
            (entry.line.len() + metadata_bytes) as u64
        })
        .sum()
}

/// The O(rows) walk the counters replaced, kept as the thing tests compare
/// them against.
#[cfg(test)]
fn snapshot_bytes(snapshot: &MemTableSnapshot) -> u64 {
    let mut bytes = 0u64;
    for (tenant, streams) in snapshot {
        for (labels, entries) in streams {
            bytes += stream_overhead_bytes(tenant, labels);
            bytes += entries_bytes(entries);
        }
    }
    bytes
}

/// Merge `source` into `target`, reporting the stream overhead the merge made
/// redundant.
///
/// A stream present in both buffers was counted once in each, but the merged
/// buffer holds one copy of its identity. Returning the difference keeps the
/// byte counters exactly equal to a full walk without either side having to
/// perform one — the collision count is bounded by the stream count, not the
/// row count.
fn merge_snapshot(target: &mut MemTableSnapshot, source: MemTableSnapshot) -> u64 {
    let mut redundant_overhead = 0u64;
    for (tenant, streams) in source {
        let tenant_bytes = tenant.as_str().len();
        let tenant_streams = target.entry(tenant).or_default();
        for (labels, entries) in streams {
            let overhead = labels_overhead_bytes(tenant_bytes, &labels);
            let stream = tenant_streams.entry(labels).or_default();
            if !stream.is_empty() {
                redundant_overhead += overhead;
            }
            stream.extend(entries);
        }
    }
    redundant_overhead
}

#[allow(clippy::too_many_arguments)]
fn scan_memtable_stream(
    labels: &Labels,
    entries: &[LogEntry],
    line_filters: &[LineFilter],
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_limit: Option<usize>,
    cancellation: Option<&AtomicBool>,
    scanned_rows: &mut usize,
    grouped: &mut BTreeMap<Labels, Vec<LogEntry>>,
    scan_stopped: &mut bool,
) {
    let mut ordered: Vec<&LogEntry> = entries.iter().collect();
    ordered.sort_unstable_by_key(|entry| entry.timestamp_ns);
    if !forward {
        ordered.reverse();
    }
    let mut matched = 0usize;
    for entry in ordered {
        if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            *scan_stopped = true;
            break;
        }
        *scanned_rows = scanned_rows.saturating_add(1);
        if entry.timestamp_ns >= start_ns
            && entry.timestamp_ns <= end_ns
            && line_filters
                .iter()
                .all(|filter| filter.matches(&entry.line))
        {
            grouped
                .entry(labels.clone())
                .or_default()
                .push(entry.clone());
            matched += 1;
            // Each stream contributes at most `limit` rows to the global top
            // or bottom `limit`, so older rows from this stream cannot affect
            // the final result once its own candidate set is full.
            if !forward && matched >= limit {
                break;
            }
        }
        if scan_limit.is_some_and(|limit| *scanned_rows >= limit) {
            *scan_stopped = true;
            break;
        }
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            flushing: RwLock::new(None),
            inner_bytes: AtomicU64::new(0),
            flushing_bytes: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, tenant: TenantId, labels: Labels, entries: Vec<LogEntry>) {
        let mut delta = entries_bytes(&entries);
        let mut inner = self.inner.write().unwrap();
        let overhead = stream_overhead_bytes(&tenant, &labels);
        let stream = inner.entry(tenant).or_default().entry(labels).or_default();
        if stream.is_empty() {
            delta += overhead;
        }
        stream.extend(entries);
        // Published under the same write lock as the mutation, so a reader
        // that sees the entries also sees them counted.
        self.inner_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn begin_flush(&self) -> MemTableSnapshot {
        let mut inner = self.inner.write().unwrap();
        let mut flushing = self.flushing.write().unwrap();
        let mut snapshot = std::mem::take(&mut *inner);
        let moved = self.inner_bytes.swap(0, Ordering::Relaxed);
        let mut redundant_overhead = 0;
        if let Some(previous_snapshot) = flushing.take() {
            redundant_overhead = merge_snapshot(&mut snapshot, previous_snapshot);
        }
        *flushing = Some(snapshot.clone());
        // `flushing_bytes` already covers the snapshot that was merged in.
        self.flushing_bytes
            .fetch_add(moved.saturating_sub(redundant_overhead), Ordering::Relaxed);
        snapshot
    }

    pub fn commit_flush(&self) {
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
        self.flushing_bytes.store(0, Ordering::Relaxed);
    }

    pub fn abort_flush(&self, snapshot: MemTableSnapshot) {
        let mut inner = self.inner.write().unwrap();
        let redundant_overhead = merge_snapshot(&mut inner, snapshot);
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
        // The aborted snapshot is the one `flushing_bytes` was counting, so it
        // moves back wholesale rather than being recomputed.
        let returned = self.flushing_bytes.swap(0, Ordering::Relaxed);
        self.inner_bytes.fetch_add(
            returned.saturating_sub(redundant_overhead),
            Ordering::Relaxed,
        );
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read().unwrap();
        if !inner.is_empty() {
            return false;
        }
        let flushing = self.flushing.read().unwrap();
        flushing.as_ref().map(|m| m.is_empty()).unwrap_or(true)
    }

    /// Every tenant with entries that have not been flushed yet, including the
    /// ones in a flush that is still in flight. A tenant that has only ever
    /// pushed is invisible in `meta.json` until its first flush, so the
    /// unknown-tenant gauge reads this too.
    pub fn tenants(&self) -> BTreeSet<TenantId> {
        let inner = self.inner.read().unwrap();
        let flushing = self.flushing.read().unwrap();
        inner
            .keys()
            .chain(flushing.iter().flat_map(|snapshot| snapshot.keys()))
            .cloned()
            .collect()
    }

    /// Bytes held across both buffers, in O(1).
    ///
    /// Takes no lock: the two counters are maintained under the same write
    /// locks as the buffers, and a reader that lands between the pair sees a
    /// value that is one in-flight mutation stale. Callers use it for
    /// thresholds, and no threshold is meaningful at that resolution.
    pub fn approximate_size(&self) -> usize {
        self.inner_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.flushing_bytes.load(Ordering::Relaxed)) as usize
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Vec<StreamResult> {
        self.query_with_scan_limit(
            tenant,
            matchers,
            line_filters,
            start_ns,
            end_ns,
            limit,
            forward,
            None,
            None,
        )
        .results
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_scan_limit(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> QueryResult {
        if limit == 0 {
            return QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
                scanned_bytes: 0,
            };
        }
        let mut grouped: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
        let mut scanned_rows = 0usize;
        let mut scan_stopped = false;

        // begin_flush and abort_flush acquire inner before flushing. Holding
        // both read guards in that order prevents observing the same entry in
        // both buffers if a flush starts between the two reads.
        let inner = self.inner.read().unwrap();
        if let Some(streams) = inner.get(tenant) {
            for (labels, entries) in streams.iter() {
                if !matchers.iter().all(|m| m.matches(labels)) {
                    continue;
                }
                scan_memtable_stream(
                    labels,
                    entries,
                    line_filters,
                    start_ns,
                    end_ns,
                    limit,
                    forward,
                    scan_limit,
                    cancellation,
                    &mut scanned_rows,
                    &mut grouped,
                    &mut scan_stopped,
                );
                if scan_stopped {
                    break;
                }
            }
        }
        let flushing = self.flushing.read().unwrap();
        if !scan_stopped && let Some(streams) = flushing.as_ref().and_then(|f| f.get(tenant)) {
            for (labels, entries) in streams {
                if !matchers.iter().all(|m| m.matches(labels)) {
                    continue;
                }
                scan_memtable_stream(
                    labels,
                    entries,
                    line_filters,
                    start_ns,
                    end_ns,
                    limit,
                    forward,
                    scan_limit,
                    cancellation,
                    &mut scanned_rows,
                    &mut grouped,
                    &mut scan_stopped,
                );
                if scan_stopped {
                    break;
                }
            }
        }
        drop(flushing);
        drop(inner);

        let mut all_entries: Vec<(Labels, LogEntry)> = grouped
            .into_iter()
            .flat_map(|(labels, entries)| {
                entries
                    .into_iter()
                    .map(move |entry| (labels.clone(), entry))
            })
            .collect();
        if forward {
            all_entries.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all_entries.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        all_entries.truncate(limit);
        let mut result_groups: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
        for (labels, entry) in all_entries {
            result_groups.entry(labels).or_default().push(entry);
        }
        let results = result_groups
            .into_iter()
            .map(|(labels, entries)| StreamResult { labels, entries })
            .collect();

        QueryResult {
            results,
            scanned_rows,
            scanned_bytes: 0,
        }
    }

    /// Run `visit` over both buffers of one tenant while holding the read
    /// guards in the order `begin_flush`/`abort_flush` acquire them, so an
    /// entry can never be observed in both buffers or in neither.
    /// Visit each of the tenant's streams, restricted to entries the tenant's
    /// retention still covers. `retention_floor_ns` of `None` visits
    /// everything, which is the fail-open behaviour for an unknown tenant.
    fn for_each_tenant_stream(
        &self,
        tenant: &TenantId,
        window: MetadataWindow,
        mut visit: impl FnMut(&Labels, &[LogEntry]),
    ) {
        let inner = self.inner.read().unwrap();
        let flushing = self.flushing.read().unwrap();
        let mut retained = Vec::new();
        // Entry granularity rather than stream granularity: unlike a part,
        // whose bounds are already in its metadata, a memtable stream has to be
        // walked anyway, so filtering it exactly costs nothing extra.
        let mut visit_retained = |labels: &Labels, entries: &[LogEntry]| {
            retained.clear();
            retained.extend(
                entries
                    .iter()
                    .filter(|entry| window.contains(entry.timestamp_ns))
                    .cloned(),
            );
            if !retained.is_empty() {
                visit(labels, &retained);
            }
        };
        if let Some(streams) = inner.get(tenant) {
            for (labels, entries) in streams {
                visit_retained(labels, entries);
            }
        }
        if let Some(streams) = flushing.as_ref().and_then(|f| f.get(tenant)) {
            for (labels, entries) in streams {
                visit_retained(labels, entries);
            }
        }
    }

    pub fn label_names(&self, tenant: &TenantId, window: MetadataWindow) -> Vec<String> {
        let mut names = BTreeSet::new();
        self.for_each_tenant_stream(tenant, window, |labels, _| {
            for k in labels.keys() {
                names.insert(k.clone());
            }
        });
        names.into_iter().collect()
    }

    pub fn label_values(
        &self,
        tenant: &TenantId,
        name: &str,
        window: MetadataWindow,
    ) -> Vec<String> {
        let mut values = BTreeSet::new();
        self.for_each_tenant_stream(tenant, window, |labels, _| {
            if let Some(v) = labels.get(name) {
                values.insert(v.clone());
            }
        });
        values.into_iter().collect()
    }

    pub fn series(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        window: MetadataWindow,
    ) -> Vec<Labels> {
        let mut result: BTreeSet<Labels> = BTreeSet::new();
        self.for_each_tenant_stream(tenant, window, |labels, _| {
            if matchers.iter().all(|m| m.matches(labels)) {
                result.insert(labels.clone());
            }
        });
        result.into_iter().collect()
    }

    /// Process-wide totals for the operator metrics endpoint. Not a query
    /// path: nothing here is returned to a tenant.
    pub fn global_stats(&self) -> IndexStats {
        let mut streams = 0usize;
        let mut entries = 0usize;
        let mut bytes = 0u64;
        let inner = self.inner.read().unwrap();
        let flushing = self.flushing.read().unwrap();
        for snapshot in std::iter::once(&*inner).chain(flushing.as_ref()) {
            for tenant_streams in snapshot.values() {
                streams += tenant_streams.len();
                for stream in tenant_streams.values() {
                    entries += stream.len();
                    for entry in stream {
                        bytes += entry.line.len() as u64;
                    }
                }
            }
        }
        IndexStats {
            streams,
            entries,
            bytes,
        }
    }

    pub fn stats(&self, tenant: &TenantId, window: MetadataWindow) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        self.for_each_tenant_stream(tenant, window, |labels, stream| {
            stream_set.insert(labels.clone());
            entries += stream.len();
            for e in stream {
                bytes += e.line.len() as u64;
            }
        });
        IndexStats {
            streams: stream_set.len(),
            entries,
            bytes,
        }
    }
}

pub struct IndexStats {
    pub streams: usize,
    pub entries: usize,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The counters replace an O(rows) walk, so what has to hold is that they
    /// still say what the walk would. Checked after each transition rather
    /// than only at the end: a counter that drifts and later recovers is a
    /// counter that reported a wrong threshold in between.
    #[test]
    fn the_size_counters_track_the_walked_total_across_a_flush_cycle() {
        let memtable = MemTable::new();
        let walked = |table: &MemTable| {
            let inner = table.inner.read().unwrap();
            let flushing = table.flushing.read().unwrap();
            snapshot_bytes(&inner) + flushing.as_ref().map(snapshot_bytes).unwrap_or(0)
        };
        let tenant = crate::tenant::test_tenant();
        let labels: Labels = [("app".to_string(), "sizes".to_string())]
            .into_iter()
            .collect();

        memtable.insert(
            tenant.clone(),
            labels.clone(),
            vec![sample_entry("first", 1)],
        );
        assert_eq!(memtable.approximate_size() as u64, walked(&memtable));

        // A second insert into the same stream must not re-count the stream's
        // own identity.
        memtable.insert(
            tenant.clone(),
            labels.clone(),
            vec![sample_entry("second", 2)],
        );
        assert_eq!(memtable.approximate_size() as u64, walked(&memtable));

        let snapshot = memtable.begin_flush();
        assert_eq!(memtable.approximate_size() as u64, walked(&memtable));

        // An insert while a flush is in flight lands in the other buffer.
        memtable.insert(tenant, labels, vec![sample_entry("third", 3)]);
        assert_eq!(memtable.approximate_size() as u64, walked(&memtable));

        memtable.abort_flush(snapshot);
        assert_eq!(memtable.approximate_size() as u64, walked(&memtable));

        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        drop(snapshot);
        assert_eq!(memtable.approximate_size(), 0);
        assert_eq!(walked(&memtable), 0);
    }

    fn sample_entry(line: &str, ts_ns: i64) -> LogEntry {
        LogEntry {
            timestamp_ns: ts_ns,
            line: line.to_string(),
            structured_metadata: vec![],
        }
    }

    fn sample_labels() -> Labels {
        std::iter::once(("app".to_string(), "test".to_string())).collect()
    }

    fn tenant(name: &str) -> TenantId {
        TenantId::parse(name).unwrap()
    }

    fn sample_tenant() -> TenantId {
        tenant("acme")
    }

    #[test]
    fn a_tenant_never_sees_another_tenants_entries() {
        let memtable = MemTable::new();
        memtable.insert(
            tenant("acme"),
            sample_labels(),
            vec![sample_entry("acme line", 100)],
        );
        memtable.insert(
            tenant("globex"),
            sample_labels(),
            vec![sample_entry("globex line", 100)],
        );

        let acme = memtable.query(&tenant("acme"), &[], &[], i64::MIN, i64::MAX, 100, true);
        let lines: Vec<_> = acme
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["acme line"]);

        assert_eq!(
            memtable
                .stats(&tenant("acme"), MetadataWindow::unbounded())
                .entries,
            1
        );
        assert_eq!(
            memtable
                .stats(&tenant("globex"), MetadataWindow::unbounded())
                .entries,
            1
        );
        assert!(
            memtable
                .query(&tenant("initech"), &[], &[], i64::MIN, i64::MAX, 100, true)
                .is_empty(),
            "an unknown tenant must see nothing"
        );
        assert!(
            memtable
                .label_names(&tenant("initech"), MetadataWindow::unbounded())
                .is_empty()
        );
        assert!(
            memtable
                .series(&tenant("initech"), &[], MetadataWindow::unbounded())
                .is_empty()
        );
    }

    #[test]
    fn flushing_buffer_visible_during_flush() {
        // begin_flush는 inner를 비우고 flushing 버퍼로 옮긴다.
        // unified_query는 flushing 버퍼까지 조회하므로 flush 진행 중에도
        // 데이터는 사라지지 않는다 (이슈 #2 복구).
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("hello", 100)],
        );

        let snapshot = mt.begin_flush();
        assert_eq!(snapshot.len(), 1);

        let results = mt.query(&sample_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 1,
            "flushing buffer should remain visible during flush"
        );

        mt.commit_flush();
        let results2 = mt.query(&sample_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total2: usize = results2.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total2, 0,
            "after commit_flush, no data should remain visible"
        );
    }

    #[test]
    fn abort_flush_restores_to_inner() {
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("hello", 100)],
        );

        let snapshot = mt.begin_flush();
        mt.abort_flush(snapshot);

        let results = mt.query(&sample_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1, "abort_flush should restore data to inner");
        assert!(!mt.is_empty());
    }

    #[test]
    fn begin_flush_keeps_query_consistent_with_concurrent_insert() {
        // flush 진행 중 새로 들어온 데이터도 보여야 한다.
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("first", 100)],
        );

        let _snapshot = mt.begin_flush();
        // flush 진행 중 새 데이터 수신
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("second", 200)],
        );

        let results = mt.query(&sample_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 2,
            "both flushing buffer and inner should be visible concurrently"
        );
    }

    #[test]
    fn stats_does_not_count_same_stream_twice_during_flush() {
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("first", 100)],
        );
        let _snapshot = mt.begin_flush();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("second", 200)],
        );

        let stats = mt.stats(&sample_tenant(), MetadataWindow::unbounded());
        assert_eq!(stats.streams, 1);
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn begin_flush_preserves_previous_uncommitted_snapshot() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("first", 100)],
        );

        let _first_snapshot = memtable.begin_flush();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("second", 200)],
        );

        let second_snapshot = memtable.begin_flush();
        let snapshot_entries: usize = second_snapshot
            .values()
            .flat_map(|streams| streams.values())
            .map(Vec::len)
            .sum();
        assert_eq!(snapshot_entries, 2);

        memtable.abort_flush(second_snapshot);
        let results = memtable.query(&sample_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total_entries: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total_entries, 2);
    }

    #[test]
    fn query_includes_the_end_timestamp() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("at the inclusive end", i64::MAX)],
        );

        let results = memtable.query(&sample_tenant(), &[], &[], i64::MAX, i64::MAX, 100, true);
        let total_entries: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total_entries, 1);
    }
}
