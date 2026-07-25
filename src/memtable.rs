use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::logql::{LabelMatcher, LineFilter};
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
}

fn merge_snapshot(target: &mut MemTableSnapshot, source: MemTableSnapshot) {
    for (tenant, streams) in source {
        let tenant_streams = target.entry(tenant).or_default();
        for (labels, entries) in streams {
            tenant_streams.entry(labels).or_default().extend(entries);
        }
    }
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
        }
    }

    pub fn insert(&self, tenant: TenantId, labels: Labels, entries: Vec<LogEntry>) {
        let mut inner = self.inner.write().unwrap();
        let stream = inner.entry(tenant).or_default().entry(labels).or_default();
        stream.extend(entries);
    }

    pub fn begin_flush(&self) -> MemTableSnapshot {
        let mut inner = self.inner.write().unwrap();
        let mut flushing = self.flushing.write().unwrap();
        let mut snapshot = std::mem::take(&mut *inner);
        if let Some(previous_snapshot) = flushing.take() {
            merge_snapshot(&mut snapshot, previous_snapshot);
        }
        *flushing = Some(snapshot.clone());
        snapshot
    }

    pub fn commit_flush(&self) {
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
    }

    pub fn abort_flush(&self, snapshot: MemTableSnapshot) {
        let mut inner = self.inner.write().unwrap();
        merge_snapshot(&mut inner, snapshot);
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read().unwrap();
        if !inner.is_empty() {
            return false;
        }
        let flushing = self.flushing.read().unwrap();
        flushing.as_ref().map(|m| m.is_empty()).unwrap_or(true)
    }

    pub fn approximate_size(&self) -> usize {
        fn snapshot_bytes(snapshot: &MemTableSnapshot) -> usize {
            let mut bytes = 0usize;
            for (tenant, streams) in snapshot {
                for (labels, entries) in streams {
                    bytes += tenant.as_str().len();
                    for (k, v) in labels {
                        bytes += k.len() + v.len();
                    }
                    for e in entries {
                        bytes += e.line.len();
                        for (k, v) in &e.structured_metadata {
                            bytes += k.len() + v.len();
                        }
                    }
                }
            }
            bytes
        }

        let inner = self.inner.read().unwrap();
        let mut bytes = snapshot_bytes(&inner);
        // Keep the lock order aligned with begin_flush/abort_flush. The
        // size is therefore computed from one consistent pair of buffers.
        let flushing = self.flushing.read().unwrap();
        if let Some(f) = flushing.as_ref() {
            bytes += snapshot_bytes(f);
        }
        drop(flushing);
        drop(inner);
        bytes
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
        retention_floor_ns: Option<i64>,
        mut visit: impl FnMut(&Labels, &[LogEntry]),
    ) {
        let inner = self.inner.read().unwrap();
        let flushing = self.flushing.read().unwrap();
        let mut retained = Vec::new();
        let mut visit_retained = |labels: &Labels, entries: &[LogEntry]| match retention_floor_ns {
            None => visit(labels, entries),
            Some(floor_ns) => {
                retained.clear();
                retained.extend(
                    entries
                        .iter()
                        .filter(|entry| entry.timestamp_ns >= floor_ns)
                        .cloned(),
                );
                if !retained.is_empty() {
                    visit(labels, &retained);
                }
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

    pub fn label_names(&self, tenant: &TenantId, retention_floor_ns: Option<i64>) -> Vec<String> {
        let mut names = BTreeSet::new();
        self.for_each_tenant_stream(tenant, retention_floor_ns, |labels, _| {
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
        retention_floor_ns: Option<i64>,
    ) -> Vec<String> {
        let mut values = BTreeSet::new();
        self.for_each_tenant_stream(tenant, retention_floor_ns, |labels, _| {
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
        retention_floor_ns: Option<i64>,
    ) -> Vec<Labels> {
        let mut result: BTreeSet<Labels> = BTreeSet::new();
        self.for_each_tenant_stream(tenant, retention_floor_ns, |labels, _| {
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

    pub fn stats(&self, tenant: &TenantId, retention_floor_ns: Option<i64>) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        self.for_each_tenant_stream(tenant, retention_floor_ns, |labels, stream| {
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

        assert_eq!(memtable.stats(&tenant("acme"), None).entries, 1);
        assert_eq!(memtable.stats(&tenant("globex"), None).entries, 1);
        assert!(
            memtable
                .query(&tenant("initech"), &[], &[], i64::MIN, i64::MAX, 100, true)
                .is_empty(),
            "an unknown tenant must see nothing"
        );
        assert!(memtable.label_names(&tenant("initech"), None).is_empty());
        assert!(memtable.series(&tenant("initech"), &[], None).is_empty());
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

        let stats = mt.stats(&sample_tenant(), None);
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
