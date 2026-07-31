use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::logql::{LabelMatcher, LineFilter};
use crate::part::{MetadataWindow, QueryTimeRange};
use crate::tenant::TenantId;

pub type Labels = BTreeMap<String, String>;

/// One stream's label set, shared by every row that belongs to the stream
/// instead of copied into each of them.
///
/// The memtable already held one `Labels` per stream, and every hop after it
/// held one per *row*: `Row::from_entry`, the part writer's stream index and
/// stream set, the reader, the registry, the executor and the metric path. That
/// was measured at 1 326-1 345 bytes and 11-27 allocations per row
/// ([`docs/MEMORY_ATTRIBUTION.md`](../docs/MEMORY_ATTRIBUTION.md) hypothesis 2)
/// and 721 MiB live in the flush arena — the largest live term in the process.
///
/// `Arc` rather than an interned handle: an intern table needs a global map, a
/// lock on the ingest path and a policy for when an entry may be dropped, and
/// buys 4 bytes per row over a pointer. Sharing is already scoped by the
/// structures that hold it — a memtable stream, a `Vec<Row>` for one flush, one
/// query's result — so a refcount expresses exactly the lifetime that is
/// wanted and nothing has to decide when to evict.
pub type SharedLabels = Arc<Labels>;

/// One tenant's streams. The MemTable keeps a map of these rather than one
/// flat `(tenant, labels)` map so that every read path has to name a tenant
/// before it can reach any entry.
pub type TenantStreams = HashMap<SharedLabels, Vec<LogEntry>>;
pub type MemTableSnapshot = HashMap<TenantId, TenantStreams>;

#[derive(Clone)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: Vec<(String, String)>,
}

pub struct StreamResult {
    pub labels: SharedLabels,
    pub entries: Vec<LogEntry>,
}

pub struct QueryResult {
    pub results: Vec<StreamResult>,
    pub scanned_rows: usize,
    pub scanned_bytes: u64,
}

pub struct MemTable {
    inner: RwLock<MemTableSnapshot>,
    /// The snapshot a flush is currently writing, shared with the flush rather
    /// than copied to it.
    ///
    /// It was cloned, which doubled the memtable's memory at exactly the moment
    /// the memtable is largest — and the failure mode this guards against is a
    /// flush that cannot keep up, where the buffer is larger still.
    flushing: RwLock<Option<Arc<MemTableSnapshot>>>,
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

/// Sorted by key, duplicate keys collapsed to their first occurrence.
pub fn canonicalize_structured_metadata(pairs: &mut Vec<(String, String)>) {
    if pairs.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        return;
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs.dedup_by(|later, earlier| later.0 == earlier.0);
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

/// Take ownership of a shared snapshot, copying only if someone else still
/// holds it. On the paths that call this the other reference has already been
/// dropped, so the copy is a fallback that keeps a stray share from being
/// silently discarded rather than an expected cost.
fn unwrap_snapshot(snapshot: Arc<MemTableSnapshot>) -> MemTableSnapshot {
    Arc::try_unwrap(snapshot).unwrap_or_else(|shared| (*shared).clone())
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

/// One stream, offered to the sink in the query's direction.
///
/// The stream is sorted first, so the sink's frontier ends it: the first entry
/// on the far side of the frontier means every later one in this direction is
/// too. That replaces "each stream contributes at most `limit` rows", which was
/// only sound when nothing filtered rows after the scan — with a `| json |
/// field=` stage the surviving count is not the scanned count, and the old rule
/// was therefore restricted to backward scans and bypassed entirely by
/// `normal_scan_limit = usize::MAX`.
#[allow(clippy::too_many_arguments)]
fn scan_memtable_stream(
    labels: &SharedLabels,
    entries: &[LogEntry],
    line_filters: &[LineFilter],
    range: QueryTimeRange,
    forward: bool,
    scan_limit: Option<usize>,
    cancellation: Option<&AtomicBool>,
    scanned_rows: &mut usize,
    sink: &mut dyn crate::part::RowSink,
    scan_stopped: &mut bool,
) -> Result<(), String> {
    let mut ordered: Vec<&LogEntry> = entries.iter().collect();
    ordered.sort_unstable_by_key(|entry| entry.timestamp_ns);
    if !forward {
        ordered.reverse();
    }
    for entry in ordered {
        if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            *scan_stopped = true;
            break;
        }
        if crate::part::beyond_frontier(sink.frontier_ns(), forward, entry.timestamp_ns) {
            break;
        }
        if scan_limit.is_some_and(|limit| *scanned_rows >= limit) {
            *scan_stopped = true;
            break;
        }
        *scanned_rows = scanned_rows.saturating_add(1);
        if range.contains(entry.timestamp_ns)
            && line_filters
                .iter()
                .all(|filter| filter.matches(&entry.line))
        {
            sink.accept(labels, entry.clone())?;
        }
    }
    Ok(())
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
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

    pub fn insert(&self, tenant: TenantId, labels: Labels, mut entries: Vec<LogEntry>) {
        // Every ingest path — Loki push in both encodings, OTLP, and journal
        // replay of any of them — converges here, so this is where structured
        // metadata takes its canonical form: sorted by key, one value per key,
        // first occurrence winning. First-wins is the visibility the pipeline
        // already has (`fields.entry(name).or_insert`), so canonicalizing does
        // not change what a query can see; OTLP genuinely produces duplicates
        // when a record attribute shares a name with a resource attribute.
        // Sorted-unique is what lets the part format store the pairs as
        // columns and rebuild them with a merge instead of a sort, and it
        // makes `Row::sort_key`'s dedup order-insensitive — strictly stronger,
        // which is the safe direction for at-least-once replay.
        for entry in &mut entries {
            canonicalize_structured_metadata(&mut entry.structured_metadata);
        }
        let mut delta = entries_bytes(&entries);
        let mut inner = self.inner.write().unwrap();
        let overhead = stream_overhead_bytes(&tenant, &labels);
        let streams = inner.entry(tenant).or_default();
        // One hash of the label set, whether or not the stream is new. Looking
        // the key up first to avoid allocating an `Arc` for a stream that is
        // already buffered costs a second hash of the whole `BTreeMap`, which
        // measured dearer than the 32-byte allocation it saves.
        match streams.entry(Arc::new(labels)) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                let stream = occupied.get_mut();
                if stream.is_empty() {
                    delta += overhead;
                }
                stream.extend(entries);
            }
            // Moved, not extended into an empty vector: a stream's first push
            // used to copy every `LogEntry` it carried.
            std::collections::hash_map::Entry::Vacant(vacant) => {
                delta += overhead;
                vacant.insert(entries);
            }
        }
        // Published under the same write lock as the mutation, so a reader
        // that sees the entries also sees them counted.
        self.inner_bytes.fetch_add(delta, Ordering::Relaxed);
    }

    /// Move the buffer into the flushing slot and hand the flush a share of it.
    ///
    /// The `Arc` is what makes this cheap: reads consult the flushing snapshot
    /// as well as the live buffer, so it has to stay reachable here, but there
    /// is no reason for the flush to hold a second copy of it.
    pub fn begin_flush(&self) -> Arc<MemTableSnapshot> {
        let mut inner = self.inner.write().unwrap();
        let mut flushing = self.flushing.write().unwrap();
        let mut snapshot = std::mem::take(&mut *inner);
        let moved = self.inner_bytes.swap(0, Ordering::Relaxed);
        let mut redundant_overhead = 0;
        if let Some(previous_snapshot) = flushing.take() {
            redundant_overhead = merge_snapshot(&mut snapshot, unwrap_snapshot(previous_snapshot));
        }
        let snapshot = Arc::new(snapshot);
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

    pub fn abort_flush(&self, snapshot: Arc<MemTableSnapshot>) {
        let mut inner = self.inner.write().unwrap();
        // Clearing the slot first drops this snapshot's other reference, so the
        // unwrap below takes ownership instead of copying. The lock order is
        // the one every other path uses — inner, then flushing — because
        // reversing it here would be the deadlock the ordering exists to avoid.
        let mut flushing = self.flushing.write().unwrap();
        *flushing = None;
        let redundant_overhead = merge_snapshot(&mut inner, unwrap_snapshot(snapshot));
        // The aborted snapshot is the one `flushing_bytes` was counting, so it
        // moves back wholesale rather than being recomputed.
        let returned = self.flushing_bytes.swap(0, Ordering::Relaxed);
        self.inner_bytes.fetch_add(
            returned.saturating_sub(redundant_overhead),
            Ordering::Relaxed,
        );
    }

    /// Whether the tenant already has this stream buffered, in either the live
    /// buffer or the one being flushed. Both count: a stream mid-flush is not a
    /// new stream, and treating it as one would charge a tenant twice for it
    /// every time a flush is in progress.
    pub fn contains_stream(&self, tenant: &TenantId, labels: &Labels) -> bool {
        if self
            .inner
            .read()
            .unwrap()
            .get(tenant)
            .is_some_and(|streams| streams.contains_key(labels))
        {
            return true;
        }
        self.flushing
            .read()
            .unwrap()
            .as_deref()
            .and_then(|snapshot| snapshot.get(tenant))
            .is_some_and(|streams| streams.contains_key(labels))
    }

    /// Every stream the tenant has buffered. Small by construction — it is
    /// bounded by what a flush interval accumulates — and only walked when a
    /// genuinely new stream appears.
    pub fn tenant_streams(&self, tenant: &TenantId) -> Vec<SharedLabels> {
        let mut streams: BTreeSet<SharedLabels> = BTreeSet::new();
        if let Some(buffered) = self.inner.read().unwrap().get(tenant) {
            streams.extend(buffered.keys().cloned());
        }
        if let Some(flushing) = self
            .flushing
            .read()
            .unwrap()
            .as_deref()
            .and_then(|snapshot| snapshot.get(tenant))
        {
            streams.extend(flushing.keys().cloned());
        }
        streams.into_iter().collect()
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

    pub fn query(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        range: QueryTimeRange,
        limit: usize,
        forward: bool,
    ) -> Vec<StreamResult> {
        self.query_with_scan_limit(
            tenant,
            matchers,
            line_filters,
            range,
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
        range: QueryTimeRange,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> QueryResult {
        let mut rows = crate::part::TopKRows::new(limit, forward);
        // A `TopKRows` never refuses a row, so the scan cannot fail here; the
        // fallible signature is for the executor's sink, which runs the pipeline.
        let scanned_rows = self
            .scan_into(
                tenant,
                matchers,
                line_filters,
                range,
                forward,
                scan_limit,
                cancellation,
                &mut rows,
            )
            .unwrap_or_default();
        QueryResult {
            results: rows.into_stream_results(),
            scanned_rows,
            scanned_bytes: 0,
        }
    }

    /// Both buffers' rows for one tenant, offered to `sink` in the query's
    /// direction.
    ///
    /// Streams are visited in label order, which is what the `BTreeMap` this
    /// used to group into gave for free and what a bounded sink needs kept: it
    /// breaks ties by arrival, so the arrival order is part of which rows a
    /// limited query returns. `HashMap` iteration order would have made that
    /// answer differ between two runs of the same query.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_into(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        range: QueryTimeRange,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
        sink: &mut dyn crate::part::RowSink,
    ) -> Result<usize, String> {
        let mut scanned_rows = 0usize;
        if sink.is_closed() {
            return Ok(scanned_rows);
        }
        let mut scan_stopped = false;

        // begin_flush and abort_flush acquire inner before flushing. Holding
        // both read guards in that order prevents observing the same entry in
        // both buffers if a flush starts between the two reads.
        let inner = self.inner.read().unwrap();
        let flushing = self.flushing.read().unwrap();
        // The live buffer first, so that for a stream present in both the rows
        // that were pushed earlier also arrive earlier. `sort_by` is stable, so
        // ordering by label set does not disturb that.
        let mut streams: Vec<(&SharedLabels, &[LogEntry])> = Vec::new();
        for buffer in std::iter::once(&*inner).chain(flushing.as_deref()) {
            if let Some(tenant_streams) = buffer.get(tenant) {
                streams.extend(
                    tenant_streams
                        .iter()
                        .filter(|(labels, _)| matchers.iter().all(|m| m.matches(labels)))
                        .map(|(labels, entries)| (labels, entries.as_slice())),
                );
            }
        }
        streams.sort_by(|left, right| left.0.cmp(right.0));

        let mut result = Ok(());
        for (labels, entries) in streams {
            result = scan_memtable_stream(
                labels,
                entries,
                line_filters,
                range,
                forward,
                scan_limit,
                cancellation,
                &mut scanned_rows,
                sink,
                &mut scan_stopped,
            );
            if result.is_err() || scan_stopped {
                break;
            }
        }
        drop(flushing);
        drop(inner);
        result.map(|()| scanned_rows)
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
                visit_retained(labels.as_ref(), entries);
            }
        }
        if let Some(streams) = flushing.as_ref().and_then(|f| f.get(tenant)) {
            for (labels, entries) in streams {
                visit_retained(labels.as_ref(), entries);
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
        for snapshot in std::iter::once(&*inner).chain(flushing.as_deref()) {
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

    /// Insert is the choke point every ingest path converges on, so metadata
    /// leaving the memtable is canonical whatever order or duplication it
    /// arrived with — OTLP genuinely duplicates a key when a record attribute
    /// shares a name with a resource attribute, and first-wins is the
    /// visibility the pipeline already gives the earlier value.
    #[test]
    fn inserted_metadata_is_sorted_and_first_wins_on_duplicate_keys() {
        let memtable = MemTable::new();
        let tenant = crate::tenant::test_tenant();
        let labels: Labels = [("app".to_string(), "canon".to_string())]
            .into_iter()
            .collect();
        memtable.insert(
            tenant.clone(),
            labels,
            vec![LogEntry {
                timestamp_ns: 1,
                line: "line".to_string(),
                structured_metadata: vec![
                    ("zeta".to_string(), "z".to_string()),
                    ("alpha".to_string(), "record".to_string()),
                    ("alpha".to_string(), "resource".to_string()),
                ],
            }],
        );
        let snapshot = memtable.begin_flush();
        let entry = &snapshot[&tenant].values().next().unwrap()[0];
        assert_eq!(
            entry.structured_metadata,
            vec![
                ("alpha".to_string(), "record".to_string()),
                ("zeta".to_string(), "z".to_string()),
            ]
        );
        memtable.abort_flush(snapshot);
    }

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
            snapshot_bytes(&inner) + flushing.as_deref().map(snapshot_bytes).unwrap_or(0)
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

        let acme = memtable.query(
            &tenant("acme"),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
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
                .query(
                    &tenant("initech"),
                    &[],
                    &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    100,
                    true
                )
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
        // begin_flush clears inner and moves the data to the flushing buffer.
        // unified_query also scans the flushing buffer, so data is not lost during flushing (#2 regression).
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("hello", 100)],
        );

        let snapshot = mt.begin_flush();
        assert_eq!(snapshot.len(), 1);

        let results = mt.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 1,
            "flushing buffer should remain visible during flush"
        );

        mt.commit_flush();
        let results2 = mt.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
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

        let results = mt.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1, "abort_flush should restore data to inner");
        assert!(!mt.is_empty());
    }

    /// The snapshot is shared with the flush, not copied to it. The copy
    /// doubled memtable memory at exactly the moment the memtable is largest,
    /// and the failure this guards against — a flush that cannot keep up — is
    /// where the buffer is larger still.
    #[test]
    fn a_flush_shares_the_snapshot_instead_of_duplicating_it() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("shared", 100)],
        );

        let snapshot = memtable.begin_flush();
        assert_eq!(
            Arc::strong_count(&snapshot),
            2,
            "the flush and the memtable hold the same snapshot, not two of them"
        );

        // Committing releases the memtable's share, leaving the flush's own.
        memtable.commit_flush();
        assert_eq!(Arc::strong_count(&snapshot), 1);
    }

    /// Aborting has to put the entries back, which needs ownership. The
    /// memtable's share is dropped first so that ownership is taken rather than
    /// the snapshot being copied on the way back in.
    #[test]
    fn an_aborted_flush_returns_its_entries_without_copying_them() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("returned", 100)],
        );
        let before = memtable.approximate_size();

        let snapshot = memtable.begin_flush();
        assert!(
            memtable
                .query(
                    &sample_tenant(),
                    &[],
                    &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    10,
                    true
                )
                .len()
                == 1
        );
        memtable.abort_flush(snapshot);

        let results = memtable.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            10,
            true,
        );
        let total: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total, 1, "the aborted entries are back in the live buffer");
        assert_eq!(
            memtable.approximate_size(),
            before,
            "and the accounting returns with them"
        );
    }

    #[test]
    fn begin_flush_keeps_query_consistent_with_concurrent_insert() {
        // Data inserted while flushing is in progress must also be visible.
        let mt = MemTable::new();
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("first", 100)],
        );

        let _snapshot = mt.begin_flush();
        // Receive new data while flushing is in progress.
        mt.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("second", 200)],
        );

        let results = mt.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
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
        let results = memtable.query(
            &sample_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
        let total_entries: usize = results.iter().map(|stream| stream.entries.len()).sum();
        assert_eq!(total_entries, 2);
    }

    #[test]
    fn the_range_decides_whether_a_row_on_end_is_returned() {
        let memtable = MemTable::new();
        memtable.insert(
            sample_tenant(),
            sample_labels(),
            vec![sample_entry("on the boundary", 200)],
        );
        let rows = |range| {
            memtable
                .query(&sample_tenant(), &[], &[], range, 100, true)
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>()
        };

        // This assertion used to be the whole test, under a name that made the
        // scan's inclusive `end` sound like the contract. It is not: it is what
        // a closed window means, and a log query does not ask for one.
        assert_eq!(rows(QueryTimeRange::closed(100, 200)), 1);
        assert_eq!(
            rows(QueryTimeRange::half_open(100, 200)),
            0,
            "a row at exactly `end` is outside a Loki log window"
        );
        assert_eq!(
            rows(QueryTimeRange::half_open(200, 300)),
            1,
            "`start` is included on both contracts"
        );
        assert_eq!(
            rows(QueryTimeRange::half_open(200, 200)),
            0,
            "an empty window returns nothing, not the row on its boundary"
        );
        assert_eq!(
            rows(QueryTimeRange::half_open(300, 100)),
            0,
            "an inverted window is empty rather than inverted"
        );
    }
}
