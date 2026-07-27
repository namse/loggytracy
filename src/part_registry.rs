use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::logql::{LabelMatcher, LineFilter};
use crate::memtable::{IndexStats, Labels, QueryResult, StreamResult};
use crate::object_storage::Manifest;
use crate::part::{
    ExactFieldPredicate, ExactFieldPruning, MetadataWindow, Part, PartReader, discover_parts,
};
use crate::tenant::TenantId;

/// The streams a part holds, per tenant.
///
/// A part's stream list is shared across its tenants, so it is filtered through
/// each tenant's row groups — the same path a `series` query takes, and for the
/// same reason: the part-wide list would attribute a neighbour's streams to
/// every tenant in the part.
fn reader_stream_keys(reader: &PartReader) -> Vec<(TenantId, Vec<u64>)> {
    reader
        .meta()
        .tenants
        .iter()
        .map(|segment| {
            let keys = reader
                .series(&segment.tenant, &[])
                .iter()
                .map(stream_key)
                .collect();
            (segment.tenant.clone(), keys)
        })
        .collect()
}

/// A stream's identity, hashed.
///
/// Hashed rather than stored: the point of a cardinality limit is that this set
/// stays small, but "small" is per tenant and there can be many tenants, and a
/// `Labels` map per entry would cost far more than the limit it enforces. A
/// collision would count a genuinely new stream as one that already exists,
/// which admits one stream too many rather than refusing a legitimate one — the
/// direction a limit should err in.
pub fn stream_key(labels: &Labels) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, value) in labels {
        name.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Per-tenant stream sets, reference counted.
///
/// A stream lives in every part that holds a row for it, so removing one part
/// must not remove the stream while another still has it. The count is what
/// makes register/replace/unregister reversible.
#[derive(Default)]
struct StreamCensus {
    tenants: HashMap<TenantId, HashMap<u64, u32>>,
}

impl StreamCensus {
    fn add(&mut self, tenant: &TenantId, keys: impl IntoIterator<Item = u64>) {
        let streams = self.tenants.entry(tenant.clone()).or_default();
        for key in keys {
            *streams.entry(key).or_insert(0) += 1;
        }
    }

    fn remove(&mut self, tenant: &TenantId, keys: impl IntoIterator<Item = u64>) {
        let Some(streams) = self.tenants.get_mut(tenant) else {
            return;
        };
        for key in keys {
            if let Some(count) = streams.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    streams.remove(&key);
                }
            }
        }
        if streams.is_empty() {
            self.tenants.remove(tenant);
        }
    }

    fn contains(&self, tenant: &TenantId, key: u64) -> bool {
        self.tenants
            .get(tenant)
            .is_some_and(|streams| streams.contains_key(&key))
    }

    fn count(&self, tenant: &TenantId) -> usize {
        self.tenants.get(tenant).map_or(0, HashMap::len)
    }
}

/// What the shared-part layout costs across the current part set.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct LayoutTotals {
    /// `Σ parts.tenants.len()` — the number of (tenant, part) pairs, which is
    /// the unit this layout actually charges in.
    pub tenant_segments: u64,
    /// Bloom and stream-index bytes held resident by open parts. The local
    /// cache budget does not cover these.
    pub sidecar_resident_bytes: u64,
    /// Total `meta.json` across parts, which startup parses before serving.
    pub meta_bytes: u64,
}

impl LayoutTotals {
    fn add(&mut self, reader: &PartReader) {
        self.tenant_segments = self
            .tenant_segments
            .saturating_add(reader.meta().tenants.len() as u64);
        self.sidecar_resident_bytes = self
            .sidecar_resident_bytes
            .saturating_add(reader.index_resident_bytes());
        self.meta_bytes = self.meta_bytes.saturating_add(reader.meta().meta_bytes);
    }

    fn remove(&mut self, reader: &PartReader) {
        self.tenant_segments = self
            .tenant_segments
            .saturating_sub(reader.meta().tenants.len() as u64);
        self.sidecar_resident_bytes = self
            .sidecar_resident_bytes
            .saturating_sub(reader.index_resident_bytes());
        self.meta_bytes = self.meta_bytes.saturating_sub(reader.meta().meta_bytes);
    }
}

pub struct PartRegistry {
    inner: RwLock<HashMap<String, Arc<PartReader>>>,
    /// Running totals for the layout gauges, maintained as the set changes.
    ///
    /// These were published from the merge tick, which meant they read zero in
    /// exactly the configuration that produces the part counts they exist to
    /// describe — measuring part accumulation requires turning merge off.
    /// Recomputing per scrape is the other trap, since it is O(parts × tenants)
    /// on an unauthenticated endpoint. Maintaining them here costs O(1) per
    /// registry change and cannot fall out of step with the set it describes.
    layout: RwLock<LayoutTotals>,
    /// Which streams each tenant currently has on disk, so ingest can tell a
    /// new stream from one that already exists without walking every part.
    streams: RwLock<StreamCensus>,
    operation_lock: Arc<tokio::sync::RwLock<()>>,
}

impl PartRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            layout: RwLock::new(LayoutTotals::default()),
            streams: RwLock::new(StreamCensus::default()),
            operation_lock: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    pub fn operation_lock(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.operation_lock.clone()
    }

    /// Every tenant that owns a segment in some part. Visits under the read
    /// guard instead of going through `snapshot`, which would clone one `Arc`
    /// per part; `/metrics` asks for this on every scrape.
    pub fn visit_tenants(&self, mut visit: impl FnMut(&TenantId)) {
        for reader in self.inner.read().unwrap().values() {
            for segment in &reader.meta().tenants {
                visit(&segment.tenant);
            }
        }
    }

    pub fn load_from_disk(parts_root: &Path) -> Result<Self, String> {
        let registry = Self::new();
        registry.reload_from_disk(parts_root)?;
        Ok(registry)
    }

    pub fn reload_from_disk(&self, parts_root: &Path) -> Result<(), String> {
        let parts = discover_parts(parts_root)?;
        self.replace_all(parts, false)
    }

    pub fn load_from_manifest(parts_root: &Path, manifest: &Manifest) -> Result<Self, String> {
        let registry = Self::new();
        registry.reload_from_manifest(parts_root, manifest)?;
        Ok(registry)
    }

    pub fn reload_from_manifest(
        &self,
        parts_root: &Path,
        manifest: &Manifest,
    ) -> Result<(), String> {
        let mut parts = Vec::with_capacity(manifest.parts.len());
        for descriptor in &manifest.parts {
            let dir = parts_root.join(&descriptor.partition).join(&descriptor.id);
            let part = crate::part::load_part(&dir).map_err(|error| {
                format!("failed to load manifest part {}: {error}", descriptor.id)
            })?;
            if part.meta.id != descriptor.id || part.meta.partition != descriptor.partition {
                return Err(format!(
                    "cached part metadata does not match manifest descriptor {}/{}",
                    descriptor.partition, descriptor.id
                ));
            }
            parts.push(part);
        }
        self.replace_all(parts, true)
    }

    fn replace_all(&self, parts: Vec<Part>, allow_missing_data: bool) -> Result<(), String> {
        let mut opened = Vec::with_capacity(parts.len());
        for part in parts {
            let id = part.meta.id.clone();
            let reader = if allow_missing_data {
                PartReader::open_cached(part)
            } else {
                PartReader::open(part)
            }
            .map_err(|e| format!("failed to open part {id} during startup: {e}"))?;
            opened.push((id, Arc::new(reader)));
        }
        let count = opened.len();
        let readers: HashMap<String, Arc<PartReader>> = opened.into_iter().collect();
        self.reset_layout(&readers);
        *self.inner.write().unwrap() = readers;
        if count > 0 {
            tracing::info!(parts = count, "loaded parts from disk");
        }
        Ok(())
    }

    pub fn has_missing_cache_files(&self) -> bool {
        self.inner
            .read()
            .unwrap()
            .values()
            .any(|reader| !reader.part().data_path().exists())
    }

    pub fn candidate_part_ids(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> std::collections::HashSet<String> {
        self.candidate_part_ids_with_exact_fields(tenant, matchers, &[], &[], start_ns, end_ns)
    }

    /// Plans against catalog-resident indexes only, including the optional
    /// BTF2/BTF3 exact-field blooms. BTF1 readers conservatively return
    /// candidates; BTF2 also conservatively scans typed canonical predicates.
    pub fn candidate_part_ids_with_exact_fields(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        start_ns: i64,
        end_ns: i64,
    ) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, reader)| {
                reader.may_match_exact_fields(
                    tenant,
                    matchers,
                    line_filters,
                    exact_fields,
                    start_ns,
                    end_ns,
                )
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn missing_data_ids(
        &self,
        ids: &std::collections::HashSet<String>,
    ) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(id, reader)| ids.contains(*id) && !reader.part().data_path().exists())
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn register(&self, parts: Vec<Part>) -> Result<Vec<String>, String> {
        let mut opened = Vec::with_capacity(parts.len());
        for part in parts {
            let id = part.meta.id.clone();
            let reader = PartReader::open(part)
                .map_err(|e| format!("failed to open freshly written part {id}: {e}"))?;
            opened.push((id, Arc::new(reader)));
        }
        let ids = opened.iter().map(|(id, _)| id.clone()).collect();
        let mut inner = self.inner.write().unwrap();
        let mut layout = self.layout.write().unwrap();
        let mut census = self.streams.write().unwrap();
        for (id, reader) in opened {
            // Registering an id that is already present replaces it, so its
            // predecessor has to leave the derived indexes with it.
            if let Some(previous) = inner.insert(id, reader.clone()) {
                layout.remove(&previous);
                for (tenant, keys) in reader_stream_keys(&previous) {
                    census.remove(&tenant, keys);
                }
            }
            layout.add(&reader);
            for (tenant, keys) in reader_stream_keys(&reader) {
                census.add(&tenant, keys);
            }
        }
        Ok(ids)
    }

    pub fn unregister(&self, ids: &[String]) {
        let mut inner = self.inner.write().unwrap();
        let mut layout = self.layout.write().unwrap();
        let mut census = self.streams.write().unwrap();
        for id in ids {
            if let Some(removed) = inner.remove(id) {
                layout.remove(&removed);
                for (tenant, keys) in reader_stream_keys(&removed) {
                    census.remove(&tenant, keys);
                }
            }
        }
    }

    pub fn replace(&self, old_ids: &[String], new_parts: Vec<Part>) -> Result<Vec<String>, String> {
        if new_parts.is_empty() {
            return Err("cannot replace parts with an empty part set".to_string());
        }

        // Open every replacement before taking the registry mutation. If any
        // one is corrupt, the old set remains queryable and removable only by
        // a later successful merge.
        let mut opened = Vec::with_capacity(new_parts.len());
        for part in new_parts {
            let id = part.meta.id.clone();
            match PartReader::open(part) {
                Ok(reader) => {
                    opened.push((id, Arc::new(reader)));
                }
                Err(e) => {
                    return Err(format!("failed to open fresh merged part {}: {}", id, e));
                }
            }
        }

        let new_ids: Vec<String> = opened.iter().map(|(id, _)| id.clone()).collect();
        let mut inner = self.inner.write().unwrap();
        let mut layout = self.layout.write().unwrap();
        let mut census = self.streams.write().unwrap();
        for (id, reader) in opened {
            if let Some(previous) = inner.insert(id, reader.clone()) {
                layout.remove(&previous);
                for (tenant, keys) in reader_stream_keys(&previous) {
                    census.remove(&tenant, keys);
                }
            }
            layout.add(&reader);
            for (tenant, keys) in reader_stream_keys(&reader) {
                census.add(&tenant, keys);
            }
        }
        for id in old_ids {
            if let Some(removed) = inner.remove(id) {
                layout.remove(&removed);
                for (tenant, keys) in reader_stream_keys(&removed) {
                    census.remove(&tenant, keys);
                }
            }
        }
        Ok(new_ids)
    }

    pub fn snapshot(&self) -> Vec<Arc<PartReader>> {
        self.inner.read().unwrap().values().cloned().collect()
    }

    pub fn layout_totals(&self) -> LayoutTotals {
        *self.layout.read().unwrap()
    }

    /// Rebuild the derived indexes from a whole set, for the paths that replace
    /// it outright rather than adding and removing.
    fn reset_layout(&self, readers: &HashMap<String, Arc<PartReader>>) {
        let mut totals = LayoutTotals::default();
        let mut census = StreamCensus::default();
        for reader in readers.values() {
            totals.add(reader);
            for (tenant, keys) in reader_stream_keys(reader) {
                census.add(&tenant, keys);
            }
        }
        *self.layout.write().unwrap() = totals;
        *self.streams.write().unwrap() = census;
    }

    /// Whether the tenant already has this stream on disk.
    pub fn contains_stream(&self, tenant: &TenantId, key: u64) -> bool {
        self.streams.read().unwrap().contains(tenant, key)
    }

    /// Parts holding at least one row for the tenant. Its share of the
    /// (tenant, part) pairs the layout charges in.
    pub fn tenant_part_count(&self, tenant: &TenantId) -> usize {
        self.inner
            .read()
            .unwrap()
            .values()
            .filter(|reader| reader.meta().tenant_segment(tenant).is_some())
            .count()
    }

    /// Distinct streams the tenant has on disk.
    pub fn tenant_stream_count(&self, tenant: &TenantId) -> usize {
        self.streams.read().unwrap().count(tenant)
    }

    pub fn part_count(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    /// Every registered part's directory. What eviction walks instead of the
    /// filesystem.
    pub fn part_dirs(&self) -> Vec<std::path::PathBuf> {
        self.inner
            .read()
            .unwrap()
            .values()
            .map(|reader| reader.part().dir.clone())
            .collect()
    }

    pub fn part_ids(&self) -> std::collections::HashSet<String> {
        self.inner.read().unwrap().keys().cloned().collect()
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
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                tenant,
                matchers,
                ExactFieldPruning::new(line_filters, &[]),
                start_ns,
                end_ns,
                limit,
                forward,
                None,
                None,
            )?
            .results)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                tenant, matchers, pruning, start_ns, end_ns, limit, forward, None, None,
            )?
            .results)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning_and_scan_limit(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        self.query_with_exact_field_pruning_and_scan_limits(
            tenant,
            matchers,
            pruning,
            start_ns,
            end_ns,
            limit,
            forward,
            scan_limit,
            None,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning_and_scan_limits(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        scan_bytes_limit: Option<u64>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        let readers = self.snapshot();
        if readers.is_empty() {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
                scanned_bytes: 0,
            });
        }

        let mut candidates: Vec<Arc<PartReader>> = readers
            .into_iter()
            .filter(|r| {
                // Part-level time pruning is per tenant: a shared part spans
                // every tenant's range, so the part-wide min/max would keep
                // parts that hold nothing for this tenant.
                let Some(segment) = r.meta().tenant_segment(tenant) else {
                    return false;
                };
                segment.max_ts_ns >= start_ns
                    && segment.min_ts_ns <= end_ns
                    && (matchers.is_empty()
                        || r.meta()
                            .streams
                            .iter()
                            .any(|labels| matchers.iter().all(|matcher| matcher.matches(labels))))
            })
            .collect();
        candidates.sort_by_key(|r| {
            r.meta()
                .tenant_segment(tenant)
                .map(|segment| segment.min_ts_ns)
                .unwrap_or(i64::MAX)
        });
        if !forward {
            candidates.reverse();
        }

        let mut all: Vec<(Labels, crate::memtable::LogEntry)> = Vec::new();
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0u64;
        for reader in &candidates {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                break;
            }
            // Every part must be considered. Parts can contain overlapping and
            // out-of-order timestamp ranges, so reaching the global limit in
            // an earlier part is not a safe reason to skip later parts.
            let part_limit = limit;
            let part_scan_limit = scan_limit.map(|budget| budget.saturating_sub(scanned_rows));
            let part_scan_bytes_limit =
                scan_bytes_limit.map(|budget| budget.saturating_sub(scanned_bytes));
            if part_scan_limit == Some(0) {
                break;
            }
            if part_scan_bytes_limit == Some(0) {
                break;
            }
            let access_marker = reader.part().dir.join(".access");
            match std::fs::symlink_metadata(&access_marker) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    tracing::warn!(
                        path = %access_marker.display(),
                        "refusing to follow symlinked cache access marker"
                    );
                }
                Ok(_) | Err(_) => {
                    let _ = std::fs::write(&access_marker, []);
                }
            }
            let result = reader
                .query_with_exact_field_pruning_and_scan_limits(
                    tenant,
                    matchers,
                    pruning,
                    start_ns,
                    end_ns,
                    part_limit,
                    forward,
                    part_scan_limit,
                    part_scan_bytes_limit,
                    cancellation,
                )
                .map_err(|error| {
                    format!("failed to query part {}: {error}", reader.part().meta.id)
                })?;
            scanned_rows = scanned_rows.saturating_add(result.scanned_rows);
            scanned_bytes = scanned_bytes.saturating_add(result.scanned_bytes);
            for sr in result.results {
                for entry in sr.entries {
                    all.push((sr.labels.clone(), entry));
                }
            }
            if part_scan_limit.is_some_and(|limit| result.scanned_rows >= limit) {
                break;
            }
            if part_scan_bytes_limit.is_some_and(|limit| result.scanned_bytes >= limit) {
                break;
            }
        }

        if forward {
            all.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        all.truncate(limit);

        Ok(QueryResult {
            results: crate::part::group_by_labels(all),
            scanned_rows,
            scanned_bytes,
        })
    }

    /// Whether the part still holds anything for the tenant that its
    /// retention covers. The stream index has no per-stream timestamps, so
    /// this prunes at part granularity: a part entirely below the floor
    /// contributes nothing, and one that straddles it contributes all of its
    /// labels. `None` prunes nothing.
    fn within_window(reader: &PartReader, tenant: &TenantId, window: MetadataWindow) -> bool {
        reader
            .meta()
            .tenant_segment(tenant)
            .is_some_and(|segment| window.overlaps(segment.min_ts_ns, segment.max_ts_ns))
    }

    pub fn label_names(&self, tenant: &TenantId, window: MetadataWindow) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_window(&reader, tenant, window) {
                continue;
            }
            for name in reader.label_names(tenant) {
                set.insert(name);
            }
        }
        set.into_iter().collect()
    }

    pub fn label_values(
        &self,
        tenant: &TenantId,
        name: &str,
        window: MetadataWindow,
    ) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_window(&reader, tenant, window) {
                continue;
            }
            for v in reader.label_values(tenant, name) {
                set.insert(v);
            }
        }
        set.into_iter().collect()
    }

    pub fn series(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        window: MetadataWindow,
    ) -> Vec<Labels> {
        let mut set: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_window(&reader, tenant, window) {
                continue;
            }
            for labels in reader.series(tenant, matchers) {
                set.insert(labels);
            }
        }
        set.into_iter().collect()
    }

    /// Process-wide totals for the operator metrics endpoint.
    pub fn global_stats(&self) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for reader in self.snapshot() {
            for labels in &reader.meta().streams {
                stream_set.insert(labels.clone());
            }
            entries += reader.meta().row_count as usize;
            if let Ok(metadata) = std::fs::metadata(reader.part().data_path()) {
                bytes += metadata.len();
            }
        }
        IndexStats {
            streams: stream_set.len(),
            entries,
            bytes,
        }
    }

    pub fn stats(&self, tenant: &TenantId, window: MetadataWindow) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for reader in self.snapshot() {
            if !Self::within_window(&reader, tenant, window) {
                continue;
            }
            let Some(segment) = reader.meta().tenant_segment(tenant) else {
                continue;
            };
            for labels in reader.series(tenant, &[]) {
                stream_set.insert(labels);
            }
            entries += segment.row_count as usize;
            // Attribute the part's stored bytes in proportion to the tenant's
            // share of its rows; a shared object has no per-tenant file size.
            if let Ok(metadata) = std::fs::metadata(reader.part().data_path()) {
                let part_rows = reader.meta().row_count.max(1);
                bytes +=
                    (metadata.len() as u128 * segment.row_count as u128 / part_rows as u128) as u64;
            }
        }
        IndexStats {
            streams: stream_set.len(),
            entries,
            bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Labels;
    use crate::part::{self, BLOOM_FILE, Row};
    use crate::tenant::test_tenant;
    use std::collections::BTreeMap;

    fn temp_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "loggytracy-registry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn row(line: &str, timestamp_ns: i64) -> Row {
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        Row {
            tenant: test_tenant(),
            timestamp_ns,
            labels,
            line: line.to_string(),
            structured_metadata: vec![],
        }
    }

    #[test]
    fn query_considers_later_parts_with_overlapping_out_of_order_timestamps() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let first =
            part::flush_rows(vec![row("first", 0), row("late", 1_000)], &parts_root, 100).unwrap();
        let second = part::flush_rows(vec![row("out-of-order", 1)], &parts_root, 100).unwrap();
        registry.register(first).unwrap();
        registry.register(second).unwrap();

        let result = registry
            .query(&test_tenant(), &[], &[], 0, 1_000, 2, true)
            .unwrap();
        let entries: Vec<_> = result
            .into_iter()
            .flat_map(|stream| stream.entries)
            .map(|entry| (entry.timestamp_ns, entry.line))
            .collect();
        assert_eq!(
            entries,
            vec![(0, "first".to_string()), (1, "out-of-order".to_string())]
        );
    }

    #[test]
    fn replace_keeps_old_set_when_any_new_part_fails_to_open() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let old = part::flush_rows(
            vec![row("old", 1_700_000_000_000_000_000)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let old_id = old.meta.id.clone();
        registry.register(vec![old.clone()]).unwrap();

        let new = part::flush_rows(
            vec![row("new", 1_700_000_000_000_000_001)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        std::fs::write(new.dir.join(BLOOM_FILE), b"corrupt").unwrap();

        let result = registry.replace(&[old_id], vec![part::load_part(&new.dir).unwrap()]);
        assert!(result.is_err());
        assert_eq!(registry.part_count(), 1);
        let results = registry
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        assert_eq!(results.iter().map(|r| r.entries.len()).sum::<usize>(), 1);
        assert!(old.dir.exists());
        assert!(new.dir.exists());
    }

    #[test]
    fn register_is_atomic_when_any_part_fails_to_open() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let valid = part::flush_rows(
            vec![row("valid", 1_700_000_000_000_000_000)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let corrupt = part::flush_rows(
            vec![row("corrupt", 1_700_086_400_000_000_000)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        std::fs::write(corrupt.dir.join(BLOOM_FILE), b"corrupt").unwrap();

        let result = registry.register(vec![valid, corrupt]);

        assert!(result.is_err());
        assert_eq!(registry.part_count(), 0);
    }

    #[test]
    fn load_from_disk_fails_when_a_part_cannot_be_opened() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let part = part::flush_rows(
            vec![row("corrupt", 1_700_000_000_000_000_000)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        std::fs::write(part.dir.join(BLOOM_FILE), b"corrupt").unwrap();

        let result = PartRegistry::load_from_disk(&parts_root);

        assert!(result.is_err());
    }

    #[test]
    fn query_propagates_part_read_failure() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let part = part::flush_rows(
            vec![row("will become unreadable", 1_700_000_000_000_000_000)],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let data_path = part.data_path();
        registry.register(vec![part]).unwrap();

        std::fs::write(data_path, b"corrupt").unwrap();

        let result = registry.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        assert!(result.is_err());
    }

    #[test]
    fn query_includes_i64_max_timestamp() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let parts =
            part::flush_rows(vec![row("maximum timestamp", i64::MAX)], &parts_root, 100).unwrap();
        registry.register(parts).unwrap();

        let results = registry
            .query(&test_tenant(), &[], &[], i64::MAX, i64::MAX, 100, true)
            .expect("part query");
        assert_eq!(results.iter().map(|r| r.entries.len()).sum::<usize>(), 1);
    }

    #[test]
    fn exact_field_candidates_preserve_row_group_time_correlation() {
        let dir = temp_dir();
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let first_ts = 1_700_000_000_000_000_000;
        let second_ts = first_ts + 1_000_000_000;
        let mut first = row("first", first_ts);
        first.structured_metadata = vec![("trace_id".to_string(), "first".to_string())];
        let mut second = row("second", second_ts);
        second.structured_metadata = vec![("trace_id".to_string(), "second".to_string())];
        let parts = part::flush_rows(vec![first, second], &parts_root, 1).unwrap();
        let part_id = parts[0].meta.id.clone();
        let registry = PartRegistry::new();
        registry.register(parts).unwrap();

        let predicate = ExactFieldPredicate::new("trace_id", "second");
        assert!(
            registry
                .candidate_part_ids_with_exact_fields(
                    &test_tenant(),
                    &[],
                    &[],
                    std::slice::from_ref(&predicate),
                    i64::MIN,
                    i64::MAX,
                )
                .contains(&part_id)
        );
        assert!(
            registry
                .candidate_part_ids_with_exact_fields(
                    &test_tenant(),
                    &[],
                    &[],
                    &[predicate],
                    first_ts,
                    first_ts,
                )
                .is_empty(),
            "a field value in a later row group must not force restoration"
        );
    }
}
