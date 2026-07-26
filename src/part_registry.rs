use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::logql::{LabelMatcher, LineFilter};
use crate::memtable::{IndexStats, Labels, QueryResult, StreamResult};
use crate::object_storage::Manifest;
use crate::part::{ExactFieldPredicate, ExactFieldPruning, Part, PartReader, discover_parts};
use crate::tenant::TenantId;

pub struct PartRegistry {
    inner: RwLock<HashMap<String, Arc<PartReader>>>,
    operation_lock: Arc<tokio::sync::RwLock<()>>,
}

impl PartRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
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
        *self.inner.write().unwrap() = opened.into_iter().collect();
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
        self.inner.write().unwrap().extend(opened);
        Ok(ids)
    }

    pub fn unregister(&self, ids: &[String]) {
        let mut inner = self.inner.write().unwrap();
        for id in ids {
            inner.remove(id);
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
        for (id, reader) in opened {
            inner.insert(id, reader);
        }
        for id in old_ids {
            inner.remove(id);
        }
        Ok(new_ids)
    }

    pub fn snapshot(&self) -> Vec<Arc<PartReader>> {
        self.inner.read().unwrap().values().cloned().collect()
    }

    pub fn part_count(&self) -> usize {
        self.inner.read().unwrap().len()
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
    fn within_retention(
        reader: &PartReader,
        tenant: &TenantId,
        retention_floor_ns: Option<i64>,
    ) -> bool {
        let Some(floor_ns) = retention_floor_ns else {
            return true;
        };
        reader
            .meta()
            .tenant_segment(tenant)
            .is_some_and(|segment| segment.max_ts_ns >= floor_ns)
    }

    pub fn label_names(&self, tenant: &TenantId, retention_floor_ns: Option<i64>) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_retention(&reader, tenant, retention_floor_ns) {
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
        retention_floor_ns: Option<i64>,
    ) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_retention(&reader, tenant, retention_floor_ns) {
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
        retention_floor_ns: Option<i64>,
    ) -> Vec<Labels> {
        let mut set: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();
        for reader in self.snapshot() {
            if !Self::within_retention(&reader, tenant, retention_floor_ns) {
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

    pub fn stats(&self, tenant: &TenantId, retention_floor_ns: Option<i64>) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for reader in self.snapshot() {
            if !Self::within_retention(&reader, tenant, retention_floor_ns) {
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
