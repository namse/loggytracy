use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::logql::{LabelMatcher, LineFilter};
use crate::memtable::{IndexStats, Labels, StreamResult};
use crate::object_storage::Manifest;
use crate::part::{Part, PartReader, discover_parts};

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
        matchers: &[LabelMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, reader)| {
                let meta = reader.meta();
                meta.max_ts_ns >= start_ns
                    && meta.min_ts_ns <= end_ns
                    && (matchers.is_empty()
                        || meta
                            .streams
                            .iter()
                            .any(|labels| matchers.iter().all(|matcher| matcher.matches(labels))))
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

    pub fn query(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        let readers = self.snapshot();
        if readers.is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates: Vec<Arc<PartReader>> = readers
            .into_iter()
            .filter(|r| {
                let m = r.meta();
                m.max_ts_ns >= start_ns
                    && m.min_ts_ns <= end_ns
                    && (matchers.is_empty()
                        || m.streams
                            .iter()
                            .any(|labels| matchers.iter().all(|matcher| matcher.matches(labels))))
            })
            .collect();
        candidates.sort_by_key(|r| r.meta().min_ts_ns);
        if !forward {
            candidates.reverse();
        }

        let mut all: Vec<(Labels, crate::memtable::LogEntry)> = Vec::new();
        for reader in &candidates {
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
            let results = reader
                .query(matchers, line_filters, start_ns, end_ns, limit, forward)
                .map_err(|error| {
                    format!("failed to query part {}: {error}", reader.part().meta.id)
                })?;
            for sr in results {
                for entry in sr.entries {
                    all.push((sr.labels.clone(), entry));
                }
            }
        }

        if forward {
            all.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        all.truncate(limit);

        Ok(crate::part::group_by_labels(all))
    }

    pub fn label_names(&self) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            for name in reader.label_names() {
                set.insert(name.clone());
            }
        }
        set.into_iter().collect()
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        let mut set = BTreeSet::new();
        for reader in self.snapshot() {
            for v in reader.label_values(name) {
                set.insert(v);
            }
        }
        set.into_iter().collect()
    }

    pub fn series(&self, matchers: &[LabelMatcher]) -> Vec<Labels> {
        let mut set: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();
        for reader in self.snapshot() {
            for labels in reader.series(matchers) {
                set.insert(labels);
            }
        }
        set.into_iter().collect()
    }

    pub fn stats(&self) -> IndexStats {
        let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for reader in self.snapshot() {
            for labels in &reader.meta().streams {
                stream_set.insert(labels.clone());
            }
            entries += reader.meta().row_count as usize;
            if let Ok(meta) = std::fs::metadata(reader.part().data_path()) {
                bytes += meta.len();
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
            timestamp_ns,
            labels,
            line: line.to_string(),
            structured_metadata: vec![],
        }
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
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
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

        let result = registry.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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
            .query(&[], &[], i64::MAX, i64::MAX, 100, true)
            .expect("part query");
        assert_eq!(results.iter().map(|r| r.entries.len()).sum::<usize>(), 1);
    }
}
