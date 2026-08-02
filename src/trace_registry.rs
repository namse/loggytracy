use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::object_storage::TraceManifest;
use crate::tenant::TenantId;
use crate::trace::TraceSpan;
use crate::trace_part::{TracePart, TracePartReader, discover_trace_parts};

pub struct TraceRegistry {
    inner: RwLock<HashMap<String, Arc<TracePartReader>>>,
    operation_lock: Arc<tokio::sync::RwLock<()>>,
}

impl TraceRegistry {
    pub fn standalone() -> Self {
        Self::new(Arc::new(tokio::sync::RwLock::new(())))
    }

    pub fn new(operation_lock: Arc<tokio::sync::RwLock<()>>) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            operation_lock,
        }
    }

    pub fn operation_lock(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.operation_lock.clone()
    }

    /// Every tenant that owns a segment in some trace part. See
    /// `PartRegistry::visit_tenants`.
    pub fn visit_tenants(&self, mut visit: impl FnMut(&TenantId)) {
        for reader in self.inner.read().unwrap().values() {
            for segment in &reader.part().meta.tenants {
                visit(&segment.tenant);
            }
        }
    }

    pub fn load_from_disk(
        traces_root: &Path,
        operation_lock: Arc<tokio::sync::RwLock<()>>,
    ) -> Result<Self, String> {
        let registry = Self::new(operation_lock);
        registry.reload_from_disk(traces_root)?;
        Ok(registry)
    }

    pub fn reload_from_disk(&self, traces_root: &Path) -> Result<(), String> {
        let parts = discover_trace_parts(traces_root)?;
        let mut readers = HashMap::new();
        for part in parts {
            let id = part.meta.id.clone();
            let reader = TracePartReader::open(part)
                .map_err(|error| format!("failed to open trace part {id}: {error}"))?;
            readers.insert(id, Arc::new(reader));
        }
        *self.inner.write().unwrap() = readers;
        Ok(())
    }

    pub fn load_from_manifest(
        traces_root: &Path,
        manifest: &TraceManifest,
        operation_lock: Arc<tokio::sync::RwLock<()>>,
    ) -> Result<Self, String> {
        let registry = Self::new(operation_lock);
        let mut readers = HashMap::new();
        for descriptor in &manifest.parts {
            let dir = traces_root.join(&descriptor.partition).join(&descriptor.id);
            let part = crate::trace_part::load_trace_part(&dir).map_err(|error| {
                format!(
                    "failed to load trace manifest part {}: {error}",
                    descriptor.id
                )
            })?;
            if part.meta.id != descriptor.id || part.meta.partition != descriptor.partition {
                return Err(format!(
                    "cached trace part metadata does not match manifest descriptor {}/{}",
                    descriptor.partition, descriptor.id
                ));
            }
            let reader = TracePartReader::open_cached(part).map_err(|error| {
                format!(
                    "failed to open trace manifest part {}: {error}",
                    descriptor.id
                )
            })?;
            readers.insert(descriptor.id.clone(), Arc::new(reader));
        }
        *registry.inner.write().unwrap() = readers;
        Ok(registry)
    }

    /// Open readers for freshly written trace parts, without touching the
    /// registry — the same open-outside-the-lock split as
    /// [`crate::part_registry::PartRegistry::open_parts`].
    pub fn open_parts(parts: Vec<TracePart>) -> Result<Vec<(String, Arc<TracePartReader>)>, String> {
        let mut readers = Vec::with_capacity(parts.len());
        for part in parts {
            let id = part.meta.id.clone();
            let reader = TracePartReader::open(part)
                .map_err(|error| format!("failed to open trace part {id}: {error}"))?;
            readers.push((id, Arc::new(reader)));
        }
        Ok(readers)
    }

    pub fn register(&self, parts: Vec<TracePart>) -> Result<Vec<String>, String> {
        Ok(self.register_opened(Self::open_parts(parts)?))
    }

    pub fn register_opened(
        &self,
        readers: Vec<(String, Arc<TracePartReader>)>,
    ) -> Vec<String> {
        let ids = readers.iter().map(|(id, _)| id.clone()).collect();
        self.inner.write().unwrap().extend(readers);
        ids
    }

    pub fn unregister(&self, ids: &[String]) {
        let mut inner = self.inner.write().unwrap();
        for id in ids {
            inner.remove(id);
        }
    }

    pub fn snapshot(&self) -> Vec<Arc<TracePartReader>> {
        self.inner.read().unwrap().values().cloned().collect()
    }

    pub fn candidate_part_ids(
        &self,
        tenant: &TenantId,
        trace_id: &str,
    ) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, reader)| reader.may_match_trace_id(tenant, trace_id))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Parts that hold any row for `tenant`. Used to pin the restore set for
    /// an unfiltered search, which would otherwise restore every tenant's
    /// object bodies.
    pub fn tenant_part_ids(&self, tenant: &TenantId) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, reader)| reader.part().meta.tenant_row_groups(tenant).is_some())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// The same set narrowed to a time range. Restoring a part is a download,
    /// so this decides the cost of the request before any of it is paid.
    pub fn tenant_part_ids_in_range(
        &self,
        tenant: &TenantId,
        start_ns: i64,
        end_ns: i64,
    ) -> std::collections::HashSet<String> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, reader)| {
                reader
                    .part()
                    .meta
                    .tenant_overlaps_range(tenant, start_ns, end_ns)
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

    /// See `PartRegistry::part_dirs`.
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

    pub fn query_trace_id(
        &self,
        tenant: &TenantId,
        trace_id: &str,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Vec<TraceSpan>, String> {
        let mut readers = self.snapshot();
        readers.sort_by_key(|reader| reader.part().meta.min_ts_ns);
        let mut spans = Vec::new();
        for reader in readers {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("trace query timed out".to_string());
            }
            if !reader.may_match_trace_id(tenant, trace_id) {
                continue;
            }
            let remaining = scan_limit
                .map(|limit| limit.saturating_sub(spans.len()))
                .unwrap_or(usize::MAX);
            spans.extend(reader.query_trace_id_limited(
                tenant,
                trace_id,
                remaining,
                cancellation,
            )?);
        }
        spans.sort_by(|left, right| {
            left.start_time_ns
                .cmp(&right.start_time_ns)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        Ok(spans)
    }

    pub fn query_all(
        &self,
        tenant: &TenantId,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Vec<TraceSpan>, String> {
        self.query_range(tenant, None, scan_limit, cancellation)
    }

    /// Every span for the tenant, or only those starting inside `range`.
    pub fn query_range(
        &self,
        tenant: &TenantId,
        range: Option<(i64, i64)>,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Vec<TraceSpan>, String> {
        let mut spans = Vec::new();
        for reader in self.snapshot() {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("trace query timed out".to_string());
            }
            let remaining = scan_limit
                .map(|limit| limit.saturating_sub(spans.len()))
                .unwrap_or(usize::MAX);
            let part_spans = match range {
                Some((start_ns, end_ns)) => {
                    reader.query_range_limited(tenant, start_ns, end_ns, remaining, cancellation)?
                }
                None => reader.query_all_limited(tenant, remaining, cancellation)?,
            };
            spans.extend(part_spans);
        }
        spans.sort_by(|left, right| {
            left.start_time_ns
                .cmp(&right.start_time_ns)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        Ok(spans)
    }

    pub fn part_count(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}
