use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::backpressure::IngestGate;
use crate::config::Config;
use crate::journal::Journal;
use crate::memtable::MemTable;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::RemoteCache;
use crate::part_registry::PartRegistry;
use crate::shutdown::ShutdownState;
use crate::tenant_policy::TenantPolicy;
use crate::trace_registry::TraceRegistry;

/// Runtime resources shared by HTTP handlers and background workers.
pub struct AppState {
    pub config: Arc<Config>,
    pub query_scan_semaphore: Arc<tokio::sync::Semaphore>,
    pub metric_evaluation_semaphore: Arc<tokio::sync::Semaphore>,
    pub trace_scan_semaphore: Arc<tokio::sync::Semaphore>,
    pub memtable: Arc<MemTable>,
    pub journal: Arc<Journal>,
    pub parts: Arc<PartRegistry>,
    pub trace_parts: Arc<TraceRegistry>,
    pub flush_healthy: Arc<AtomicBool>,
    pub merge_healthy: Arc<AtomicBool>,
    pub retention_healthy: Arc<AtomicBool>,
    pub otlp_healthy: Arc<AtomicBool>,
    pub remote_cache: Option<Arc<RemoteCache>>,
    /// The tenant→retention snapshot, read by query handlers to clamp a
    /// requested range to what the tenant is still entitled to see.
    pub tenant_policy: Arc<TenantPolicy>,
    pub metrics: Arc<RuntimeMetrics>,
    pub shutdown: Arc<ShutdownState>,
    /// Shared with the OTLP service so both protocols answer to one set of
    /// thresholds.
    pub ingest_gate: Arc<IngestGate>,
}

/// Dependencies that are created during startup or by a test fixture.
pub struct AppStateDependencies {
    pub memtable: Arc<MemTable>,
    pub journal: Arc<Journal>,
    pub parts: Arc<PartRegistry>,
    pub trace_parts: Arc<TraceRegistry>,
    pub flush_healthy: Arc<AtomicBool>,
    pub merge_healthy: Arc<AtomicBool>,
    pub retention_healthy: Arc<AtomicBool>,
    pub otlp_healthy: Arc<AtomicBool>,
    pub remote_cache: Option<Arc<RemoteCache>>,
    pub tenant_policy: Arc<TenantPolicy>,
    pub metrics: Arc<RuntimeMetrics>,
    pub shutdown: Arc<ShutdownState>,
}

impl AppState {
    /// Construct state and all endpoint semaphores from one configuration.
    /// Keeping this in one place prevents production and test limits from
    /// silently diverging.
    pub fn from_config(config: Arc<Config>, dependencies: AppStateDependencies) -> Self {
        let ingest_gate = Arc::new(IngestGate::new(
            dependencies.journal.clone(),
            config.clone(),
            dependencies.metrics.clone(),
        ));
        Self {
            ingest_gate,
            query_scan_semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_query_scans,
            )),
            metric_evaluation_semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_metric_evaluations,
            )),
            trace_scan_semaphore: Arc::new(tokio::sync::Semaphore::new(
                config.max_concurrent_trace_scans,
            )),
            config,
            memtable: dependencies.memtable,
            journal: dependencies.journal,
            parts: dependencies.parts,
            trace_parts: dependencies.trace_parts,
            flush_healthy: dependencies.flush_healthy,
            merge_healthy: dependencies.merge_healthy,
            retention_healthy: dependencies.retention_healthy,
            otlp_healthy: dependencies.otlp_healthy,
            remote_cache: dependencies.remote_cache,
            tenant_policy: dependencies.tenant_policy,
            metrics: dependencies.metrics,
            shutdown: dependencies.shutdown,
        }
    }
}
