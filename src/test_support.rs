use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::AppState;
use crate::app_state::AppStateDependencies;
use crate::config::Config;
use crate::journal::Journal;
use crate::memtable::MemTable;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::RemoteCache;
use crate::part_registry::PartRegistry;
use crate::shutdown::ShutdownState;
use crate::tenant_policy::TenantPolicy;
use crate::trace_registry::TraceRegistry;

/// Canonical state fixture used by Loki, Tempo, and ingest tests.
pub fn state(
    config: Config,
    memtable: Arc<MemTable>,
    journal: Arc<Journal>,
    parts: Arc<PartRegistry>,
    trace_parts: Arc<TraceRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
) -> Arc<AppState> {
    state_with_tenant_policy(
        config,
        memtable,
        journal,
        parts,
        trace_parts,
        remote_cache,
        Arc::new(TenantPolicy::disabled()),
    )
}

/// The same fixture with an explicit tenant policy, for retention tests.
pub fn state_with_tenant_policy(
    config: Config,
    memtable: Arc<MemTable>,
    journal: Arc<Journal>,
    parts: Arc<PartRegistry>,
    trace_parts: Arc<TraceRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    tenant_policy: Arc<TenantPolicy>,
) -> Arc<AppState> {
    Arc::new(AppState::from_config(
        Arc::new(config),
        AppStateDependencies {
            memtable,
            journal,
            parts,
            trace_parts,
            flush_healthy: Arc::new(AtomicBool::new(true)),
            merge_healthy: Arc::new(AtomicBool::new(true)),
            retention_healthy: Arc::new(AtomicBool::new(true)),
            otlp_healthy: Arc::new(AtomicBool::new(true)),
            remote_cache,
            tenant_policy,
            metrics: Arc::new(RuntimeMetrics::new()),
            shutdown: Arc::new(ShutdownState::new()),
        },
    ))
}
