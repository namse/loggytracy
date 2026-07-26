use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::interval;

use crate::config::Config;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::{ManifestPart, RemoteCache, TraceManifestPart};
use crate::part;
use crate::part_registry::PartRegistry;
use crate::shutdown::wait_for_drain;
use crate::tenant_policy::TenantPolicy;
use crate::trace_registry::TraceRegistry;

#[allow(clippy::too_many_arguments)]
pub async fn retention_loop(
    registry: Arc<PartRegistry>,
    trace_registry: Arc<TraceRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    tenant_policy: Arc<TenantPolicy>,
    metrics: Arc<RuntimeMetrics>,
    healthy: Arc<AtomicBool>,
    mut drain_rx: watch::Receiver<bool>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.retention_interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Nothing here fetches a policy: the control plane pushes them and this
    // loop only applies what is already in memory. loggytracy never calls out.
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = wait_for_drain(&mut drain_rx) => return,
        }
        if let Err(error) = retention_once(
            &registry,
            &trace_registry,
            remote_cache.as_deref(),
            &config,
            &tenant_policy,
        )
        .await
        {
            metrics
                .retention_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            healthy.store(false, Ordering::Release);
            if let Some(cache) = remote_cache.as_deref() {
                cache.mark_remote_unhealthy();
            }
            tracing::error!(%error, "retention iteration failed");
        } else {
            metrics
                .retention_success
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            healthy.store(true, Ordering::Release);
        }
    }
}

async fn retention_once(
    registry: &PartRegistry,
    trace_registry: &TraceRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
    tenant_policy: &TenantPolicy,
) -> Result<(), String> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before UNIX epoch: {error}"))?
        .as_nanos();
    retention_once_at(
        registry,
        trace_registry,
        remote_cache,
        config,
        tenant_policy,
        now_ns,
    )
    .await
}

/// What decides that a part has expired.
///
/// The two modes are never mixed: `config.validate` rejects a configuration
/// that sets both a global period and a policy endpoint.
enum Expiry {
    Global { cutoff_ns: i64 },
    PerTenant(crate::tenant_policy::Cutoffs),
}

impl Expiry {
    /// `None` means "delete nothing this tick", which covers both retention
    /// being off and the policy endpoint never having answered.
    fn resolve(config: &Config, tenant_policy: &TenantPolicy, now_ns: u128) -> Option<Self> {
        let clamped_now_ns = now_ns.min(i64::MAX as u128) as i64;
        if tenant_policy.is_enabled() {
            return tenant_policy
                .cutoffs_at(clamped_now_ns)
                .map(Self::PerTenant);
        }
        let retention_period = config.retention_period?;
        Some(Self::Global {
            cutoff_ns: now_ns
                .saturating_sub(retention_period.as_nanos())
                .min(i64::MAX as u128) as i64,
        })
    }

    fn log_part_expired(&self, meta: &crate::part::PartMeta) -> bool {
        match self {
            Self::Global { cutoff_ns } => meta.max_ts_ns < *cutoff_ns,
            Self::PerTenant(cutoffs) => cutoffs.log_part_fully_expired(meta),
        }
    }

    fn trace_part_expired(&self, meta: &crate::trace_part::TracePartMeta) -> bool {
        match self {
            Self::Global { cutoff_ns } => meta.max_ts_ns < *cutoff_ns,
            Self::PerTenant(cutoffs) => cutoffs.trace_part_fully_expired(meta),
        }
    }

    fn mode(&self) -> &'static str {
        match self {
            Self::Global { .. } => "global",
            Self::PerTenant(_) => "per-tenant",
        }
    }
}

async fn retention_once_at(
    registry: &PartRegistry,
    trace_registry: &TraceRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
    tenant_policy: &TenantPolicy,
    now_ns: u128,
) -> Result<(), String> {
    let Some(expiry) = Expiry::resolve(config, tenant_policy, now_ns) else {
        return Ok(());
    };
    let batch_size = config.retention_batch_size.max(1);

    // Snapshot under a shared lifecycle guard. Remote manifest I/O happens
    // after releasing it, so retention cannot stop flush/merge/eviction for a
    // network round trip and queries can continue concurrently.
    let guard = registry.operation_lock().read_owned().await;
    let mut log_parts: Vec<_> = registry
        .snapshot()
        .into_iter()
        .filter(|reader| expiry.log_part_expired(reader.meta()))
        .map(|reader| {
            (
                ManifestPart {
                    id: reader.meta().id.clone(),
                    partition: reader.meta().partition.clone(),
                },
                reader.part().dir.clone(),
            )
        })
        .collect();
    let mut trace_parts: Vec<_> = trace_registry
        .snapshot()
        .into_iter()
        .filter(|reader| expiry.trace_part_expired(&reader.part().meta))
        .map(|reader| {
            (
                TraceManifestPart {
                    id: reader.part().meta.id.clone(),
                    partition: reader.part().meta.partition.clone(),
                },
                reader.part().dir.clone(),
            )
        })
        .collect();
    log_parts.sort_by_key(|(part, _)| part.id.clone());
    trace_parts.sort_by_key(|(part, _)| part.id.clone());
    log_parts.truncate(batch_size);
    trace_parts.truncate(batch_size);

    if log_parts.is_empty() && trace_parts.is_empty() {
        return Ok(());
    }
    drop(guard);

    // Retire local bodies before the remote manifest CAS. If the process
    // crashes after this point, startup restores still-active descriptors from
    // the old manifest; if the CAS already happened, the descriptor is absent
    // and cannot be resurrected by local-cache reconciliation.
    let mut removed = 0usize;
    let mut removed_log_ids = Vec::new();
    let mut removed_trace_ids = Vec::new();
    {
        let _guard = registry.operation_lock().write_owned().await;
        for (descriptor, dir) in &log_parts {
            let still_active = registry
                .snapshot()
                .into_iter()
                .find(|reader| reader.meta().id == descriptor.id)
                .is_some_and(|reader| reader.part().dir == *dir);
            if still_active {
                part::remove_part_dirs(std::slice::from_ref(dir))?;
                removed_log_ids.push(descriptor.id.clone());
                removed += 1;
            }
        }
        for (descriptor, dir) in &trace_parts {
            let still_active = trace_registry
                .snapshot()
                .into_iter()
                .find(|reader| reader.part().meta.id == descriptor.id)
                .is_some_and(|reader| reader.part().dir == *dir);
            if still_active {
                part::remove_part_dirs(std::slice::from_ref(dir))?;
                removed_trace_ids.push(descriptor.id.clone());
                removed += 1;
            }
        }
        if remote_cache.is_none() {
            registry.unregister(&removed_log_ids);
            trace_registry.unregister(&removed_trace_ids);
        }
    }

    if let Some(cache) = remote_cache {
        if !removed_log_ids.is_empty() {
            let ids = removed_log_ids.clone();
            let epoch = cache.remote_operation_epoch();
            match tokio::time::timeout(
                config.max_retention_runtime,
                cache.storage.publish(&[], &ids),
            )
            .await
            {
                Ok(Ok(_)) => cache.mark_remote_healthy_since(epoch),
                Ok(Err(error)) => {
                    cache.mark_remote_unhealthy();
                    return Err(error);
                }
                Err(_) => {
                    cache.mark_remote_unhealthy();
                    return Err("object-store retention timed out".to_string());
                }
            }
        }
        if !removed_trace_ids.is_empty() {
            let descriptors: Vec<_> = trace_parts
                .iter()
                .filter(|(part, _)| removed_trace_ids.iter().any(|id| id == &part.id))
                .map(|(part, _)| part.clone())
                .collect();
            let epoch = cache.remote_operation_epoch();
            match tokio::time::timeout(
                config.max_retention_runtime,
                cache.storage.remove_trace_parts(&descriptors),
            )
            .await
            {
                Ok(Ok(_)) => cache.mark_remote_healthy_since(epoch),
                Ok(Err(error)) => {
                    cache.mark_remote_unhealthy();
                    return Err(error);
                }
                Err(_) => {
                    cache.mark_remote_unhealthy();
                    return Err("trace object-store retention timed out".to_string());
                }
            }
        }
    }

    if remote_cache.is_some() {
        let _guard = registry.operation_lock().write_owned().await;
        registry.unregister(&removed_log_ids);
        trace_registry.unregister(&removed_trace_ids);
    }
    if let Some(cache) = remote_cache {
        let epoch = cache.remote_operation_epoch();
        match tokio::time::timeout(
            config.max_retention_runtime,
            cache
                .storage
                .garbage_collect_orphans(config.retention_grace_period),
        )
        .await
        {
            Ok(Ok(_)) => cache.mark_remote_healthy_since(epoch),
            Ok(Err(error)) => {
                cache.mark_remote_unhealthy();
                return Err(error);
            }
            Err(_) => {
                cache.mark_remote_unhealthy();
                return Err("remote retention garbage collection timed out".to_string());
            }
        }
    }
    tracing::info!(
        removed,
        mode = expiry.mode(),
        "retention removed expired parts"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::memtable::Labels;
    use crate::part::{self, Row};
    use crate::tenant::TenantId;
    use crate::tenant_policy::TenantRetention;
    use crate::trace::TraceSpan;

    fn tenant(raw: &str) -> TenantId {
        TenantId::parse(raw).expect("valid tenant id")
    }

    fn row_for(owner: &str, timestamp_ns: i64) -> Row {
        Row {
            tenant: tenant(owner),
            timestamp_ns,
            labels: [("app".to_string(), owner.to_string())]
                .into_iter()
                .collect(),
            line: format!("{owner} line"),
            structured_metadata: vec![],
        }
    }

    fn span_for(owner: &str, trace_id: &str, timestamp_ns: i64) -> TraceSpan {
        TraceSpan {
            tenant: tenant(owner),
            trace_id: trace_id.to_string(),
            span_id: format!("{owner}-span"),
            start_time_ns: timestamp_ns,
            end_time_ns: timestamp_ns + 1,
            span: Default::default(),
            resource: None,
            resource_schema_url: String::new(),
            scope: None,
            scope_schema_url: String::new(),
        }
    }

    fn policy_with(entries: &[(&str, TenantRetention)]) -> TenantPolicy {
        let policy = TenantPolicy::enabled_for_test();
        policy.install_for_test(
            entries
                .iter()
                .map(|(name, retention)| (tenant(name), *retention))
                .collect(),
        );
        policy
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("loggytracy-{label}-{}", uuid::Uuid::new_v4()))
    }

    /// Per-tenant retention runs with no global period, so every per-tenant
    /// test uses this configuration.
    fn per_tenant_config(root: std::path::PathBuf) -> Config {
        Config {
            data_dir: root,
            retention_period: None,
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn a_part_survives_while_any_tenant_in_it_is_unknown() {
        let root = temp_root("retention-unknown-tenant");
        let parts_root = root.join("parts");
        let parts = part::flush_rows(
            vec![row_for("alpha", 1_000), row_for("beta", 1_000)],
            &parts_root,
            100,
        )
        .unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        // alpha has expired many times over; beta was never mentioned by the
        // control plane, and unknown means keep.
        let policy = policy_with(&[("alpha", TenantRetention::Finite(Duration::from_nanos(1)))]);

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root),
            &policy,
            1_000_000,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert!(parts[0].dir.exists());
    }

    /// Unknown means keep, in whichever state the data happens to be. Nothing
    /// deletes memtable entries for retention — the query floor makes expired
    /// ones invisible and the flush carries them to a part — so an unknown
    /// tenant that has only pushed must come through a tick untouched, and so
    /// must the part it eventually becomes.
    #[tokio::test]
    async fn an_unknown_tenant_survives_retention_in_the_memtable_and_in_a_part() {
        let root = temp_root("retention-unknown-memtable");
        let parts_root = root.join("parts");
        let config = per_tenant_config(root);
        let registry = Arc::new(PartRegistry::new());
        let trace_registry = Arc::new(TraceRegistry::standalone());
        // Everything the control plane knows about has expired many times
        // over; `unmentioned` is absent from it entirely.
        let policy = policy_with(&[("alpha", TenantRetention::Finite(Duration::from_nanos(1)))]);

        let memtable = crate::memtable::MemTable::new();
        memtable.insert(
            tenant("unmentioned"),
            [("app".to_string(), "unmentioned".to_string())]
                .into_iter()
                .collect(),
            vec![crate::memtable::LogEntry {
                timestamp_ns: 1_000,
                line: "still in the memtable".to_string(),
                structured_metadata: Vec::new(),
            }],
        );

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &config,
            &policy,
            1_000_000,
        )
        .await
        .unwrap();

        let in_memory = memtable.query(
            &tenant("unmentioned"),
            &[],
            &[],
            i64::MIN,
            i64::MAX,
            10,
            true,
        );
        assert_eq!(
            in_memory.iter().flat_map(|stream| &stream.entries).count(),
            1
        );

        // The same rows once they have been flushed, in a part of their own.
        let parts =
            part::flush_rows(vec![row_for("unmentioned", 1_000)], &parts_root, 100).unwrap();
        registry.register(parts.clone()).unwrap();

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &config,
            &policy,
            1_000_000,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert!(parts[0].dir.exists());
    }

    #[tokio::test]
    async fn a_part_whose_every_tenant_expired_takes_the_free_whole_delete_path() {
        let root = temp_root("retention-all-expired");
        let parts_root = root.join("parts");
        let parts = part::flush_rows(
            vec![row_for("alpha", 1_000), row_for("beta", 1_000)],
            &parts_root,
            100,
        )
        .unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let policy = policy_with(&[
            ("alpha", TenantRetention::Finite(Duration::from_nanos(1))),
            ("beta", TenantRetention::Finite(Duration::from_nanos(1))),
        ]);

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root),
            &policy,
            1_000_000,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 0);
        assert!(!parts[0].dir.exists());
    }

    #[tokio::test]
    async fn an_infinite_tenant_keeps_its_part_forever() {
        let root = temp_root("retention-infinite");
        let parts_root = root.join("parts");
        let parts = part::flush_rows(vec![row_for("intern", 1_000)], &parts_root, 100).unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let policy = policy_with(&[("intern", TenantRetention::Infinite)]);

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root),
            &policy,
            i64::MAX as u128,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 1);
    }

    #[tokio::test]
    async fn an_endpoint_that_never_answered_deletes_nothing() {
        let root = temp_root("retention-no-snapshot");
        let parts_root = root.join("parts");
        let parts = part::flush_rows(vec![row_for("alpha", 1_000)], &parts_root, 100).unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        // Enabled, but no successful fetch has ever happened.
        let policy = TenantPolicy::enabled_for_test();

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root),
            &policy,
            i64::MAX as u128,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert!(parts[0].dir.exists());
    }

    #[tokio::test]
    async fn traces_follow_the_same_per_tenant_rules() {
        let root = temp_root("retention-traces");
        let traces_root = root.join("traces");
        let spans = vec![
            span_for("alpha", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1_000),
            span_for("beta", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1_000),
        ];
        let parts = crate::trace_part::flush_trace_spans(spans, &traces_root, 100).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let trace_registry = Arc::new(TraceRegistry::standalone());
        trace_registry.register(parts.clone()).unwrap();

        // beta is unknown, so the shared trace part stays.
        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root.clone()),
            &policy_with(&[("alpha", TenantRetention::Finite(Duration::from_nanos(1)))]),
            1_000_000,
        )
        .await
        .unwrap();
        assert_eq!(trace_registry.part_count(), 1);

        // Once beta has a policy too, the whole part goes.
        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &per_tenant_config(root),
            &policy_with(&[
                ("alpha", TenantRetention::Finite(Duration::from_nanos(1))),
                ("beta", TenantRetention::Finite(Duration::from_nanos(1))),
            ]),
            1_000_000,
        )
        .await
        .unwrap();
        assert_eq!(trace_registry.part_count(), 0);
        assert!(!parts[0].dir.exists());
    }

    #[tokio::test]
    async fn an_upgrade_keeps_data_alive_past_the_old_cutoff() {
        let root = temp_root("retention-upgrade");
        let parts_root = root.join("parts");
        let parts = part::flush_rows(vec![row_for("alpha", 1_000)], &parts_root, 100).unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let config = per_tenant_config(root);

        // The upgraded plan is applied at deletion time, not at write time, so
        // data written under the old plan is covered by the new one.
        let upgraded = policy_with(&[(
            "alpha",
            TenantRetention::Finite(Duration::from_nanos(1_000_000)),
        )]);
        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &config,
            &upgraded,
            1_000_500,
        )
        .await
        .unwrap();
        assert_eq!(registry.part_count(), 1);

        let downgraded =
            policy_with(&[("alpha", TenantRetention::Finite(Duration::from_nanos(100)))]);
        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &config,
            &downgraded,
            1_000_500,
        )
        .await
        .unwrap();
        assert_eq!(registry.part_count(), 0);
    }

    #[tokio::test]
    async fn retention_is_disabled_without_a_period() {
        let root =
            std::env::temp_dir().join(format!("loggytracy-retention-{}", uuid::Uuid::new_v4()));
        let registry = Arc::new(PartRegistry::new());
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let config = Config {
            data_dir: root,
            ..Config::default()
        };
        retention_once(
            &registry,
            &trace_registry,
            None,
            &config,
            &TenantPolicy::disabled(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn retention_removes_an_expired_local_part() {
        let root =
            std::env::temp_dir().join(format!("loggytracy-retention-{}", uuid::Uuid::new_v4()));
        let parts_root = root.join("parts");
        let labels: Labels = [("app".to_string(), "retention".to_string())]
            .into_iter()
            .collect();
        let parts = part::flush_rows(
            vec![Row {
                tenant: crate::tenant::test_tenant(),
                timestamp_ns: 1_000,
                labels,
                line: "expired".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let config = Config {
            data_dir: root,
            retention_period: Some(Duration::from_secs(1)),
            ..Config::default()
        };
        retention_once(
            &registry,
            &trace_registry,
            None,
            &config,
            &TenantPolicy::disabled(),
        )
        .await
        .unwrap();
        assert_eq!(registry.part_count(), 0);
        assert!(!parts[0].dir.exists());
    }

    #[tokio::test]
    async fn retention_removes_expired_parts_from_a_remote_manifest() {
        let root =
            std::env::temp_dir().join(format!("loggytracy-retention-{}", uuid::Uuid::new_v4()));
        let parts_root = root.join("parts");
        let labels: Labels = [("app".to_string(), "remote-retention".to_string())]
            .into_iter()
            .collect();
        let parts = part::flush_rows(
            vec![Row {
                tenant: crate::tenant::test_tenant(),
                timestamp_ns: 1_000,
                labels,
                line: "expired remotely".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap();
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        storage.publish(&parts, &[]).await.unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let remote = RemoteCache::new(storage.clone(), parts_root.clone());
        let config = Config {
            data_dir: root,
            retention_period: Some(Duration::from_secs(1)),
            ..Config::default()
        };

        retention_once(
            &registry,
            &trace_registry,
            Some(&remote),
            &config,
            &TenantPolicy::disabled(),
        )
        .await
        .unwrap();

        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
        assert_eq!(registry.part_count(), 0);
        assert!(!parts[0].dir.exists());
    }

    #[tokio::test]
    async fn retention_clock_keeps_the_cutoff_boundary() {
        let root = std::env::temp_dir().join(format!(
            "loggytracy-retention-boundary-{}",
            uuid::Uuid::new_v4()
        ));
        let parts_root = root.join("parts");
        let parts = part::flush_rows(
            vec![Row {
                tenant: crate::tenant::test_tenant(),
                timestamp_ns: 90,
                labels: [("app".to_string(), "boundary".to_string())]
                    .into_iter()
                    .collect(),
                line: "boundary".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            16,
        )
        .unwrap();
        let registry = Arc::new(PartRegistry::new());
        registry.register(parts.clone()).unwrap();
        let trace_registry = Arc::new(TraceRegistry::standalone());
        let config = Config {
            data_dir: root,
            retention_period: Some(Duration::from_nanos(10)),
            ..Config::default()
        };

        retention_once_at(
            &registry,
            &trace_registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            100,
        )
        .await
        .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert!(parts[0].dir.exists());
    }
}
