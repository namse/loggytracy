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
use crate::trace_registry::TraceRegistry;

pub async fn retention_loop(
    registry: Arc<PartRegistry>,
    trace_registry: Arc<TraceRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    metrics: Arc<RuntimeMetrics>,
    healthy: Arc<AtomicBool>,
    mut drain_rx: watch::Receiver<bool>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.retention_interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = wait_for_drain(&mut drain_rx) => return,
        }
        if let Err(error) =
            retention_once(&registry, &trace_registry, remote_cache.as_deref(), &config).await
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
) -> Result<(), String> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before UNIX epoch: {error}"))?
        .as_nanos();
    retention_once_at(registry, trace_registry, remote_cache, config, now_ns).await
}

async fn retention_once_at(
    registry: &PartRegistry,
    trace_registry: &TraceRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
    now_ns: u128,
) -> Result<(), String> {
    let Some(retention_period) = config.retention_period else {
        return Ok(());
    };
    let cutoff_ns = now_ns
        .saturating_sub(retention_period.as_nanos())
        .min(i64::MAX as u128) as i64;
    let batch_size = config.retention_batch_size.max(1);

    // Snapshot under a shared lifecycle guard. Remote manifest I/O happens
    // after releasing it, so retention cannot stop flush/merge/eviction for a
    // network round trip and queries can continue concurrently.
    let guard = registry.operation_lock().read_owned().await;
    let mut log_parts: Vec<_> = registry
        .snapshot()
        .into_iter()
        .filter(|reader| reader.meta().max_ts_ns < cutoff_ns)
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
        .filter(|reader| reader.part().meta.max_ts_ns < cutoff_ns)
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
            match tokio::time::timeout(config.max_restore_runtime, cache.storage.publish(&[], &ids))
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
                config.max_restore_runtime,
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
            config.max_restore_runtime,
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
    tracing::info!(removed, cutoff_ns, "retention removed expired parts");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::memtable::Labels;
    use crate::part::{self, Row};

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
        retention_once(&registry, &trace_registry, None, &config)
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
        retention_once(&registry, &trace_registry, None, &config)
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

        retention_once(&registry, &trace_registry, Some(&remote), &config)
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

        retention_once_at(&registry, &trace_registry, None, &config, 100)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert!(parts[0].dir.exists());
    }
}
