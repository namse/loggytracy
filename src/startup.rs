use crate::config::Config;
use crate::journal::{self, Journal};
use crate::memtable::MemTable;
use crate::part::cleanup_tmp;
use crate::trace;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::app_state::AppStateDependencies;
use crate::flush;
use crate::merge;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::{ObjectStorage, RemoteCache};
use crate::part_registry::PartRegistry;
use crate::retention;
use crate::router;
use crate::trace_ingest;
use crate::{AppState, trace_registry};

#[allow(dead_code)]
pub fn recover(config: &Config, memtable: &MemTable) -> Result<(), String> {
    recover_with_traces(config, memtable, &trace::TraceMemTable::new())
}

pub fn recover_with_traces(
    config: &Config,
    memtable: &MemTable,
    trace_memtable: &trace::TraceMemTable,
) -> Result<(), String> {
    let parts_root = config.data_dir.join("parts");
    std::fs::create_dir_all(&parts_root).map_err(|e| e.to_string())?;
    cleanup_tmp(&parts_root)?;
    let traces_root = config.data_dir.join("traces");
    std::fs::create_dir_all(&traces_root).map_err(|e| e.to_string())?;
    cleanup_tmp(&traces_root)?;

    let wal_path = config.data_dir.join("journal.wal");
    let ckpt_path = config.data_dir.join("journal.ckpt");
    let (ckpt_start, replay_end) =
        journal::replay_with_traces(&wal_path, &ckpt_path, memtable, trace_memtable)?;
    tracing::info!(
        checkpoint = ckpt_start,
        replay_end,
        "journal recovery complete"
    );

    if wal_path.exists()
        && replay_end
            < std::fs::metadata(&wal_path)
                .map_err(|e| e.to_string())?
                .len()
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .map_err(|e| e.to_string())?;
        f.set_len(replay_end).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        drop(f);
        tracing::info!(replay_end, "truncated corrupt WAL tail");
    }
    Ok(())
}

pub async fn run(config: Arc<Config>) {
    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        panic!("failed to create data dir: {}", e);
    }

    let memtable = Arc::new(MemTable::new());
    let trace_memtable = Arc::new(trace::TraceMemTable::new());

    recover_with_traces(&config, &memtable, &trace_memtable)
        .unwrap_or_else(|e| panic!("recovery failed: {e}"));

    let object_storage = config
        .object_store_url
        .as_deref()
        .map(ObjectStorage::from_url)
        .transpose()
        .unwrap_or_else(|error| panic!("object-store initialization failed: {error}"))
        .map(Arc::new);
    let remote_manifest = if let Some(storage) = &object_storage {
        let checkpoint = journal::read_checkpoint(&config.data_dir.join("journal.ckpt"))
            .unwrap_or_else(|error| panic!("failed to read journal checkpoint: {error}"));
        storage
            .reconcile_flush_transaction(&config.data_dir, checkpoint)
            .await
            .unwrap_or_else(|error| panic!("flush transaction recovery failed: {error}"));
        let restored = storage
            .reconcile_local_cache(&config.data_dir.join("parts"))
            .await
            .unwrap_or_else(|error| panic!("object-store recovery failed: {error}"));
        tracing::info!(
            generation = restored.generation,
            parts = restored.parts.len(),
            "restored object-store manifest"
        );
        Some(restored)
    } else {
        None
    };
    let remote_trace_manifest = if let Some(storage) = &object_storage {
        let restored = storage
            .reconcile_trace_local_cache(&config.data_dir.join("traces"))
            .await
            .unwrap_or_else(|error| panic!("trace object-store recovery failed: {error}"));
        tracing::info!(
            generation = restored.generation,
            parts = restored.parts.len(),
            "restored trace object-store manifest"
        );
        Some(restored)
    } else {
        None
    };

    let parts_root = config.data_dir.join("parts");
    let parts = Arc::new(
        match &remote_manifest {
            Some(manifest) => PartRegistry::load_from_manifest(&parts_root, manifest),
            None => PartRegistry::load_from_disk(&parts_root),
        }
        .unwrap_or_else(|e| panic!("failed to load parts: {e}")),
    );
    let trace_registry = Arc::new(
        match &remote_trace_manifest {
            Some(manifest) => trace_registry::TraceRegistry::load_from_manifest(
                &config.data_dir.join("traces"),
                manifest,
                parts.operation_lock(),
            ),
            None => trace_registry::TraceRegistry::load_from_disk(
                &config.data_dir.join("traces"),
                parts.operation_lock(),
            ),
        }
        .unwrap_or_else(|e| panic!("failed to load trace parts: {e}")),
    );
    let remote_cache = object_storage
        .as_ref()
        .map(|storage| Arc::new(RemoteCache::new(storage.clone(), parts_root.clone())));

    let journal = Arc::new(
        Journal::spawn_with_traces(&config, memtable.clone(), trace_memtable)
            .expect("failed to initialize journal"),
    );

    let flush_healthy = Arc::new(AtomicBool::new(true));
    let merge_healthy = Arc::new(AtomicBool::new(true));
    let retention_healthy = Arc::new(AtomicBool::new(true));
    let metrics = Arc::new(RuntimeMetrics::new());

    {
        let memtable = memtable.clone();
        let journal = journal.clone();
        let registry = parts.clone();
        let trace_memtable = journal.trace_memtable();
        let trace_registry = trace_registry.clone();
        let config = config.clone();
        let task_health = flush_healthy.clone();
        let monitor_health = flush_healthy.clone();
        let cache = remote_cache.clone();
        let metrics = metrics.clone();
        let handle = tokio::spawn(async move {
            flush::flush_loop(
                memtable,
                trace_memtable,
                journal,
                registry,
                trace_registry,
                cache,
                config,
                task_health,
                metrics,
            )
            .await;
        });
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::error!("flush task terminated unexpectedly"),
                Err(error) => tracing::error!(%error, "flush task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        });
    }

    {
        let registry = parts.clone();
        let config = config.clone();
        let task_health = merge_healthy.clone();
        let monitor_health = merge_healthy.clone();
        let cache = remote_cache.clone();
        let metrics = metrics.clone();
        let handle = tokio::spawn(async move {
            merge::merge_loop(registry, cache, config, task_health, metrics).await;
        });
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::error!("merge task terminated unexpectedly"),
                Err(error) => tracing::error!(%error, "merge task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        });
    }

    if let Some(cache) = remote_cache.clone() {
        let config = config.clone();
        let registry = parts.clone();
        let trace_registry = trace_registry.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.cache_eviction_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let _guard = registry.operation_lock().write_owned().await;
                let eligible = registry.part_ids();
                let (log_result, trace_result) = match cache.storage.evict_cache(
                    &cache.parts_root,
                    config.cache_max_bytes,
                    &eligible,
                ) {
                    Ok(bytes) => {
                        let trace_budget = config.cache_max_bytes.saturating_sub(bytes);
                        let result = cache.storage.evict_trace_cache(
                            &cache.trace_parts_root(),
                            trace_budget,
                            &trace_registry.part_ids(),
                        );
                        (Ok(bytes), result)
                    }
                    Err(error) => (Err(error), Ok(0)),
                };
                match (log_result, trace_result) {
                    (Ok(bytes), Ok(trace_bytes)) => {
                        metrics.cache_evictions.fetch_add(1, Ordering::Relaxed);
                        cache.mark_cache_healthy();
                        tracing::debug!(
                            bytes,
                            trace_bytes,
                            "local log and trace cache eviction complete"
                        );
                    }
                    (Err(error), _) | (_, Err(error)) => {
                        cache.mark_cache_unhealthy();
                        tracing::warn!(%error, "local part cache eviction failed");
                    }
                }
            }
        });
    }

    {
        let registry = parts.clone();
        let trace_registry = trace_registry.clone();
        let remote_cache = remote_cache.clone();
        let config = config.clone();
        let metrics = metrics.clone();
        let retention_health = retention_healthy.clone();
        tokio::spawn(async move {
            retention::retention_loop(
                registry,
                trace_registry,
                remote_cache,
                config,
                metrics,
                retention_health,
            )
            .await;
        });
    }

    let otlp_journal = journal.clone();
    let otlp_healthy = Arc::new(AtomicBool::new(true));
    let state = Arc::new(AppState::from_config(
        config.clone(),
        AppStateDependencies {
            memtable,
            journal,
            parts,
            trace_parts: trace_registry,
            flush_healthy,
            merge_healthy,
            retention_healthy,
            otlp_healthy: otlp_healthy.clone(),
            remote_cache,
            metrics,
        },
    ));

    let app = router::build_router(state);

    let otlp_addr = config
        .otlp_grpc_addr
        .parse()
        .unwrap_or_else(|error| panic!("invalid OTLP gRPC address: {error}"));
    let otlp_service = trace_ingest::TraceIngestService::new(otlp_journal);
    let otlp_task_health = otlp_healthy;
    tokio::spawn(async move {
        if let Err(error) = tonic::transport::Server::builder()
            .add_service(otlp_service.into_server())
            .serve(otlp_addr)
            .await
        {
            tracing::error!(%error, "OTLP gRPC server failed");
        }
        otlp_task_health.store(false, Ordering::Release);
    });

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");
    tracing::info!(addr = %config.listen_addr, "loggytracy listening");
    axum::serve(listener, app).await.expect("server error");
}
