use crate::config::Config;
use crate::journal::{self, Journal};
use crate::memtable::MemTable;
use crate::part::cleanup_tmp;
use crate::trace;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::app_state::AppStateDependencies;
use crate::flush;
use crate::log_ingest;
use crate::merge;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::{ObjectStorage, RemoteCache};
use crate::part_registry::PartRegistry;
use crate::retention;
use crate::router;
use crate::tenant_policy::TenantPolicy;
use crate::trace_ingest;
use crate::{AppState, trace_registry};

/// Retry a startup step that talks to the object store.
///
/// Startup used to panic on the first failure, which does not distinguish a
/// network blip from real corruption: an object store that is unavailable for
/// ten seconds turned into a crash loop, and every restart in that loop dropped
/// the process before it could accept the ingest its clients were still
/// sending.
///
/// Bounded rather than infinite. Absorbing a transient outage is the point;
/// waiting forever on a permanently misconfigured store would just replace a
/// visible crash with an invisible hang. Past the budget the process exits and
/// the orchestrator's own restart backoff takes over, which is the right place
/// for escalation to live.
async fn with_object_store_retry<T, F, Fut>(what: &str, budget: Duration, mut step: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let started = tokio::time::Instant::now();
    let mut backoff = Duration::from_millis(250);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match step().await {
            Ok(value) => {
                if attempt > 1 {
                    tracing::info!(what, attempt, "object-store startup step recovered");
                }
                return value;
            }
            Err(error) => {
                let elapsed = started.elapsed();
                if elapsed >= budget {
                    panic!(
                        "{what} failed for {elapsed:?} across {attempt} attempts and the startup \
budget is exhausted: {error}"
                    );
                }
                tracing::warn!(
                    what,
                    attempt,
                    %error,
                    "object-store startup step failed; retrying"
                );
                tokio::time::sleep(backoff.min(budget.saturating_sub(elapsed))).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

#[allow(dead_code)]
pub fn recover(config: &Config, memtable: &MemTable) -> Result<journal::ReplayReport, String> {
    recover_with_traces(config, memtable, &trace::TraceMemTable::new())
}

pub fn recover_with_traces(
    config: &Config,
    memtable: &MemTable,
    trace_memtable: &trace::TraceMemTable,
) -> Result<journal::ReplayReport, String> {
    let parts_root = config.data_dir.join("parts");
    std::fs::create_dir_all(&parts_root).map_err(|e| e.to_string())?;
    cleanup_tmp(&parts_root)?;
    let traces_root = config.data_dir.join("traces");
    std::fs::create_dir_all(&traces_root).map_err(|e| e.to_string())?;
    cleanup_tmp(&traces_root)?;

    let wal_path = config.data_dir.join("journal.wal");
    let ckpt_path = config.data_dir.join("journal.ckpt");
    let replay = journal::replay_reporting(&wal_path, &ckpt_path, memtable, trace_memtable)?;
    let (ckpt_start, replay_end) = (replay.checkpoint, replay.end_offset);
    if replay.records > 0 {
        // Said at WARN rather than INFO. Delivery is at-least-once, so these
        // entries may already be durable in parts and about to be written
        // again — the number is the upper bound on what this restart
        // duplicated, and it is the only place that says so.
        tracing::warn!(
            records = replay.records,
            entries = replay.entries,
            checkpoint = ckpt_start,
            replay_end,
            "journal replay put records back: the previous run did not checkpoint them, so up to \
this many entries may be duplicated in storage"
        );
    } else {
        tracing::info!(
            checkpoint = ckpt_start,
            replay_end,
            "journal recovery complete"
        );
    }

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
    Ok(replay)
}

pub async fn run(config: Arc<Config>) {
    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        panic!("failed to create data dir: {}", e);
    }

    let startup_budget = config.startup_retry_budget;
    let clock = crate::clock::Clock::system();
    let memtable = Arc::new(MemTable::new());
    let trace_memtable = Arc::new(trace::TraceMemTable::new());

    let replay = recover_with_traces(&config, &memtable, &trace_memtable)
        .unwrap_or_else(|e| panic!("recovery failed: {e}"));

    let object_storage = config
        .object_store_url
        .as_deref()
        .map(ObjectStorage::from_url)
        .transpose()
        .unwrap_or_else(|error| panic!("object-store initialization failed: {error}"))
        .map(Arc::new);
    // Claimed before anything else touches the prefix, and before the workers
    // exist: from here on, any manifest write by an older instance fails
    // instead of racing this one. The architecture always assumed a single
    // writer; this is what makes the assumption hold when an orchestrator
    // starts the replacement before the original has finished draining.
    if let Some(storage) = &object_storage {
        storage.claim_writer_epoch().await.unwrap_or_else(|error| {
            panic!("failed to claim the object-store writer epoch: {error}")
        });
    }
    let remote_manifest = if let Some(storage) = &object_storage {
        let checkpoint = journal::read_checkpoint(&config.data_dir.join("journal.ckpt"))
            .unwrap_or_else(|error| panic!("failed to read journal checkpoint: {error}"));
        with_object_store_retry("flush transaction recovery", startup_budget, || {
            storage.reconcile_flush_transaction(&config.data_dir, checkpoint)
        })
        .await;
        let parts_root = config.data_dir.join("parts");
        let restored =
            with_object_store_retry("local cache reconciliation", startup_budget, || {
                storage.reconcile_local_cache(&parts_root)
            })
            .await;
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
        let traces_root = config.data_dir.join("traces");
        let restored =
            with_object_store_retry("trace cache reconciliation", startup_budget, || {
                storage.reconcile_trace_local_cache(&traces_root)
            })
            .await;
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
    // Published once. These describe this process's own recovery rather than a
    // rate, so a scrape after any uptime still reports what the restart did.
    metrics
        .wal_replayed_records
        .store(replay.records, Ordering::Relaxed);
    metrics
        .wal_replayed_entries
        .store(replay.entries, Ordering::Relaxed);
    let shutdown = Arc::new(crate::shutdown::ShutdownState::new());
    if let Some(storage) = &object_storage {
        storage.set_fence_sink(shutdown.clone());
    }
    // Loaded before the workers spawn, and fatal on failure — the same class as
    // a manifest that cannot be read. Booting with a silently empty map would
    // unclamp every query and hand back data a downgrade had already hidden.
    // An empty *listing* is a different thing and boots normally: it means
    // nothing has been pushed yet, and an unknown tenant keeps its data.
    let tenant_policy = Arc::new(
        with_object_store_retry("tenant policy load", startup_budget, || {
            TenantPolicy::load(&config, object_storage.clone())
        })
        .await,
    );

    // Loaded on the same terms and for a stronger reason: booting without a
    // request that was accepted would serve lines a tenant asked to have
    // deleted.
    let delete_requests = Arc::new(crate::delete_requests::DeleteRequests::new(
        object_storage.clone(),
    ));
    let loaded_delete_requests =
        with_object_store_retry("delete request load", startup_budget, || {
            let delete_requests = delete_requests.clone();
            async move { delete_requests.load().await }
        })
        .await;
    if loaded_delete_requests > 0 {
        tracing::info!(
            requests = loaded_delete_requests,
            "restored outstanding deletion requests"
        );
    }

    // Handles for every background worker. Shutdown signals them through the
    // drain watch and then joins them here before the final force-flush, so the
    // force-flush is the only writer touching the registry and object store.
    let mut worker_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Cheap Arc clones retained for the final force-flush; the originals are
    // moved into the workers and the shared AppState below.
    let finalize_memtable = memtable.clone();
    let finalize_journal = journal.clone();
    let finalize_parts = parts.clone();
    let finalize_trace_registry = trace_registry.clone();
    let finalize_remote_cache = remote_cache.clone();
    let finalize_config = config.clone();
    let finalize_shutdown = shutdown.clone();

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
        let drain_rx = shutdown.subscribe();
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
                drain_rx,
            )
            .await;
        });
        worker_handles.push(tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::info!("flush task stopped"),
                Err(error) => tracing::error!(%error, "flush task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        }));
    }

    {
        let registry = parts.clone();
        let config = config.clone();
        let task_health = merge_healthy.clone();
        let monitor_health = merge_healthy.clone();
        let cache = remote_cache.clone();
        let metrics = metrics.clone();
        let drain_rx = shutdown.subscribe();
        let policy = tenant_policy.clone();
        let deletes = delete_requests.clone();
        let handle = tokio::spawn(async move {
            merge::merge_loop(
                registry,
                cache,
                config,
                policy,
                deletes,
                task_health,
                metrics,
                drain_rx,
            )
            .await;
        });
        worker_handles.push(tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::info!("merge task stopped"),
                Err(error) => tracing::error!(%error, "merge task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        }));
    }

    if let Some(cache) = remote_cache.clone() {
        let config = config.clone();
        let registry = parts.clone();
        let trace_registry = trace_registry.clone();
        let metrics = metrics.clone();
        let mut drain_rx = shutdown.subscribe();
        worker_handles.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.cache_eviction_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = crate::shutdown::wait_for_drain(&mut drain_rx) => return,
                }
                let guard = registry.operation_lock().write_owned().await;
                // Eviction deletes part bodies, and a merge rewrite reads its
                // inputs — and opens its unregistered outputs — under only
                // the deletion lock. Operation first, then deletion.
                let deletion_guard = registry.deletion_lock().write_owned().await;
                let eligible = registry.part_dirs();
                let trace_eligible = trace_registry.part_dirs();
                // Eviction walks the whole parts tree with `read_dir` and a
                // `symlink_metadata` per entry. That is synchronous work, and
                // running it inline blocked a runtime worker for as long as the
                // tree took to traverse — on the same thread pool serving the
                // queries this lock is already holding up.
                //
                // The guard moves into the blocking task rather than being
                // released first. Deciding what to evict and doing it has to
                // stay atomic with respect to a reader pinning a part, or
                // eviction could delete a body a query had just claimed.
                let cache_for_eviction = cache.clone();
                let budget = config.cache_max_bytes;
                let evicted = tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    let _deletion_guard = deletion_guard;
                    match cache_for_eviction.storage.evict_cache(
                        &cache_for_eviction.parts_root,
                        budget,
                        &eligible,
                    ) {
                        Ok(bytes) => {
                            let trace_budget = budget.saturating_sub(bytes);
                            let result = cache_for_eviction.storage.evict_trace_cache(
                                &cache_for_eviction.trace_parts_root(),
                                trace_budget,
                                &trace_eligible,
                            );
                            (Ok(bytes), result)
                        }
                        Err(error) => (Err(error), Ok(0)),
                    }
                })
                .await;
                let (log_result, trace_result) = match evicted {
                    Ok(results) => results,
                    // The blocking pool is shared, so a panic in another task
                    // cannot reach here; this is a join failure, which means
                    // the eviction did not run rather than that it half ran.
                    Err(error) => (Err(format!("eviction task failed: {error}")), Ok(0)),
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
        }));
    }

    {
        let registry = parts.clone();
        let trace_registry = trace_registry.clone();
        let remote_cache = remote_cache.clone();
        let config = config.clone();
        let metrics = metrics.clone();
        let policy = tenant_policy.clone();
        let retention_journal = journal.clone();
        let retention_health = retention_healthy.clone();
        let drain_rx = shutdown.subscribe();
        worker_handles.push(tokio::spawn(async move {
            retention::retention_loop(
                registry,
                trace_registry,
                remote_cache,
                config,
                policy,
                retention_journal,
                metrics,
                retention_health,
                drain_rx,
            )
            .await;
        }));
    }

    let otlp_journal = journal.clone();
    let otlp_healthy = Arc::new(AtomicBool::new(true));
    let state = Arc::new(AppState::from_config(
        config.clone(),
        AppStateDependencies {
            memtable,
            journal,
            parts: parts.clone(),
            trace_parts: trace_registry,
            flush_healthy,
            merge_healthy,
            retention_healthy,
            otlp_healthy: otlp_healthy.clone(),
            remote_cache,
            tenant_policy,
            metrics,
            shutdown: shutdown.clone(),
            clock: clock.clone(),
            delete_requests: Some(delete_requests),
        },
    ));

    let ingest_gate = state.ingest_gate.clone();
    let tenant_quota = state.tenant_quota.clone();
    let app = router::build_router(state);

    // A SIGTERM/SIGINT starts draining: new ingest is rejected, and every drain
    // subscriber (the HTTP and gRPC servers plus the background workers) stops.
    {
        let signal_shutdown = shutdown.clone();
        tokio::spawn(async move {
            crate::shutdown::wait_for_signal().await;
            tracing::warn!("shutdown signal received; draining before machine replacement");
            signal_shutdown.begin_drain();
        });
    }

    if config.retention_period.is_none() && config.tenant_policy_token.is_none() {
        // Neither mechanism is configured, so nothing ever expires. This is a
        // legitimate configuration for a fixed-size dataset and a data-loss
        // trap for anything else, and it is silent either way without this.
        tracing::warn!(
            "no retention is configured: neither LOGGYTRACY_RETENTION_PERIOD nor \
LOGGYTRACY_TENANT_POLICY_TOKEN is set, so the object store grows without bound"
        );
    }
    announce_bind(&config.otlp_grpc_addr, "LOGGYTRACY_OTLP_GRPC_ADDR");
    let otlp_addr = config
        .otlp_grpc_addr
        .parse()
        .unwrap_or_else(|error| panic!("invalid OTLP gRPC address: {error}"));
    let otlp_service = trace_ingest::TraceIngestService::new(
        otlp_journal.clone(),
        shutdown.clone(),
        config.clone(),
        ingest_gate.clone(),
        tenant_quota.clone(),
    );
    // Logs and traces share the listener, the journal and the drain signal.
    // `ARCHITECTURE.md` has described OTLP as an ingest protocol from the
    // start; until this was registered, a collector exporting logs to it got
    // `UNIMPLEMENTED`.
    let otlp_log_service = log_ingest::LogIngestService::new(
        otlp_journal,
        shutdown.clone(),
        config.clone(),
        ingest_gate,
        tenant_quota,
        clock.clone(),
        parts,
    );
    let otlp_task_health = otlp_healthy;
    let mut otlp_drain = shutdown.subscribe();
    let otlp_handle = tokio::spawn(async move {
        let result = tonic::transport::Server::builder()
            .add_service(otlp_service.into_server())
            .add_service(otlp_log_service.into_server())
            .serve_with_shutdown(otlp_addr, async move {
                crate::shutdown::wait_for_drain(&mut otlp_drain).await;
            })
            .await;
        if let Err(error) = result {
            tracing::error!(%error, "OTLP gRPC server failed");
        }
        otlp_task_health.store(false, Ordering::Release);
    });

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");
    tracing::info!(addr = %config.listen_addr, "loggytracy listening");
    announce_bind(&config.listen_addr, "LOGGYTRACY_LISTEN_ADDR");
    let mut http_drain = shutdown.subscribe();
    let http_result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            crate::shutdown::wait_for_drain(&mut http_drain).await;
        })
        .await;
    if let Err(error) = http_result {
        tracing::error!(%error, "HTTP server error");
    }

    // The HTTP server has stopped accepting and finished its in-flight requests.
    // Ensure draining is set even if it exited for another reason, then wait for
    // the gRPC server and every background worker to stop.
    shutdown.begin_drain();
    if let Err(error) = otlp_handle.await {
        tracing::error!(%error, "OTLP gRPC task join failed");
    }
    for handle in worker_handles {
        if let Err(error) = handle.await {
            tracing::error!(%error, "background worker join failed");
        }
    }

    tracing::info!("servers drained; force-flushing before exit");
    let trace_memtable = finalize_journal.trace_memtable();
    let outcome = crate::shutdown::finalize_flush(crate::shutdown::FinalizeContext {
        shutdown: finalize_shutdown,
        memtable: finalize_memtable,
        trace_memtable,
        journal: finalize_journal,
        registry: finalize_parts,
        trace_registry: finalize_trace_registry,
        remote_cache: finalize_remote_cache,
        config: finalize_config,
    })
    .await;
    match outcome {
        crate::shutdown::ShutdownOutcome::Durable => {
            tracing::info!("graceful shutdown complete; all acknowledged data is durable");
        }
        crate::shutdown::ShutdownOutcome::AbortedByOperator => {
            // Force-flush did not reach durability; the operator forced the exit.
            // Exit non-zero so an automated controller never mistakes this for a
            // clean shutdown and discards the disk — the WAL still holds the
            // unflushed data and a restart on this same disk recovers it.
            tracing::error!(
                "shutdown aborted before durability; acknowledged data is only on the WAL. \
Restart on THIS disk to recover; do not discard the disk or replace the machine."
            );
            std::process::exit(1);
        }
        crate::shutdown::ShutdownOutcome::Fenced => {
            // Same disposition as an operator abort, for a different reason:
            // another writer owns the prefix, so publishing is not something
            // this process can retry its way out of.
            tracing::error!(
                "shutdown after being fenced by a newer writer; acknowledged data is only on the \
WAL. Reconcile THIS disk before discarding it, and check that two instances were not started \
against the same LOGGYTRACY_OBJECT_STORE_URL."
            );
            std::process::exit(1);
        }
    }
}

/// Say plainly which side of the trust boundary a listener ended up on.
///
/// Loopback is the default, so a deployment that forgot to configure the
/// address gets a log line explaining why nothing can reach it rather than a
/// silent success. A non-loopback address is the operator's decision, and the
/// line records that it was made — there is no TLS or authentication here, so
/// whatever draws the boundary is outside this process.
fn is_loopback_addr(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map(|(host, _)| host).unwrap_or(addr);
    matches!(
        host.trim_matches(['[', ']']),
        "127.0.0.1" | "::1" | "localhost"
    )
}

fn announce_bind(addr: &str, knob: &str) {
    if is_loopback_addr(addr) {
        tracing::info!(
            %addr,
            "bound to loopback only; set {knob} to accept traffic from outside this machine"
        );
    } else {
        tracing::warn!(
            %addr,
            "bound to a non-loopback address: this process has no TLS and no authentication, \
        and X-Scope-OrgID is trusted without proof. Keep it inside a trust boundary"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// The default has to be the safe one, because the unsafe configuration is
    /// the one that works. There is no TLS or authentication here and
    /// `X-Scope-OrgID` is trusted without proof, so a listener reachable from
    /// off the machine is a decision, not a default.
    #[test]
    fn the_default_listeners_stay_inside_the_machine() {
        let config = Config::default();
        assert!(
            is_loopback_addr(&config.listen_addr),
            "{}",
            config.listen_addr
        );
        assert!(
            is_loopback_addr(&config.otlp_grpc_addr),
            "{}",
            config.otlp_grpc_addr
        );
    }

    #[test]
    fn a_bind_address_is_classified_by_host_not_by_spelling() {
        for addr in ["127.0.0.1:3100", "localhost:3100", "[::1]:4317"] {
            assert!(is_loopback_addr(addr), "{addr}");
        }
        for addr in ["0.0.0.0:3100", "10.0.0.4:3100", "[::]:4317"] {
            assert!(!is_loopback_addr(addr), "{addr}");
        }
    }

    /// The whole point of virtual time. This budget is five minutes by default;
    /// asserting anything about it against a real clock would mean a five-minute
    /// test, which is why the behaviour went untested when it was written.
    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_is_absorbed_rather_than_becoming_a_crash_loop() {
        let attempts = AtomicU32::new(0);
        let value = with_object_store_retry("probe", Duration::from_secs(300), || {
            let seen = attempts.fetch_add(1, Ordering::Relaxed);
            async move {
                if seen < 4 {
                    Err(format!("transient {seen}"))
                } else {
                    Ok("recovered")
                }
            }
        })
        .await;

        assert_eq!(value, "recovered");
        assert_eq!(attempts.load(Ordering::Relaxed), 5);
    }

    /// Bounded on purpose. A store that is misconfigured rather than briefly
    /// unavailable must eventually surface, or a visible crash is replaced by an
    /// invisible hang.
    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_gives_up_once_the_budget_is_spent() {
        let started = tokio::time::Instant::now();
        let outcome = tokio::spawn(async {
            with_object_store_retry::<(), _, _>("probe", Duration::from_secs(300), || async {
                Err("always".to_string())
            })
            .await
        })
        .await;

        let error = outcome.expect_err("an exhausted budget must panic");
        assert!(error.is_panic());
        // It really did wait out the budget — in virtual time, so the test costs
        // nothing — rather than giving up on the first attempt.
        assert!(
            started.elapsed() >= Duration::from_secs(300),
            "gave up after only {:?}",
            started.elapsed()
        );
    }

    /// Backoff has to actually back off. Retrying a dead store in a tight loop
    /// is its own denial of service, against a dependency that is already
    /// struggling.
    #[tokio::test(start_paused = true)]
    async fn retries_back_off_instead_of_spinning() {
        let attempts = Arc::new(AtomicU32::new(0));
        let counter = attempts.clone();
        let handle = tokio::spawn(async move {
            with_object_store_retry::<(), _, _>("probe", Duration::from_secs(300), || {
                counter.fetch_add(1, Ordering::Relaxed);
                async { Err("always".to_string()) }
            })
            .await
        });
        let _ = handle.await;

        // 250 ms doubling to a 10 s ceiling reaches 300 s in well under a
        // hundred attempts; a spinning loop would be in the millions.
        let total = attempts.load(Ordering::Relaxed);
        assert!(
            (5..100).contains(&total),
            "{total} attempts does not look like exponential backoff"
        );
    }
}
