use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
// tokio's Instant, not std's: it follows `tokio::time::pause()`, which is what
// lets a test assert this loop's cadence in microseconds instead of waiting out
// `flush_max_interval` in real seconds.
use tokio::time::Instant;

use tokio::sync::watch;
use tokio::time::interval;

use crate::shutdown::wait_for_drain;

use crate::config::Config;
use crate::journal::Journal;
use crate::memtable::MemTable;
use crate::metrics::RuntimeMetrics;
use crate::object_storage::{
    FlushTransaction, ManifestPart, RemoteCache, TraceManifestPart, clear_flush_transaction,
    write_flush_transaction,
};
use crate::part::{self};
use crate::part_registry::PartRegistry;
use crate::trace::TraceMemTable;
use crate::trace_part;
use crate::trace_registry::TraceRegistry;

#[allow(clippy::too_many_arguments)]
pub async fn flush_loop(
    memtable: Arc<MemTable>,
    trace_memtable: Arc<TraceMemTable>,
    journal: Arc<Journal>,
    registry: Arc<PartRegistry>,
    trace_registry: Arc<TraceRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    healthy: Arc<AtomicBool>,
    metrics: Arc<RuntimeMetrics>,
    mut drain_rx: watch::Receiver<bool>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.flush_check_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_flush = Instant::now();
    let mut pending_checkpoint = None;

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = wait_for_drain(&mut drain_rx) => {
                tracing::info!("flush loop stopping for shutdown; final force-flush will drain remaining data");
                return;
            }
        }
        if pending_checkpoint.is_some() {
            let pending_offset = pending_checkpoint.expect("checked pending checkpoint");
            match retry_pending_checkpoint(
                &journal,
                &mut pending_checkpoint,
                remote_cache.is_some(),
            )
            .await
            {
                Ok(offset) => {
                    metrics.flush_success.fetch_add(1, Ordering::Relaxed);
                    if remote_cache.is_some()
                        && let Err(error) = clear_flush_transaction(&config.data_dir)
                    {
                        tracing::warn!(
                            %error,
                            "failed to clear committed flush transaction after checkpoint retry"
                        );
                    }
                    healthy.store(true, Ordering::Release);
                    last_flush = Instant::now();
                    tracing::info!(offset, "advanced previously failed journal checkpoint");
                }
                Err(error) => {
                    metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                    healthy.store(false, Ordering::Release);
                    tracing::error!(
                        %error,
                        offset = pending_offset,
                        "failed to retry journal checkpoint"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
        if memtable.is_empty() && trace_memtable.is_empty() {
            continue;
        }
        let size = memtable
            .approximate_size()
            .saturating_add(trace_memtable.approximate_size());
        let elapsed = last_flush.elapsed();
        if (size as u64) < config.flush_max_bytes && elapsed < config.flush_max_interval {
            continue;
        }
        match flush_once(
            &memtable,
            &trace_memtable,
            &journal,
            &registry,
            &trace_registry,
            remote_cache.as_deref(),
            &config,
            &mut pending_checkpoint,
        )
        .await
        {
            Ok(()) => {
                metrics.flush_success.fetch_add(1, Ordering::Relaxed);
                healthy.store(true, Ordering::Release);
                last_flush = Instant::now();
            }
            Err(e) => {
                metrics.flush_errors.fetch_add(1, Ordering::Relaxed);
                healthy.store(false, Ordering::Release);
                tracing::error!(error = %e, "flush iteration failed");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Borrowed inputs for a single force-flush pass during shutdown.
pub struct ForceFlush<'a> {
    pub memtable: &'a MemTable,
    pub trace_memtable: &'a TraceMemTable,
    pub journal: &'a Journal,
    pub registry: &'a PartRegistry,
    pub trace_registry: &'a TraceRegistry,
    pub remote_cache: Option<&'a RemoteCache>,
    pub config: &'a Config,
    pub pending_checkpoint: &'a mut Option<u64>,
}

/// Perform one force-flush pass, ignoring the size/interval thresholds the
/// background loop uses. First retry any checkpoint the loop left pending, then
/// flush the current MemTable snapshot. Returns `Ok(true)` once nothing remains
/// to flush and the checkpoint is durable, `Ok(false)` when progress was made
/// but more work remains, and `Err` on a failed attempt the caller should
/// retry.
pub async fn force_flush_pass(pass: ForceFlush<'_>) -> Result<bool, String> {
    let ForceFlush {
        memtable,
        trace_memtable,
        journal,
        registry,
        trace_registry,
        remote_cache,
        config,
        pending_checkpoint,
    } = pass;

    if pending_checkpoint.is_some() {
        retry_pending_checkpoint(journal, pending_checkpoint, remote_cache.is_some())
            .await
            .map_err(|error| format!("failed to retry pending journal checkpoint: {error}"))?;
        if remote_cache.is_some()
            && let Err(error) = clear_flush_transaction(&config.data_dir)
        {
            tracing::warn!(%error, "failed to clear committed flush transaction after checkpoint retry");
        }
    }

    flush_once(
        memtable,
        trace_memtable,
        journal,
        registry,
        trace_registry,
        remote_cache,
        config,
        pending_checkpoint,
    )
    .await?;

    Ok(memtable.is_empty() && trace_memtable.is_empty() && pending_checkpoint.is_none())
}

async fn retry_pending_checkpoint(
    journal: &Journal,
    pending_checkpoint: &mut Option<u64>,
    compact: bool,
) -> Result<u64, String> {
    let offset = pending_checkpoint
        .as_ref()
        .copied()
        .ok_or_else(|| "no journal checkpoint is pending".to_string())?;
    if compact {
        journal
            .compact_checkpoint(offset)
            .await
            .map_err(|e| e.to_string())?;
    } else {
        journal.set_checkpoint(offset).map_err(|e| e.to_string())?;
    }
    *pending_checkpoint = None;
    Ok(offset)
}

#[allow(clippy::too_many_arguments)]
async fn flush_once(
    memtable: &MemTable,
    trace_memtable: &TraceMemTable,
    journal: &Journal,
    registry: &PartRegistry,
    trace_registry: &TraceRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
    pending_checkpoint: &mut Option<u64>,
) -> Result<(), String> {
    let ckpt = journal.checkpoint().await.map_err(|e| e.to_string())?;
    if ckpt.snapshot.is_empty() && ckpt.trace_snapshot.is_empty() {
        memtable.commit_flush();
        trace_memtable.commit_flush();
        if let Err(error) = advance_checkpoint(journal, ckpt.offset, remote_cache.is_some()).await {
            // The part/WAL boundary is ambiguous for both local checkpoint
            // writes and remote WAL compaction. Retain the offset so a
            // transient failure is retried even when no new ingest arrives.
            *pending_checkpoint = Some(ckpt.offset);
            return Err(error.to_string());
        }
        if remote_cache.is_some()
            && let Err(error) = clear_flush_transaction(&config.data_dir)
        {
            tracing::warn!(%error, "failed to clear committed flush transaction");
        }
        return Ok(());
    }
    let rows = {
        let _arena = crate::memprof::enter(crate::memprof::Arena::Flush);
        part::rows_from_snapshot(&ckpt.snapshot)
    };
    let trace_spans = ckpt.trace_snapshot;
    // An `Arc` clone: the flush reads the buffer the memtable still holds for
    // the abort path, rather than being handed a copy of it.
    let trace_spans_for_flush = trace_spans.clone();
    let total_rows = rows.len().saturating_add(trace_spans.len());
    // Prevent eviction from observing a freshly committed directory before
    // it has been published and installed in the registry.
    let cache_guard = match remote_cache {
        Some(_) => Some(registry.operation_lock().read_owned().await),
        None => None,
    };
    let parts_root = config.data_dir.join("parts");
    if let Err(error) = std::fs::create_dir_all(&parts_root) {
        memtable.abort_flush(ckpt.snapshot);
        trace_memtable.abort_flush(trace_spans);
        return Err(error.to_string());
    }

    let row_group_size = config.row_group_size;
    let result = match tokio::task::spawn_blocking({
        let parts_root = parts_root.clone();
        let traces_root = config.data_dir.join("traces");
        move || {
            let _arena = crate::memprof::enter(crate::memprof::Arena::Flush);
            let log_parts = part::flush_rows(rows, &parts_root, row_group_size)?;
            let trace_parts = match trace_part::flush_trace_spans(
                &trace_spans_for_flush,
                &traces_root,
                row_group_size,
            ) {
                Ok(parts) => parts,
                Err(error) => {
                    let log_dirs = part_dirs(&log_parts);
                    let cleanup = cleanup_part_directories(&log_dirs, &[]);
                    return Err(match cleanup {
                        Ok(()) => std::io::Error::other(format!("trace flush failed: {error}")),
                        Err(cleanup_error) => std::io::Error::other(format!(
                            "trace flush failed: {error}; log-part rollback failed: {cleanup_error}"
                        )),
                    });
                }
            };
            Ok::<_, std::io::Error>((log_parts, trace_parts))
        }
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            memtable.abort_flush(ckpt.snapshot);
            trace_memtable.abort_flush(trace_spans);
            return Err(format!("flush task join failed: {}", error));
        }
    };

    match result {
        Ok((new_parts, new_trace_parts)) => {
            let n = new_parts.len();
            let new_part_dirs: Vec<_> = new_parts.iter().map(|part| part.dir.clone()).collect();
            let new_trace_part_dirs: Vec<_> = new_trace_parts
                .iter()
                .map(|part| part.dir.clone())
                .collect();
            if let Some(cache) = remote_cache {
                let checkpoint =
                    match crate::journal::read_checkpoint(&config.data_dir.join("journal.ckpt")) {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).ok();
                            memtable.abort_flush(ckpt.snapshot);
                            trace_memtable.abort_flush(trace_spans);
                            return Err(error.to_string());
                        }
                    };
                if let Err(error) = cache
                    .storage
                    .reconcile_flush_transaction(&config.data_dir, checkpoint)
                    .await
                {
                    cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).ok();
                    memtable.abort_flush(ckpt.snapshot);
                    trace_memtable.abort_flush(trace_spans);
                    return Err(format!("failed to reconcile flush transaction: {error}"));
                }
                let transaction = FlushTransaction {
                    offset: ckpt.offset,
                    log_parts: new_parts.iter().map(ManifestPart::from).collect(),
                    trace_parts: new_trace_parts
                        .iter()
                        .map(|part| TraceManifestPart {
                            id: part.meta.id.clone(),
                            partition: part.meta.partition.clone(),
                        })
                        .collect(),
                };
                if let Err(error) = write_flush_transaction(&config.data_dir, &transaction) {
                    cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).ok();
                    memtable.abort_flush(ckpt.snapshot);
                    trace_memtable.abort_flush(trace_spans);
                    return Err(format!("failed to record flush transaction: {error}"));
                }
                if let Err(error) = cache.storage.publish(&new_parts, &[]).await {
                    cache.record_remote_failure();
                    let rollback_error = cache
                        .storage
                        .rollback_flush_transaction(&config.data_dir)
                        .await
                        .err();
                    let cleanup_error =
                        cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).err();
                    memtable.abort_flush(ckpt.snapshot);
                    trace_memtable.abort_flush(trace_spans);
                    return Err(match cleanup_error {
                        Some(cleanup_error) => format!(
                            "object-store publish failed: {error}; failed to clean local parts: {cleanup_error}"
                        ),
                        None => match rollback_error {
                            Some(rollback_error) => format!(
                                "object-store publish failed: {error}; rollback failed: {rollback_error}"
                            ),
                            None => format!("object-store publish failed: {error}"),
                        },
                    });
                }
                if let Err(error) = cache.storage.publish_trace_parts(&new_trace_parts).await {
                    cache.record_remote_failure();
                    let rollback_error = cache
                        .storage
                        .rollback_flush_transaction(&config.data_dir)
                        .await
                        .err();
                    let cleanup_error =
                        cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).err();
                    memtable.abort_flush(ckpt.snapshot);
                    trace_memtable.abort_flush(trace_spans);
                    return Err(match cleanup_error {
                        Some(cleanup_error) => format!(
                            "trace object-store publish failed: {error}; failed to clean local parts: {cleanup_error}"
                        ),
                        None => match rollback_error {
                            Some(rollback_error) => format!(
                                "trace object-store publish failed: {error}; rollback failed: {rollback_error}"
                            ),
                            None => format!("trace object-store publish failed: {error}"),
                        },
                    });
                }
                cache.record_remote_success();
            }
            // A query holds the operation read lock for its complete
            // memtable/part snapshot. Publish may happen under a read lock,
            // but registry installation and memtable commit must be one
            // write-locked visibility transition; otherwise a query can see
            // the flushing snapshot and its newly registered part together.
            drop(cache_guard);
            let _visibility_guard = registry.operation_lock().write_owned().await;
            let registered_log_ids = match registry.register(new_parts) {
                Ok(ids) => ids,
                Err(error) => {
                    // Once a remote manifest has made these parts visible it is
                    // unsafe to delete them. Recovery will reopen them from the
                    // manifest; reinserting the snapshot preserves at-least-once
                    // semantics until then.
                    let cleanup_error = if remote_cache.is_none() {
                        cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).err()
                    } else {
                        None
                    };
                    memtable.abort_flush(ckpt.snapshot);
                    trace_memtable.abort_flush(trace_spans);
                    return Err(match cleanup_error {
                        Some(cleanup_error) => {
                            format!("{error}; failed to clean rejected parts: {cleanup_error}")
                        }
                        None => error,
                    });
                }
            };
            if let Err(error) = trace_registry.register(new_trace_parts) {
                registry.unregister(&registered_log_ids);
                let cleanup_error = if remote_cache.is_none() {
                    cleanup_part_directories(&new_part_dirs, &new_trace_part_dirs).err()
                } else {
                    None
                };
                memtable.abort_flush(ckpt.snapshot);
                trace_memtable.abort_flush(trace_spans);
                return Err(match cleanup_error {
                    Some(cleanup_error) => {
                        format!("{error}; failed to clean rejected parts: {cleanup_error}")
                    }
                    None => error,
                });
            }
            if let Err(error) =
                advance_checkpoint(journal, ckpt.offset, remote_cache.is_some()).await
            {
                // The part is already durable and visible. A checkpoint error
                // is ambiguous: rename may have succeeded before a directory
                // fsync failed. Rolling the part back could therefore lose
                // data on restart, while leaving the flushing snapshot in
                // memory would make every retry write it into another part.
                // Commit the in-memory side and retain this offset for the
                // flush loop to retry before it attempts any later flush.
                memtable.commit_flush();
                trace_memtable.commit_flush();
                *pending_checkpoint = Some(ckpt.offset);
                return Err(format!(
                    "parts were committed but journal checkpoint could not be advanced: {error}"
                ));
            }
            if remote_cache.is_some()
                && let Err(error) = clear_flush_transaction(&config.data_dir)
            {
                // The checkpoint is the commit record. A leftover intent is
                // harmless and startup will clear it after observing the
                // committed checkpoint.
                tracing::warn!(%error, "failed to clear committed flush transaction");
            }
            memtable.commit_flush();
            trace_memtable.commit_flush();
            tracing::info!(
                offset = ckpt.offset,
                rows = total_rows,
                parts = n,
                "flushed memtable to parts"
            );
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = %e, "flush_rows failed, reinserting into memtable");
            memtable.abort_flush(ckpt.snapshot);
            trace_memtable.abort_flush(trace_spans);
            Err(e.to_string())
        }
    }
}

async fn advance_checkpoint(
    journal: &Journal,
    offset: u64,
    compact: bool,
) -> Result<(), std::io::Error> {
    if compact {
        journal.compact_checkpoint(offset).await
    } else {
        journal.set_checkpoint(offset)
    }
}

fn part_dirs(parts: &[part::Part]) -> Vec<std::path::PathBuf> {
    parts.iter().map(|part| part.dir.clone()).collect()
}

fn cleanup_part_directories(
    log_dirs: &[std::path::PathBuf],
    trace_dirs: &[std::path::PathBuf],
) -> Result<(), String> {
    let mut dirs = log_dirs.to_vec();
    dirs.extend_from_slice(trace_dirs);
    part::remove_part_dirs(&dirs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{Labels, LogEntry};
    use crate::tenant::test_tenant;

    fn temp_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "loggytracy-flush-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn checkpoint_failure_does_not_flush_same_snapshot_again() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let labels: Labels = [("app".to_string(), "test".to_string())]
            .into_iter()
            .collect();
        memtable.insert(
            test_tenant(),
            labels,
            vec![LogEntry {
                timestamp_ns: 1_700_000_000_000_000_000,
                line: "only once".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let journal = Journal::spawn(&config, memtable.clone()).unwrap();
        let registry = PartRegistry::new();
        let trace_memtable = journal.trace_memtable();
        let trace_registry = TraceRegistry::new(registry.operation_lock());
        let mut pending_checkpoint = None;

        // Force write_checkpoint's temporary-file creation to fail.
        std::fs::create_dir_all(data_dir.join("journal.ckpt.tmp")).unwrap();

        let first = flush_once(
            &memtable,
            &trace_memtable,
            &journal,
            &registry,
            &trace_registry,
            None,
            &config,
            &mut pending_checkpoint,
        )
        .await;
        assert!(first.is_err());
        assert!(memtable.is_empty());
        assert_eq!(registry.part_count(), 1);
        let failed_offset = pending_checkpoint.expect("checkpoint retry must be retained");

        std::fs::remove_dir(data_dir.join("journal.ckpt.tmp")).unwrap();
        let retried_offset = retry_pending_checkpoint(&journal, &mut pending_checkpoint, false)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert_eq!(retried_offset, failed_offset);
        assert_eq!(
            crate::journal::read_checkpoint(journal.ckpt_path()).unwrap(),
            failed_offset
        );
        assert!(pending_checkpoint.is_none());
        let results = registry
            .query(
                &test_tenant(),
                &[],
                &[],
                crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                100,
                true,
            )
            .unwrap();
        assert_eq!(results.iter().map(|r| r.entries.len()).sum::<usize>(), 1);
    }
}
