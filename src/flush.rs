use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tokio::time::interval;

use crate::config::Config;
use crate::journal::Journal;
use crate::memtable::MemTable;
use crate::object_storage::RemoteCache;
use crate::part::{self};
use crate::part_registry::PartRegistry;

pub async fn flush_loop(
    memtable: Arc<MemTable>,
    journal: Arc<Journal>,
    registry: Arc<PartRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    healthy: Arc<AtomicBool>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.flush_check_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_flush = Instant::now();
    let mut pending_checkpoint = None;

    loop {
        ticker.tick().await;
        if pending_checkpoint.is_some() {
            let pending_offset = pending_checkpoint.expect("checked pending checkpoint");
            match retry_pending_checkpoint(&journal, &mut pending_checkpoint) {
                Ok(offset) => {
                    healthy.store(true, Ordering::Release);
                    last_flush = Instant::now();
                    tracing::info!(offset, "advanced previously failed journal checkpoint");
                }
                Err(error) => {
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
        if memtable.is_empty() {
            continue;
        }
        let size = memtable.approximate_size();
        let elapsed = last_flush.elapsed();
        if (size as u64) < config.flush_max_bytes && elapsed < config.flush_max_interval {
            continue;
        }
        match flush_once(
            &memtable,
            &journal,
            &registry,
            remote_cache.as_deref(),
            &config,
            &mut pending_checkpoint,
        )
        .await
        {
            Ok(()) => {
                healthy.store(true, Ordering::Release);
                last_flush = Instant::now();
            }
            Err(e) => {
                healthy.store(false, Ordering::Release);
                tracing::error!(error = %e, "flush iteration failed");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

fn retry_pending_checkpoint(
    journal: &Journal,
    pending_checkpoint: &mut Option<u64>,
) -> Result<u64, String> {
    let offset = pending_checkpoint
        .as_ref()
        .copied()
        .ok_or_else(|| "no journal checkpoint is pending".to_string())?;
    journal.set_checkpoint(offset).map_err(|e| e.to_string())?;
    *pending_checkpoint = None;
    Ok(offset)
}

async fn flush_once(
    memtable: &MemTable,
    journal: &Journal,
    registry: &PartRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
    pending_checkpoint: &mut Option<u64>,
) -> Result<(), String> {
    let ckpt = journal.checkpoint().await.map_err(|e| e.to_string())?;
    if ckpt.snapshot.is_empty() {
        memtable.commit_flush();
        if let Err(error) = advance_checkpoint(journal, ckpt.offset, remote_cache.is_some()).await {
            if remote_cache.is_none() {
                *pending_checkpoint = Some(ckpt.offset);
            }
            return Err(error.to_string());
        }
        return Ok(());
    }
    let rows = part::rows_from_snapshot(&ckpt.snapshot);
    let total_rows = rows.len();
    // Prevent eviction from observing a freshly committed directory before
    // it has been published and installed in the registry.
    let _cache_guard = match remote_cache {
        Some(_) => Some(registry.operation_lock().read_owned().await),
        None => None,
    };
    let parts_root = config.data_dir.join("parts");
    if let Err(error) = std::fs::create_dir_all(&parts_root) {
        memtable.abort_flush(ckpt.snapshot);
        return Err(error.to_string());
    }

    let row_group_size = config.row_group_size;
    let result = match tokio::task::spawn_blocking({
        let parts_root = parts_root.clone();
        move || part::flush_rows(rows, &parts_root, row_group_size)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            memtable.abort_flush(ckpt.snapshot);
            return Err(format!("flush task join failed: {}", error));
        }
    };

    match result {
        Ok(new_parts) => {
            let n = new_parts.len();
            let new_part_dirs: Vec<_> = new_parts.iter().map(|part| part.dir.clone()).collect();
            if let Some(cache) = remote_cache
                && let Err(error) = cache.storage.publish(&new_parts, &[]).await
            {
                let cleanup_error = part::remove_part_dirs(&new_part_dirs).err();
                memtable.abort_flush(ckpt.snapshot);
                return Err(match cleanup_error {
                    Some(cleanup_error) => format!(
                        "object-store publish failed: {error}; failed to clean local parts: {cleanup_error}"
                    ),
                    None => format!("object-store publish failed: {error}"),
                });
            }
            if let Err(error) = registry.register(new_parts) {
                // Once a remote manifest has made these parts visible it is
                // unsafe to delete them. Recovery will reopen them from the
                // manifest; reinserting the snapshot preserves at-least-once
                // semantics until then.
                let cleanup_error = if remote_cache.is_none() {
                    part::remove_part_dirs(&new_part_dirs).err()
                } else {
                    None
                };
                memtable.abort_flush(ckpt.snapshot);
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
                if remote_cache.is_none() {
                    *pending_checkpoint = Some(ckpt.offset);
                }
                return Err(format!(
                    "parts were committed but journal checkpoint could not be advanced: {error}"
                ));
            }
            memtable.commit_flush();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{Labels, LogEntry};

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
            labels,
            vec![LogEntry {
                timestamp_ns: 1_700_000_000_000_000_000,
                line: "only once".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let journal = Journal::spawn(&config, memtable.clone()).unwrap();
        let registry = PartRegistry::new();
        let mut pending_checkpoint = None;

        // Force write_checkpoint's temporary-file creation to fail.
        std::fs::create_dir_all(data_dir.join("journal.ckpt.tmp")).unwrap();

        let first = flush_once(
            &memtable,
            &journal,
            &registry,
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
        let retried_offset = retry_pending_checkpoint(&journal, &mut pending_checkpoint).unwrap();

        assert_eq!(registry.part_count(), 1);
        assert_eq!(retried_offset, failed_offset);
        assert_eq!(
            crate::journal::read_checkpoint(journal.ckpt_path()).unwrap(),
            failed_offset
        );
        assert!(pending_checkpoint.is_none());
        let results = registry
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
            .unwrap();
        assert_eq!(results.iter().map(|r| r.entries.len()).sum::<usize>(), 1);
    }
}
