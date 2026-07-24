use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::time::interval;

use crate::config::Config;
use crate::memtable::Labels;
use crate::object_storage::RemoteCache;
use crate::part::{self, PartReader};
use crate::part_registry::PartRegistry;

pub async fn merge_loop(
    registry: Arc<PartRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    healthy: Arc<AtomicBool>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.merge_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match merge_once(&registry, remote_cache.as_deref(), &config).await {
            Ok(()) => healthy.store(true, Ordering::Release),
            Err(e) => {
                healthy.store(false, Ordering::Release);
                tracing::error!(error = %e, "merge iteration failed");
            }
        }
    }
}

async fn merge_once(
    registry: &PartRegistry,
    remote_cache: Option<&RemoteCache>,
    config: &Config,
) -> Result<(), String> {
    let readers = registry.snapshot();
    if readers.is_empty() {
        return Ok(());
    }

    let mut by_partition: HashMap<String, Vec<Arc<PartReader>>> = HashMap::new();
    for r in readers {
        by_partition
            .entry(r.meta().partition.clone())
            .or_default()
            .push(r);
    }

    let parts_root = config.data_dir.join("parts");
    let mut errors = Vec::new();

    for (partition, mut parts) in by_partition {
        parts.sort_by_key(|r| r.meta().row_count);

        // 너무 큰 단일 part는 제외 (이미 충분히 큼)
        // 작은 part들을 그룹지어 합친다. 단순화: 파티션 내에서 merge_min_part_count개 이상이면
        // 가장 작은 것부터 merge_target_part_rows에 도달할 때까지 그룹화.
        let groups = group_for_merge(&parts, config);
        for group in groups {
            if group.len() < config.merge_min_part_count.max(2) {
                continue;
            }
            let old_ids: Vec<String> = group.iter().map(|r| r.meta().id.clone()).collect();
            let old_dirs: Vec<std::path::PathBuf> =
                group.iter().map(|r| r.part().dir.clone()).collect();

            // A merge removes its input directories after swapping the
            // registry generation. Hold the exclusive lifecycle guard for the
            // complete transaction so a query cannot retain an old reader and
            // then try to open its Parquet file after deletion. This lock is
            // also shared with remote cache restoration and eviction.
            let _part_guard = registry.operation_lock().write_owned().await;
            if let Some(cache) = remote_cache {
                let required: std::collections::HashSet<String> = old_ids.iter().cloned().collect();
                let missing = registry.missing_data_ids(&required);
                if !missing.is_empty() {
                    let epoch = cache.remote_operation_epoch();
                    if let Err(error) = cache
                        .storage
                        .restore_parts(&cache.parts_root, &missing)
                        .await
                    {
                        cache.mark_remote_unhealthy();
                        return Err(error);
                    }
                    cache.mark_remote_healthy_since(epoch);
                }
            }

            let rows_result = tokio::task::spawn_blocking({
                let group = group.clone();
                move || read_all_rows(&group)
            })
            .await
            .map_err(|e| format!("merge read task join failed: {}", e))?;

            let rows = match rows_result {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, partition = %partition, "merge read failed, skipping group");
                    errors.push(format!("merge read failed for partition {partition}: {e}"));
                    continue;
                }
            };

            let row_group_size = config.row_group_size;
            let merge_result = tokio::task::spawn_blocking({
                let parts_root = parts_root.clone();
                let old_dirs = old_dirs.clone();
                move || {
                    part::flush_rows_with_merge_tombstone(
                        rows,
                        &parts_root,
                        row_group_size,
                        &old_dirs,
                    )
                }
            })
            .await
            .map_err(|e| format!("merge write task join failed: {}", e))?;

            match merge_result {
                Ok(new_parts) => {
                    let new_n = new_parts.len();
                    let new_part_dirs: Vec<std::path::PathBuf> =
                        new_parts.iter().map(|p| p.dir.clone()).collect();
                    let cleanup_old_dirs = match verify_merge_tombstones(
                        &new_part_dirs,
                        &parts_root,
                        &old_dirs,
                    ) {
                        Ok(tombstoned_old_dirs) => tombstoned_old_dirs,
                        Err(error) => {
                            // The tombstone is the durable description of the
                            // replacement transaction. Never publish a merged
                            // part or delete its inputs unless that description
                            // can be read back and matches the intended inputs.
                            tracing::error!(
                                error = %error,
                                partition = %partition,
                                "merged part tombstone verification failed; keeping old parts"
                            );
                            if let Err(cleanup_error) = part::remove_part_dirs(&new_part_dirs) {
                                tracing::warn!(
                                    error = %cleanup_error,
                                    "failed to remove merged parts with invalid tombstones"
                                );
                            }
                            errors.push(format!(
                                "merged part tombstone verification failed for partition {partition}: {error}"
                            ));
                            continue;
                        }
                    };
                    if let Some(cache) = remote_cache {
                        let epoch = cache.remote_operation_epoch();
                        if let Err(error) = cache.storage.publish(&new_parts, &old_ids).await {
                            cache.mark_remote_unhealthy();
                            tracing::error!(
                                %error,
                                partition = %partition,
                                "merged parts could not be published; keeping old parts"
                            );
                            if let Err(cleanup_error) = part::remove_part_dirs(&new_part_dirs) {
                                tracing::warn!(%cleanup_error, "failed to remove unpublished merged parts");
                            }
                            errors.push(format!(
                                "object-store merge publish failed for partition {partition}: {error}"
                            ));
                            continue;
                        }
                        cache.mark_remote_healthy_since(epoch);
                    }
                    match registry.replace(&old_ids, new_parts) {
                        Ok(_) => {
                            if let Err(error) = part::remove_part_dirs(&cleanup_old_dirs) {
                                tracing::warn!(
                                    error = %error,
                                    "old part cleanup incomplete; retaining merge tombstones"
                                );
                            } else {
                                for new_dir in &new_part_dirs {
                                    if let Err(e) = part::remove_merge_tombstone(new_dir) {
                                        tracing::warn!(
                                            error = %e,
                                            ?new_dir,
                                            "failed to remove merge tombstone (will be cleaned on next discover)"
                                        );
                                    }
                                }
                            }
                            tracing::info!(
                                partition = %partition,
                                merged = old_ids.len(),
                                produced = new_n,
                                "merge completed"
                            );
                        }
                        Err(e) => {
                            // The tombstone is part of each new directory,
                            // so remove those directories as one failed
                            // transaction. Old registry entries and old data
                            // remain intact.
                            tracing::error!(
                                error = %e,
                                partition = %partition,
                                "merged part validation failed; keeping old parts"
                            );
                            if remote_cache.is_none()
                                && let Err(cleanup_error) = part::remove_part_dirs(&new_part_dirs)
                            {
                                tracing::warn!(
                                    error = %cleanup_error,
                                    "failed to remove invalid merged parts"
                                );
                            }
                            errors.push(format!(
                                "merged part validation failed for partition {partition}: {e}"
                            ));
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, partition = %partition, "merge flush_rows failed");
                    errors.push(format!("merge write failed for partition {partition}: {e}"));
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn canonical_dir_set(dirs: &[PathBuf]) -> Result<std::collections::BTreeSet<PathBuf>, String> {
    dirs.iter()
        .map(|dir| {
            std::fs::canonicalize(dir)
                .map_err(|error| format!("failed to canonicalize {}: {error}", dir.display()))
        })
        .collect()
}

fn verify_merge_tombstones(
    new_part_dirs: &[PathBuf],
    parts_root: &Path,
    expected_old_dirs: &[PathBuf],
) -> Result<Vec<PathBuf>, String> {
    if new_part_dirs.is_empty() {
        return Err("merge produced no replacement parts".to_string());
    }
    if expected_old_dirs.is_empty() {
        return Err("merge has no input parts".to_string());
    }

    let expected = canonical_dir_set(expected_old_dirs)?;
    let mut cleanup_old_dirs = None;
    for new_dir in new_part_dirs {
        let tombstoned_old_dirs = part::read_merge_tombstone_dirs(new_dir, parts_root)
            .map_err(|error| format!("failed to read {}: {error}", new_dir.display()))?;
        if tombstoned_old_dirs.is_empty() {
            return Err(format!(
                "merge tombstone in {} contains no input parts",
                new_dir.display()
            ));
        }
        let actual = canonical_dir_set(&tombstoned_old_dirs)?;
        if actual != expected {
            return Err(format!(
                "merge tombstone in {} does not match the intended input parts",
                new_dir.display()
            ));
        }
        if cleanup_old_dirs.is_none() {
            cleanup_old_dirs = Some(tombstoned_old_dirs);
        }
    }

    cleanup_old_dirs.ok_or_else(|| "merge produced no replacement parts".to_string())
}

fn group_for_merge(parts: &[Arc<PartReader>], config: &Config) -> Vec<Vec<Arc<PartReader>>> {
    let mut groups: Vec<Vec<Arc<PartReader>>> = Vec::new();
    let mut current: Vec<Arc<PartReader>> = Vec::new();
    let mut current_rows: u64 = 0;
    // A merge must always make progress. Treat the target row count as a soft
    // limit until enough parts have accumulated; otherwise, for example, four
    // 300k-row parts with a 1M target are split into groups of 3 and 1 and are
    // skipped forever by merge_min_part_count=4.
    let min_part_count = config.merge_min_part_count.max(2);
    for r in parts {
        if r.meta().row_count >= config.merge_max_part_rows {
            // 이미 큰 part는 merge 그룹에 넣지 않음
            continue;
        }

        let next_rows = current_rows.saturating_add(r.meta().row_count);
        if !current.is_empty() && next_rows > config.merge_max_part_rows {
            // The target is soft, but the maximum is a hard output bound.
            // Finalize the current group before adding a part that would make
            // the replacement oversized. merge_once will skip it if it does
            // not contain enough inputs to make progress.
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
        }

        current_rows = current_rows.saturating_add(r.meta().row_count);
        current.push(r.clone());
        if current.len() >= min_part_count && current_rows >= config.merge_target_part_rows {
            groups.push(std::mem::take(&mut current));
            current_rows = 0;
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

fn read_all_rows(readers: &[Arc<PartReader>]) -> Result<Vec<part::Row>, String> {
    let mut rows: Vec<part::Row> = Vec::new();
    for reader in readers {
        let results = reader.query_all()?;
        for sr in results {
            let labels: Labels = sr.labels;
            for entry in sr.entries {
                rows.push(part::Row {
                    timestamp_ns: entry.timestamp_ns,
                    labels: labels.clone(),
                    line: entry.line,
                    structured_metadata: entry.structured_metadata,
                });
            }
        }
    }
    rows.sort_by_key(|r| r.timestamp_ns);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::memtable::Labels;
    use crate::part::{self};
    use crate::part_registry::PartRegistry;

    fn tmp_dir(label: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-merge-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn make_rows(n: usize, start_ts: i64, suffix: &str) -> Vec<part::Row> {
        let mut labels: Labels = std::collections::BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        (0..n)
            .map(|i| part::Row {
                timestamp_ns: start_ts + i as i64,
                labels: labels.clone(),
                line: format!("{}-line-{}", suffix, i),
                structured_metadata: vec![],
            })
            .collect()
    }

    #[tokio::test]
    async fn merge_consolidates_small_parts() {
        let dir = tmp_dir("consolidate");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        for batch in 0..5u64 {
            let rows = make_rows(10, (batch * 1000) as i64, &format!("b{}", batch));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }
        assert_eq!(registry.part_count(), 5);

        merge_once(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);

        let results = registry
            .query(&[], &[], i64::MIN, i64::MAX, 1000, true)
            .expect("part query");
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 50);
    }

    #[tokio::test]
    async fn merge_waits_for_active_query_lifecycle_guard_before_deleting_inputs() {
        let dir = tmp_dir("query_lifecycle");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        for batch in 0..2u64 {
            let rows = make_rows(10, (batch * 1000) as i64, &format!("b{batch}"));
            registry
                .register(part::flush_rows(rows, &parts_root, config.row_group_size).unwrap())
                .unwrap();
        }
        let old_dirs: Vec<_> = registry
            .snapshot()
            .iter()
            .map(|reader| reader.part().dir.clone())
            .collect();

        let query_guard = registry.operation_lock().read_owned().await;
        let merge_registry = registry.clone();
        let merge_config = config.clone();
        let mut merge =
            tokio::spawn(async move { merge_once(&merge_registry, None, &merge_config).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut merge)
                .await
                .is_err(),
            "merge deleted inputs while a query lifecycle guard was active"
        );
        assert!(old_dirs.iter().all(|dir| dir.exists()));

        drop(query_guard);
        merge.await.unwrap().unwrap();
        assert!(old_dirs.iter().all(|dir| !dir.exists()));
        assert_eq!(registry.part_count(), 1);
    }

    #[tokio::test]
    async fn merge_skips_when_too_few_parts() {
        let dir = tmp_dir("skip");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        for batch in 0..2u64 {
            let rows = make_rows(10, (batch * 1000) as i64, &format!("b{}", batch));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }
        assert_eq!(registry.part_count(), 2);

        merge_once(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 2);
    }

    #[test]
    fn malformed_merge_tombstone_is_rejected_before_old_parts_are_touched() {
        let dir = tmp_dir("malformed_tombstone");
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = PartRegistry::new();
        let mut old_dirs = Vec::new();

        for batch in 0..2 {
            let parts = part::flush_rows(
                make_rows(1, 1_700_000_000_000_000_000 + batch, &format!("old{batch}")),
                &parts_root,
                100,
            )
            .unwrap();
            old_dirs.extend(parts.iter().map(|part| part.dir.clone()));
            registry.register(parts).unwrap();
        }

        let merged_rows = read_all_rows(&registry.snapshot()).unwrap();
        let replacements =
            part::flush_rows_with_merge_tombstone(merged_rows, &parts_root, 100, &old_dirs)
                .unwrap();
        let replacement_dirs: Vec<_> = replacements.iter().map(|part| part.dir.clone()).collect();
        std::fs::write(
            replacement_dirs[0].join(part::MERGE_TOMBSTONE_FILE),
            b"{not-json",
        )
        .unwrap();

        let error = verify_merge_tombstones(&replacement_dirs, &parts_root, &old_dirs)
            .expect_err("an unreadable tombstone must fence the merge");
        assert!(error.contains("failed to read"));

        // This is the state the merge error path preserves: the unregistered
        // replacement can be discarded, while every registered input remains.
        part::remove_part_dirs(&replacement_dirs).unwrap();
        assert_eq!(registry.part_count(), 2);
        assert!(old_dirs.iter().all(|dir| dir.exists()));
        let rows = read_all_rows(&registry.snapshot()).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn merge_target_is_soft_until_minimum_part_count_is_reached() {
        let dir = tmp_dir("soft_target");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10_000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        // Each group of three is below the minimum count, while four parts
        // exceed the target. The target must yield so the merge can progress.
        for batch in 0..4u64 {
            let rows = make_rows(300, (batch * 1000) as i64, &format!("b{batch}"));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }
        assert_eq!(registry.part_count(), 4);

        merge_once(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);
        let results = registry
            .query(&[], &[], i64::MIN, i64::MAX, 2000, true)
            .expect("part query");
        assert_eq!(
            results
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            1200
        );
    }

    #[tokio::test]
    async fn merge_never_produces_a_part_above_the_maximum_row_count() {
        let dir = tmp_dir("hard_max");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1_000,
            merge_max_part_rows: 100,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        for batch in 0..4u64 {
            let rows = make_rows(30, (batch * 1_000) as i64, &format!("b{batch}"));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }

        merge_once(&registry, None, &config).await.unwrap();

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(
            snapshot
                .iter()
                .all(|part| part.meta().row_count <= config.merge_max_part_rows)
        );
        assert_eq!(
            read_all_rows(&snapshot).unwrap().len(),
            120,
            "the hard maximum must not drop rows"
        );
    }

    #[tokio::test]
    async fn merge_preserves_query_and_pruning() {
        let dir = tmp_dir("prune_after");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        for batch in 0..3u64 {
            let rows = make_rows(20, (batch * 100_000) as i64, &format!("batch{}", batch));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }
        merge_once(&registry, None, &config).await.unwrap();
        assert_eq!(registry.part_count(), 1);

        // bloom prune after merge
        let f = crate::logql::LineFilter::Contains("zzzzz".to_string());
        let r = registry
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);

        // label prune after merge
        let m = crate::logql::LabelMatcher::new(
            "app".to_string(),
            crate::logql::MatcherOp::Eq,
            "missing".to_string(),
        )
        .unwrap();
        let r = registry
            .query(&[m], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);

        // label hit
        let m = crate::logql::LabelMatcher::new(
            "app".to_string(),
            crate::logql::MatcherOp::Eq,
            "test".to_string(),
        )
        .unwrap();
        let r = registry
            .query(&[m], &[], i64::MIN, i64::MAX, 1000, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 60);
    }

    #[tokio::test]
    async fn merge_preserves_i64_max_timestamp() {
        let dir = tmp_dir("max_timestamp");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        for timestamp_ns in [i64::MAX - 1, i64::MAX] {
            let parts = part::flush_rows(
                make_rows(1, timestamp_ns, &timestamp_ns.to_string()),
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(parts).unwrap();
        }

        merge_once(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);
        let rows = read_all_rows(&registry.snapshot()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.last().map(|row| row.timestamp_ns), Some(i64::MAX));
    }
}
