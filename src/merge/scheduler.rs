pub async fn merge_loop(
    registry: Arc<PartRegistry>,
    remote_cache: Option<Arc<RemoteCache>>,
    config: Arc<Config>,
    healthy: Arc<AtomicBool>,
    metrics: Arc<RuntimeMetrics>,
) {
    healthy.store(true, Ordering::Release);
    let mut ticker = interval(config.merge_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        match merge_once(&registry, remote_cache.as_deref(), &config).await {
            Ok(()) => {
                metrics.merge_success.fetch_add(1, Ordering::Relaxed);
                healthy.store(true, Ordering::Release)
            }
            Err(e) => {
                metrics.merge_errors.fetch_add(1, Ordering::Relaxed);
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
    let mut groups_processed = 0usize;

    'partitions: for (partition, mut parts) in by_partition {
        parts.sort_by_key(|r| r.meta().row_count);

        // 너무 큰 단일 part는 제외 (이미 충분히 큼)
        // 작은 part들을 그룹지어 합친다. 단순화: 파티션 내에서 merge_min_part_count개 이상이면
        // 가장 작은 것부터 merge_target_part_rows에 도달할 때까지 그룹화.
        let groups = group_for_merge(&parts, config);
        for group in groups {
            if group.len() < config.merge_min_part_count.max(2) {
                continue;
            }
            if groups_processed >= config.merge_max_groups_per_tick {
                break 'partitions;
            }
            groups_processed += 1;
            let old_ids: Vec<String> = group.iter().map(|r| r.meta().id.clone()).collect();
            let old_dirs: Vec<std::path::PathBuf> =
                group.iter().map(|r| r.part().dir.clone()).collect();

            // Keep input directories alive while the potentially expensive
            // read/write preparation runs, but allow queries to proceed. The
            // exclusive lifecycle lock is acquired only for final revalidation
            // and registry replacement below.
            let part_guard = registry.operation_lock().read_owned().await;
            if let Some(cache) = remote_cache {
                let required: std::collections::HashSet<String> = old_ids.iter().cloned().collect();
                let missing = registry.missing_data_ids(&required);
                if !missing.is_empty() {
                    let epoch = cache.remote_operation_epoch();
                    let restore = tokio::time::timeout(
                        config.max_restore_runtime,
                        cache.storage.restore_parts(&cache.parts_root, &missing),
                    )
                    .await;
                    match restore {
                        Ok(Ok(())) => cache.mark_remote_healthy_since(epoch),
                        Ok(Err(error)) => {
                            cache.mark_remote_unhealthy();
                            return Err(error);
                        }
                        Err(_) => {
                            cache.mark_remote_unhealthy();
                            return Err("object store restore timed out".to_string());
                        }
                    }
                }
            }

            let rows_result = tokio::task::spawn_blocking({
                let group = group.clone();
                let max_memory_bytes = config.merge_max_memory_bytes;
                move || read_all_rows_with_limit(&group, max_memory_bytes)
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
            drop(part_guard);

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
                    let part_guard = registry.operation_lock().read_owned().await;
                    let active_ids = registry.part_ids();
                    if old_ids.iter().any(|id| !active_ids.contains(id)) {
                        drop(part_guard);
                        if let Err(cleanup_error) = part::remove_part_dirs(&new_part_dirs) {
                            tracing::warn!(%cleanup_error, "failed to clean merge output after input replacement");
                        }
                        tracing::debug!(partition = %partition, "merge inputs changed while preparing output");
                        continue;
                    }
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
                    drop(part_guard);
                    let _visibility_guard = registry.operation_lock().write_owned().await;
                    let active_ids = registry.part_ids();
                    if old_ids.iter().any(|id| !active_ids.contains(id)) {
                        if let Err(cleanup_error) = part::remove_part_dirs(&new_part_dirs) {
                            tracing::warn!(%cleanup_error, "failed to clean merge output after final input replacement");
                        }
                        continue;
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
