    use super::*;
    use crate::delete_requests::DeleteMasks;
    use crate::tenant::test_tenant;
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
                tenant: test_tenant(),
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

        merge_once_without_retention(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);

        let results = registry
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 1000, true)
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
            tokio::spawn(async move { merge_once_without_retention(&merge_registry, None, &merge_config).await });
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

        merge_once_without_retention(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 2);
    }

    /// The shared-part layout charges per (tenant, part) pair, not per row. A
    /// tenant with almost no data still pays a row group, two blooms and a
    /// `meta.json` segment in every part it appears in, and that pair count is
    /// what these totals exist to make visible.
    ///
    /// They live on the registry rather than being published by a worker,
    /// because the configuration that produces a large part set is the one that
    /// turns merge off — a merge-tick gauge reads zero in exactly the case it
    /// was added to describe. That was not a hypothetical: it is what the first
    /// run measuring this actually reported.
    #[tokio::test]
    async fn layout_totals_count_tenant_part_pairs_and_survive_a_silent_merge() {
        async fn measure(label: &str, tenants: usize) -> (crate::part_registry::LayoutTotals, u64) {
            let dir = tmp_dir(label);
            let config = Config {
                data_dir: dir.clone(),
                // No merging, which is both the interesting case and the one
                // the previous implementation could not report on.
                merge_min_part_count: 100,
                ..Config::default()
            };
            let parts_root = dir.join("parts");
            std::fs::create_dir_all(&parts_root).unwrap();
            let registry = Arc::new(PartRegistry::new());

            let mut labels: Labels = std::collections::BTreeMap::new();
            labels.insert("app".to_string(), "test".to_string());
            for batch in 0..4u64 {
                let rows: Vec<part::Row> = (0..200)
                    .map(|row_index| part::Row {
                        tenant: crate::tenant::TenantId::parse(&format!(
                            "t{:04}",
                            row_index % tenants
                        ))
                        .unwrap(),
                        timestamp_ns: (batch * 1000) as i64 + row_index as i64,
                        labels: labels.clone(),
                        line: format!("batch {batch} row {row_index} of a log line"),
                        structured_metadata: vec![],
                    })
                    .collect();
                let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
                registry.register(parts).unwrap();
            }

            // A merge tick that does nothing must not change the answer.
            merge_once(
                &registry,
                None,
                &config,
                &TenantPolicy::disabled(),
                &DeleteMasks::default(),
                None,
                &RuntimeMetrics::new(),
            )
            .await
            .unwrap();

            let rows: u64 = registry
                .snapshot()
                .iter()
                .map(|reader| reader.meta().row_count)
                .sum();
            assert_eq!(rows, 800, "same rows in both runs");
            (registry.layout_totals(), registry.part_count() as u64)
        }

        let (narrow, narrow_parts) = measure("layout-narrow", 1).await;
        let (wide, wide_parts) = measure("layout-wide", 20).await;

        assert_eq!(narrow_parts, wide_parts, "same part count in both runs");
        assert_eq!(narrow.tenant_segments, 4, "one tenant in each of four parts");
        assert_eq!(wide.tenant_segments, 80, "twenty tenants in each of four parts");
        assert!(
            wide.sidecar_resident_bytes > narrow.sidecar_resident_bytes,
            "a pair carries its own blooms: {} should exceed {}",
            wide.sidecar_resident_bytes,
            narrow.sidecar_resident_bytes
        );
        assert!(
            wide.meta_bytes > narrow.meta_bytes,
            "a pair carries its own metadata segment: {} should exceed {}",
            wide.meta_bytes,
            narrow.meta_bytes
        );
    }

    /// The totals are maintained as the set changes, so a merge that replaces
    /// parts has to take the old pairs out with the inputs. A total that only
    /// ever grows would read as unbounded fragmentation on an engine that was
    /// consolidating correctly.
    #[tokio::test]
    async fn merging_parts_removes_their_pairs_from_the_totals() {
        let dir = tmp_dir("layout-merge");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 10_000,
            merge_max_part_rows: 100_000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());

        let mut labels: Labels = std::collections::BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        for batch in 0..4u64 {
            let rows: Vec<part::Row> = (0..40)
                .map(|row_index| part::Row {
                    tenant: crate::tenant::TenantId::parse(&format!("t{:02}", row_index % 5))
                        .unwrap(),
                    timestamp_ns: (batch * 1000) as i64 + row_index as i64,
                    labels: labels.clone(),
                    line: format!("batch {batch} row {row_index}"),
                    structured_metadata: vec![],
                })
                .collect();
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            registry.register(parts).unwrap();
        }
        let before = registry.layout_totals();
        assert_eq!(before.tenant_segments, 20, "five tenants in each of four parts");

        merge_once_without_retention(&registry, None, &config)
            .await
            .unwrap();

        let after = registry.layout_totals();
        assert_eq!(registry.part_count(), 1);
        assert_eq!(
            after.tenant_segments, 5,
            "one part holding five tenants, not the sum of what came before"
        );
        assert!(after.sidecar_resident_bytes < before.sidecar_resident_bytes);
        assert!(after.meta_bytes < before.meta_bytes);
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

        merge_once_without_retention(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);
        let results = registry
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 2000, true)
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

        merge_once_without_retention(&registry, None, &config).await.unwrap();

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
        merge_once_without_retention(&registry, None, &config).await.unwrap();
        assert_eq!(registry.part_count(), 1);

        // bloom prune after merge
        let f = crate::logql::LineFilter::Contains("zzzzz".to_string());
        let r = registry
            .query(&test_tenant(), &[], &[f], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
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
            .query(&test_tenant(), &[m], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
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
            .query(&test_tenant(), &[m], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 1000, true)
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

        merge_once_without_retention(&registry, None, &config).await.unwrap();

        assert_eq!(registry.part_count(), 1);
        let rows = read_all_rows(&registry.snapshot()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.last().map(|row| row.timestamp_ns), Some(i64::MAX));
    }


    fn tenant_id(raw: &str) -> crate::tenant::TenantId {
        crate::tenant::TenantId::parse(raw).expect("valid tenant id")
    }

    fn tenant_row(owner: &str, timestamp_ns: i64) -> part::Row {
        part::Row {
            tenant: tenant_id(owner),
            timestamp_ns,
            labels: [("app".to_string(), owner.to_string())]
                .into_iter()
                .collect(),
            line: format!("{owner}-{timestamp_ns}"),
            structured_metadata: vec![],
        }
    }

    fn policy_with(entries: &[(&str, crate::tenant_policy::TenantRetention)]) -> TenantPolicy {
        let policy = TenantPolicy::enabled_for_test();
        policy.install_for_test(
            entries
                .iter()
                .map(|(name, retention)| (tenant_id(name), *retention))
                .collect(),
        );
        policy
    }

    /// A part too small for an ordinary merge group still gets rewritten when
    /// enough of it has expired, and the surviving tenant comes through with a
    /// correct tenant index.
    #[tokio::test]
    async fn a_partially_expired_part_is_rewritten_and_the_other_tenant_survives() {
        let dir = tmp_dir("partial-expiry");
        let config = Config {
            data_dir: dir.clone(),
            // Far above the single part below: only the expired share can
            // make this part eligible.
            merge_min_part_count: 4,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![
                tenant_row("alpha", 1_000),
                tenant_row("alpha", 1_001),
                tenant_row("beta", 1_002),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        let original_id = parts[0].meta.id.clone();

        // alpha is two of three rows, so the expired fraction clears the 0.5
        // default threshold.
        let policy = policy_with(&[
            (
                "alpha",
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
            ),
            ("beta", crate::tenant_policy::TenantRetention::Infinite),
        ]);
        let metrics = RuntimeMetrics::new();

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        let reader = registry.snapshot().remove(0);
        assert_ne!(reader.meta().id, original_id, "the part must be replaced");
        assert_eq!(reader.meta().row_count, 1);
        let tenants: Vec<String> = reader
            .meta()
            .tenants
            .iter()
            .map(|segment| segment.tenant.to_string())
            .collect();
        assert_eq!(tenants, vec!["beta".to_string()]);
        assert_eq!(reader.meta().tenants[0].row_count, 1);
        assert_eq!(reader.meta().tenants[0].row_group_start, 0);
        assert_eq!(
            reader.meta().tenants[0].row_group_end,
            reader.meta().row_group_count
        );

        let surviving = registry
            .query(&tenant_id("beta"), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
            .unwrap();
        assert_eq!(
            surviving
                .iter()
                .flat_map(|stream| &stream.entries)
                .map(|entry| entry.line.clone())
                .collect::<Vec<_>>(),
            vec!["beta-1002".to_string()]
        );
        assert!(
            registry
                .query(&tenant_id("alpha"), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                .unwrap()
                .is_empty()
        );

        assert_eq!(
            metrics
                .retention_expired_rows_dropped
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(metrics.retention_parts_rewritten.load(Ordering::Relaxed), 1);
    }

    /// Below the threshold the rows stay on disk — already invisible to
    /// queries — instead of paying for a rewrite.
    #[tokio::test]
    async fn a_barely_expired_part_is_left_alone() {
        let dir = tmp_dir("below-threshold");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![
                tenant_row("alpha", 1_000),
                tenant_row("beta", 1_001),
                tenant_row("beta", 1_002),
                tenant_row("beta", 1_003),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        let original_id = parts[0].meta.id.clone();

        let policy = policy_with(&[
            (
                "alpha",
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
            ),
            ("beta", crate::tenant_policy::TenantRetention::Infinite),
        ]);

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &RuntimeMetrics::new())
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert_eq!(registry.snapshot()[0].meta().id, original_id);
    }

    /// Deleting a tenant is `retention: "0"`, and deletion has to mean
    /// something: the same part the threshold would have left alone is
    /// rewritten when the expired rows belong to a tenant at zero retention.
    #[tokio::test]
    async fn a_tenant_at_zero_retention_ignores_the_rewrite_threshold() {
        let dir = tmp_dir("zero-retention");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        // One expired row in four: exactly the shape the threshold leaves
        // alone in `a_barely_expired_part_is_left_alone`.
        let parts = part::flush_rows(
            vec![
                tenant_row("alpha", 1_000),
                tenant_row("beta", 1_001),
                tenant_row("beta", 1_002),
                tenant_row("beta", 1_003),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        let original_id = parts[0].meta.id.clone();

        let policy = policy_with(&[
            (
                "alpha",
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::ZERO),
            ),
            ("beta", crate::tenant_policy::TenantRetention::Infinite),
        ]);
        let metrics = RuntimeMetrics::new();

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        let reader = registry.snapshot().remove(0);
        assert_ne!(
            reader.meta().id,
            original_id,
            "a deleted tenant's rows are reclaimed regardless of the threshold"
        );
        assert_eq!(reader.meta().row_count, 3);
        assert!(
            registry
                .query(&tenant_id("alpha"), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            metrics
                .retention_expired_rows_dropped
                .load(Ordering::Relaxed),
            1
        );

        // The rows are gone, so the next tick has nothing left to reclaim and
        // must not copy the part onto itself.
        let rewritten_id = reader.meta().id.clone();
        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();
        assert_eq!(registry.snapshot()[0].meta().id, rewritten_id);
    }

    /// An unknown tenant is never dropped, even from a part that merge is
    /// rewriting for another reason.
    #[tokio::test]
    async fn merge_never_drops_rows_for_an_unknown_tenant() {
        let dir = tmp_dir("unknown-tenant-merge");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        for timestamp_ns in [1_000i64, 2_000] {
            let parts = part::flush_rows(
                vec![
                    tenant_row("alpha", timestamp_ns),
                    tenant_row("unmentioned", timestamp_ns),
                ],
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(parts).unwrap();
        }

        let policy = policy_with(&[(
            "alpha",
            crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
        )]);

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &RuntimeMetrics::new())
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        let survivors = registry
            .query(
                &tenant_id("unmentioned"),
                &[],
                &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                10,
                true,
            )
            .unwrap();
        assert_eq!(
            survivors
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            2
        );
    }

    /// Group selection and the read budget have to be in the same unit, or a
    /// group is selected under one limit and then always fails the other. The
    /// recorded size is what makes that true, so it has to equal what a read
    /// actually accumulates — measured on highly compressible rows, where the
    /// compressed file size the old code used is nowhere near it.
    #[test]
    fn a_parts_recorded_size_equals_what_reading_it_materializes() {
        let dir = tmp_dir("materialized-bytes");
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let rows: Vec<part::Row> = (0..512)
            .map(|index| part::Row {
                tenant: test_tenant(),
                timestamp_ns: 1_700_000_000_000_000_000 + index,
                labels: [("app".to_string(), "compressible".to_string())]
                    .into_iter()
                    .collect(),
                line: "a".repeat(512),
                structured_metadata: vec![],
            })
            .collect();
        let expected: u64 = rows.iter().map(part::Row::materialized_bytes).sum();
        let parts = part::flush_rows(rows, &parts_root, 64).unwrap();
        assert_eq!(parts.len(), 1);

        let reader = PartReader::open(parts[0].clone()).unwrap();
        assert_eq!(reader.meta().materialized_bytes, expected);
        let read_back: u64 = reader
            .read_all_rows(None)
            .unwrap()
            .iter()
            .map(part::Row::materialized_bytes)
            .sum();
        assert_eq!(read_back, expected);

        // The compressed body is far smaller, which is exactly why using it as
        // the group-selection measure admitted groups that could never be read.
        let compressed = std::fs::metadata(parts[0].data_path()).unwrap().len();
        assert!(
            compressed < expected / 4,
            "expected the compressed body ({compressed}) to be far below {expected}"
        );
    }

    fn wide_tenant_row(owner: &str, timestamp_ns: i64) -> part::Row {
        part::Row {
            tenant: tenant_id(owner),
            timestamp_ns,
            labels: [("app".to_string(), owner.to_string())]
                .into_iter()
                .collect(),
            line: format!("{owner}-{timestamp_ns}-{}", "x".repeat(1024)),
            structured_metadata: vec![],
        }
    }

    /// Deleting a tenant is `retention: "0"`, and a deletion that a large part
    /// can indefinitely postpone is not a deletion. When the part does not fit
    /// in `merge_max_memory_bytes` as a whole, the rewrite proceeds a row-group
    /// window at a time instead of being skipped forever.
    #[tokio::test]
    async fn a_part_too_large_to_materialize_is_rewritten_in_row_group_windows() {
        let dir = tmp_dir("retention-windowed-rewrite");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            retention_period: None,
            // One row per row group, and a budget that fits a single row but
            // not the whole part.
            row_group_size: 1,
            merge_max_memory_bytes: 2 * 1024,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![
                wide_tenant_row("deleted", 1_000),
                wide_tenant_row("deleted", 1_001),
                wide_tenant_row("kept", 1_002),
                wide_tenant_row("kept", 1_003),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        assert_eq!(registry.snapshot()[0].row_group_count(), 4);

        let policy = policy_with(&[
            (
                "deleted",
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::ZERO),
            ),
            ("kept", crate::tenant_policy::TenantRetention::Infinite),
        ]);
        let metrics = RuntimeMetrics::new();

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();

        assert_eq!(
            metrics.retention_rewrite_skipped.load(Ordering::Relaxed),
            0,
            "the rewrite must happen rather than be skipped"
        );
        assert!(!parts[0].dir.exists(), "the input part must be replaced");

        // Every one of the deleted tenant's rows is gone from the replacement,
        // and the surviving tenant is untouched.
        let survivors = registry.snapshot();
        assert!(!survivors.is_empty());
        for reader in &survivors {
            assert!(
                reader
                    .meta()
                    .tenants
                    .iter()
                    .all(|segment| segment.tenant != tenant_id("deleted")),
                "a replacement part still indexes the deleted tenant"
            );
        }
        let kept_rows: usize = survivors
            .iter()
            .map(|reader| {
                reader
                    .query(
                        &tenant_id("kept"),
                        &[],
                        Default::default(), crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                        100,
                        true,
                    )
                    .unwrap()
                    .iter()
                    .map(|stream| stream.entries.len())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(kept_rows, 2, "the surviving tenant lost rows");
        assert_eq!(
            metrics.retention_expired_rows_dropped.load(Ordering::Relaxed),
            2
        );
    }

    /// A part that does not fit in `merge_max_memory_bytes` fails to read on
    /// every tick. When retention is the only reason it was selected, that has
    /// to stay a counted skip: reporting it would hold `merge_healthy` low
    /// forever — and `/ready` at 503 — over reclamation that was never
    /// required for correctness. Expired rows are already invisible to queries.
    #[tokio::test]
    async fn an_unreadable_retention_only_group_is_skipped_instead_of_failing_the_tick() {
        let dir = tmp_dir("retention-group-too-large");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 4,
            retention_period: None,
            // Below one row, so reading the part always exceeds the budget.
            merge_max_memory_bytes: 1,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![
                tenant_row("alpha", 1_000),
                tenant_row("alpha", 1_001),
                tenant_row("beta", 1_002),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        let policy = policy_with(&[
            (
                "alpha",
                crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
            ),
            ("beta", crate::tenant_policy::TenantRetention::Infinite),
        ]);
        let metrics = RuntimeMetrics::new();

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert_eq!(registry.snapshot()[0].meta().id, parts[0].meta.id);
        assert_eq!(
            metrics.retention_rewrite_skipped.load(Ordering::Relaxed),
            1
        );

        // An ordinary merge group is a different matter: it is work that has
        // to happen, so a read failure there still fails the tick.
        for timestamp_ns in [2_000i64, 2_001, 2_002] {
            let more = part::flush_rows(
                vec![tenant_row("beta", timestamp_ns)],
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(more).unwrap();
        }
        assert!(
            merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &RuntimeMetrics::new())
                .await
                .is_err()
        );
    }

    /// Retention retiring a part while merge is publishing is not a failure of
    /// the store or of merge. The CAS refuses the replacement precisely so two
    /// outputs cannot both survive; nothing was written, and the next tick sees
    /// the new state. Reporting it as a store error took `/ready` to 503 — via
    /// both `merge_healthy` and the object-store health flag — over a race that
    /// cost nothing.
    #[tokio::test]
    async fn inputs_replaced_while_publishing_is_skipped_not_failed() {
        let dir = tmp_dir("publish-inputs-changed");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();

        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let registry = Arc::new(PartRegistry::new());
        let mut ids = Vec::new();
        for batch in 0..2u64 {
            let rows = make_rows(10, (batch * 1000) as i64, &format!("b{batch}"));
            let parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
            storage.publish(&parts, &[]).await.unwrap();
            ids.push(parts[0].meta.id.clone());
            registry.register(parts).unwrap();
        }
        let cache = RemoteCache::new(storage.clone(), parts_root.clone());

        // Retention retires one input from the manifest only. The registry
        // still advertises it, so merge selects both and gets as far as the
        // publish before the CAS notices.
        storage
            .publish(&[], std::slice::from_ref(&ids[1]))
            .await
            .unwrap();

        let metrics = RuntimeMetrics::new();
        merge_once(
            &registry,
            Some(&cache),
            &config,
            &TenantPolicy::disabled(),
            &DeleteMasks::default(),
            None,
            &metrics,
        )
        .await
        .expect("a replaced input is skipped, not reported as a merge failure");
        assert!(
            cache.is_remote_healthy(),
            "the object store answered correctly and must not be marked unhealthy"
        );
        assert_eq!(
            registry.part_count(),
            2,
            "the inputs are left for the next tick to re-evaluate"
        );
        assert_eq!(
            metrics.merge_inputs_changed.load(Ordering::Relaxed),
            1,
            "skipping quietly must still be countable"
        );
    }

    /// The commit point is the successful publish. Past it merge must reconcile
    /// the registry to the manifest rather than abandon its output, so
    /// `replace` has to tolerate an input another writer already retired —
    /// otherwise merge would drop a part the manifest already lists and leave
    /// the registry serving inputs the manifest no longer has.
    #[test]
    fn replace_tolerates_an_input_another_writer_already_retired() {
        let dir = tmp_dir("replace-retired-input");
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();

        let registry = PartRegistry::new();
        let kept = part::flush_rows(make_rows(4, 0, "kept"), &parts_root, 16).unwrap();
        let kept_id = kept[0].meta.id.clone();
        registry.register(kept).unwrap();
        let output = part::flush_rows(make_rows(4, 100, "output"), &parts_root, 16).unwrap();
        let output_id = output[0].meta.id.clone();

        let new_ids = registry
            .replace(&[kept_id.clone(), "retired-by-retention".to_string()], output)
            .expect("an already absent input must not fail the replacement");

        assert_eq!(new_ids, vec![output_id.clone()]);
        assert_eq!(registry.part_ids(), [output_id].into_iter().collect());
        assert!(!registry.part_ids().contains(&kept_id));
    }

    /// `retention_parts_rewritten` answers "how much extra I/O is retention
    /// paying for". A size-driven merge that happens to drop expired rows on the
    /// way is I/O that was going to happen anyway, so only the rows belong in
    /// the count — attributing the parts to retention overstated its cost and
    /// made the number unusable for sizing.
    #[tokio::test]
    async fn an_ordinary_merge_that_drops_expired_rows_is_not_a_retention_rewrite() {
        let dir = tmp_dir("ordinary-merge-drop");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_target_part_rows: 1000,
            merge_max_part_rows: 10000,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        // Two parts in one partition reach merge_min_part_count on their own,
        // so this group exists for size and not for retention. `beta` keeps the
        // output non-empty; `alpha` has expired many times over.
        for timestamp_ns in [1_000i64, 1_001] {
            let parts = part::flush_rows(
                vec![
                    tenant_row("alpha", timestamp_ns),
                    tenant_row("beta", timestamp_ns),
                ],
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(parts).unwrap();
        }
        let policy = policy_with(&[(
            "alpha",
            crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
        )]);
        let metrics = RuntimeMetrics::new();

        merge_once(&registry, None, &config, &policy, &DeleteMasks::default(), None, &metrics)
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        assert_eq!(
            metrics
                .retention_expired_rows_dropped
                .load(Ordering::Relaxed),
            2,
            "the reclaimed rows are still counted"
        );
        assert_eq!(
            metrics.retention_parts_rewritten.load(Ordering::Relaxed),
            0,
            "a merge that would have happened anyway is not a retention rewrite"
        );
    }

    /// Merge debt is the operator's view of pending merge work. A part that only
    /// retention would rewrite is pending merge work, so leaving it out hid
    /// retention backlog entirely: `retention_rewrite_skipped` reports rewrites
    /// that already failed, and nothing reported the ones still queued.
    #[tokio::test]
    async fn merge_debt_counts_a_retention_forced_group() {
        let dir = tmp_dir("debt-retention-group");
        let config = Config {
            data_dir: dir.clone(),
            // Nothing here can form an ordinary group.
            merge_min_part_count: 4,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![
                tenant_row("alpha", 1_000),
                tenant_row("alpha", 1_001),
                tenant_row("beta", 1_002),
            ],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts).unwrap();

        assert_eq!(
            merge_debt_part_count(&registry, &config, None, &DeleteMasks::default()),
            0,
            "with no policy there is no debt: the group is below the minimum"
        );

        let policy = policy_with(&[(
            "alpha",
            crate::tenant_policy::TenantRetention::Finite(std::time::Duration::from_nanos(1)),
        )]);
        let cutoffs = policy.cutoffs_now().unwrap();
        assert_eq!(
            merge_debt_part_count(&registry, &config, Some(&cutoffs), &DeleteMasks::default()),
            1,
            "the part two thirds of which has expired is pending merge work"
        );
    }

    /// Hiding a line is the promise; removing it is the obligation. A rewrite
    /// must actually drop the covered rows, and must leave the rest of the
    /// part — other tenants, other streams, other times — intact.
    #[tokio::test]
    async fn a_rewrite_removes_the_rows_a_deletion_covers() {
        let dir = tmp_dir("delete-rewrite");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        for timestamp_ns in [1_000i64, 2_000] {
            let parts = part::flush_rows(
                vec![
                    tenant_row("alpha", timestamp_ns),
                    tenant_row("beta", timestamp_ns),
                ],
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(parts).unwrap();
        }

        let requests = crate::delete_requests::DeleteRequests::new(None);
        requests
            .submit(&tenant_id("alpha"), r#"{app="alpha"}"#, 0, 1_500, 1_500)
            .await
            .expect("a valid request");

        merge_once(
            &registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            &requests.masks(),
            None,
            &RuntimeMetrics::new(),
        )
        .await
        .unwrap();

        let mut survivors: Vec<String> = registry
            .snapshot()
            .iter()
            .flat_map(|reader| reader.read_all_rows(None).unwrap())
            .map(|row| row.line)
            .collect();
        survivors.sort();
        assert_eq!(
            survivors,
            vec![
                "alpha-2000".to_string(),
                "beta-1000".to_string(),
                "beta-2000".to_string()
            ],
            "only alpha's row inside the window is gone"
        );

        let metas: Vec<part::PartMeta> = registry
            .snapshot()
            .into_iter()
            .map(|reader| reader.meta().clone())
            .collect();
        requests.mark_processed(&metas);
        assert_eq!(
            requests.list(&tenant_id("alpha"))[0].status,
            crate::delete_requests::DeleteStatus::Processed,
            "with no part left that could hold a covered row, the request is applied"
        );
    }

    /// Measured on the two-hour soak: SIGTERM to exit took 118 seconds, and 117
    /// of them were merge groups that *started after* the signal. The loop's
    /// own select only sees the drain between ticks, so a tick holding several
    /// groups ran every one of them out.
    #[tokio::test]
    async fn a_draining_merge_does_not_start_another_group() {
        let dir = tmp_dir("drain-mid-tick");
        let config = Config {
            data_dir: dir.clone(),
            merge_min_part_count: 2,
            merge_max_groups_per_tick: 8,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        for timestamp_ns in 0..4i64 {
            let parts = part::flush_rows(
                vec![tenant_row("alpha", 1_000 + timestamp_ns)],
                &parts_root,
                config.row_group_size,
            )
            .unwrap();
            registry.register(parts).unwrap();
        }
        let before = registry.part_count();
        assert!(before > 1, "there is work for a merge to do");

        let (_sender, draining) = tokio::sync::watch::channel(true);
        merge_once(
            &registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            &DeleteMasks::default(),
            Some(&draining),
            &RuntimeMetrics::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            registry.part_count(),
            before,
            "a tick that begins while draining takes no group at all"
        );

        // And the same tick without the drain does the work, so the guard is
        // what stopped it rather than the group selection.
        let (_sender, running) = tokio::sync::watch::channel(false);
        merge_once(
            &registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            &DeleteMasks::default(),
            Some(&running),
            &RuntimeMetrics::new(),
        )
        .await
        .unwrap();
        assert!(registry.part_count() < before);
    }

    /// A part too large for any ordinary group used to keep the rows somebody
    /// asked to have removed until retention deleted the whole part. The rows
    /// were unreadable the whole time, but "deleted" was describing a mask
    /// rather than a removal.
    #[tokio::test]
    async fn a_deletion_makes_a_part_worth_rewriting_on_its_own() {
        let dir = tmp_dir("delete-selects");
        let config = Config {
            data_dir: dir.clone(),
            // No group will ever form: one part, and the minimum is four.
            merge_min_part_count: 4,
            retention_period: None,
            ..Config::default()
        };
        let parts_root = dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let registry = Arc::new(PartRegistry::new());
        let parts = part::flush_rows(
            vec![tenant_row("alpha", 1_000), tenant_row("beta", 1_001)],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        registry.register(parts.clone()).unwrap();
        let original_id = parts[0].meta.id.clone();

        // Without a request the part stays: nothing makes it eligible.
        merge_once(
            &registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            &DeleteMasks::default(),
            None,
            &RuntimeMetrics::new(),
        )
        .await
        .unwrap();
        assert_eq!(registry.snapshot()[0].meta().id, original_id);

        let requests = crate::delete_requests::DeleteRequests::new(None);
        requests
            .submit(&tenant_id("alpha"), r#"{app="alpha"}"#, 0, 2_000, 2_000)
            .await
            .expect("a valid request");

        merge_once(
            &registry,
            None,
            &config,
            &TenantPolicy::disabled(),
            &requests.masks(),
            None,
            &RuntimeMetrics::new(),
        )
        .await
        .unwrap();

        let survivors: Vec<String> = registry
            .snapshot()
            .iter()
            .flat_map(|reader| reader.read_all_rows(None).unwrap())
            .map(|row| row.line)
            .collect();
        assert_eq!(
            survivors,
            vec!["beta-1001".to_string()],
            "the request alone made the part eligible, and only its rows went"
        );
    }
