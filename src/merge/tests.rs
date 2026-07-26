    use super::*;
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
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 1000, true)
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
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 2000, true)
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
            .query(&test_tenant(), &[], &[f], i64::MIN, i64::MAX, 100, true)
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
            .query(&test_tenant(), &[m], &[], i64::MIN, i64::MAX, 100, true)
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
            .query(&test_tenant(), &[m], &[], i64::MIN, i64::MAX, 1000, true)
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

        merge_once(&registry, None, &config, &policy, &metrics)
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
            .query(&tenant_id("beta"), &[], &[], i64::MIN, i64::MAX, 10, true)
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
                .query(&tenant_id("alpha"), &[], &[], i64::MIN, i64::MAX, 10, true)
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

        merge_once(&registry, None, &config, &policy, &RuntimeMetrics::new())
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

        merge_once(&registry, None, &config, &policy, &metrics)
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
                .query(&tenant_id("alpha"), &[], &[], i64::MIN, i64::MAX, 10, true)
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
        merge_once(&registry, None, &config, &policy, &metrics)
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

        merge_once(&registry, None, &config, &policy, &RuntimeMetrics::new())
            .await
            .unwrap();

        assert_eq!(registry.part_count(), 1);
        let survivors = registry
            .query(
                &tenant_id("unmentioned"),
                &[],
                &[],
                i64::MIN,
                i64::MAX,
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

        merge_once(&registry, None, &config, &policy, &metrics)
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
            merge_once(&registry, None, &config, &policy, &RuntimeMetrics::new())
                .await
                .is_err()
        );
    }
