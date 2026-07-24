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

