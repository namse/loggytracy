    use super::*;
    use crate::memtable::Labels;
    use crate::part::Row;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "loggytracy-object-store-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn row(line: &str) -> Row {
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        Row {
            timestamp_ns: 1_700_000_000_000_000_000,
            labels,
            line: line.to_string(),
            structured_metadata: Vec::new(),
        }
    }

    #[tokio::test]
    async fn flush_transaction_rolls_back_partial_cross_domain_publication() {
        let root = temp_dir("flush-transaction");
        let parts_root = root.join("parts");
        let parts = crate::part::flush_rows(vec![row("transaction")], &parts_root, 16).unwrap();
        let storage = ObjectStorage::in_memory();
        storage.publish(&parts, &[]).await.unwrap();
        let transaction = FlushTransaction {
            offset: 10,
            log_parts: parts.iter().map(ManifestPart::from).collect(),
            trace_parts: Vec::new(),
        };
        write_flush_transaction(&root, &transaction).unwrap();

        storage.reconcile_flush_transaction(&root, 0).await.unwrap();

        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
        assert!(!parts[0].dir.exists());
        assert!(!root.join(FLUSH_TRANSACTION_FILE).exists());
    }

    #[tokio::test]
    async fn committed_flush_transaction_is_only_cleared_after_checkpoint() {
        let root = temp_dir("flush-committed");
        let parts_root = root.join("parts");
        let parts = crate::part::flush_rows(vec![row("committed")], &parts_root, 16).unwrap();
        let storage = ObjectStorage::in_memory();
        storage.publish(&parts, &[]).await.unwrap();
        let transaction = FlushTransaction {
            offset: 10,
            log_parts: parts.iter().map(ManifestPart::from).collect(),
            trace_parts: Vec::new(),
        };
        write_flush_transaction(&root, &transaction).unwrap();

        storage.reconcile_flush_transaction(&root, 10).await.unwrap();

        assert_eq!(storage.load_manifest().await.unwrap().parts.len(), 1);
        assert!(parts[0].dir.exists());
        assert!(!root.join(FLUSH_TRANSACTION_FILE).exists());
    }

    #[test]
    fn object_store_environment_keys_are_normalized_and_explicit_values_win() {
        let options: BTreeMap<_, _> = normalized_object_store_options([
            ("AWS_ACCESS_KEY_ID".to_string(), "aws-key".to_string()),
            ("AWS_REGION".to_string(), "ap-northeast-2".to_string()),
            (
                "OBJECT_STORE_AWS_ACCESS_KEY_ID".to_string(),
                "explicit-key".to_string(),
            ),
            (
                "OBJECT_STORE_ENDPOINT".to_string(),
                "http://minio:9000".to_string(),
            ),
            ("UNRELATED".to_string(), "ignored".to_string()),
        ])
        .into_iter()
        .collect();

        assert_eq!(options.get("aws_access_key_id").unwrap(), "explicit-key");
        assert_eq!(options.get("aws_region").unwrap(), "ap-northeast-2");
        assert_eq!(options.get("endpoint").unwrap(), "http://minio:9000");
        assert!(!options.contains_key("unrelated"));
    }

    #[tokio::test]
    async fn restores_queryable_part_after_cache_is_deleted() {
        let storage = ObjectStorage::in_memory();
        let source = temp_dir("source").join("parts");
        let parts = part::flush_rows(vec![row("from object store")], &source, 100).unwrap();
        storage.publish(&parts, &[]).await.unwrap();

        std::fs::remove_dir_all(&source).unwrap();
        let manifest = storage.restore_catalog(&source).await.unwrap();
        assert!(
            !source
                .join(&manifest.parts[0].partition)
                .join(&manifest.parts[0].id)
                .join(DATA_FILE)
                .exists()
        );
        let ids = manifest.parts.iter().map(|part| part.id.clone()).collect();
        storage.restore_parts(&source, &ids).await.unwrap();
        let registry =
            crate::part_registry::PartRegistry::load_from_manifest(&source, &manifest).unwrap();
        let result = registry
            .query(&[], &[], i64::MIN, i64::MAX, 10, true)
            .unwrap();
        assert_eq!(result[0].entries[0].line, "from object store");
    }

    #[tokio::test]
    async fn manifest_replace_is_atomic_and_increments_generation() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("replace").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        let first = storage.publish(&old, &[]).await.unwrap();
        assert_eq!(first.generation, 1);

        let old_id = old[0].meta.id.clone();
        let mut replacement_row = row("new");
        replacement_row.timestamp_ns += 1;
        let new = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        let second = storage.publish(&new, &[old_id]).await.unwrap();
        assert_eq!(second.generation, 2);
        assert_eq!(second.parts.len(), 1);
        assert_eq!(second.parts[0].id, new[0].meta.id);
    }

    #[tokio::test]
    async fn competing_replacement_cannot_publish_duplicate_rows() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("replacement-conflict").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut first_row = row("first replacement");
        first_row.timestamp_ns += 1;
        let first = part::flush_rows(vec![first_row], &root, 100).unwrap();
        storage
            .publish(&first, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        // Repeating the exact operation is idempotent, but a different output
        // for the already-consumed input generation is a conflict.
        storage
            .publish(&first, std::slice::from_ref(&old_id))
            .await
            .unwrap();
        let mut second_row = row("second replacement");
        second_row.timestamp_ns += 2;
        let second = part::flush_rows(vec![second_row], &root, 100).unwrap();
        let error = storage
            .publish(&second, std::slice::from_ref(&old_id))
            .await
            .unwrap_err();
        assert!(error.contains("manifest replacement conflict"));

        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, first[0].meta.id);
    }

    #[tokio::test]
    async fn reconciliation_publishes_local_only_parts_without_resurrecting_stale_parts() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut replacement_row = row("replacement");
        replacement_row.timestamp_ns += 1;
        let replacement = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        storage
            .publish(&replacement, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        let mut local_row = row("written while object store was disabled");
        local_row.timestamp_ns += 2;
        let local_only = part::flush_rows(vec![local_row], &root, 100).unwrap();
        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();

        let ids: HashSet<_> = reconciled
            .parts
            .iter()
            .map(|part| part.id.as_str())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(replacement[0].meta.id.as_str()));
        assert!(ids.contains(local_only[0].meta.id.as_str()));
        assert!(!ids.contains(old_id.as_str()));
    }

    #[tokio::test]
    async fn reconciliation_finishes_an_interrupted_merge_replacement() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-uncommitted-merge").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_dirs: Vec<_> = old.iter().map(|part| part.dir.clone()).collect();

        let mut replacement_row = row("uncommitted replacement");
        replacement_row.timestamp_ns += 1;
        let replacement =
            part::flush_rows_with_merge_tombstone(vec![replacement_row], &root, 100, &old_dirs)
                .unwrap();
        assert!(replacement[0].dir.join(part::MERGE_TOMBSTONE_FILE).exists());

        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(reconciled.parts.len(), 1);
        assert_eq!(reconciled.parts[0].id, replacement[0].meta.id);
        assert!(!old[0].dir.exists());
        assert!(!replacement[0].dir.join(part::MERGE_TOMBSTONE_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_a_local_only_merge_into_an_empty_manifest() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-local-only-merge").join("parts");
        let old = part::flush_rows(vec![row("old local")], &root, 100).unwrap();
        let mut replacement_row = row("merged while local only");
        replacement_row.timestamp_ns += 1;
        let replacement = part::flush_rows_with_merge_tombstone(
            vec![replacement_row],
            &root,
            100,
            &[old[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, replacement[0].meta.id);
        assert!(!old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_an_unbalanced_local_only_merge_tree() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-unbalanced-local-merge").join("parts");
        let first = part::flush_rows(vec![row("first")], &root, 100).unwrap();
        let second = part::flush_rows(vec![row("second")], &root, 100).unwrap();
        let third = part::flush_rows(vec![row("third")], &root, 100).unwrap();
        let middle = part::flush_rows_with_merge_tombstone(
            vec![row("middle")],
            &root,
            100,
            &[first[0].dir.clone(), second[0].dir.clone()],
        )
        .unwrap();
        let newest = part::flush_rows_with_merge_tombstone(
            vec![row("newest")],
            &root,
            100,
            &[middle[0].dir.clone(), third[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, newest[0].meta.id);
        assert!(!first[0].dir.exists());
        assert!(!second[0].dir.exists());
        assert!(!third[0].dir.exists());
        assert!(!middle[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_independent_local_only_merge_trees() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-independent-local-merges").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();

        let first = part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone()],
        )
        .unwrap();
        let second = part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[second_old[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        let active: HashSet<_> = manifest.parts.iter().map(|part| part.id.as_str()).collect();
        assert_eq!(
            manifest.generation, 1,
            "all roots must be seeded by one CAS"
        );
        assert_eq!(active.len(), 2);
        assert!(active.contains(first[0].meta.id.as_str()));
        assert!(active.contains(second[0].meta.id.as_str()));
        assert!(!first_old[0].dir.exists());
        assert!(!second_old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_after_atomic_local_only_root_seed() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-after-root-seed").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();
        let first = part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone()],
        )
        .unwrap();
        let second = part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[second_old[0].dir.clone()],
        )
        .unwrap();

        // State after the atomic root manifest CAS but before local tombstone
        // cleanup, equivalent to a crash at that reconciliation boundary.
        let seeded = storage
            .publish(&[first[0].clone(), second[0].clone()], &[])
            .await
            .unwrap();
        assert_eq!(seeded.generation, 1);
        assert!(first_old[0].dir.exists());
        assert!(second_old[0].dir.exists());

        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();
        let active: HashSet<_> = reconciled
            .parts
            .iter()
            .map(|part| part.id.as_str())
            .collect();
        assert_eq!(active.len(), 2);
        assert!(active.contains(first[0].meta.id.as_str()));
        assert!(active.contains(second[0].meta.id.as_str()));
        assert!(!first_old[0].dir.exists());
        assert!(!second_old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_rejects_overlapping_local_only_merge_roots_before_publish() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-overlapping-local-merges").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let shared_old = part::flush_rows(vec![row("shared old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();

        part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone(), shared_old[0].dir.clone()],
        )
        .unwrap();
        part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[shared_old[0].dir.clone(), second_old[0].dir.clone()],
        )
        .unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("overlapping local-only merge roots"));
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[tokio::test]
    async fn reconciliation_rejects_a_stale_competing_merge_replacement() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-competing-merge").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut winner_row = row("remote winner");
        winner_row.timestamp_ns += 1;
        let winner = part::flush_rows(vec![winner_row], &root, 100).unwrap();
        storage
            .publish(&winner, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        let mut loser_row = row("stale local loser");
        loser_row.timestamp_ns += 2;
        let old_dirs = vec![old[0].dir.clone()];
        let loser =
            part::flush_rows_with_merge_tombstone(vec![loser_row], &root, 100, &old_dirs).unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("local merge replacement conflict"));

        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, winner[0].meta.id);
        assert!(loser[0].dir.exists(), "ambiguous local data was deleted");
    }

    #[tokio::test]
    async fn reconciliation_accepts_an_already_active_merge_chain_descendant() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-active-descendant").join("parts");
        let oldest = part::flush_rows(vec![row("oldest")], &root, 100).unwrap();
        storage.publish(&oldest, &[]).await.unwrap();

        let mut middle_row = row("middle");
        middle_row.timestamp_ns += 1;
        let middle = part::flush_rows_with_merge_tombstone(
            vec![middle_row],
            &root,
            100,
            &[oldest[0].dir.clone()],
        )
        .unwrap();
        storage
            .publish(&middle, &[oldest[0].meta.id.clone()])
            .await
            .unwrap();

        let mut newest_row = row("newest");
        newest_row.timestamp_ns += 2;
        let newest = part::flush_rows_with_merge_tombstone(
            vec![newest_row],
            &root,
            100,
            &[middle[0].dir.clone()],
        )
        .unwrap();
        storage
            .publish(&newest, &[middle[0].meta.id.clone()])
            .await
            .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, newest[0].meta.id);
        assert!(!oldest[0].dir.exists());
        assert!(!middle[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_a_partial_local_part_upload() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-partial-upload").join("parts");
        let local = part::flush_rows(vec![row("partial upload")], &root, 100).unwrap();
        let descriptor = ManifestPart::from(&local[0]);
        let data = tokio::fs::read(local[0].data_path()).await.unwrap();
        storage
            .store
            .put_opts(
                &storage.part_path(&descriptor, DATA_FILE),
                data.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();

        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, local[0].meta.id);
        assert!(!local[0].dir.join(UPLOAD_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_a_fully_uploaded_uncommitted_part() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-complete-upload").join("parts");
        let local = part::flush_rows(vec![row("uploaded before crash")], &root, 100).unwrap();

        // State after upload_part completed but before the manifest CAS.
        write_upload_marker(&local[0]).unwrap();
        storage.upload_part(&local[0]).await.unwrap();
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, local[0].meta.id);
        assert!(!local[0].dir.join(UPLOAD_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_repairs_corrupt_active_catalog_before_local_scan() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-corrupt-catalog").join("parts");
        let local = part::flush_rows(vec![row("repair catalog")], &root, 100).unwrap();
        storage.publish(&local, &[]).await.unwrap();
        std::fs::write(local[0].meta_path(), b"not json").unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        let registry =
            crate::part_registry::PartRegistry::load_from_manifest(&root, &manifest).unwrap();

        assert_eq!(registry.part_count(), 1);
    }

    #[tokio::test]
    async fn publish_rejects_different_bytes_under_an_existing_part_key() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("immutable-collision").join("parts");
        let local = part::flush_rows(vec![row("original")], &root, 100).unwrap();
        storage.publish(&local, &[]).await.unwrap();
        let descriptor = ManifestPart::from(&local[0]);
        storage
            .store
            .put_opts(
                &storage.part_path(&descriptor, DATA_FILE),
                bytes::Bytes::from_static(b"different").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let error = storage.publish(&local, &[]).await.unwrap_err();

        assert!(error.contains("immutable object collision"));
        assert!(
            local[0].dir.join(UPLOAD_MARKER_FILE).exists(),
            "publication intent must be durable before object upload"
        );
    }

    #[tokio::test]
    async fn file_backend_can_update_an_existing_manifest() {
        let remote = temp_dir("file-backend");
        let url = url::Url::from_directory_path(&remote).unwrap();
        let storage = ObjectStorage::from_url(url.as_str()).unwrap();
        let root = temp_dir("file-backend-parts").join("parts");

        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        assert_eq!(storage.publish(&old, &[]).await.unwrap().generation, 1);

        let old_id = old[0].meta.id.clone();
        let mut replacement_row = row("new");
        replacement_row.timestamp_ns += 1;
        let new = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        let manifest = storage.publish(&new, &[old_id]).await.unwrap();

        assert_eq!(manifest.generation, 2);
        assert_eq!(storage.load_manifest().await.unwrap().generation, 2);
        assert_eq!(manifest.parts[0].id, new[0].meta.id);
    }

    #[tokio::test]
    async fn invalid_part_is_rejected_before_manifest_update() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("invalid-publish").join("parts");
        let parts = part::flush_rows(vec![row("corrupt me")], &root, 100).unwrap();
        std::fs::write(parts[0].data_path(), b"not parquet").unwrap();

        let error = storage.publish(&parts, &[]).await.unwrap_err();

        assert!(error.contains("refusing to publish invalid part"));
        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.generation, 0);
        assert!(manifest.parts.is_empty());
    }

    #[test]
    fn eviction_respects_cache_limit() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("evict").join("parts");
        let parts = part::flush_rows(vec![row("evict me")], &root, 100).unwrap();
        let eligible = [parts[0].meta.id.clone()].into_iter().collect();
        assert_eq!(storage.evict_cache(&root, 0, &eligible).unwrap(), 0);
        assert!(!parts[0].data_path().exists());
        assert!(parts[0].meta_path().exists());
        assert!(parts[0].bloom_path().exists());
    }

    #[test]
    fn eviction_preserves_parts_absent_from_the_registry() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("evict-stale").join("parts");
        let active = part::flush_rows(vec![row("active")], &root, 100).unwrap();
        let mut stale_row = row("stale");
        stale_row.timestamp_ns += 1;
        let stale = part::flush_rows(vec![stale_row], &root, 100).unwrap();
        let active_bytes = std::fs::metadata(active[0].data_path()).unwrap().len();
        let eligible = [active[0].meta.id.clone()].into_iter().collect();

        assert_eq!(
            storage.evict_cache(&root, u64::MAX, &eligible).unwrap(),
            active_bytes
        );
        assert!(active[0].dir.exists());
        assert!(stale[0].dir.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cache_restore_and_eviction_reject_symlinked_partitions() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let source = temp_dir("symlink-source").join("parts");
        let parts = part::flush_rows(vec![row("remote")], &source, 100).unwrap();
        let manifest = storage.publish(&parts, &[]).await.unwrap();
        let descriptor = &manifest.parts[0];

        let cache = temp_dir("symlink-cache").join("parts");
        std::fs::create_dir_all(&cache).unwrap();
        let outside = temp_dir("symlink-outside");
        let outside_part = outside.join(&descriptor.id);
        std::fs::create_dir_all(&outside_part).unwrap();
        let outside_data = outside_part.join(DATA_FILE);
        std::fs::write(&outside_data, b"must survive").unwrap();
        symlink(&outside, cache.join(&descriptor.partition)).unwrap();

        let restore_error = storage.restore_catalog(&cache).await.unwrap_err();
        assert!(restore_error.contains("symlinked cache directory"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        let eligible = [descriptor.id.clone()].into_iter().collect();
        let eviction_error = storage.evict_cache(&cache, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("symlinked cache partition"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        let empty_storage = ObjectStorage::in_memory();
        let migration_error = empty_storage
            .reconcile_local_cache(&cache)
            .await
            .unwrap_err();
        assert!(migration_error.contains("symlinked cache partition"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        std::fs::remove_file(cache.join(&descriptor.partition)).unwrap();
        let cache_part = cache.join(&descriptor.partition).join(&descriptor.id);
        std::fs::create_dir_all(&cache_part).unwrap();
        symlink(&outside_data, cache_part.join(DATA_FILE)).unwrap();
        let file_error = storage.restore_catalog(&cache).await.unwrap_err();
        assert!(file_error.contains("symlinked cache file"));
        let eviction_error = storage.evict_cache(&cache, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("symlinked cache data file"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconciliation_rejects_a_symlinked_upload_marker() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("symlink-upload-marker").join("parts");
        let parts = part::flush_rows(vec![row("local")], &root, 100).unwrap();
        let outside = temp_dir("symlink-marker-target").join("outside.txt");
        std::fs::write(&outside, b"must survive").unwrap();
        symlink(&outside, parts[0].dir.join(UPLOAD_MARKER_FILE)).unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("symlinked cache file"));
        let publish_error = storage.publish(&parts, &[]).await.unwrap_err();
        assert!(publish_error.contains("symlinked upload marker"));
        assert_eq!(std::fs::read(&outside).unwrap(), b"must survive");
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn migration_and_upload_reject_symlinked_immutable_part_files() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("symlink-immutable-part").join("parts");
        let parts = part::flush_rows(vec![row("local")], &root, 100).unwrap();
        let outside = temp_dir("symlink-immutable-target").join(DATA_FILE);
        std::fs::rename(parts[0].data_path(), &outside).unwrap();
        let expected = std::fs::read(&outside).unwrap();
        symlink(&outside, parts[0].data_path()).unwrap();

        let migration_error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(migration_error.contains("symlinked cache file"));
        assert_eq!(std::fs::read(&outside).unwrap(), expected);
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());

        let upload_error = storage.publish(&parts, &[]).await.unwrap_err();
        assert!(upload_error.contains("unsafe part file"));
        assert_eq!(std::fs::read(&outside).unwrap(), expected);
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restore_reconcile_and_eviction_reject_a_symlinked_cache_root() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let outside_root = temp_dir("symlink-cache-root-outside").join("parts");
        let parts = part::flush_rows(vec![row("outside")], &outside_root, 100).unwrap();
        storage.publish(&parts, &[]).await.unwrap();
        let outside_data = parts[0].data_path();
        let link_parent = temp_dir("symlink-cache-root-link");
        let cache_link = link_parent.join("parts");
        symlink(&outside_root, &cache_link).unwrap();

        let restore_error = storage.restore_catalog(&cache_link).await.unwrap_err();
        assert!(restore_error.contains("unsafe cache root"));
        assert!(outside_data.exists());

        let eligible = [parts[0].meta.id.clone()].into_iter().collect();
        let eviction_error = storage.evict_cache(&cache_link, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("unsafe cache root"));
        assert!(outside_data.exists());

        let empty_storage = ObjectStorage::in_memory();
        let reconcile_error = empty_storage
            .reconcile_local_cache(&cache_link)
            .await
            .unwrap_err();
        assert!(reconcile_error.contains("unsafe cache root"));
        assert!(outside_data.exists());
    }

    #[tokio::test]
    async fn trace_parts_publish_restore_and_eviction_round_trip() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("trace-round-trip").join("traces");
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_000_000_100,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let spans = crate::trace::normalize_request(request).unwrap();
        let parts = crate::trace_part::flush_trace_spans(spans, &root, 100).unwrap();
        let manifest = storage.publish_trace_parts(&parts).await.unwrap();
        assert_eq!(manifest.parts.len(), 1);

        let id = manifest.parts[0].id.clone();
        std::fs::remove_file(parts[0].data_path()).unwrap();
        storage.restore_trace_catalog(&root).await.unwrap();
        assert!(
            storage
                .restore_trace_parts(&root, &std::iter::once(id.clone()).collect())
                .await
                .is_ok()
        );
        assert!(parts[0].data_path().exists());

        storage
            .evict_trace_cache(&root, 0, &std::iter::once(id.clone()).collect())
            .unwrap();
        assert!(!parts[0].data_path().exists());
        storage
            .restore_trace_parts(&root, &std::iter::once(id).collect())
            .await
            .unwrap();
        let reader = crate::trace_part::TracePartReader::open(
            crate::trace_part::load_trace_part(&parts[0].dir).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.query_trace_id(&"01".repeat(16)).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn trace_reconciliation_publishes_local_only_parts() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("trace-local-migration").join("traces");
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![7; 16],
                        span_id: vec![8; 8],
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_000_000_100,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let spans = crate::trace::normalize_request(request).unwrap();
        let parts = crate::trace_part::flush_trace_spans(spans, &root, 100).unwrap();

        let manifest = storage.reconcile_trace_local_cache(&root).await.unwrap();

        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, parts[0].meta.id);
        assert_eq!(storage.load_trace_manifest().await.unwrap().parts.len(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn trace_cache_rejects_symlinked_immutable_files() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let source = temp_dir("trace-symlink-source").join("traces");
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![9; 16],
                        span_id: vec![1; 8],
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let spans = crate::trace::normalize_request(request).unwrap();
        let parts = crate::trace_part::flush_trace_spans(spans, &source, 100).unwrap();
        let manifest = storage.publish_trace_parts(&parts).await.unwrap();
        let root = temp_dir("trace-symlink-cache").join("traces");
        let descriptor = &manifest.parts[0];
        let cache_part = root.join(&descriptor.partition).join(&descriptor.id);
        std::fs::create_dir_all(&cache_part).unwrap();
        let outside = temp_dir("trace-symlink-target").join(TRACE_DATA_FILE);
        std::fs::write(&outside, b"must survive").unwrap();
        symlink(&outside, cache_part.join(TRACE_DATA_FILE)).unwrap();

        let restore_error = storage.restore_trace_catalog(&root).await.unwrap_err();
        assert!(restore_error.contains("symlinked trace cache file"));
        let eligible = std::iter::once(descriptor.id.clone()).collect();
        let eviction_error = storage.evict_trace_cache(&root, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("symlinked trace cache file"));
        assert_eq!(std::fs::read(&outside).unwrap(), b"must survive");
    }

    #[tokio::test]
    async fn seeded_fault_store_publishes_losslessly_after_injected_errors() {
        // Tier B recovery gate: a high write-error rate makes most publish
        // attempts fail, but retrying the same acknowledged parts must
        // eventually land every one in the manifest with no loss and no
        // duplication. A fixed seed makes the injected-error sequence
        // reproducible.
        let root = temp_dir("fault-recovery");
        let parts_root = root.join("parts");
        let parts = crate::part::flush_rows(
            vec![row("first"), row("second"), row("third")],
            &parts_root,
            16,
        )
        .unwrap();
        let config = super::fault_store::FaultConfig::for_test(0, 0, 0, 0.6, 0x00c0_ffee_5eed);
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let store: Arc<dyn ObjectStore> =
            Arc::new(super::fault_store::LatencyFaultStore::new(inner, config));
        let storage = ObjectStorage::from_store(store, "loggytracy-fault");

        let mut attempts = 0;
        loop {
            attempts += 1;
            match storage.publish(&parts, &[]).await {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        error.contains("injected object-store write failure")
                            || error.contains("failed to"),
                        "unexpected error: {error}"
                    );
                    assert!(attempts < 1000, "publish never recovered");
                }
            }
        }
        assert!(attempts > 1, "test seed did not inject any failure");

        let manifest = storage.load_manifest().await.unwrap();
        let mut ids: Vec<_> = manifest.parts.iter().map(|part| part.id.clone()).collect();
        ids.sort();
        let mut expected: Vec<_> = parts.iter().map(|part| part.meta.id.clone()).collect();
        expected.sort();
        assert_eq!(ids, expected, "every acknowledged part must be present exactly once");
    }

    #[test]
    fn manifest_rejects_relative_path_components() {
        for value in [".", "..", "a/b", ""] {
            let manifest = Manifest {
                format_version: MANIFEST_FORMAT_VERSION,
                generation: 1,
                parts: vec![ManifestPart {
                    id: value.to_string(),
                    partition: "2026-01-01".to_string(),
                }],
            };
            assert!(validate_manifest(&manifest).is_err(), "accepted {value:?}");
        }
    }
