    use super::*;
    use crate::tenant::test_tenant;
    use crate::memtable::MemTable;
    use crate::proto::{EntryAdapter, StreamAdapter};
    use std::sync::Arc;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-journal-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn make_push_req(streams: &[(&str, Vec<(&str, i64)>)]) -> Vec<u8> {
        let streams: Vec<StreamAdapter> = streams
            .iter()
            .map(|(labels, entries)| StreamAdapter {
                labels: labels.to_string(),
                entries: entries
                    .iter()
                    .map(|(line, ts)| EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: *ts,
                            nanos: 0,
                        }),
                        line: line.to_string(),
                        structured_metadata: vec![],
                    })
                    .collect(),
                hash: 0,
            })
            .collect();
        let req = PushRequest { streams };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        buf
    }

    struct Harness {
        journal: Journal,
        memtable: Arc<MemTable>,
    }

    async fn harness(name: &str) -> Harness {
        let dir = tmp_dir(name);
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Journal::spawn(&config, memtable.clone()).unwrap();
        Harness { journal, memtable }
    }

    async fn push(h: &Harness, raw: Vec<u8>) {
        let req = PushRequest::decode(raw.as_slice()).unwrap();
        let mut streams = Vec::with_capacity(req.streams.len());
        for stream in &req.streams {
            let labels = proto::parse_labels(&stream.labels).unwrap();
            let entries: Vec<LogEntry> = stream
                .entries
                .iter()
                .map(|e| LogEntry {
                    timestamp_ns: e.timestamp_ns().unwrap(),
                    line: e.line.clone(),
                    structured_metadata: e
                        .structured_metadata
                        .iter()
                        .map(|lp| (lp.name.clone(), lp.value.clone()))
                        .collect(),
                })
                .collect();
            streams.push((labels, entries));
        }
        h.journal.append(test_tenant(), raw, streams).await.unwrap();
    }

    #[tokio::test]
    async fn append_and_checkpoint() {
        let h = harness("append_checkpoint").await;
        push(&h, make_push_req(&[("{app=\"a\"}", vec![("hi", 100)])])).await;
        push(&h, make_push_req(&[("{app=\"b\"}", vec![("yo", 200)])])).await;

        let ckpt = h.journal.checkpoint().await.unwrap();
        assert!(ckpt.offset > 0);
        assert_eq!(ckpt.snapshot.len(), 1, "one tenant produced both streams");
        assert_eq!(ckpt.snapshot[&test_tenant()].len(), 2);
        h.journal.set_checkpoint(ckpt.offset).unwrap();

        let (start, end) = replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &MemTable::new(),
            &test_tenant(),
        )
        .unwrap();
        assert_eq!(start, ckpt.offset);
        assert_eq!(end, ckpt.offset);
    }

    #[tokio::test]
    async fn compact_checkpoint_retains_appends_after_snapshot() {
        let h = harness("compact_retains_suffix").await;
        push(
            &h,
            make_push_req(&[("{app=\"flushed\"}", vec![("old", 100)])]),
        )
        .await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        push(
            &h,
            make_push_req(&[("{app=\"inflight\"}", vec![("new", 200)])]),
        )
        .await;
        let before = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();

        assert_eq!(read_checkpoint(h.journal.ckpt_path()).unwrap(), 0);
        let after = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        assert!(after < before);
        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        ).unwrap();
        let results = restored.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true);
        let lines: Vec<_> = results
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn compaction_failure_does_not_fence_journal_writer() {
        let h = harness("compact_retry").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        let compact_tmp = h.journal.wal_path().with_extension("wal.compact.tmp");
        std::fs::create_dir_all(&compact_tmp).unwrap();
        assert!(
            h.journal
                .compact_checkpoint(checkpoint.offset)
                .await
                .is_err()
        );
        std::fs::remove_dir(&compact_tmp).unwrap();

        push(&h, make_push_req(&[("{app=\"new\"}", vec![("new", 200)])])).await;
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();
        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        ).unwrap();
        let lines: Vec<_> = restored
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true)
            .into_iter()
            .flat_map(|stream| stream.entries.into_iter().map(|entry| entry.line))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn compaction_retry_after_rename_failure_keeps_acknowledged_suffix() {
        let h = harness("compact_rename_retry").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        FAIL_AFTER_COMPACTION_RENAME.store(true, Ordering::Release);
        assert!(
            h.journal
                .compact_checkpoint(checkpoint.offset)
                .await
                .is_err()
        );

        // The writer was reopened after the injected post-rename failure;
        // this append must remain in the replacement WAL before retry.
        push(&h, make_push_req(&[("{app=\"new\"}", vec![("new", 200)])])).await;
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();

        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        ).unwrap();
        let lines: Vec<_> = restored
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true)
            .into_iter()
            .flat_map(|stream| stream.entries.into_iter().map(|entry| entry.line))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn replay_rolls_back_uncommitted_compaction_before_rename() {
        let h = harness("compact_replay_rollback").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        let source_len = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        let state_path = h.journal.wal_path().with_file_name(COMPACTION_STATE_FILE);
        let tmp_path = h.journal.wal_path().with_extension("wal.compact.tmp");
        let state = CompactionState {
            phase: 1,
            offset: checkpoint.offset,
            source_len,
            retained_len: 0,
        };
        write_compaction_state(&state_path, &state, 1).unwrap();
        write_checkpoint(h.journal.ckpt_path(), 0).unwrap();
        std::fs::write(&tmp_path, []).unwrap();

        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        ).unwrap();
        assert!(
            restored
                .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 10, true)
                .is_empty()
        );
        assert_eq!(
            read_checkpoint(h.journal.ckpt_path()).unwrap(),
            checkpoint.offset
        );
        assert!(!state_path.exists());
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn health_turns_false_when_writer_stops() {
        let h = harness("writer_health").await;
        let health = h.journal.healthy.clone();

        drop(h.journal);
        tokio::time::timeout(Duration::from_secs(1), async {
            while health.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("journal health did not reflect writer shutdown");
    }

    #[tokio::test]
    async fn replay_restores_unflushed_data() {
        let h = harness("replay_unflushed").await;
        push(
            &h,
            make_push_req(&[("{app=\"a\"}", vec![("line1", 100), ("line2", 200)])]),
        )
        .await;
        let mt = MemTable::new();
        let (start, end) = replay(h.journal.wal_path(), h.journal.ckpt_path(), &mt, &test_tenant()).unwrap();
        assert_eq!(start, 0);
        assert!(end > 0);
        let results = mt.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn replay_truncates_crc_corruption_at_tail() {
        let dir = tmp_dir("replay_crc_corruption");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let data = make_push_req(&[("{app=\"a\"}", vec![("line", 100)])]);
        let mut record = Vec::new();
        record.extend_from_slice(&(data.len() as u32).to_le_bytes());
        record.extend_from_slice(&(crc32fast::hash(&data) ^ 1).to_le_bytes());
        record.extend_from_slice(&data);
        std::fs::write(&wal_path, record).unwrap();

        let (start, end) = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant()).unwrap();

        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn replay_does_not_allocate_declared_length_for_a_partial_tail() {
        let dir = tmp_dir("replay_oversized_partial_tail");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let mut header = Vec::new();
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        std::fs::write(&wal_path, header).unwrap();

        let (start, end) = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant()).unwrap();

        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn replay_rejects_crc_corruption_before_valid_records() {
        let dir = tmp_dir("replay_interior_crc_corruption");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let first = make_push_req(&[("{app=\"a\"}", vec![("bad", 100)])]);
        let second = make_push_req(&[("{app=\"b\"}", vec![("good", 200)])]);
        let mut wal = Vec::new();
        wal.extend_from_slice(&(first.len() as u32).to_le_bytes());
        wal.extend_from_slice(&(crc32fast::hash(&first) ^ 1).to_le_bytes());
        wal.extend_from_slice(&first);
        wal.extend_from_slice(&(second.len() as u32).to_le_bytes());
        wal.extend_from_slice(&crc32fast::hash(&second).to_le_bytes());
        wal.extend_from_slice(&second);
        std::fs::write(&wal_path, wal).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_checkpoint_without_wal() {
        let dir = tmp_dir("checkpoint_without_wal");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        write_checkpoint(&ckpt_path, 128).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_checkpoint_beyond_wal() {
        let dir = tmp_dir("checkpoint_beyond_wal");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        std::fs::write(&wal_path, [0u8; 16]).unwrap();
        write_checkpoint(&ckpt_path, 32).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_malformed_checkpoint() {
        let dir = tmp_dir("malformed_checkpoint");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        std::fs::write(&wal_path, []).unwrap();

        for bytes in [&[1u8, 2, 3][..], &[0u8; 9][..]] {
            std::fs::write(&ckpt_path, bytes).unwrap();
            let error = replay(&wal_path, &ckpt_path, &MemTable::new(), &test_tenant())
                .expect_err("malformed checkpoint must stop recovery");
            assert!(error.contains("exactly 8 bytes"));
        }
    }

    #[test]
    fn spawn_reports_wal_open_failure_synchronously() {
        let dir = tmp_dir("spawn_open_failure");
        std::fs::create_dir_all(dir.join(WAL_FILE)).unwrap();
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };

        let result = Journal::spawn(&config, std::sync::Arc::new(MemTable::new()));

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn checkpoint_clears_memtable_and_persists_offset() {
        let h = harness("ckpt_clear").await;
        push(&h, make_push_req(&[("{app=\"a\"}", vec![("x", 1)])])).await;
        let ckpt = h.journal.checkpoint().await.unwrap();
        // checkpoint는 inner를 비우고 flushing 버퍼로 옮김; unified_query는 여전히 해당 데이터를 본다.
        // flush 완료를 시뮬레이션하기 위해 commit_flush 호출.
        h.memtable.commit_flush();
        h.journal.set_checkpoint(ckpt.offset).unwrap();
        assert_eq!(h.memtable.approximate_size(), 0);

        push(&h, make_push_req(&[("{app=\"b\"}", vec![("y", 2)])])).await;

        let mt = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &mt, &test_tenant()).unwrap();
        let results = mt.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn replay_restores_each_record_under_its_own_tenant() {
        let harness = harness("replay_tenant").await;
        let acme = TenantId::parse("acme").unwrap();
        let globex = TenantId::parse("globex").unwrap();
        harness
            .journal
            .append(
                acme.clone(),
                make_push_req(&[(r#"{app="a"}"#, vec![("acme line", 100)])]),
                vec![],
            )
            .await
            .unwrap();
        harness
            .journal
            .append(
                globex.clone(),
                make_push_req(&[(r#"{app="b"}"#, vec![("globex line", 200)])]),
                vec![],
            )
            .await
            .unwrap();

        let restored = MemTable::new();
        replay(
            harness.journal.wal_path(),
            harness.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        )
        .unwrap();

        let lines = |tenant: &TenantId| -> Vec<String> {
            restored
                .query(tenant, &[], &[], i64::MIN, i64::MAX, 100, true)
                .into_iter()
                .flat_map(|stream| stream.entries)
                .map(|entry| entry.line)
                .collect()
        };
        assert_eq!(lines(&acme), vec!["acme line"]);
        assert_eq!(lines(&globex), vec!["globex line"]);
        assert!(lines(&test_tenant()).is_empty());
    }

    #[test]
    fn replay_attributes_a_pre_tenancy_record_to_the_default_tenant() {
        // A WAL written before tenancy holds bare PushRequest bytes with no
        // framing. Upgrading must recover that data, not reject it.
        let dir = tmp_dir("legacy_record");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let payload = make_push_req(&[(r#"{app="legacy"}"#, vec![("from before tenancy", 100)])]);
        let mut wal = Vec::new();
        wal.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        wal.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        wal.extend_from_slice(&payload);
        std::fs::write(&wal_path, &wal).unwrap();

        let restored = MemTable::new();
        replay(&wal_path, &ckpt_path, &restored, &test_tenant()).unwrap();

        let entries = restored.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        assert_eq!(
            entries
                .into_iter()
                .flat_map(|stream| stream.entries)
                .map(|entry| entry.line)
                .collect::<Vec<_>>(),
            vec!["from before tenancy"]
        );
    }
