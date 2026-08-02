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

    /// The WAL holds the OTLP export as it arrived, so replay must produce
    /// exactly what ingest's own normalization produced before the crash —
    /// promoted labels, structured metadata and all.
    #[tokio::test]
    async fn an_otlp_log_record_replays_by_its_own_kind() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let attr = |key: &str, value: &str| KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        };
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![attr("service.name", "api")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 100,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("hello".to_string())),
                        }),
                        attributes: vec![attr("trace_id", "abc123")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let streams = crate::otlp_log::normalize_request(&request).unwrap();
        let encoded = Prost014Message::encode_to_vec(&request);

        let h = harness("otlp_replay").await;
        h.journal
            .append_otlp_logs(test_tenant(), encoded, streams)
            .await
            .unwrap();

        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        )
        .unwrap();
        let results = restored.query(
            &test_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            10,
            true,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].labels.get("service_name").unwrap(), "api");
        assert_eq!(results[0].entries[0].line, "hello");
        assert_eq!(
            results[0].entries[0].structured_metadata,
            vec![("trace_id".to_string(), "abc123".to_string())]
        );
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
        let results = restored.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true);
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
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
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

        inject_compaction_fault(h.journal.wal_path(), CompactionFault::AfterRename);
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
            .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
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
            offset: checkpoint.offset,
            source_len,
            retained_len: 0,
        };
        write_compaction_state(&state_path, &state).unwrap();
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
                .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                .is_empty()
        );
        assert_eq!(
            read_checkpoint(h.journal.ckpt_path()).unwrap(),
            checkpoint.offset
        );
        assert!(!state_path.exists());
        assert!(!tmp_path.exists());
    }

    /// Records the caller just flushed, so the next compaction offset is a
    /// fresh one in whatever coordinate system the WAL is currently in.
    async fn flush_round(harness: &Harness, label: &str, line: &str) -> u64 {
        push(
            harness,
            make_push_req(&[(&format!("{{app=\"{label}\"}}"), vec![(line, 100)])]),
        )
        .await;
        let checkpoint = harness.journal.checkpoint().await.unwrap();
        harness.memtable.commit_flush();
        checkpoint.offset
    }

    /// Compaction resets the checkpoint to zero, so every offset after the
    /// first one lives in a new coordinate system. Comparing them across that
    /// reset used to wedge the flush loop forever whenever a batch was smaller
    /// than its predecessor, which is most batches.
    #[tokio::test]
    async fn consecutive_compactions_truncate_whatever_the_batch_sizes_are() {
        let h = harness("consecutive_compactions").await;
        // Shrinking, then equal, then growing: the three ways the next offset
        // can compare against the previous one.
        for (label, line) in [
            ("aaaaaaaaaaaaaaaa", "first"),
            ("b", "second"),
            ("b", "second"),
            ("cccccccccccccccccccccccc", "third"),
        ] {
            let offset = flush_round(&h, label, line).await;
            h.journal.compact_checkpoint(offset).await.unwrap();
            assert_eq!(
                std::fs::metadata(h.journal.wal_path()).unwrap().len(),
                0,
                "compaction left the WAL untruncated for {label}"
            );
            assert_eq!(read_checkpoint(h.journal.ckpt_path()).unwrap(), 0);
        }
        let state_path = h.journal.wal_path().with_file_name(COMPACTION_STATE_FILE);
        assert!(!state_path.exists(), "the intent record outlived compaction");
    }

    /// The removal is the last durable step. A crash there leaves a phase-1
    /// record whose rename already committed. `flush.rs` retries the same
    /// offset, which that record must absorb as "already done" — applying a
    /// pre-reset offset to the replacement WAL would truncate live records.
    /// Only after it is retired can the next offset compact normally.
    #[tokio::test]
    async fn a_failed_state_removal_is_settled_by_the_callers_retry() {
        let h = harness("compact_state_removal_retry").await;
        let first = flush_round(&h, "old", "old").await;
        inject_compaction_fault(h.journal.wal_path(), CompactionFault::BeforeStateRemoval);
        assert!(h.journal.compact_checkpoint(first).await.is_err());

        let state_path = h.journal.wal_path().with_file_name(COMPACTION_STATE_FILE);
        assert!(state_path.exists(), "the injected failure kept the record");

        // The retry `flush.rs` issues: the same offset, now stale.
        h.journal.compact_checkpoint(first).await.unwrap();
        assert!(!state_path.exists());

        // A fresh offset in the post-reset coordinate system still compacts.
        let second = flush_round(&h, "new", "new").await;
        h.journal.compact_checkpoint(second).await.unwrap();
        assert_eq!(std::fs::metadata(h.journal.wal_path()).unwrap().len(), 0);

        let restored = MemTable::new();
        replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &restored,
            &test_tenant(),
        )
        .unwrap();
        assert!(
            restored
                .query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 10, true)
                .is_empty(),
            "both batches were flushed, so replay must yield nothing"
        );
    }

    /// A single connection used to be capped at `1000/max_batch_ms` pushes per
    /// second because the batch loop waited out the full linger even with an
    /// empty channel. Sequential appends must now cost what the writes cost,
    /// not what the timer costs.
    #[tokio::test]
    async fn sequential_appends_do_not_wait_out_a_batch_timer() {
        let h = harness("no_batch_linger").await;
        assert_eq!(
            Config::default().max_batch_ms,
            0,
            "the default must not linger"
        );

        let started = std::time::Instant::now();
        for index in 0..20 {
            push(
                &h,
                make_push_req(&[("{app=\"nolinger\"}", vec![("line", 100 + index)])]),
            )
            .await;
        }
        let elapsed = started.elapsed();

        // 20 sequential round trips. Under the old default (200 ms linger) this
        // was ~4 s; the bound here is loose enough to survive a slow disk and
        // still fail decisively if the timer comes back.
        assert!(
            elapsed < Duration::from_millis(1500),
            "20 sequential appends took {elapsed:?}, which means something is waiting"
        );
    }

    /// The linger still works when asked for: it is an opt-in trade of latency
    /// for fewer fsyncs, not the default.
    #[tokio::test]
    async fn a_configured_linger_still_batches() {
        let dir = tmp_dir("batch_linger");
        let config = Config {
            data_dir: dir,
            max_batch_ms: 80,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());

        // Two appends issued concurrently: the second must join the first's
        // batch rather than waiting for its own.
        let started = std::time::Instant::now();
        let one = {
            let journal = journal.clone();
            tokio::spawn(async move {
                journal
                    .append(
                        test_tenant(),
                        make_push_req(&[("{app=\"a\"}", vec![("first", 100)])]),
                        Vec::new(),
                    )
                    .await
            })
        };
        let two = {
            let journal = journal.clone();
            tokio::spawn(async move {
                journal
                    .append(
                        test_tenant(),
                        make_push_req(&[("{app=\"b\"}", vec![("second", 200)])]),
                        Vec::new(),
                    )
                    .await
            })
        };
        one.await.unwrap().unwrap();
        two.await.unwrap().unwrap();

        assert!(
            started.elapsed() < Duration::from_millis(400),
            "both appends should have shared one linger window"
        );
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
        let results = mt.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true);
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
        // checkpoint clears inner and moves the data to the flushing buffer; unified_query still sees it.
        // Call commit_flush to simulate completed flushing.
        h.memtable.commit_flush();
        h.journal.set_checkpoint(ckpt.offset).unwrap();
        assert_eq!(h.memtable.approximate_size(), 0);

        push(&h, make_push_req(&[("{app=\"b\"}", vec![("y", 2)])])).await;

        let mt = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &mt, &test_tenant()).unwrap();
        let results = mt.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true);
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
                .query(tenant, &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true)
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

        let entries = restored.query(&test_tenant(), &[], &[], crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX), 100, true);
        assert_eq!(
            entries
                .into_iter()
                .flat_map(|stream| stream.entries)
                .map(|entry| entry.line)
                .collect::<Vec<_>>(),
            vec!["from before tenancy"]
        );
    }
