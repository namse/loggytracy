    use super::*;
    use crate::tenant::test_tenant;
    use crate::journal::Journal;
    use crate::memtable::{LogEntry, MemTable};
    use crate::part;
    use crate::part_registry::PartRegistry;
    use crate::proto::{self, PushRequest};
    use prost::Message;

    const CRASH_MODE_ENV: &str = "LOGGYTRACY_CRASH_TEST_MODE";
    const CRASH_DIR_ENV: &str = "LOGGYTRACY_CRASH_TEST_DIR";

    fn tmp_data_dir(label: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-e2e-{}-{}-{}",
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

    fn build_push_req() -> Vec<u8> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let streams = vec![crate::proto::StreamAdapter {
            labels: r#"{app="test-app", host="local"}"#.to_string(),
            entries: vec![
                crate::proto::EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now - 60,
                        nanos: 0,
                    }),
                    line: "hello world from loggytracy".to_string(),
                    structured_metadata: vec![],
                },
                crate::proto::EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now - 30,
                        nanos: 0,
                    }),
                    line: "error connecting to database".to_string(),
                    structured_metadata: vec![],
                },
                crate::proto::EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now,
                        nanos: 0,
                    }),
                    line: "third line all good".to_string(),
                    structured_metadata: vec![],
                },
            ],
            hash: 0,
        }];
        let req = PushRequest { streams };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        buf
    }

    async fn ingest_once(journal: &Journal, raw: &[u8]) {
        let req = PushRequest::decode(raw).unwrap();
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
        journal.append(test_tenant(), raw.to_vec(), streams).await.unwrap();
    }

    #[tokio::test]
    async fn e2e_recovery_persists_flushed_data() {
        let dir = tmp_data_dir("recovery");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        // Phase 1: ingest + flush
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;

        let ckpt = journal.checkpoint().await.unwrap();
        assert_eq!(ckpt.snapshot.len(), 1);
        let rows = part::rows_from_snapshot(&ckpt.snapshot);
        assert_eq!(rows.len(), 3);
        let parts_root = config.data_dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let new_parts = part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
        assert_eq!(new_parts.len(), 1);
        journal.set_checkpoint(ckpt.offset).unwrap();

        // Simulate shutdown: drop journal & memtable
        drop(journal);
        drop(memtable);

        // Phase 2: restart — empty memtable, replay journal, load parts
        let memtable2 = MemTable::new();
        let wal = config.data_dir.join("journal.wal");
        let ckpt_p = config.data_dir.join("journal.ckpt");
        let (start, end) = journal::replay(&wal, &ckpt_p, &memtable2, &test_tenant()).unwrap();
        assert_eq!(start, ckpt.offset);
        assert_eq!(end, ckpt.offset);
        assert!(memtable2.is_empty(), "no in-flight data after full flush");

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert_eq!(registry.part_count(), 1);

        let results = registry
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3);

        // label index lookup
        let m = crate::logql::LabelMatcher::new(
            "app".to_string(),
            crate::logql::MatcherOp::Eq,
            "test-app".to_string(),
        )
        .unwrap();
        let results = registry
            .query(&test_tenant(), &[m], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3);

        let m_miss = crate::logql::LabelMatcher::new(
            "app".to_string(),
            crate::logql::MatcherOp::Eq,
            "missing".to_string(),
        )
        .unwrap();
        let results = registry
            .query(&test_tenant(), &[m_miss], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn e2e_inflight_data_restored_from_journal() {
        let dir = tmp_data_dir("inflight");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;
        // do NOT checkpoint/flush — crash before flush
        drop(journal);
        drop(memtable);

        let memtable2 = MemTable::new();
        let wal = config.data_dir.join("journal.wal");
        let ckpt_p = config.data_dir.join("journal.ckpt");
        let (start, end) = journal::replay(&wal, &ckpt_p, &memtable2, &test_tenant()).unwrap();
        assert_eq!(start, 0);
        assert!(end > 0);

        let registry = PartRegistry::load_from_disk(&config.data_dir.join("parts")).unwrap();
        assert_eq!(registry.part_count(), 0);

        let results = memtable2.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn e2e_recovery_truncates_crc_corrupt_wal_tail() {
        let dir = tmp_data_dir("crc_tail");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;
        drop(journal);

        let wal_path = config.data_dir.join("journal.wal");
        let mut wal = std::fs::read(&wal_path).unwrap();
        let valid_len = wal.len();
        wal.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        wal.extend_from_slice(&(crc32fast::hash(&raw) ^ 1).to_le_bytes());
        wal.extend_from_slice(&raw);
        std::fs::write(&wal_path, wal).unwrap();

        let recovered = MemTable::new();
        recover(&config, &recovered).unwrap();

        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            valid_len as u64
        );
        let results = recovered.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        assert_eq!(
            results
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            3
        );
    }

    #[tokio::test]
    async fn e2e_partial_flush_then_restart() {
        // Some data is flushed and some is in flight; recover it from both locations on restart.
        let dir = tmp_data_dir("partial");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());

        // first batch — flush
        let raw1 = build_push_req();
        ingest_once(&journal, &raw1).await;
        let ckpt1 = journal.checkpoint().await.unwrap();
        let rows1 = part::rows_from_snapshot(&ckpt1.snapshot);
        let parts_root = config.data_dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        part::flush_rows(rows1, &parts_root, config.row_group_size).unwrap();
        journal.set_checkpoint(ckpt1.offset).unwrap();

        // second batch — in-flight (no flush)
        let raw2 = build_push_req();
        ingest_once(&journal, &raw2).await;

        drop(journal);
        drop(memtable);

        let memtable2 = MemTable::new();
        let wal = config.data_dir.join("journal.wal");
        let ckpt_p = config.data_dir.join("journal.ckpt");
        journal::replay(&wal, &ckpt_p, &memtable2, &test_tenant()).unwrap();

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert_eq!(registry.part_count(), 1);

        // disk: 3, memtable: 3
        let disk_results = registry
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let disk_total: usize = disk_results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(disk_total, 3);

        let mem_results = memtable2.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let mem_total: usize = mem_results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(mem_total, 3);
    }

    #[tokio::test]
    async fn e2e_bloom_pruning_on_disk_part() {
        let dir = tmp_data_dir("bloom_prune");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;

        let ckpt = journal.checkpoint().await.unwrap();
        let rows = part::rows_from_snapshot(&ckpt.snapshot);
        let parts_root = config.data_dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        part::flush_rows(rows, &parts_root, config.row_group_size).unwrap();
        journal.set_checkpoint(ckpt.offset).unwrap();
        drop(journal);
        drop(memtable);

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert_eq!(registry.part_count(), 1);

        // An existing substring.
        let f = crate::logql::LineFilter::Contains("database".to_string());
        let r = registry
            .query(&test_tenant(), &[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);

        // A nonexistent substring — bloom pruning.
        let f = crate::logql::LineFilter::Contains("zzzzzz-no-such-substr".to_string());
        let r = registry
            .query(&test_tenant(), &[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn e2e_double_restart_without_flush_no_loss() {
        // #1 regression: in-flight data must survive two "restart -> restart" cycles without flushing.
        let dir = tmp_data_dir("double_restart");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;
        // Shut down without flushing.
        drop(journal);
        drop(memtable);

        // First restart.
        let memtable1 = MemTable::new();
        recover(&config, &memtable1).expect("recover 1");
        let r1 = memtable1.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let t1: usize = r1.iter().map(|s| s.entries.len()).sum();
        assert_eq!(t1, 3, "first restart should restore in-flight data");
        drop(memtable1);

        // Second restart — the same data must be recovered because checkpoint did not advance.
        let memtable2 = MemTable::new();
        recover(&config, &memtable2).expect("recover 2");
        let r2 = memtable2.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        let t2: usize = r2.iter().map(|s| s.entries.len()).sum();
        assert_eq!(t2, 3, "second restart must NOT lose in-flight data");
    }

    fn run_crash_helper(mode: &str) -> std::path::PathBuf {
        let dir = tmp_data_dir(mode);
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", "tests::crash_recovery_helper", "--nocapture"])
            .env(CRASH_MODE_ENV, mode)
            .env(CRASH_DIR_ENV, &dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn crash helper");
        assert!(!status.success(), "crash helper must terminate abruptly");
        dir
    }

    #[tokio::test]
    async fn crash_recovery_helper() {
        let Ok(mode) = std::env::var(CRASH_MODE_ENV) else {
            return;
        };
        let dir = std::path::PathBuf::from(
            std::env::var_os(CRASH_DIR_ENV).expect("crash test data directory"),
        );
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        match mode.as_str() {
            "wal_after_ack" => {
                let memtable = Arc::new(MemTable::new());
                let journal = Journal::spawn(&config, memtable).unwrap();
                let raw = build_push_req();
                ingest_once(&journal, &raw).await;
            }
            "flush_before_checkpoint" => {
                let memtable = Arc::new(MemTable::new());
                let journal = Journal::spawn(&config, memtable).unwrap();
                let raw = build_push_req();
                ingest_once(&journal, &raw).await;
                let checkpoint = journal.checkpoint().await.unwrap();
                let parts_root = dir.join("parts");
                std::fs::create_dir_all(&parts_root).unwrap();
                part::flush_rows(
                    part::rows_from_snapshot(&checkpoint.snapshot),
                    &parts_root,
                    config.row_group_size,
                )
                .unwrap();
                // Deliberately do not advance the checkpoint. Recovery must
                // preserve both copies according to the documented
                // at-least-once flush-boundary semantics.
            }
            "merge_before_cleanup" => {
                let parts_root = dir.join("parts");
                std::fs::create_dir_all(&parts_root).unwrap();
                let labels: crate::memtable::Labels =
                    [("app".to_string(), "crash-merge".to_string())]
                        .into_iter()
                        .collect();
                let rows = [
                    part::Row {
                        tenant: test_tenant(),
                        timestamp_ns: 1_700_000_000_000_000_000,
                        labels: labels.clone(),
                        line: "old-one".to_string(),
                        structured_metadata: Vec::new(),
                    },
                    part::Row {
                        tenant: test_tenant(),
                        timestamp_ns: 1_700_000_001_000_000_000,
                        labels,
                        line: "old-two".to_string(),
                        structured_metadata: Vec::new(),
                    },
                ];
                let mut old_parts = Vec::new();
                for row in &rows {
                    old_parts.extend(
                        part::flush_rows(vec![row.clone()], &parts_root, config.row_group_size)
                            .unwrap(),
                    );
                }
                let old_dirs: Vec<_> = old_parts.iter().map(|p| p.dir.clone()).collect();
                part::flush_rows_with_merge_tombstone(
                    rows.to_vec(),
                    &parts_root,
                    config.row_group_size,
                    &old_dirs,
                )
                .unwrap();
                // Old parts are intentionally still present. Startup discovery
                // must finish the tombstone transaction.
            }
            other => panic!("unknown crash test mode: {other}"),
        }

        // End the child without running destructors, matching a process crash
        // rather than an orderly test shutdown.
        std::process::abort();
    }

    #[test]
    fn process_crash_after_wal_ack_recovers_unflushed_data() {
        let dir = run_crash_helper("wal_after_ack");
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let recovered = MemTable::new();

        recover(&config, &recovered).expect("recover acknowledged WAL");

        let results = recovered.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
        assert_eq!(
            results
                .iter()
                .map(|stream| stream.entries.len())
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn process_crash_between_part_commit_and_checkpoint_is_at_least_once() {
        let dir = run_crash_helper("flush_before_checkpoint");
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let recovered = MemTable::new();

        recover(&config, &recovered).expect("recover pre-checkpoint crash");
        let registry = PartRegistry::load_from_disk(&config.data_dir.join("parts")).unwrap();

        let memory_rows: usize = recovered
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        let part_rows: usize = registry
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .unwrap()
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        assert_eq!((memory_rows, part_rows), (3, 3));
    }

    #[test]
    fn process_crash_after_merge_commit_finishes_tombstone_recovery() {
        let dir = run_crash_helper("merge_before_cleanup");
        let registry = PartRegistry::load_from_disk(&dir.join("parts")).unwrap();

        assert_eq!(registry.part_count(), 1);
        let rows: usize = registry
            .query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true)
            .unwrap()
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        assert_eq!(rows, 2);
    }

    fn tenant_push_body(tenant: &str, line: &str) -> Vec<u8> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let request = PushRequest {
            streams: vec![crate::proto::StreamAdapter {
                labels: format!(r#"{{app="{tenant}-app"}}"#),
                entries: vec![crate::proto::EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now,
                        nanos: 0,
                    }),
                    line: line.to_string(),
                    structured_metadata: vec![],
                }],
                hash: 0,
            }],
        };
        let mut encoded = Vec::new();
        request.encode(&mut encoded).unwrap();
        snap::raw::Encoder::new().compress_vec(&encoded).unwrap()
    }

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Two tenants pushing through the real router must not be able to read,
    /// enumerate, or count each other's data — before or after a flush turns
    /// the memtable into one shared part.
    #[tokio::test]
    async fn two_tenants_never_see_each_others_logs_over_http() {
        use tower::ServiceExt;

        let data_dir = tmp_data_dir("tenant_isolation");
        let config = crate::config::Config {
            data_dir: data_dir.clone(),
            ..crate::config::Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let parts = Arc::new(PartRegistry::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            journal.clone(),
            parts.clone(),
            trace_parts.clone(),
            None,
        );

        let push = |tenant: &'static str, line: &'static str| {
            let state = state.clone();
            async move {
                let request = axum::http::Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/push")
                    .header("content-type", "application/x-protobuf")
                    .header(crate::tenant::TENANT_HEADER, tenant)
                    .body(axum::body::Body::from(tenant_push_body(tenant, line)))
                    .unwrap();
                let response = crate::build_router(state).oneshot(request).await.unwrap();
                assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
            }
        };
        push("acme", "acme secret").await;
        push("globex", "globex secret").await;

        let get = |tenant: &'static str, uri: &'static str| {
            let state = state.clone();
            async move {
                let request = axum::http::Request::builder()
                    .uri(uri)
                    .header(crate::tenant::TENANT_HEADER, tenant)
                    .body(axum::body::Body::empty())
                    .unwrap();
                let response = crate::build_router(state).oneshot(request).await.unwrap();
                assert_eq!(response.status(), axum::http::StatusCode::OK, "{uri}");
                json_body(response).await
            }
        };

        let query_uri = "/loki/api/v1/query_range?query=%7B%7D&start=0&limit=100&direction=forward";
        for stage in ["memtable", "part"] {
            let acme = get("acme", query_uri).await;
            let globex = get("globex", query_uri).await;
            let acme_lines = acme["data"]["result"].to_string();
            let globex_lines = globex["data"]["result"].to_string();
            assert!(acme_lines.contains("acme secret"), "{stage}: {acme_lines}");
            assert!(
                !acme_lines.contains("globex secret"),
                "{stage}: acme read globex's line: {acme_lines}"
            );
            assert!(
                !globex_lines.contains("acme secret"),
                "{stage}: globex read acme's line: {globex_lines}"
            );

            let values = get("acme", "/loki/api/v1/label/app/values").await;
            assert_eq!(
                values["data"],
                serde_json::json!(["acme-app"]),
                "{stage}: label values leaked another tenant"
            );

            let stats = get("acme", "/loki/api/v1/index/stats").await;
            assert_eq!(stats["data"]["streams"], 1, "{stage}");
            assert_eq!(stats["data"]["entries"], 1, "{stage}");

            let series = get("acme", "/loki/api/v1/series?match%5B%5D=%7B%7D").await;
            assert_eq!(series["data"].as_array().unwrap().len(), 1, "{stage}");

            if stage == "memtable" {
                // Flush both tenants into one shared part and repeat every
                // assertion against the on-disk path.
                let mut pending_checkpoint = None;
                crate::flush::force_flush_pass(crate::flush::ForceFlush {
                    memtable: &memtable,
                    trace_memtable: &journal.trace_memtable(),
                    journal: &journal,
                    registry: &parts,
                    trace_registry: &trace_parts,
                    remote_cache: None,
                    config: &config,
                    pending_checkpoint: &mut pending_checkpoint,
                })
                .await
                .unwrap();
                assert_eq!(parts.part_count(), 1, "both tenants share one part");
                assert!(memtable.is_empty());
            }
        }

        // A tenant that has never written sees nothing at all.
        let stranger = get("initech", query_uri).await;
        assert_eq!(stranger["data"]["result"], serde_json::json!([]));
    }
