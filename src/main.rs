mod bloom;
mod config;
mod flush;
mod ingest;
mod journal;
mod logql;
mod memtable;
mod merge;
mod part;
mod part_registry;
mod proto;
mod query;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::routing::{get, post};

use config::Config;
use journal::Journal;
use memtable::MemTable;
use part::cleanup_tmp;
use part_registry::PartRegistry;

pub struct AppState {
    pub memtable: Arc<MemTable>,
    pub journal: Arc<Journal>,
    pub parts: Arc<PartRegistry>,
    pub flush_healthy: Arc<AtomicBool>,
    pub merge_healthy: Arc<AtomicBool>,
}

fn recover(config: &Config, memtable: &MemTable) -> Result<(), String> {
    let parts_root = config.data_dir.join("parts");
    std::fs::create_dir_all(&parts_root).map_err(|e| e.to_string())?;
    cleanup_tmp(&parts_root);

    let wal_path = config.data_dir.join("journal.wal");
    let ckpt_path = config.data_dir.join("journal.ckpt");

    let (ckpt_start, replay_end) = journal::replay(&wal_path, &ckpt_path, memtable)?;
    tracing::info!(
        checkpoint = ckpt_start,
        replay_end,
        "journal recovery complete"
    );

    if wal_path.exists()
        && replay_end
            < std::fs::metadata(&wal_path)
                .map_err(|e| e.to_string())?
                .len()
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .map_err(|e| e.to_string())?;
        f.set_len(replay_end).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        drop(f);
        tracing::info!(replay_end, "truncated corrupt WAL tail");
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("loggytracy=debug,info")
        .init();

    let config = Arc::new(Config::default());

    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        panic!("failed to create data dir: {}", e);
    }

    let memtable = Arc::new(MemTable::new());

    recover(&config, &memtable).unwrap_or_else(|e| panic!("recovery failed: {e}"));

    let parts = Arc::new(
        PartRegistry::load_from_disk(&config.data_dir.join("parts"))
            .unwrap_or_else(|e| panic!("failed to load parts: {e}")),
    );

    let journal =
        Arc::new(Journal::spawn(&config, memtable.clone()).expect("failed to initialize journal"));

    let flush_healthy = Arc::new(AtomicBool::new(true));
    let merge_healthy = Arc::new(AtomicBool::new(true));

    {
        let memtable = memtable.clone();
        let journal = journal.clone();
        let registry = parts.clone();
        let config = config.clone();
        let task_health = flush_healthy.clone();
        let monitor_health = flush_healthy.clone();
        let handle = tokio::spawn(async move {
            flush::flush_loop(memtable, journal, registry, config, task_health).await;
        });
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::error!("flush task terminated unexpectedly"),
                Err(error) => tracing::error!(%error, "flush task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        });
    }

    {
        let registry = parts.clone();
        let config = config.clone();
        let task_health = merge_healthy.clone();
        let monitor_health = merge_healthy.clone();
        let handle = tokio::spawn(async move {
            merge::merge_loop(registry, config, task_health).await;
        });
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => tracing::error!("merge task terminated unexpectedly"),
                Err(error) => tracing::error!(%error, "merge task failed"),
            }
            monitor_health.store(false, Ordering::Release);
        });
    }

    let state = Arc::new(AppState {
        memtable,
        journal,
        parts,
        flush_healthy,
        merge_healthy,
    });

    let app = Router::new()
        .route("/loki/api/v1/push", post(ingest::push))
        .route("/loki/api/v1/query_range", get(query::query_range))
        .route("/loki/api/v1/query", get(query::query))
        .route("/loki/api/v1/series", get(query::series))
        .route("/loki/api/v1/labels", get(query::labels))
        .route("/loki/api/v1/label/{name}/values", get(query::label_values))
        .route("/loki/api/v1/status/buildinfo", get(query::buildinfo))
        .route("/loki/api/v1/index/stats", get(query::index_stats))
        .route("/ready", get(query::ready))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");
    tracing::info!(addr = %config.listen_addr, "loggytracy listening");
    axum::serve(listener, app).await.expect("server error");
}

#[cfg(test)]
mod tests {
    use super::*;
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
        journal.append(raw.to_vec(), streams).await.unwrap();
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
        let (start, end) = journal::replay(&wal, &ckpt_p, &memtable2).unwrap();
        assert_eq!(start, ckpt.offset);
        assert_eq!(end, ckpt.offset);
        assert!(memtable2.is_empty(), "no in-flight data after full flush");

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert_eq!(registry.part_count(), 1);

        let results = registry
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
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
            .query(&[m], &[], i64::MIN, i64::MAX, 100, true)
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
            .query(&[m_miss], &[], i64::MIN, i64::MAX, 100, true)
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
        let (start, end) = journal::replay(&wal, &ckpt_p, &memtable2).unwrap();
        assert_eq!(start, 0);
        assert!(end > 0);

        let registry = PartRegistry::load_from_disk(&config.data_dir.join("parts")).unwrap();
        assert_eq!(registry.part_count(), 0);

        let results = memtable2.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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
        let results = recovered.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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
        // 일부 flush, 일부 in-flight. 재시작 시 두 곳 모두에서 복원.
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
        journal::replay(&wal, &ckpt_p, &memtable2).unwrap();

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert_eq!(registry.part_count(), 1);

        // disk: 3, memtable: 3
        let disk_results = registry
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let disk_total: usize = disk_results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(disk_total, 3);

        let mem_results = memtable2.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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

        // 존재하는 부분문자열
        let f = crate::logql::LineFilter::Contains("database".to_string());
        let r = registry
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);

        // 존재하지 않는 부분문자열 — bloom 프루닝
        let f = crate::logql::LineFilter::Contains("zzzzzz-no-such-substr".to_string());
        let r = registry
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn e2e_double_restart_without_flush_no_loss() {
        // #1 회귀: flush 없이 "재시작 → 재시작" 두 번 해도 in-flight 데이터가 유지되어야 한다.
        let dir = tmp_data_dir("double_restart");
        let config = Config {
            data_dir: dir.clone(),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let raw = build_push_req();
        ingest_once(&journal, &raw).await;
        // flush 없이 종료
        drop(journal);
        drop(memtable);

        // 첫 재시작
        let memtable1 = MemTable::new();
        recover(&config, &memtable1).expect("recover 1");
        let r1 = memtable1.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let t1: usize = r1.iter().map(|s| s.entries.len()).sum();
        assert_eq!(t1, 3, "first restart should restore in-flight data");
        drop(memtable1);

        // 두 번째 재시작 — checkpoint가 전진하지 않았으므로 동일 데이터가 다시 복원되어야 함
        let memtable2 = MemTable::new();
        recover(&config, &memtable2).expect("recover 2");
        let r2 = memtable2.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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
                        timestamp_ns: 1_700_000_000_000_000_000,
                        labels: labels.clone(),
                        line: "old-one".to_string(),
                        structured_metadata: Vec::new(),
                    },
                    part::Row {
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

        let results = recovered.query(&[], &[], i64::MIN, i64::MAX, 100, true);
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
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        let part_rows: usize = registry
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
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
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
            .unwrap()
            .iter()
            .map(|stream| stream.entries.len())
            .sum();
        assert_eq!(rows, 2);
    }
}
