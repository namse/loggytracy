use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use prost::Message;

use crate::AppState;
use crate::memtable::LogEntry;
use crate::proto::{self, PushRequest};

pub async fn push(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.shutdown.is_draining() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "server is draining for shutdown".to_string(),
        ));
    }
    state
        .metrics
        .ingest_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let result = push_inner(&state, headers, body).await;
    if result.is_err() {
        state
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    result
}

async fn push_inner(
    state: &Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let decompressed = if content_type.contains("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON push not supported in M0, use protobuf+snappy".to_string(),
        ));
    } else {
        snap::raw::Decoder::new()
            .decompress_vec(&body)
            .map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("snappy decompress failed: {}", e),
                )
            })?
    };

    let push_req = PushRequest::decode(decompressed.as_slice()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("protobuf decode failed: {}", e),
        )
    })?;

    let mut parsed: Vec<(std::collections::BTreeMap<String, String>, Vec<LogEntry>)> =
        Vec::with_capacity(push_req.streams.len());
    for stream in &push_req.streams {
        let labels = proto::parse_labels(&stream.labels).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid labels '{}': {}", stream.labels, e),
            )
        })?;
        let entries: Vec<LogEntry> = stream
            .entries
            .iter()
            .map(|e| {
                let timestamp_ns = e.timestamp_ns().map_err(|error| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("invalid entry timestamp: {error}"),
                    )
                })?;
                Ok(LogEntry {
                    timestamp_ns,
                    line: e.line.clone(),
                    structured_metadata: e
                        .structured_metadata
                        .iter()
                        .map(|lp| (lp.name.clone(), lp.value.clone()))
                        .collect(),
                })
            })
            .collect::<Result<_, (StatusCode, String)>>()?;
        parsed.push((labels, entries));
    }

    state
        .journal
        .append(decompressed, parsed)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("journal write failed: {}", e),
            )
        })?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::journal;
    use crate::memtable::MemTable;
    use crate::part_registry::PartRegistry;
    use crate::proto::{EntryAdapter, StreamAdapter};
    use std::path::PathBuf;
    use std::time::Duration;

    static DIR_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn tmp_data_dir(label: &str) -> PathBuf {
        let c = DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-ingest-{}-{}-{}-{}",
            label,
            std::process::id(),
            c,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn build_snappy_push(label: &str, line: &str, ts_secs: i64) -> Vec<u8> {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: format!(r#"{{app="{}"}}"#, label),
                entries: vec![EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: ts_secs,
                        nanos: 0,
                    }),
                    line: line.to_string(),
                    structured_metadata: vec![],
                }],
                hash: 0,
            }],
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let mut enc = snap::raw::Encoder::new();
        enc.compress_vec(&buf).unwrap()
    }

    #[tokio::test]
    async fn push_then_memtable_has_data_before_ack() {
        let dir = tmp_data_dir("ack_atomic");
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let parts = Arc::new(PartRegistry::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            journal.clone(),
            parts,
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
        );

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let body = build_snappy_push("ack-test", "atomic insert check", now);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());

        let status = push(State(state.clone()), headers, Bytes::from(body))
            .await
            .expect("push");
        assert_eq!(status, StatusCode::NO_CONTENT);

        // push가 204를 반환한 시점에 writer가 이미 insert했는지 확인 (#2 원자성)
        let results = memtable.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);
    }

    #[tokio::test]
    async fn push_rejects_timestamp_outside_i64_nanosecond_range() {
        let dir = tmp_data_dir("timestamp_overflow");
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let state = crate::test_support::state(
            config.clone(),
            memtable.clone(),
            journal,
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
        );
        let body = build_snappy_push("bad-time", "must be rejected", 9_223_372_037);
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());

        let error = push(State(state), headers, Bytes::from(body))
            .await
            .expect_err("out-of-range timestamp must fail");

        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert!(memtable.is_empty());
    }

    #[tokio::test]
    async fn push_flush_loop_persists_through_restart() {
        let dir = tmp_data_dir("full_pipeline");
        let config = Config {
            data_dir: dir.clone(),
            flush_max_interval: Duration::from_millis(50),
            flush_check_interval: Duration::from_millis(20),
            ..Config::default()
        };

        let memtable = Arc::new(MemTable::new());
        let parts_root = config.data_dir.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let parts = Arc::new(PartRegistry::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
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

        let flush_handle = {
            let memtable = memtable.clone();
            let journal = journal.clone();
            let parts = parts.clone();
            let trace_memtable = journal.trace_memtable();
            let trace_parts = trace_parts.clone();
            let config = std::sync::Arc::new(config.clone());
            let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
            tokio::spawn(async move {
                crate::flush::flush_loop(
                    memtable,
                    trace_memtable,
                    journal,
                    parts,
                    trace_parts,
                    None,
                    config,
                    healthy,
                    Arc::new(crate::metrics::RuntimeMetrics::new()),
                    tokio::sync::watch::channel(false).1,
                )
                .await;
            })
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        for i in 0..3u64 {
            let body = build_snappy_push("pipeline-app", &format!("line-{}", i), now + i as i64);
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "application/x-protobuf".parse().unwrap());
            let status = push(State(state.clone()), headers, Bytes::from(body))
                .await
                .expect("push");
            assert_eq!(status, StatusCode::NO_CONTENT);
        }

        // flush_loop가 모든 데이터를 part로 내려보낼 때까지 대기
        let matcher = crate::logql::LabelMatcher::new(
            "app".to_string(),
            crate::logql::MatcherOp::Eq,
            "pipeline-app".to_string(),
        )
        .unwrap();
        let mut flushed_total = 0;
        for _ in 0..300 {
            let r = parts
                .query(
                    std::slice::from_ref(&matcher),
                    &[],
                    i64::MIN,
                    i64::MAX,
                    100,
                    true,
                )
                .expect("part query");
            flushed_total = r.iter().map(|s| s.entries.len()).sum::<usize>();
            // flush가 commit_flush까지 끝난 시점에서 inner와 flushing 모두 비어있어야 한다.
            if flushed_total == 3 && memtable.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            flushed_total, 3,
            "flush did not persist all 3 entries to parts"
        );

        // memtable도 완전히 비워졌는지 확인 (모두 flush됨)
        assert!(
            memtable.is_empty(),
            "memtable should be empty after full flush"
        );

        // 서버 종료 시뮬레이션
        flush_handle.abort();
        drop(state);
        drop(parts);
        drop(journal);
        drop(memtable);

        // 재시작: recovery + parts 로드
        let memtable2 = MemTable::new();
        let wal = dir.join("journal.wal");
        let ckpt = dir.join("journal.ckpt");
        let (cs, re) = journal::replay(&wal, &ckpt, &memtable2).expect("replay");
        assert!(
            memtable2.is_empty(),
            "after full flush, replay should yield no in-flight data"
        );
        let _ = (cs, re);

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert!(registry.part_count() >= 1);

        let r = registry
            .query(&[matcher], &[], i64::MIN, i64::MAX, 100, true)
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3, "data must persist across restart");
    }
}
