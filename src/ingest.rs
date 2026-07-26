use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use prost::Message;

use crate::AppState;
use crate::backpressure::IngestError;
use crate::config::Config;
use crate::memtable::LogEntry;
use crate::proto::{self, PushRequest};

pub async fn push(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, IngestError> {
    if state.shutdown.is_draining() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "server is draining for shutdown".to_string(),
        )
            .into());
    }
    // Ahead of the request counter as well as the body work: a refusal is not
    // an ingest the server attempted.
    state.ingest_gate.check()?;
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
    result.map_err(IngestError::from)
}

/// Accepted timestamp band around the server clock, resolved once per request.
/// Timestamps outside it are rejected: a far-past entry lands in a partition
/// retention already swept, and a far-future entry lands in one whose
/// `max_ts_ns` never falls behind the retention cutoff, so it is never expired.
struct TimestampWindow {
    oldest_ns: Option<i64>,
    newest_ns: Option<i64>,
}

impl TimestampWindow {
    fn from_config(config: &Config) -> Self {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        Self {
            oldest_ns: config
                .max_timestamp_age
                .map(|age| now_ns.saturating_sub(duration_to_ns(age))),
            newest_ns: config
                .max_timestamp_skew
                .map(|skew| now_ns.saturating_add(duration_to_ns(skew))),
        }
    }

    fn validate(&self, timestamp_ns: i64) -> Result<(), String> {
        if self.oldest_ns.is_some_and(|oldest| timestamp_ns < oldest) {
            return Err(format!(
                "entry timestamp {timestamp_ns} is older than the accepted window; \
raise LOGGYTRACY_MAX_TIMESTAMP_AGE or disable it with 'off'"
            ));
        }
        if self.newest_ns.is_some_and(|newest| timestamp_ns > newest) {
            return Err(format!(
                "entry timestamp {timestamp_ns} is further in the future than the accepted window; \
check the client clock and timestamp units, or raise LOGGYTRACY_MAX_TIMESTAMP_SKEW"
            ));
        }
        Ok(())
    }
}

fn duration_to_ns(duration: std::time::Duration) -> i64 {
    duration.as_nanos().min(i64::MAX as u128) as i64
}

fn validate_labels(
    labels: &std::collections::BTreeMap<String, String>,
    config: &Config,
) -> Result<(), String> {
    if labels.len() > config.max_label_names_per_stream {
        return Err(format!(
            "stream has {} labels, exceeding the maximum of {}",
            labels.len(),
            config.max_label_names_per_stream
        ));
    }
    for (name, value) in labels {
        if name.len() > config.max_label_name_bytes {
            return Err(format!(
                "label name is {} bytes, exceeding the maximum of {}",
                name.len(),
                config.max_label_name_bytes
            ));
        }
        if value.len() > config.max_label_value_bytes {
            return Err(format!(
                "value of label '{name}' is {} bytes, exceeding the maximum of {}",
                value.len(),
                config.max_label_value_bytes
            ));
        }
    }
    Ok(())
}

async fn push_inner(
    state: &Arc<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    // Resolve the tenant before anything else touches the body: every input
    // limit, and the journal append itself, is attributed to it.
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "JSON push is not supported, use protobuf+snappy".to_string(),
        ));
    }
    let limits = &state.config;
    if body.len() > limits.max_push_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "push body is {} bytes, exceeding the maximum of {}",
                body.len(),
                limits.max_push_bytes
            ),
        ));
    }
    // The snappy header declares the decompressed length and `decompress_vec`
    // allocates it before validating the stream, so an attacker-chosen header
    // would otherwise size the allocation. Check the declared length first.
    let declared = snap::raw::decompress_len(&body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid snappy header: {error}"),
        )
    })?;
    if declared > limits.max_decompressed_push_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "push declares {declared} decompressed bytes, exceeding the maximum of {}",
                limits.max_decompressed_push_bytes
            ),
        ));
    }
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("snappy decompress failed: {}", e),
            )
        })?;

    let push_req = PushRequest::decode(decompressed.as_slice()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("protobuf decode failed: {}", e),
        )
    })?;

    let timestamp_window = TimestampWindow::from_config(limits);
    let mut parsed: Vec<(std::collections::BTreeMap<String, String>, Vec<LogEntry>)> =
        Vec::with_capacity(push_req.streams.len());
    for stream in &push_req.streams {
        let labels = proto::parse_labels(&stream.labels).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid labels '{}': {}", stream.labels, e),
            )
        })?;
        validate_labels(&labels, limits).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
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
                timestamp_window
                    .validate(timestamp_ns)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
                if e.line.len() > limits.max_line_bytes {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "log line is {} bytes, exceeding the maximum of {}",
                            e.line.len(),
                            limits.max_line_bytes
                        ),
                    ));
                }
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
        .append(tenant, decompressed, parsed)
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
    use crate::tenant::test_tenant;
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
        let mut headers = crate::tenant::test_tenant_headers();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());

        let status = push(State(state.clone()), headers, Bytes::from(body))
            .await
            .expect("push");
        assert_eq!(status, StatusCode::NO_CONTENT);

        // push가 204를 반환한 시점에 writer가 이미 insert했는지 확인 (#2 원자성)
        let results = memtable.query(&test_tenant(), &[], &[], i64::MIN, i64::MAX, 100, true);
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
        let mut headers = crate::tenant::test_tenant_headers();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());

        let error = push(State(state), headers, Bytes::from(body))
            .await
            .expect_err("out-of-range timestamp must fail");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(memtable.is_empty());
    }

    fn protobuf_headers() -> HeaderMap {
        let mut headers = crate::tenant::test_tenant_headers();
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());
        headers
    }

    async fn limits_state(config: Config) -> Arc<crate::AppState> {
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        crate::test_support::state(
            config,
            memtable,
            journal,
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
        )
    }

    fn now_secs() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[tokio::test]
    async fn push_rejects_a_snappy_header_declaring_more_than_the_limit() {
        let config = Config {
            data_dir: tmp_data_dir("declared_length"),
            max_decompressed_push_bytes: 1024,
            ..Config::default()
        };
        let state = limits_state(config).await;
        // A four-byte varint header declaring ~256 MiB with no payload: the
        // guard must reject it before `decompress_vec` sizes an allocation.
        let body = vec![0x80, 0x80, 0x80, 0x80, 0x01];

        let error = push(State(state), protobuf_headers(), Bytes::from(body))
            .await
            .expect_err("an oversized declared length must be rejected");

        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            error.message.contains("decompressed bytes"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn push_rejects_a_body_over_the_configured_limit() {
        let config = Config {
            data_dir: tmp_data_dir("body_limit"),
            max_push_bytes: 32,
            max_decompressed_push_bytes: 64,
            ..Config::default()
        };
        let state = limits_state(config).await;
        let body = build_snappy_push("body-limit", &"x".repeat(4096), now_secs());

        let error = push(State(state), protobuf_headers(), Bytes::from(body))
            .await
            .expect_err("an oversized body must be rejected");

        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn push_rejects_a_line_over_the_configured_limit() {
        let config = Config {
            data_dir: tmp_data_dir("line_limit"),
            max_line_bytes: 16,
            ..Config::default()
        };
        let state = limits_state(config).await;
        let body = build_snappy_push("line-limit", &"x".repeat(64), now_secs());

        let error = push(State(state.clone()), protobuf_headers(), Bytes::from(body))
            .await
            .expect_err("an oversized line must be rejected");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(state.memtable.is_empty());
    }

    #[tokio::test]
    async fn push_rejects_too_many_labels() {
        let config = Config {
            data_dir: tmp_data_dir("label_count"),
            max_label_names_per_stream: 2,
            ..Config::default()
        };
        let state = limits_state(config).await;
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{a="1",b="2",c="3"}"#.to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(::prost_types::Timestamp {
                        seconds: now_secs(),
                        nanos: 0,
                    }),
                    line: "too many labels".to_string(),
                    structured_metadata: vec![],
                }],
                hash: 0,
            }],
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let body = snap::raw::Encoder::new().compress_vec(&buf).unwrap();

        let error = push(State(state.clone()), protobuf_headers(), Bytes::from(body))
            .await
            .expect_err("an oversized label set must be rejected");

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("3 labels"), "{}", error.message);
        assert!(state.memtable.is_empty());
    }

    #[tokio::test]
    async fn push_rejects_timestamps_outside_the_accepted_window() {
        let config = Config {
            data_dir: tmp_data_dir("timestamp_window"),
            max_timestamp_age: Some(Duration::from_secs(3600)),
            max_timestamp_skew: Some(Duration::from_secs(3600)),
            ..Config::default()
        };
        let state = limits_state(config).await;

        let ancient = build_snappy_push("skew", "far past", now_secs() - 86_400);
        let ancient_error = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(ancient),
        )
        .await
        .expect_err("a far-past timestamp must be rejected");
        assert_eq!(ancient_error.status, StatusCode::BAD_REQUEST);
        assert!(
            ancient_error.message.contains("older than"),
            "{}",
            ancient_error.message
        );

        // A unit mix-up (seconds sent where nanoseconds were meant) is the
        // realistic source of far-future partitions retention never expires.
        let future = build_snappy_push("skew", "far future", now_secs() + 86_400);
        let future_error = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(future),
        )
        .await
        .expect_err("a far-future timestamp must be rejected");
        assert_eq!(future_error.status, StatusCode::BAD_REQUEST);
        assert!(
            future_error.message.contains("future"),
            "{}",
            future_error.message
        );

        assert!(state.memtable.is_empty());
    }

    #[tokio::test]
    async fn push_accepts_old_timestamps_when_the_window_is_disabled() {
        let config = Config {
            data_dir: tmp_data_dir("timestamp_window_off"),
            max_timestamp_age: None,
            max_timestamp_skew: None,
            ..Config::default()
        };
        let state = limits_state(config).await;
        let body = build_snappy_push("backfill", "historical import", 1_000_000);

        let status = push(State(state.clone()), protobuf_headers(), Bytes::from(body))
            .await
            .expect("a disabled window must accept any in-range timestamp");

        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(!state.memtable.is_empty());
    }

    #[tokio::test]
    async fn push_is_refused_once_the_memtable_is_over_its_limit() {
        let config = Config {
            data_dir: tmp_data_dir("memtable_backpressure"),
            // Below the flush trigger is rejected by `validate`, so both move
            // together: the point is a ceiling one push can cross.
            flush_max_bytes: 1,
            max_memtable_bytes: Some(1),
            ..Config::default()
        };
        let state = limits_state(config).await;

        let accepted = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backpressure", "first line", now_secs())),
        )
        .await
        .expect("the first push is under the limit");
        assert_eq!(accepted, StatusCode::NO_CONTENT);

        let refused = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backpressure", "second line", now_secs())),
        )
        .await
        .expect_err("a full memtable must be refused");

        assert_eq!(refused.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            refused.retry_after.is_some(),
            "429 must tell the client when"
        );
        assert!(
            refused.message.contains("memtable holds"),
            "{}",
            refused.message
        );
        assert_eq!(
            state
                .metrics
                .ingest_throttled
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn push_is_refused_once_the_wal_backlog_is_over_its_limit() {
        let config = Config {
            data_dir: tmp_data_dir("wal_backpressure"),
            max_memtable_bytes: None,
            max_wal_backlog_bytes: Some(1),
            ..Config::default()
        };
        let state = limits_state(config).await;

        push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backlog", "first line", now_secs())),
        )
        .await
        .expect("an empty WAL accepts the first push");

        let refused = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backlog", "second line", now_secs())),
        )
        .await
        .expect_err("an unretired WAL backlog must be refused");
        assert_eq!(refused.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            refused.message.contains("WAL backlog"),
            "{}",
            refused.message
        );

        // Retiring the backlog reopens ingest: backpressure is a moving state,
        // not a latch.
        let checkpoint = state.journal.checkpoint().await.unwrap();
        state.memtable.commit_flush();
        state.journal.set_checkpoint(checkpoint.offset).unwrap();
        push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backlog", "third line", now_secs())),
        )
        .await
        .expect("a retired backlog accepts writes again");
    }

    #[tokio::test]
    async fn disabled_limits_accept_everything() {
        let config = Config {
            data_dir: tmp_data_dir("backpressure_off"),
            max_memtable_bytes: None,
            max_wal_backlog_bytes: None,
            ..Config::default()
        };
        let state = limits_state(config).await;
        for index in 0..8 {
            push(
                State(state.clone()),
                protobuf_headers(),
                Bytes::from(build_snappy_push(
                    "unbounded",
                    &format!("line {index}"),
                    now_secs(),
                )),
            )
            .await
            .expect("disabled limits never refuse");
        }
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
            let mut headers = crate::tenant::test_tenant_headers();
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
                    &test_tenant(),
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
        let (cs, re) = journal::replay(&wal, &ckpt, &memtable2, &test_tenant()).expect("replay");
        assert!(
            memtable2.is_empty(),
            "after full flush, replay should yield no in-flight data"
        );
        let _ = (cs, re);

        let registry = PartRegistry::load_from_disk(&parts_root).unwrap();
        assert!(registry.part_count() >= 1);

        let r = registry
            .query(
                &test_tenant(),
                &[matcher],
                &[],
                i64::MIN,
                i64::MAX,
                100,
                true,
            )
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3, "data must persist across restart");
    }
}
