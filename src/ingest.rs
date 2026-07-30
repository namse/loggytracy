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
    if state.shutdown.is_fenced() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "this instance has been fenced by a newer writer and is shutting down".to_string(),
        )
            .into());
    }
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
    result
}

/// Accepted timestamp band around the server clock, resolved once per request.
/// Timestamps outside it are rejected: a far-past entry lands in a partition
/// retention already swept, and a far-future entry lands in one whose
/// `max_ts_ns` never falls behind the retention cutoff, so it is never expired.
pub struct TimestampWindow {
    oldest_ns: Option<i64>,
    newest_ns: Option<i64>,
}

impl TimestampWindow {
    pub fn from_config(config: &Config, clock: &crate::clock::Clock) -> Self {
        let now_ns = clock.now_ns();
        Self {
            oldest_ns: config
                .max_timestamp_age
                .map(|age| now_ns.saturating_sub(duration_to_ns(age))),
            newest_ns: config
                .max_timestamp_skew
                .map(|skew| now_ns.saturating_add(duration_to_ns(skew))),
        }
    }

    pub fn validate(&self, timestamp_ns: i64) -> Result<(), String> {
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

pub fn validate_labels(
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
) -> Result<StatusCode, IngestError> {
    // Resolve the tenant before anything else touches the body: every input
    // limit, and the journal append itself, is attributed to it.
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    // Charged on the wire size, before the body is decompressed or decoded: a
    // tenant over its rate must not be able to spend this instance's CPU on a
    // request that will not be accepted.
    state.tenant_quota.check(&tenant, body.len() as u64)?;

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let limits = &state.config;
    if body.len() > limits.max_push_bytes {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "push body is {} bytes, exceeding the maximum of {}",
                body.len(),
                limits.max_push_bytes
            ),
        )
            .into());
    }
    // Promtail and several SDKs push JSON. It carries the same streams as the
    // protobuf form, so it decodes into the same shape and then follows the
    // same path — the limits below and the journal encoding are not repeated
    // for it.
    if content_type.contains("application/json") {
        let request: JsonPushRequest = serde_json::from_slice(&body).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid JSON push body: {error}"),
            )
        })?;
        let parsed = request
            .into_streams()
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        return accept_streams(state, tenant, parsed).await;
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
        )
            .into());
    }
    // Dropped before the journal `.await` below: the label is thread-local, so
    // holding it across a suspension point would tag another task's work.
    let ingest_arena = crate::memprof::enter(crate::memprof::Arena::Ingest);
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

    let timestamp_window = TimestampWindow::from_config(limits, &state.clock);
    let mut parsed: ParsedStreams = Vec::with_capacity(push_req.streams.len());
    for stream in &push_req.streams {
        let labels = proto::parse_labels(&stream.labels).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid labels '{}': {}", stream.labels, e),
            )
        })?;
        validate_labels(&labels, limits).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        state
            .tenant_quota
            .admit_stream(&tenant, &labels, &state.parts, &state.memtable)
            .map_err(|error| (error.status, error.message))?;
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

    drop(ingest_arena);
    // The protobuf body is already the journal's encoding, so it is stored as
    // it arrived rather than re-encoded from `parsed`.
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

/// Decoded streams, in the shape every ingest path converges on before the
/// input limits and the journal append.
pub type ParsedStreams = Vec<(crate::memtable::Labels, Vec<LogEntry>)>;

/// The Loki JSON push body.
///
/// A value is `[timestamp_ns, line]` with an optional third element carrying
/// structured metadata, which is how Loki spells it and therefore what the
/// clients that use this form send.
#[derive(serde::Deserialize)]
struct JsonPushRequest {
    streams: Vec<JsonStream>,
}

#[derive(serde::Deserialize)]
struct JsonStream {
    stream: std::collections::BTreeMap<String, String>,
    values: Vec<JsonValue>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum JsonValue {
    WithMetadata(String, String, std::collections::BTreeMap<String, String>),
    Bare(String, String),
}

impl JsonPushRequest {
    fn into_streams(self) -> Result<ParsedStreams, String> {
        let mut streams = Vec::with_capacity(self.streams.len());
        for stream in self.streams {
            // The same names the protobuf path rejects. A reserved label would
            // collide with a part column, and the check has to happen wherever
            // labels enter rather than only where they are parsed from text.
            for name in stream.stream.keys() {
                proto::validate_label_name(name)?;
            }
            let mut entries = Vec::with_capacity(stream.values.len());
            for value in stream.values {
                let (timestamp, line, metadata) = match value {
                    JsonValue::WithMetadata(timestamp, line, metadata) => {
                        (timestamp, line, metadata.into_iter().collect())
                    }
                    JsonValue::Bare(timestamp, line) => (timestamp, line, Vec::new()),
                };
                let timestamp_ns: i64 = timestamp.trim().parse().map_err(|_| {
                    format!("entry timestamp {timestamp:?} is not a nanosecond count")
                })?;
                entries.push(LogEntry {
                    timestamp_ns,
                    line,
                    structured_metadata: metadata,
                });
            }
            streams.push((stream.stream, entries));
        }
        Ok(streams)
    }
}

/// Validate already-decoded streams and append them, whatever encoding they
/// arrived in. The limits are the point: they exist to bound what reaches the
/// journal, so they cannot belong to one body format.
async fn accept_streams(
    state: &Arc<AppState>,
    tenant: crate::tenant::TenantId,
    parsed: ParsedStreams,
) -> Result<StatusCode, IngestError> {
    let limits = &state.config;
    let timestamp_window = TimestampWindow::from_config(limits, &state.clock);
    for (labels, entries) in &parsed {
        validate_labels(labels, limits).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        state
            .tenant_quota
            .admit_stream(&tenant, labels, &state.parts, &state.memtable)?;
        for entry in entries {
            timestamp_window
                .validate(entry.timestamp_ns)
                .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
            if entry.line.len() > limits.max_line_bytes {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "log line is {} bytes, exceeding the maximum of {}",
                        entry.line.len(),
                        limits.max_line_bytes
                    ),
                )
                    .into());
            }
        }
    }
    let encoded = proto::encode_push_request(&parsed);
    state
        .journal
        .append(tenant, encoded, parsed)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("journal write failed: {error}"),
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

        // Verify that the writer has inserted the data when push returns 204 (#2 atomicity).
        let results = memtable.query(
            &test_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            100,
            true,
        );
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

    /// The window's whole content is two boundaries, and every test until now
    /// approached them from far away — "a day old is rejected", "an hour old is
    /// accepted" — which never exercises the edge itself. A unit mix-up lands
    /// *just* outside; an ordinary clock skew lands *just* inside.
    #[tokio::test]
    async fn the_timestamp_window_accepts_its_edge_and_rejects_one_nanosecond_past_it() {
        let now_ns = 1_800_000_000_000_000_000i64;
        let age = Duration::from_secs(3600);
        let config = Config {
            data_dir: tmp_data_dir("timestamp_edge"),
            max_timestamp_age: Some(age),
            max_timestamp_skew: Some(age),
            ..Config::default()
        };
        let clock = crate::clock::Clock::fixed(now_ns);
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let state = crate::test_support::state_with_clock(
            config.clone(),
            memtable,
            journal,
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
            clock.clone(),
        );

        let window = TimestampWindow::from_config(&state.config, &clock);
        let oldest = now_ns - age.as_nanos() as i64;
        let newest = now_ns + age.as_nanos() as i64;

        assert!(window.validate(oldest).is_ok(), "the oldest edge is inside");
        assert!(window.validate(newest).is_ok(), "the newest edge is inside");
        assert!(
            window.validate(oldest - 1).is_err(),
            "one nanosecond older must be refused"
        );
        assert!(
            window.validate(newest + 1).is_err(),
            "one nanosecond newer must be refused"
        );

        // And the window travels with the clock rather than being fixed at
        // startup: a long-running process must not drift into refusing the
        // present.
        clock.advance(Duration::from_secs(7200));
        let later = TimestampWindow::from_config(&state.config, &clock);
        assert!(
            later.validate(oldest).is_err(),
            "what was the edge two hours ago is now outside"
        );
        assert!(later.validate(newest + 1).is_ok());
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

        // Backpressure is a state, not a latch. Once flush has drained the
        // buffer the next push must be accepted — otherwise a single burst
        // takes the instance out of service permanently, which is worse than
        // the unbounded growth this replaced.
        let checkpoint = state.journal.checkpoint().await.unwrap();
        state.memtable.commit_flush();
        state.journal.set_checkpoint(checkpoint.offset).unwrap();

        let accepted_again = push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("backpressure", "third line", now_secs())),
        )
        .await
        .expect("a drained memtable accepts writes again");
        assert_eq!(accepted_again, StatusCode::NO_CONTENT);
        assert_eq!(
            state
                .metrics
                .ingest_throttled
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the release must not have been counted as another refusal"
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
    async fn push_refuses_a_tenant_outside_the_allowlist() {
        let config = Config {
            data_dir: tmp_data_dir("tenant_allowlist"),
            allowed_tenants: Some([crate::tenant::test_tenant()].into_iter().collect()),
            ..Config::default()
        };
        let state = limits_state(config).await;

        push(
            State(state.clone()),
            protobuf_headers(),
            Bytes::from(build_snappy_push("listed", "accepted", now_secs())),
        )
        .await
        .expect("a listed tenant is accepted");

        let mut headers = HeaderMap::new();
        headers.insert(crate::tenant::TENANT_HEADER, "stranger".parse().unwrap());
        headers.insert("content-type", "application/x-protobuf".parse().unwrap());
        let refused = push(
            State(state.clone()),
            headers,
            Bytes::from(build_snappy_push("stranger", "refused", now_secs())),
        )
        .await
        .expect_err("an unlisted tenant must be refused");

        // 403, not 400: the request is well formed and there is nothing the
        // client can change about it.
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
        assert!(
            state
                .memtable
                .query(
                    &crate::tenant::TenantId::parse("stranger").unwrap(),
                    &[],
                    &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    10,
                    true
                )
                .is_empty(),
            "a refused tenant must not have been written"
        );
    }

    /// The quota is a different refusal from backpressure: the instance is
    /// healthy and the tenant asked for more than its policy grants. It is
    /// counted apart, it carries `Retry-After`, and — the part that matters —
    /// it does not latch, so the tenant recovers on its own.
    #[tokio::test]
    async fn push_is_refused_once_the_tenant_is_over_its_ingest_rate() {
        let clock = crate::clock::Clock::fixed(now_secs() * 1_000_000_000);
        let config = Config {
            data_dir: tmp_data_dir("tenant_rate"),
            default_tenant_ingest_bytes_per_second: Some(64),
            tenant_ingest_burst: std::time::Duration::from_secs(1),
            max_push_bytes: 64,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(journal::Journal::spawn(&config, memtable.clone()).unwrap());
        let state = crate::test_support::state_with_clock(
            config,
            memtable,
            journal,
            Arc::new(PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            None,
            clock.clone(),
        );

        let now = clock.now_ns() / 1_000_000_000;
        let body = Bytes::from(build_snappy_push("acme", "a line worth some bytes", now));
        push(State(state.clone()), protobuf_headers(), body.clone())
            .await
            .expect("the first push fits the banked budget");

        let refused = loop {
            match push(State(state.clone()), protobuf_headers(), body.clone()).await {
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert_eq!(refused.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            refused.retry_after.is_some(),
            "a throttled client has to be told how long to wait"
        );
        assert_eq!(
            state
                .metrics
                .ingest_quota_rejected
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "counted apart from backpressure, which says something else"
        );
        assert_eq!(
            state
                .metrics
                .ingest_throttled
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "this instance is not behind; the tenant is over its own rate"
        );

        // And it lets go. A rate limit that latches takes the tenant out of
        // service permanently, which is worse than the burst it refused.
        clock.advance(std::time::Duration::from_secs(60));
        push(State(state.clone()), protobuf_headers(), body)
            .await
            .expect("the budget refills");
    }

    /// Promtail and several SDKs push JSON. It carries the same streams as the
    /// protobuf form, so it has to end up in the same place — including in the
    /// WAL, which stores one encoding whatever arrived, so replay keeps one
    /// decoder.
    #[tokio::test]
    async fn a_json_push_lands_in_the_same_place_a_protobuf_one_does() {
        let config = Config {
            data_dir: tmp_data_dir("json_push"),
            ..Config::default()
        };
        let state = limits_state(config.clone()).await;
        let now = now_secs() * 1_000_000_000;

        let body = format!(
            r#"{{"streams":[{{"stream":{{"app":"json"}},"values":[["{now}","from json"],["{now}","with metadata",{{"trace_id":"abc"}}]]}}]}}"#
        );
        let mut headers = crate::tenant::test_tenant_headers();
        headers.insert("content-type", "application/json".parse().unwrap());
        push(State(state.clone()), headers, Bytes::from(body))
            .await
            .expect("a JSON push is accepted");

        let results = state.memtable.query(
            &test_tenant(),
            &[],
            &[],
            crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
            10,
            true,
        );
        assert_eq!(results[0].labels["app"], "json");
        let lines: Vec<&str> = results
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.as_str())
            .collect();
        assert_eq!(lines, vec!["from json", "with metadata"]);
        assert_eq!(
            results[0].entries[1].structured_metadata,
            vec![("trace_id".to_string(), "abc".to_string())],
            "the optional third element is structured metadata"
        );

        // The WAL holds one encoding whatever the body was.
        let replayed = Arc::new(MemTable::new());
        journal::replay_with_traces(
            &config.data_dir.join("journal.wal"),
            &config.data_dir.join("journal.ckpt"),
            &replayed,
            &crate::trace::TraceMemTable::new(),
            &config.default_tenant,
        )
        .expect("the WAL replays");
        let replayed_lines: Vec<String> = replayed
            .query(
                &test_tenant(),
                &[],
                &[],
                crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                10,
                true,
            )
            .iter()
            .flat_map(|stream| stream.entries.iter())
            .map(|entry| entry.line.clone())
            .collect();
        assert_eq!(replayed_lines, vec!["from json", "with metadata"]);
    }

    /// A JSON body is not exempt from the limits a protobuf body answers to.
    #[tokio::test]
    async fn a_json_push_answers_to_the_same_limits() {
        let state = limits_state(Config {
            data_dir: tmp_data_dir("json_push_limits"),
            max_line_bytes: 8,
            ..Config::default()
        })
        .await;
        let now = now_secs() * 1_000_000_000;
        let mut headers = crate::tenant::test_tenant_headers();
        headers.insert("content-type", "application/json".parse().unwrap());

        let oversized = format!(
            r#"{{"streams":[{{"stream":{{"app":"json"}},"values":[["{now}","far too long a line"]]}}]}}"#
        );
        let refused = push(
            State(state.clone()),
            headers.clone(),
            Bytes::from(oversized),
        )
        .await
        .expect_err("an oversized line is refused");
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);

        // A reserved label name collides with a part column, and the check has
        // to apply wherever labels enter rather than only where they are
        // parsed from the protobuf text form.
        let reserved =
            format!(r#"{{"streams":[{{"stream":{{"_msg":"x"}},"values":[["{now}","short"]]}}]}}"#);
        let refused = push(State(state.clone()), headers.clone(), Bytes::from(reserved))
            .await
            .expect_err("a reserved label name is refused");
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);

        let malformed = r#"{"streams":[{"stream":{},"values":[["not-a-timestamp","x"]]}]}"#;
        let refused = push(State(state.clone()), headers, Bytes::from(malformed))
            .await
            .expect_err("a non-numeric timestamp is refused");
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);

        assert!(
            state
                .memtable
                .query(
                    &test_tenant(),
                    &[],
                    &[],
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    10,
                    true
                )
                .is_empty(),
            "no refused push may have been written"
        );
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

        // Wait for flush_loop to write all data to a part.
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
                    crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                    100,
                    true,
                )
                .expect("part query");
            flushed_total = r.iter().map(|s| s.entries.len()).sum::<usize>();
            // Once flush reaches commit_flush, both inner and flushing should be empty.
            if flushed_total == 3 && memtable.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            flushed_total, 3,
            "flush did not persist all 3 entries to parts"
        );

        // Verify that the memtable is completely empty (everything was flushed).
        assert!(
            memtable.is_empty(),
            "memtable should be empty after full flush"
        );

        // Simulate server shutdown.
        flush_handle.abort();
        drop(state);
        drop(parts);
        drop(journal);
        drop(memtable);

        // Restart: recover and load parts.
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
                crate::part::QueryTimeRange::closed(i64::MIN, i64::MAX),
                100,
                true,
            )
            .expect("part query");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3, "data must persist across restart");
    }
}
