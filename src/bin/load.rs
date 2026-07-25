//! Closed-loop M7 load harness. Supersedes `m5_load` by pacing to a fixed
//! offered rate, excluding a warmup window from the reported percentiles,
//! forcing eviction->restore round trips, capturing the end-of-run `/metrics`
//! gauges, and emitting an explicit per-target pass/fail verdict comparable to
//! the M5 target table.
//!
//! It stays a thin client over the real HTTP/gRPC server so it is backend
//! agnostic: point it at a server whose object store is the Tier B fault store
//! (no Docker) or a Tier C MinIO, and the same workload runs against both.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;
use tonic::Request;

#[derive(Clone, PartialEq, ::prost::Message)]
struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<StreamAdapter>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct StreamAdapter {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<EntryAdapter>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<prost_types::Timestamp>,
    #[prost(string, tag = "2")]
    line: String,
}

type OtelClient = TraceServiceClient<tonic::transport::Channel>;

struct Targets {
    ack_p95_ms: f64,
    ack_p99_ms: f64,
    query_p95_ms: f64,
    rss_max_bytes: u64,
    max_error_rate: f64,
    wal_backlog_max_bytes: u64,
}

#[tokio::main]
async fn main() {
    let http_address =
        std::env::var("LOGGYTRACY_LOAD_ADDR").unwrap_or_else(|_| "127.0.0.1:3100".to_string());
    let otlp_address =
        std::env::var("LOGGYTRACY_LOAD_OTLP_ADDR").unwrap_or_else(|_| "127.0.0.1:4317".to_string());
    let duration_secs = env_usize("LOGGYTRACY_LOAD_SECONDS", 60).max(1);
    let warmup_secs =
        env_usize("LOGGYTRACY_LOAD_WARMUP_SECONDS", 10).min(duration_secs.saturating_sub(1));
    let entries_per_push = env_usize("LOGGYTRACY_LOAD_ENTRIES_PER_PUSH", 100).max(1);
    let event_bytes = env_usize("LOGGYTRACY_LOAD_EVENT_BYTES", 1024).max(32);
    let target_eps = env_u64("LOGGYTRACY_LOAD_TARGET_EPS", 0);
    let query_every = env_usize("LOGGYTRACY_LOAD_QUERY_EVERY", 20).max(1) as u64;
    let seed = env_u64("LOGGYTRACY_LOAD_SEED", 0x5eed_2026);
    let revision = std::env::var("LOGGYTRACY_BUILD_REVISION")
        .or_else(|_| std::env::var("GIT_COMMIT"))
        .unwrap_or_else(|_| "unknown".to_string());
    let machine_profile = std::env::var("LOGGYTRACY_MACHINE_PROFILE")
        .unwrap_or_else(|_| "unspecified developer machine".to_string());
    let tier = std::env::var("LOGGYTRACY_LOAD_TIER").unwrap_or_else(|_| "B".to_string());

    let targets = Targets {
        ack_p95_ms: env_f64("LOGGYTRACY_TARGET_ACK_P95_MS", 250.0),
        ack_p99_ms: env_f64("LOGGYTRACY_TARGET_ACK_P99_MS", 1000.0),
        query_p95_ms: env_f64("LOGGYTRACY_TARGET_QUERY_P95_MS", 2000.0),
        rss_max_bytes: env_u64("LOGGYTRACY_TARGET_RSS_MAX_BYTES", 4 * 1024 * 1024 * 1024),
        max_error_rate: env_f64("LOGGYTRACY_TARGET_MAX_ERROR_RATE", 0.0),
        wal_backlog_max_bytes: env_u64("LOGGYTRACY_TARGET_WAL_BACKLOG_MAX_BYTES", 16 * 1024 * 1024),
    };

    // Offered-rate pacing: one push carries `entries_per_push` events, so the
    // per-push interval that holds `target_eps` is entries/rate seconds.
    let push_interval = if target_eps > 0 {
        Some(Duration::from_secs_f64(
            entries_per_push as f64 / target_eps as f64,
        ))
    } else {
        None
    };

    let start_metrics = fetch_metrics(&http_address);
    let run_start = Instant::now();
    let warmup_end = run_start + Duration::from_secs(warmup_secs as u64);
    let deadline = run_start + Duration::from_secs(duration_secs as u64);

    // Timestamp of the earliest ingested data. Steady-state restore probes query
    // this window so, once it has aged out of a small cache, the query forces a
    // remote restore onto the measured path.
    let earliest_ns = unix_nanos();

    let mut random = seed;
    let mut pushes = 0u64;
    let mut events = 0u64;
    let mut push_errors = 0u64;
    let mut queries = 0u64;
    let mut query_errors = 0u64;
    let mut restore_probes = 0u64;
    let mut metric_queries = 0u64;
    let mut metric_errors = 0u64;
    let mut tempo_searches = 0u64;
    let mut tempo_errors = 0u64;
    let mut otlp_requests = 0u64;
    let mut otlp_errors = 0u64;

    // Warmup samples are collected but excluded from the reported percentiles.
    let mut ack_steady = Vec::new();
    let mut ack_warmup = Vec::new();
    let mut query_steady = Vec::new();
    let mut restore_probe_latency = Vec::new();
    let mut metric_steady = Vec::new();
    let mut tempo_steady = Vec::new();
    let mut rss_peak_bytes = 0u64;

    let mut otlp_client = TraceServiceClient::connect(format!("http://{otlp_address}"))
        .await
        .ok();
    if otlp_client.is_none() {
        eprintln!("warning: OTLP endpoint {otlp_address} unavailable; trace ingest skipped");
    }

    let mut next_send = Instant::now();
    while Instant::now() < deadline {
        if let Some(interval) = push_interval {
            let now = Instant::now();
            if now < next_send {
                std::thread::sleep(next_send - now);
            }
            next_send += interval;
        }

        let sequence = next_random(&mut random);
        let batch = build_push(entries_per_push, event_bytes, sequence);
        let started = Instant::now();
        let steady = started >= warmup_end;
        match request(
            &http_address,
            "POST",
            "/loki/api/v1/push",
            &batch,
            "application/x-protobuf",
        ) {
            Ok((204, _)) => {
                let elapsed = started.elapsed();
                if steady {
                    ack_steady.push(elapsed);
                } else {
                    ack_warmup.push(elapsed);
                }
                pushes += 1;
                events += entries_per_push as u64;
            }
            Ok(_) | Err(_) => push_errors += 1,
        }

        if pushes > 0 && pushes.is_multiple_of(query_every) {
            let now = unix_seconds();
            // Recent-window log query.
            let started = Instant::now();
            let path = format!(
                "/loki/api/v1/query_range?query=%7Bapp%3D%22m7-load%22%7D&start={}&end={}&step=10&limit=100&direction=backward",
                now.saturating_sub(60),
                now
            );
            match request(&http_address, "GET", &path, &[], "") {
                Ok((200, _)) => {
                    if steady {
                        query_steady.push(started.elapsed());
                    }
                    queries += 1;
                }
                Ok(_) | Err(_) => query_errors += 1,
            }

            // Forced eviction->restore probe: query the earliest window so a
            // small cache must fetch the evicted part back from the object
            // store. Only counted in steady state.
            if steady {
                let earliest_s = (earliest_ns / 1_000_000_000) as i64;
                let restore_path = format!(
                    "/loki/api/v1/query_range?query=%7Bapp%3D%22m7-load%22%7D&start={}&end={}&step=60&limit=100&direction=forward",
                    earliest_s,
                    earliest_s + 120
                );
                let started = Instant::now();
                match request(&http_address, "GET", &restore_path, &[], "") {
                    Ok((200, _)) => {
                        restore_probe_latency.push(started.elapsed());
                        restore_probes += 1;
                    }
                    Ok(_) | Err(_) => query_errors += 1,
                }
            }

            let started = Instant::now();
            let metric_path = format!(
                "/loki/api/v1/query_range?query=rate(%7Bapp%3D%22m7-load%22%7D%5B1m%5D)&start={}&end={}&step=10",
                now.saturating_sub(60),
                now
            );
            match request(&http_address, "GET", &metric_path, &[], "") {
                Ok((200, _)) => {
                    if steady {
                        metric_steady.push(started.elapsed());
                    }
                    metric_queries += 1;
                }
                Ok(_) | Err(_) => metric_errors += 1,
            }

            let started = Instant::now();
            match request(&http_address, "GET", "/api/search?limit=20", &[], "") {
                Ok((200, _)) => {
                    if steady {
                        tempo_steady.push(started.elapsed());
                    }
                    tempo_searches += 1;
                }
                Ok(_) | Err(_) => tempo_errors += 1,
            }

            otlp_requests += 1;
            if let Some(client) = otlp_client.as_mut() {
                if send_otlp(client, sequence).await.is_err() {
                    otlp_errors += 1;
                }
            } else {
                otlp_errors += 1;
            }
        }

        if pushes.is_multiple_of(10) {
            rss_peak_bytes = rss_peak_bytes.max(current_rss_bytes().unwrap_or(0));
        }
    }

    let achieved_seconds = run_start.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let achieved_eps = events as f64 / achieved_seconds;
    let end_metrics = fetch_metrics(&http_address);

    let restore_success_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_remote_restore_success_total",
    );
    let restore_errors_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_remote_restore_errors_total",
    );
    let eviction_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_cache_evictions_total",
    );
    let retention_success_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_retention_success_total",
    );
    let retention_errors_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_retention_errors_total",
    );
    let flush_success_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_flush_success_total",
    );
    let flush_errors_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_flush_errors_total",
    );
    let merge_errors_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_merge_errors_total",
    );
    let ingest_errors_delta = counter_delta(
        &start_metrics,
        &end_metrics,
        "loggytracy_ingest_errors_total",
    );

    let ack = latency_summary(&ack_steady);
    let query = latency_summary(&query_steady);
    let ack_p95 = ack["p95_ms"].as_f64().unwrap_or(f64::INFINITY);
    let ack_p99 = ack["p99_ms"].as_f64().unwrap_or(f64::INFINITY);
    let query_p95 = query["p95_ms"].as_f64().unwrap_or(f64::INFINITY);
    let request_total = pushes + push_errors;
    let error_rate = if request_total == 0 {
        0.0
    } else {
        push_errors as f64 / request_total as f64
    };

    // Final storage health. Tier B deliberately injects transient object-store
    // errors, so a nonzero flush/merge error delta is expected and is *not* a
    // failure as long as the engine recovered: the correctness invariant is that
    // acknowledged data is never lost (no ingest errors) and both the remote and
    // the cache end healthy. Transient counters are reported for interpretation.
    let remote_healthy_end = gauge(&end_metrics, "loggytracy_remote_healthy");
    let cache_healthy_end = gauge(&end_metrics, "loggytracy_cache_healthy");
    // Flush liveness. Transient injected errors are tolerated, but the flush
    // loop must keep draining the WAL: if the backlog is not bounded and no
    // flush is succeeding, the loop is wedged even though ingest still acks and
    // the remote reports healthy. This is the honest signal a wedged flush hides
    // behind a green health check.
    let wal_backlog_end = gauge(&end_metrics, "loggytracy_wal_backlog_bytes");
    let wal_backlog_bounded = wal_backlog_end <= targets.wal_backlog_max_bytes;
    // A wedged flush loop emits a persistent error roughly every tick, so its
    // error delta outnumbers its successes; a healthy loop (even with injected
    // transient faults) succeeds far more often than it fails. Require both a
    // bounded backlog and success-dominated flushes so a short run whose backlog
    // has not yet crossed the cap is still flagged.
    let flush_progressing =
        flush_success_delta > 0 && wal_backlog_bounded && flush_errors_delta <= flush_success_delta;
    let behavioral = serde_json::json!({
        "no_ingest_errors": ingest_errors_delta == 0,
        "remote_healthy_end": remote_healthy_end == 1,
        "cache_healthy_end": cache_healthy_end == 1,
        "flush_progressing": flush_progressing,
        "wal_backlog_bounded": wal_backlog_bounded,
        "flush_success_delta": flush_success_delta,
        "recovered_flush_errors": flush_errors_delta,
        "recovered_merge_errors": merge_errors_delta,
        "restore_errors": restore_errors_delta,
        "restore_observed": restore_success_delta > 0,
        "retention_observed": retention_success_delta > 0,
    });
    let behavioral_pass = ingest_errors_delta == 0
        && remote_healthy_end == 1
        && cache_healthy_end == 1
        && flush_progressing;

    let numeric = serde_json::json!({
        "ack_p95": target_row(ack_p95, targets.ack_p95_ms),
        "ack_p99": target_row(ack_p99, targets.ack_p99_ms),
        "query_p95": target_row(query_p95, targets.query_p95_ms),
        "rss_peak_bytes": target_row(rss_peak_bytes as f64, targets.rss_max_bytes as f64),
        "error_rate": target_row(error_rate, targets.max_error_rate),
        "wal_backlog_bytes": target_row(wal_backlog_end as f64, targets.wal_backlog_max_bytes as f64),
    });
    let numeric_pass = ack_p95 <= targets.ack_p95_ms
        && ack_p99 <= targets.ack_p99_ms
        && query_p95 <= targets.query_p95_ms
        && rss_peak_bytes <= targets.rss_max_bytes
        && error_rate <= targets.max_error_rate;

    let terminal_gauges = serde_json::json!({
        "memtable_bytes": gauge(&end_metrics, "loggytracy_memtable_bytes"),
        "memtable_entries": gauge(&end_metrics, "loggytracy_memtable_entries"),
        "part_count": gauge(&end_metrics, "loggytracy_part_count"),
        "part_bytes": gauge(&end_metrics, "loggytracy_part_bytes"),
        "trace_part_count": gauge(&end_metrics, "loggytracy_trace_part_count"),
        "wal_backlog_bytes": gauge(&end_metrics, "loggytracy_wal_backlog_bytes"),
        "merge_debt_parts": gauge(&end_metrics, "loggytracy_merge_debt_parts"),
    });

    let report = serde_json::json!({
        "tier": tier,
        "seed": seed,
        "build_revision": revision,
        "machine_profile": machine_profile,
        "duration_seconds": duration_secs,
        "warmup_seconds": warmup_secs,
        "offered_eps": target_eps,
        "achieved_eps": achieved_eps,
        "events": events,
        "pushes": pushes,
        "push_errors": push_errors,
        "queries": queries,
        "query_errors": query_errors,
        "restore_probes": restore_probes,
        "metric_queries": metric_queries,
        "metric_errors": metric_errors,
        "tempo_searches": tempo_searches,
        "tempo_errors": tempo_errors,
        "otlp_requests": otlp_requests,
        "otlp_errors": otlp_errors,
        "ack_latency_ms": ack,
        "ack_warmup_latency_ms": latency_summary(&ack_warmup),
        "query_latency_ms": query,
        "restore_probe_latency_ms": latency_summary(&restore_probe_latency),
        "metric_latency_ms": latency_summary(&metric_steady),
        "tempo_latency_ms": latency_summary(&tempo_steady),
        "rss_peak_bytes": rss_peak_bytes,
        "restore_success_delta": restore_success_delta,
        "restore_errors_delta": restore_errors_delta,
        "eviction_delta": eviction_delta,
        "retention_success_delta": retention_success_delta,
        "retention_errors_delta": retention_errors_delta,
        "flush_errors_delta": flush_errors_delta,
        "merge_errors_delta": merge_errors_delta,
        "ingest_errors_delta": ingest_errors_delta,
        "terminal_gauges": terminal_gauges,
        "targets": numeric,
        "behavioral": behavioral,
        "behavioral_pass": behavioral_pass,
        "numeric_pass": numeric_pass,
        "verdict": if behavioral_pass && numeric_pass { "PASS" } else if behavioral_pass { "PASS_BEHAVIORAL_ONLY" } else { "FAIL" },
    });

    let rendered = serde_json::to_string_pretty(&report).expect("report serialization");
    if let Ok(path) = std::env::var("LOGGYTRACY_LOAD_RESULT_PATH")
        && let Err(error) = std::fs::write(&path, format!("{rendered}\n"))
    {
        eprintln!("warning: failed to write result file {path}: {error}");
    }
    println!("{rendered}");
}

/// Every M5 target is an upper bound (latency, RSS, error rate), so a measured
/// value passes when it does not exceed its target.
fn target_row(measured: f64, target: f64) -> serde_json::Value {
    serde_json::json!({ "measured": measured, "target": target, "pass": measured <= target })
}

async fn send_otlp(client: &mut OtelClient, sequence: u64) -> Result<(), String> {
    let trace_id = sequence.to_be_bytes().repeat(2);
    let span_id = sequence.to_be_bytes();
    let now = unix_nanos();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id,
                    span_id: span_id.to_vec(),
                    name: "m7-load-span".to_string(),
                    start_time_unix_nano: now,
                    end_time_unix_nano: now.saturating_add(1_000_000),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    client
        .export(Request::new(request))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn build_push(count: usize, event_bytes: usize, sequence: u64) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch");
    let entries = (0..count)
        .map(|offset| {
            let mut line = format!(
                "m7-load event={} offset={offset} ",
                sequence + offset as u64
            );
            // Pad to the configured event size for realistic ~1-KiB payloads.
            if line.len() < event_bytes {
                line.push_str(&"x".repeat(event_bytes - line.len()));
            }
            EntryAdapter {
                timestamp: Some(prost_types::Timestamp {
                    seconds: now.as_secs() as i64,
                    nanos: now.subsec_nanos() as i32,
                }),
                line,
            }
        })
        .collect();
    let request = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{app="m7-load",source="load-tool"}"#.to_string(),
            entries,
            hash: sequence,
        }],
    };
    let mut encoded = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut encoded)
        .expect("protobuf encoding must work");
    snap::raw::Encoder::new()
        .compress_vec(&encoded)
        .expect("snappy encoding must work")
}

fn request(
    address: &str,
    method: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .map_err(|error| error.to_string())?;
    let content_header = if content_type.is_empty() {
        String::new()
    } else {
        format!("Content-Type: {content_type}\r\n")
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\n{content_header}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    let status = response
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "invalid HTTP response".to_string())?
        .parse()
        .map_err(|error| format!("invalid HTTP status: {error}"))?;
    Ok((status, response))
}

/// Fetches `/metrics` and parses the Prometheus text into name -> value. TYPE
/// and HELP comment lines are skipped.
fn fetch_metrics(address: &str) -> HashMap<String, f64> {
    let mut map = HashMap::new();
    let Ok((200, response)) = request(address, "GET", "/metrics", &[], "") else {
        return map;
    };
    let Some(body_start) = response.find("\r\n\r\n") else {
        return map;
    };
    for line in response[body_start + 4..].lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        if let (Some(name), Some(value)) = (parts.next(), parts.next())
            && let Ok(value) = value.parse::<f64>()
        {
            map.insert(name.to_string(), value);
        }
    }
    map
}

fn counter_delta(start: &HashMap<String, f64>, end: &HashMap<String, f64>, name: &str) -> u64 {
    let start_value = start.get(name).copied().unwrap_or(0.0);
    let end_value = end.get(name).copied().unwrap_or(0.0);
    (end_value - start_value).max(0.0) as u64
}

fn gauge(metrics: &HashMap<String, f64>, name: &str) -> u64 {
    metrics.get(name).copied().unwrap_or(0.0) as u64
}

fn latency_summary(values: &[Duration]) -> serde_json::Value {
    if values.is_empty() {
        return serde_json::json!({"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0});
    }
    let mut values: Vec<_> = values.iter().map(Duration::as_secs_f64).collect();
    values.sort_by(f64::total_cmp);
    serde_json::json!({
        "count": values.len(),
        "p50_ms": percentile(&values, 0.50) * 1_000.0,
        "p95_ms": percentile(&values, 0.95) * 1_000.0,
        "p99_ms": percentile(&values, 0.99) * 1_000.0,
    })
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    let index = ((values.len() - 1) as f64 * percentile).ceil() as usize;
    values[index.min(values.len() - 1)]
}

fn current_rss_bytes() -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()
        .ok()?;
    let kilobytes = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .ok()?;
    kilobytes.checked_mul(1024)
}

fn unix_seconds() -> i64 {
    unix_nanos().saturating_div(1_000_000_000) as i64
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn next_random(value: &mut u64) -> u64 {
    *value = value.wrapping_mul(6364136223846793005).wrapping_add(1);
    *value
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
