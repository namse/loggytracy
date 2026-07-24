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

#[tokio::main]
async fn main() {
    let http_address =
        std::env::var("LOGGYTRACY_LOAD_ADDR").unwrap_or_else(|_| "127.0.0.1:3100".to_string());
    let otlp_address =
        std::env::var("LOGGYTRACY_LOAD_OTLP_ADDR").unwrap_or_else(|_| "127.0.0.1:4317".to_string());
    let duration_secs = env_usize("LOGGYTRACY_LOAD_SECONDS", 30).max(1);
    let entries_per_push = env_usize("LOGGYTRACY_LOAD_ENTRIES_PER_PUSH", 100).max(1);
    let query_every = env_usize("LOGGYTRACY_LOAD_QUERY_EVERY", 10).max(1) as u64;
    let seed = env_u64("LOGGYTRACY_LOAD_SEED", 0x5eed_2026);
    let revision = std::env::var("LOGGYTRACY_BUILD_REVISION")
        .or_else(|_| std::env::var("GIT_COMMIT"))
        .unwrap_or_else(|_| "unknown".to_string());
    let deadline = Instant::now() + Duration::from_secs(duration_secs as u64);
    let mut random = seed;
    let mut pushes = 0u64;
    let mut events = 0u64;
    let mut push_errors = 0u64;
    let mut queries = 0u64;
    let mut query_errors = 0u64;
    let mut metric_queries = 0u64;
    let mut metric_errors = 0u64;
    let mut tempo_searches = 0u64;
    let mut tempo_errors = 0u64;
    let mut tempo_lookups = 0u64;
    let mut tempo_lookup_errors = 0u64;
    let mut otlp_requests = 0u64;
    let mut otlp_errors = 0u64;
    let mut push_latency = Vec::new();
    let mut query_latency = Vec::new();
    let mut metric_latency = Vec::new();
    let mut tempo_latency = Vec::new();
    let mut rss_peak_bytes = 0u64;
    let mut otlp_client = TraceServiceClient::connect(format!("http://{otlp_address}"))
        .await
        .ok();

    while Instant::now() < deadline {
        let sequence = next_random(&mut random);
        let batch = build_push(entries_per_push, sequence);
        let started = Instant::now();
        match request(
            &http_address,
            "POST",
            "/loki/api/v1/push",
            &batch,
            "application/x-protobuf",
        ) {
            Ok((204, _)) => {
                push_latency.push(started.elapsed());
                pushes += 1;
                events += entries_per_push as u64;
            }
            Ok(_) | Err(_) => push_errors += 1,
        }

        if pushes > 0 && pushes.is_multiple_of(query_every) {
            let now = unix_seconds();
            let started = Instant::now();
            let path = format!(
                "/loki/api/v1/query_range?query=%7Bapp%3D%22m5-load%22%7D&start={}&end={}&step=10&limit=100&direction=backward",
                now.saturating_sub(60),
                now
            );
            match request(&http_address, "GET", &path, &[], "") {
                Ok((200, _)) => {
                    query_latency.push(started.elapsed());
                    queries += 1;
                }
                Ok(_) | Err(_) => query_errors += 1,
            }

            let started = Instant::now();
            let metric_path = format!(
                "/loki/api/v1/query_range?query=rate(%7Bapp%3D%22m5-load%22%7D%5B1m%5D)&start={}&end={}&step=10",
                now.saturating_sub(60),
                now
            );
            match request(&http_address, "GET", &metric_path, &[], "") {
                Ok((200, _)) => {
                    metric_latency.push(started.elapsed());
                    metric_queries += 1;
                }
                Ok(_) | Err(_) => metric_errors += 1,
            }

            let started = Instant::now();
            match request(&http_address, "GET", "/api/search?limit=20", &[], "") {
                Ok((200, _)) => {
                    tempo_latency.push(started.elapsed());
                    tempo_searches += 1;
                }
                Ok(_) | Err(_) => tempo_errors += 1,
            }

            let trace_id = format!("{:032x}", sequence as u128);
            let started = Instant::now();
            match request(
                &http_address,
                "GET",
                &format!("/api/traces/{trace_id}"),
                &[],
                "",
            ) {
                Ok((200, _)) | Ok((404, _)) => {
                    tempo_latency.push(started.elapsed());
                    tempo_lookups += 1;
                }
                Ok(_) | Err(_) => tempo_lookup_errors += 1,
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

    let metrics_status = request(&http_address, "GET", "/metrics", &[], "")
        .ok()
        .map(|(status, _)| status);
    println!(
        "{}",
        serde_json::json!({
            "seed": seed,
            "build_revision": revision,
            "duration_seconds": duration_secs,
            "events": events,
            "pushes": pushes,
            "push_errors": push_errors,
            "queries": queries,
            "query_errors": query_errors,
            "metric_queries": metric_queries,
            "metric_errors": metric_errors,
            "tempo_searches": tempo_searches,
            "tempo_errors": tempo_errors,
            "tempo_lookups": tempo_lookups,
            "tempo_lookup_errors": tempo_lookup_errors,
            "otlp_requests": otlp_requests,
            "otlp_errors": otlp_errors,
            "events_per_second": events as f64 / duration_secs as f64,
            "push_latency_ms": latency_summary(&push_latency),
            "query_latency_ms": latency_summary(&query_latency),
            "metric_latency_ms": latency_summary(&metric_latency),
            "tempo_latency_ms": latency_summary(&tempo_latency),
            "rss_peak_bytes": rss_peak_bytes,
            "metrics_status": metrics_status,
        })
    );
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
                    name: "m5-load-span".to_string(),
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

fn build_push(count: usize, sequence: u64) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch");
    let entries = (0..count)
        .map(|offset| EntryAdapter {
            timestamp: Some(prost_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            line: format!("m5-load event={} offset={offset}", sequence + offset as u64),
        })
        .collect();
    let request = PushRequest {
        streams: vec![StreamAdapter {
            labels: r#"{app="m5-load",source="load-tool"}"#.to_string(),
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
        .set_read_timeout(Some(Duration::from_secs(60)))
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
