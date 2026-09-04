//! The bench collecty's memory experiments are run on.
//!
//! [`docs/MEMORY.md`](../docs/MEMORY.md) has nine experiments to run and each
//! one has to be comparable with the one before it, so the load and the thing
//! that takes the segments both have to be the same every time and both have
//! to be cheap enough to run over and over. That is what this is: a fixed-rate
//! OTLP generator over a fixed number of connections, and a sink that answers
//! collecty's collect route the way signy would.
//!
//! It is an example rather than a binary on purpose. An example builds against
//! the dev-dependencies, so the corpus can be real OTLP logs without the
//! shipped collecty linking the log message types to get them.
//!
//! ```
//! cargo run --release --example memrig -- --seconds 300 --eps 20000
//! ```
//!
//! The sink can be taken away for a window (`--outage-at`, `--outage-for`),
//! which is how the drain step of `MEMORY.md` §3 is reproduced without
//! stopping a process: collecty sees refusals, backs off, builds a backlog on
//! disk, and then drains it as fast as the sink will take it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use tokio::net::{TcpListener, TcpStream};

struct Args {
    collecty: String,
    sink: SocketAddr,
    eps: u64,
    connections: usize,
    seconds: u64,
    records_per_export: usize,
    outage_at: Option<u64>,
    outage_for: u64,
    report: Option<String>,
}

fn args() -> Args {
    let mut args = Args {
        collecty: "127.0.0.1:4318".to_string(),
        sink: "127.0.0.1:4319".parse().unwrap(),
        eps: 20_000,
        connections: 8,
        seconds: 300,
        records_per_export: 64,
        outage_at: None,
        outage_for: 180,
        report: None,
    };
    let mut raw = std::env::args().skip(1);
    while let Some(flag) = raw.next() {
        let mut value = || raw.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--collecty" => args.collecty = value(),
            "--sink" => args.sink = value().parse().expect("an address"),
            "--eps" => args.eps = value().parse().expect("a number"),
            "--connections" => {
                args.connections = value().parse::<usize>().expect("a number").max(1)
            }
            "--seconds" => args.seconds = value().parse().expect("a number"),
            "--records-per-export" => {
                args.records_per_export = value().parse::<usize>().expect("a number").max(1)
            }
            "--outage-at" => args.outage_at = Some(value().parse().expect("a number")),
            "--outage-for" => args.outage_for = value().parse().expect("a number"),
            "--report" => args.report = Some(value()),
            other => panic!("unknown flag {other}"),
        }
    }
    args
}

fn attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One export, distinct from every other. The shape is `examples/recompress.rs`'s,
/// which is the shape the queue's own benches use, so a segment here compresses
/// like a segment there.
fn export(records: usize, batch: usize) -> Vec<u8> {
    let lines = [
        "GET /v1/checkout 200 in 31ms",
        "connection reset by peer while reading upstream",
        "cache miss for key user:8172:profile, falling back to postgres",
        "retrying publish attempt 2 of 5 after 400ms",
    ];
    let log_records = (0..records)
        .map(|index| {
            let seq = batch * records + index;
            LogRecord {
                time_unix_nano: 1_700_000_000_000_000_000 + seq as u64 * 1_000_000,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!(
                        "{} request_id=req-{seq:08}",
                        lines[seq % lines.len()]
                    ))),
                }),
                attributes: vec![
                    attribute("http.route", "/v1/checkout"),
                    attribute("net.peer.name", &format!("upstream-{}", seq % 8)),
                ],
                ..Default::default()
            }
        })
        .collect();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute("service.name", "checkout"),
                    attribute("deployment.environment", "production"),
                ],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[derive(Default)]
struct Counts {
    offered: AtomicU64,
    accepted: AtomicU64,
    failed: AtomicU64,
    sink_requests: AtomicU64,
    sink_bytes: AtomicU64,
    sink_refused: AtomicU64,
}

/// The sink stands where signy stands: it takes a segment, reads all of it,
/// and answers with the segment number it now holds. During an outage it
/// refuses with a `503`, which is a retry for collecty rather than a drop.
async fn sink(
    request: Request<Incoming>,
    counts: Arc<Counts>,
    down: Arc<AtomicBool>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let segment = request
        .headers()
        .get("x-collecty-segment")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    if request.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }
    let body = request.into_body().collect().await;
    let bytes = body
        .map(|collected| collected.to_bytes().len())
        .unwrap_or(0);

    if down.load(Ordering::Relaxed) {
        counts.sink_refused.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Full::new(Bytes::from_static(b"the sink is away")))
            .unwrap());
    }

    counts.sink_requests.fetch_add(1, Ordering::Relaxed);
    counts.sink_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from(format!("{{\"stored\":{segment}}}"))))
        .unwrap())
}

/// One worker, one TCP connection, its own share of the rate.
///
/// A connection of its own rather than a pooled client: the scaling test of
/// `MEMORY.md` §4 varies exactly this number, so it has to mean what it says.
async fn drive(
    address: String,
    bodies: Arc<Vec<Vec<u8>>>,
    mut offset: usize,
    period: Duration,
    until: Instant,
    counts: Arc<Counts>,
) {
    let mut sender: Option<hyper::client::conn::http1::SendRequest<Full<Bytes>>> = None;
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    while Instant::now() < until {
        ticker.tick().await;
        if sender.as_ref().is_none_or(|s| s.is_closed()) {
            let Ok(stream) = TcpStream::connect(&address).await else {
                counts.failed.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let _ = stream.set_nodelay(true);
            let Ok((new_sender, connection)) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await
            else {
                counts.failed.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            tokio::spawn(async move {
                let _ = connection.await;
            });
            sender = Some(new_sender);
        }

        let body = &bodies[offset % bodies.len()];
        offset += 1;
        let request = Request::builder()
            .method(Method::POST)
            .uri(format!("http://{address}/v1/logs"))
            .header(http::header::HOST, address.as_str())
            .header(http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(Full::new(Bytes::from(body.clone())))
            .expect("a well-formed export");

        counts.offered.fetch_add(1, Ordering::Relaxed);
        match sender
            .as_mut()
            .expect("connected")
            .send_request(request)
            .await
        {
            Ok(response) => {
                let status = response.status();
                let _ = response.into_body().collect().await;
                if status.is_success() {
                    counts.accepted.fetch_add(1, Ordering::Relaxed);
                } else {
                    counts.failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                counts.failed.fetch_add(1, Ordering::Relaxed);
                sender = None;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args = args();
    let counts = Arc::new(Counts::default());
    let down = Arc::new(AtomicBool::new(false));

    let listener = TcpListener::bind(args.sink).await.expect("the sink binds");
    {
        let counts = counts.clone();
        let down = down.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let counts = counts.clone();
                let down = down.clone();
                tokio::spawn(async move {
                    let service =
                        service_fn(move |request| sink(request, counts.clone(), down.clone()));
                    let _ = server_http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    }

    // A pool of distinct exports, cycled: a real collector never sends the
    // same batch twice, and a segment full of one repeated body would
    // compress like nothing a deployment ever produces.
    let exports_per_second = (args.eps as f64 / args.records_per_export as f64).max(1.0);
    let bodies: Arc<Vec<Vec<u8>>> = Arc::new(
        (0..512)
            .map(|batch| export(args.records_per_export, batch))
            .collect(),
    );
    let body_bytes: usize = bodies.iter().map(|body| body.len()).sum();
    eprintln!(
        "memrig: {} exports/s of {} records, mean body {} B, {} connections, {} s",
        exports_per_second as u64,
        args.records_per_export,
        body_bytes / bodies.len(),
        args.connections,
        args.seconds,
    );

    let started = Instant::now();
    let until = started + Duration::from_secs(args.seconds);
    let per_worker = exports_per_second / args.connections as f64;
    let period = Duration::from_secs_f64(1.0 / per_worker);

    if let Some(at) = args.outage_at {
        let down = down.clone();
        let outage_for = args.outage_for;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(at)).await;
            eprintln!("memrig: the sink is away at t={at}s for {outage_for}s");
            down.store(true, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(outage_for)).await;
            down.store(false, Ordering::Relaxed);
            eprintln!("memrig: the sink is back at t={}s", at + outage_for);
        });
    }

    let workers: Vec<_> = (0..args.connections)
        .map(|index| {
            tokio::spawn(drive(
                args.collecty.clone(),
                bodies.clone(),
                index * 97,
                period,
                until,
                counts.clone(),
            ))
        })
        .collect();
    for worker in workers {
        let _ = worker.await;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let offered = counts.offered.load(Ordering::Relaxed);
    let accepted = counts.accepted.load(Ordering::Relaxed);
    let report = format!(
        "{{\n  \"seconds\": {elapsed:.2},\n  \"offered_exports\": {offered},\n  \
\"accepted_exports\": {accepted},\n  \"failed_exports\": {},\n  \
\"offered_eps\": {:.0},\n  \"accepted_eps\": {:.0},\n  \
\"sink_requests\": {},\n  \"sink_bytes\": {},\n  \"sink_refused\": {}\n}}\n",
        counts.failed.load(Ordering::Relaxed),
        offered as f64 * args.records_per_export as f64 / elapsed,
        accepted as f64 * args.records_per_export as f64 / elapsed,
        counts.sink_requests.load(Ordering::Relaxed),
        counts.sink_bytes.load(Ordering::Relaxed),
        counts.sink_refused.load(Ordering::Relaxed),
    );
    print!("{report}");
    if let Some(path) = args.report {
        let _ = std::fs::write(path, &report);
    }
}
