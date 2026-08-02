//! The load harness.
//!
//! The one it replaces could not measure what it claimed to, and every number
//! it produced has been retired (`docs/LOAD_RESULTS.md`, retirement header;
//! `docs/VISION.md`, "The ruler comes before the work"). The defects it had
//! and what is done about each of them:
//!
//! * **One connection, one request in flight, `Connection: close`.** Now N
//!   keep-alive connections per workload, over the hand-rolled client in
//!   `http.rs`.
//! * **Uncorrected coordinated omission.** The pacer keeps a nominal schedule
//!   and the stopwatch now starts at the **intended** send. Both numbers are
//!   reported: service time from the actual send, response time from the
//!   intended one. The gap between them is the signal that the offered rate
//!   was not achievable, and it is exactly the signal the old harness deleted.
//! * **Cardinality 1 and near-zero entropy.** The corpus is
//!   `loggytracy::corpus`, the same generator the benches measure.
//! * **Reads never contended with writes.** Queries are an independent
//!   workload with their own rate and their own connections.
//! * **Percentiles over eight samples.** Every percentile carries its sample
//!   count, and one the count cannot support is `null` with the reason.
//! * **RSS from a coarse `ps` poll.** `VmHWM` out of `/proc/<pid>/status`, and
//!   a sampled `VmRSS` series for the shape. A run that cannot read it fails.
//! * **`std::thread::sleep` inside `#[tokio::main]`.** Every wait is
//!   `tokio::time`.

mod config;
mod http;
mod matrix;
mod otlp;
mod probe;
mod stats;
mod workload;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio::time::Instant;

use config::{Config, Phase, Target};
use http::{Client, Request};
use stats::{GaugeSeries, LatencyPair, target_row, wal_backlog_drains};
use workload::{ArrivalOrder, PushGenerator, QUERY_SHAPES, QueryGenerator, result_rows};

const PUSH_CONTENT_TYPE: &str = "application/x-protobuf";
/// Row group size the engine itself flushes at, so the measured compression
/// ratio is the ratio this data will actually get on disk.
const ROW_GROUP_SIZE: usize = 8192;
/// Keeps the trace ids of a run distinct from its log identifiers while
/// staying a function of the run seed.
const OTLP_SEED_SALT: u64 = 0x07_1e_50_ed;

struct PushJob {
    intended: Instant,
    body: workload::PushBody,
}

struct QueryJob {
    intended: Instant,
    plan: workload::QueryPlan,
}

#[derive(Default)]
struct PushOutcome {
    steady: LatencyPair,
    warmup: LatencyPair,
    throttled_latency: LatencyPair,
    accepted: u64,
    throttled: u64,
    errors: u64,
    events_accepted: u64,
    events_offered: u64,
    wire_bytes: u64,
    line_bytes: u64,
    encoded_bytes: u64,
    out_of_order_entries: u64,
    max_lateness_ms: u64,
    streams_sent: u64,
    connects: u64,
    statuses: BTreeMap<u16, u64>,
    first_error: Option<String>,
}

impl PushOutcome {
    fn merge(&mut self, other: PushOutcome) {
        self.steady.extend(&other.steady);
        self.warmup.extend(&other.warmup);
        self.throttled_latency.extend(&other.throttled_latency);
        self.accepted += other.accepted;
        self.throttled += other.throttled;
        self.errors += other.errors;
        self.events_accepted += other.events_accepted;
        self.events_offered += other.events_offered;
        self.wire_bytes += other.wire_bytes;
        self.line_bytes += other.line_bytes;
        self.encoded_bytes += other.encoded_bytes;
        self.out_of_order_entries += other.out_of_order_entries;
        self.max_lateness_ms = self.max_lateness_ms.max(other.max_lateness_ms);
        self.streams_sent += other.streams_sent;
        self.connects += other.connects;
        for (status, count) in other.statuses {
            *self.statuses.entry(status).or_default() += count;
        }
        self.first_error = self.first_error.take().or(other.first_error);
    }
}

#[derive(Default)]
struct QueryOutcome {
    steady: LatencyPair,
    per_shape: BTreeMap<&'static str, LatencyPair>,
    shape_counts: BTreeMap<&'static str, u64>,
    answered: u64,
    errors: u64,
    throttled: u64,
    restore_probes: u64,
    restore_rows: u64,
    restore_probes_with_rows: u64,
    rows_returned: u64,
    connects: u64,
    statuses: BTreeMap<u16, u64>,
    first_error: Option<String>,
}

impl QueryOutcome {
    fn merge(&mut self, other: QueryOutcome) {
        self.steady.extend(&other.steady);
        for (shape, latency) in other.per_shape {
            self.per_shape.entry(shape).or_default().extend(&latency);
        }
        for (shape, count) in other.shape_counts {
            *self.shape_counts.entry(shape).or_default() += count;
        }
        self.answered += other.answered;
        self.errors += other.errors;
        self.throttled += other.throttled;
        self.restore_probes += other.restore_probes;
        self.restore_rows += other.restore_rows;
        self.restore_probes_with_rows += other.restore_probes_with_rows;
        self.rows_returned += other.rows_returned;
        self.connects += other.connects;
        for (status, count) in other.statuses {
            *self.statuses.entry(status).or_default() += count;
        }
        self.first_error = self.first_error.take().or(other.first_error);
    }
}

#[derive(Default)]
struct SampleOutcome {
    wal_backlog: GaugeSeries,
    memtable_bytes: GaugeSeries,
    part_count: GaugeSeries,
    rss: GaugeSeries,
    anon: GaugeSeries,
    health_samples: u64,
    health_healthy: u64,
    scrape_errors: u64,
    vm_hwm_bytes: Option<u64>,
    anon_peak_bytes: Option<u64>,
    file_bytes_end: Option<u64>,
    rss_error: Option<String>,
}

#[derive(Default)]
struct OtlpOutcome {
    latency: LatencyPair,
    sent: u64,
    errors: u64,
    connected: bool,
}

#[tokio::main]
async fn main() {
    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    match cfg.phase {
        Phase::Load => run_load(cfg).await,
        Phase::Seed | Phase::Matrix => run_verify(cfg).await,
    }
}

/// The seed and matrix phases, which drive the fixed dataset the comparison's
/// query numbers and its row-equality check are both taken over.
async fn run_verify(cfg: Config) {
    if let Err(error) = matrix::require_anchor(&cfg.verify) {
        eprintln!("{error}");
        std::process::exit(2);
    }
    if let Err(error) = wait_for_ready(&cfg).await {
        eprintln!("server at {} is not ready: {error}", cfg.http_address);
        std::process::exit(1);
    }
    let memory_source = cfg.memory_source();
    let memory_before = memory_source
        .as_ref()
        .ok()
        .and_then(|source| source.read().ok());

    eprintln!(
        "{} phase {:?}: generating verification corpus of {} rows",
        cfg.target.name(),
        cfg.phase,
        cfg.verify.rows
    );
    let corpus = matrix::verify_corpus(&cfg);

    // Anonymous memory has to be *sampled*, not read at the ends: the cgroup's
    // own `memory.peak` is a high-water mark but includes reclaimable page
    // cache, and this phase reads large data files, so the page cache alone
    // would carry the peak to the limit and say nothing.
    let stop = Arc::new(AtomicBool::new(false));
    let anon_peak = Arc::new(AtomicU64::new(0));
    let anon_watch = tokio::spawn({
        let stop = stop.clone();
        let anon_peak = anon_peak.clone();
        let source = cfg.memory_source();
        async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(source) = source.as_ref()
                    && let Ok(memory) = source.read()
                    && let Some(anon) = memory.anon_bytes
                {
                    anon_peak.fetch_max(anon, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    });

    let mut report = json!({
        "phase": if cfg.phase == Phase::Seed { "seed" } else { "matrix" },
        "target": cfg.target.name(),
        "run": {
            "build_revision": config::build_revision(),
            "machine_profile": config::machine_profile(),
            "seed": cfg.seed,
        },
        "verify": {
            "tenant": corpus.tenant_ids[0].as_str(),
            "anchor_ns": cfg.verify.anchor_ns,
            "span_ns": cfg.verify_span_ns(),
            "rows": corpus.entry_count(),
            "streams": corpus.streams.len(),
            "line_bytes": corpus.line_bytes(),
        },
    });

    let ok;
    match cfg.phase {
        Phase::Seed => {
            let outcome = matrix::run_seed(&cfg, &corpus).await;
            ok = outcome.errors == 0 && outcome.rows == corpus.entry_count() as u64;
            report["seed"] = json!({
                "pushes": outcome.pushes,
                "rows": outcome.rows,
                "line_bytes": outcome.line_bytes,
                "wire_bytes": outcome.wire_bytes,
                "retries": outcome.retries,
                "errors": outcome.errors,
                "statuses": outcome
                    .statuses
                    .iter()
                    .map(|(status, count)| (status.to_string(), *count))
                    .collect::<BTreeMap<_, _>>(),
                "first_error": outcome.first_error,
                "elapsed_seconds": outcome.elapsed_seconds,
                "complete": ok,
            });
        }
        Phase::Matrix => {
            let outcome = matrix::run_matrix(&cfg, &corpus).await;
            ok = outcome["shapes"]
                .as_object()
                .is_some_and(|shapes| shapes.values().all(|shape| shape["errors"] == json!(0)));
            report["matrix"] = outcome;
        }
        Phase::Load => unreachable!("run_verify is only reached for the seed and matrix phases"),
    }

    stop.store(true, Ordering::Relaxed);
    let _ = anon_watch.await;
    let memory_after = memory_source
        .as_ref()
        .ok()
        .and_then(|source| source.read().ok());
    report["memory"] = json!({
        "source": memory_source.as_ref().map(|source| source.describe()).ok(),
        "error": memory_source.as_ref().err(),
        "peak_bytes_before": memory_before.as_ref().map(|memory| memory.vm_hwm_bytes),
        "peak_bytes": memory_after.as_ref().map(|memory| memory.vm_hwm_bytes),
        "current_bytes": memory_after.as_ref().map(|memory| memory.vm_rss_bytes),
        "anon_peak_bytes": match anon_peak.load(Ordering::Relaxed) {
            0 => Value::Null,
            peak => json!(peak),
        },
        "anon_bytes_end": memory_after.as_ref().and_then(|memory| memory.anon_bytes),
        "file_bytes_end": memory_after.as_ref().and_then(|memory| memory.file_bytes),
    });
    report["config"] = serde_json::to_value(&cfg).expect("config serialization");
    report["verdict"] = json!(if ok { "PASS" } else { "FAIL" });

    let rendered = serde_json::to_string_pretty(&report).expect("report serialization");
    if let Some(path) = cfg.result_path.as_ref()
        && let Err(error) = std::fs::write(path, format!("{rendered}\n"))
    {
        eprintln!("warning: failed to write result file {path}: {error}");
    }
    println!("{rendered}");
    if !ok {
        std::process::exit(2);
    }
}

async fn run_load(cfg: Config) {
    let revision = config::build_revision();
    let machine_profile = config::machine_profile();

    eprintln!("generating corpus: {} rows", cfg.corpus_rows);
    let corpus = Arc::new(loggytracy::corpus::generate(
        &loggytracy::corpus::CorpusSpec {
            seed: cfg.seed,
            tenants: cfg.tenants,
            streams: cfg.streams,
            labels_per_stream: cfg.labels_per_stream,
            rows: cfg.corpus_rows,
            tenant_prefix: "load-tenant".to_string(),
            plain_weight: cfg.plain_weight,
            json_weight: cfg.json_weight,
            logfmt_weight: cfg.logfmt_weight,
            metadata_pairs: cfg.metadata_pairs,
            ..Default::default()
        },
    ));
    let compression = measure_compression(&corpus);

    if let Err(error) = wait_for_ready(&cfg).await {
        eprintln!("server at {} is not ready: {error}", cfg.http_address);
        std::process::exit(1);
    }

    let mut probe_client = Client::new(&cfg.http_address, cfg.request_timeout());
    let start_metrics = scrape(&mut probe_client).await.unwrap_or_default();

    let run_start = Instant::now();
    let warmup_end = run_start + Duration::from_secs(cfg.warmup_seconds);
    let deadline = run_start + Duration::from_secs(cfg.duration_seconds);
    let stop = Arc::new(AtomicBool::new(false));
    let events_accepted = Arc::new(AtomicU64::new(0));

    let (push_tx, push_rx) = mpsc::channel::<PushJob>(cfg.ingest_connections * 4);
    let push_rx = Arc::new(Mutex::new(push_rx));
    let (query_tx, query_rx) = mpsc::channel::<QueryJob>(cfg.query_connections * 4);
    let query_rx = Arc::new(Mutex::new(query_rx));

    let push_pacer = tokio::spawn(push_pacer(
        cfg.clone(),
        corpus.clone(),
        push_tx,
        stop.clone(),
        deadline,
    ));
    let query_pacer = tokio::spawn(query_pacer(
        cfg.clone(),
        corpus.clone(),
        query_tx,
        stop.clone(),
        deadline,
    ));
    let push_workers: Vec<_> = (0..cfg.ingest_connections)
        .map(|_| {
            tokio::spawn(push_worker(
                cfg.clone(),
                push_rx.clone(),
                warmup_end,
                events_accepted.clone(),
            ))
        })
        .collect();
    let query_workers: Vec<_> = (0..cfg.query_connections)
        .map(|_| tokio::spawn(query_worker(cfg.clone(), query_rx.clone(), warmup_end)))
        .collect();
    let sampler = tokio::spawn(sampler(cfg.clone(), stop.clone(), run_start));
    let otlp = tokio::spawn(otlp_workload(cfg.clone(), stop.clone(), deadline));

    // The stop condition is checked here rather than in each pacer so that
    // "enough work happened" and "long enough happened" are one decision.
    loop {
        if Instant::now() >= deadline {
            break;
        }
        if cfg.target_events > 0 && events_accepted.load(Ordering::Relaxed) >= cfg.target_events {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Relaxed);

    let _ = push_pacer.await;
    let _ = query_pacer.await;
    let mut push = PushOutcome::default();
    for worker in push_workers {
        if let Ok(outcome) = worker.await {
            push.merge(outcome);
        }
    }
    let mut query = QueryOutcome::default();
    for worker in query_workers {
        if let Ok(outcome) = worker.await {
            query.merge(outcome);
        }
    }
    let mut samples = sampler.await.unwrap_or_default();
    let otlp = otlp.await.unwrap_or_default();
    // Read once more after the workload stopped: the sampler exits before the
    // last in-flight requests land, and both `VmHWM` and cgroup `memory.peak`
    // are high-water marks, so the final read is the only one that covers the
    // whole run.
    match cfg.memory_source() {
        Ok(source) => match source.read() {
            Ok(memory) => {
                samples.vm_hwm_bytes =
                    Some(samples.vm_hwm_bytes.unwrap_or(0).max(memory.vm_hwm_bytes));
                if let Some(anon) = memory.anon_bytes {
                    samples.anon_peak_bytes = Some(samples.anon_peak_bytes.unwrap_or(0).max(anon));
                }
                samples.file_bytes_end = memory.file_bytes.or(samples.file_bytes_end);
            }
            Err(error) => {
                samples.rss_error.get_or_insert(error);
            }
        },
        Err(error) => {
            samples.rss_error.get_or_insert(error);
        }
    }

    let elapsed_seconds = run_start.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    let end_metrics = scrape(&mut probe_client).await.unwrap_or_default();

    let report = build_report(ReportInputs {
        cfg: &cfg,
        revision,
        machine_profile,
        compression,
        corpus: &corpus,
        elapsed_seconds,
        push,
        query,
        samples,
        otlp,
        start_metrics,
        end_metrics,
        ended_on: if cfg.target_events > 0
            && events_accepted.load(Ordering::Relaxed) >= cfg.target_events
        {
            "event_target"
        } else {
            "duration_cap"
        },
    });

    let rendered = serde_json::to_string_pretty(&report).expect("report serialization");
    if let Some(path) = cfg.result_path.as_ref()
        && let Err(error) = std::fs::write(path, format!("{rendered}\n"))
    {
        eprintln!("warning: failed to write result file {path}: {error}");
    }
    println!("{rendered}");
    if report["verdict"] != json!("PASS") {
        std::process::exit(2);
    }
}

async fn wait_for_ready(cfg: &Config) -> Result<(), String> {
    let mut client = Client::new(&cfg.http_address, Duration::from_secs(5));
    let mut last = "no attempt made".to_string();
    for _ in 0..60 {
        match client
            .request(&Request {
                method: "GET",
                path: cfg.target.ready_path(),
                body: &[],
                content_type: "",
                tenant: None,
            })
            .await
        {
            Ok(response) if response.status == 200 => return Ok(()),
            Ok(response) => last = format!("/ready answered {}", response.status),
            Err(error) => last = error,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

async fn scrape(client: &mut Client) -> Option<probe::Metrics> {
    let response = client
        .request(&Request {
            method: "GET",
            path: "/metrics",
            body: &[],
            content_type: "",
            tenant: None,
        })
        .await
        .ok()?;
    (response.status == 200).then(|| probe::parse_metrics(&response.body))
}

/// Issues on the nominal schedule regardless of how far behind the workers
/// are, so a job's `intended` time carries the delay the offered rate has
/// already accrued. That accrual is what coordinated omission drops.
async fn push_pacer(
    cfg: Config,
    corpus: Arc<loggytracy::corpus::Corpus>,
    tx: mpsc::Sender<PushJob>,
    stop: Arc<AtomicBool>,
    deadline: Instant,
) {
    let mut generator = PushGenerator::new(
        corpus,
        cfg.seed,
        cfg.entries_per_push,
        cfg.streams_per_push,
        ArrivalOrder {
            spread_ms: cfg.entry_spread_ms,
            late_fraction: cfg.late_fraction,
            late_max_ms: cfg.late_max_ms,
        },
        cfg.target,
    );
    let interval = cfg.push_interval();
    let mut intended = Instant::now();
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Built before the sleep so the encode cost lands in the idle gap
        // rather than between the intended send and the actual one.
        let body = generator.next_body(unix_nanos() as i64);
        let intended_at = match interval {
            Some(interval) => {
                let at = intended;
                tokio::time::sleep_until(at).await;
                // Advanced from the schedule, never from `now`: a pacer that
                // resets to the clock after a slow send is a pacer that
                // forgives the server the time it stole, which is coordinated
                // omission written into the pacing itself.
                intended = at + interval;
                at
            }
            // Unpaced: there is no schedule to be behind, so the intended time
            // is the moment the job was handed over and response time equals
            // service time. `run.pacing` says so.
            None => Instant::now(),
        };
        if tx
            .send(PushJob {
                intended: intended_at,
                body,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn push_worker(
    cfg: Config,
    rx: Arc<Mutex<mpsc::Receiver<PushJob>>>,
    warmup_end: Instant,
    events_accepted: Arc<AtomicU64>,
) -> PushOutcome {
    let mut client = Client::new(&cfg.http_address, cfg.request_timeout());
    let mut outcome = PushOutcome::default();
    loop {
        let job = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(job) = job else { break };
        outcome.events_offered += job.body.entries as u64;
        outcome.out_of_order_entries += job.body.out_of_order_entries as u64;
        outcome.max_lateness_ms = outcome.max_lateness_ms.max(job.body.max_lateness_ms);
        outcome.streams_sent += job.body.streams as u64;

        let sent = Instant::now();
        let result = client
            .request(&Request {
                method: "POST",
                path: cfg.target.push_path(),
                body: &job.body.bytes,
                content_type: PUSH_CONTENT_TYPE,
                tenant: Some(&job.body.tenant),
            })
            .await;
        let done = Instant::now();
        let queueing_ms = duration_ms(sent.saturating_duration_since(job.intended));
        let service_ms = duration_ms(done.saturating_duration_since(sent));
        let steady = job.intended >= warmup_end;

        match result {
            Ok(response) => {
                *outcome.statuses.entry(response.status).or_default() += 1;
                match response.status {
                    204 => {
                        if steady {
                            outcome.steady.record(queueing_ms, service_ms);
                        } else {
                            outcome.warmup.record(queueing_ms, service_ms);
                        }
                        outcome.accepted += 1;
                        outcome.events_accepted += job.body.entries as u64;
                        events_accepted.fetch_add(job.body.entries as u64, Ordering::Relaxed);
                        outcome.wire_bytes += job.body.bytes.len() as u64;
                        outcome.line_bytes += job.body.line_bytes as u64;
                        outcome.encoded_bytes += job.body.encoded_bytes as u64;
                    }
                    // Backpressure, not an error. Kept out of the error rate
                    // and out of the accepted latencies, because a refusal is
                    // not the latency of doing the work.
                    429 => {
                        outcome.throttled += 1;
                        if steady {
                            outcome.throttled_latency.record(queueing_ms, service_ms);
                        }
                    }
                    status => {
                        outcome.errors += 1;
                        outcome.first_error.get_or_insert_with(|| {
                            format!(
                                "{status}: {}",
                                String::from_utf8_lossy(&response.body)
                                    .chars()
                                    .take(200)
                                    .collect::<String>()
                            )
                        });
                    }
                }
            }
            Err(error) => {
                outcome.errors += 1;
                outcome.first_error.get_or_insert(error);
            }
        }
    }
    outcome.connects = client.connects;
    outcome
}

async fn query_pacer(
    cfg: Config,
    corpus: Arc<loggytracy::corpus::Corpus>,
    tx: mpsc::Sender<QueryJob>,
    stop: Arc<AtomicBool>,
    deadline: Instant,
) {
    let Some(interval) = cfg.query_interval() else {
        return;
    };
    let mut generator = QueryGenerator::new(
        corpus,
        cfg.seed,
        cfg.target,
        cfg.query_weights,
        cfg.query_window_seconds,
        cfg.restore_lookback_seconds,
        cfg.query_limit,
    );
    let mut intended = Instant::now();
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let plan = generator.next_plan(unix_seconds());
        tokio::time::sleep_until(intended).await;
        let job = QueryJob { intended, plan };
        intended += interval;
        if tx.send(job).await.is_err() {
            return;
        }
    }
}

async fn query_worker(
    cfg: Config,
    rx: Arc<Mutex<mpsc::Receiver<QueryJob>>>,
    warmup_end: Instant,
) -> QueryOutcome {
    let mut client = Client::new(&cfg.http_address, cfg.request_timeout());
    let mut outcome = QueryOutcome::default();
    loop {
        let job = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        let Some(job) = job else { break };
        let shape = job.plan.shape;
        *outcome.shape_counts.entry(shape.name()).or_default() += 1;

        let sent = Instant::now();
        let result = client
            .request(&Request {
                method: "GET",
                path: &job.plan.path,
                body: &[],
                content_type: "",
                tenant: Some(&job.plan.tenant),
            })
            .await;
        let done = Instant::now();
        let queueing_ms = duration_ms(sent.saturating_duration_since(job.intended));
        let service_ms = duration_ms(done.saturating_duration_since(sent));
        let steady = job.intended >= warmup_end;

        match result {
            Ok(response) => {
                *outcome.statuses.entry(response.status).or_default() += 1;
                match response.status {
                    200 => {
                        outcome.answered += 1;
                        let rows = result_rows(cfg.target, &response.body);
                        outcome.rows_returned += rows;
                        if shape == workload::QueryShape::RestoreProbe {
                            outcome.restore_probes += 1;
                            outcome.restore_rows += rows;
                            outcome.restore_probes_with_rows += u64::from(rows > 0);
                        }
                        if steady {
                            outcome.steady.record(queueing_ms, service_ms);
                            outcome
                                .per_shape
                                .entry(shape.name())
                                .or_default()
                                .record(queueing_ms, service_ms);
                        }
                    }
                    429 => outcome.throttled += 1,
                    status => {
                        outcome.errors += 1;
                        outcome.first_error.get_or_insert_with(|| {
                            format!(
                                "{status} on {}: {}",
                                job.plan.expression,
                                String::from_utf8_lossy(&response.body)
                                    .chars()
                                    .take(200)
                                    .collect::<String>()
                            )
                        });
                    }
                }
            }
            Err(error) => {
                outcome.errors += 1;
                outcome.first_error.get_or_insert(error);
            }
        }
    }
    outcome.connects = client.connects;
    outcome
}

/// Reads the server rather than driving it: `/proc` for memory, `/metrics` for
/// everything the engine publishes about itself.
async fn sampler(cfg: Config, stop: Arc<AtomicBool>, run_start: Instant) -> SampleOutcome {
    let mut outcome = SampleOutcome::default();
    let mut client = Client::new(&cfg.http_address, cfg.request_timeout());
    let interval = Duration::from_millis(cfg.sample_interval_ms);
    let memory_source = cfg.memory_source();
    while !stop.load(Ordering::Relaxed) {
        let elapsed = run_start.elapsed().as_secs_f64();
        match &memory_source {
            Ok(source) => match source.read() {
                Ok(memory) => {
                    outcome.rss.push(elapsed, memory.vm_rss_bytes);
                    outcome.vm_hwm_bytes =
                        Some(outcome.vm_hwm_bytes.unwrap_or(0).max(memory.vm_hwm_bytes));
                    if let Some(anon) = memory.anon_bytes {
                        outcome.anon.push(elapsed, anon);
                        outcome.anon_peak_bytes =
                            Some(outcome.anon_peak_bytes.unwrap_or(0).max(anon));
                    }
                    outcome.file_bytes_end = memory.file_bytes;
                }
                Err(error) => {
                    outcome.rss_error.get_or_insert(error);
                }
            },
            Err(error) => {
                outcome.rss_error.get_or_insert_with(|| error.clone());
            }
        }
        match scrape(&mut client).await {
            Some(metrics) => match cfg.target {
                Target::Loggytracy => {
                    outcome.wal_backlog.push(
                        elapsed,
                        probe::gauge(&metrics, "loggytracy_wal_backlog_bytes"),
                    );
                    outcome
                        .memtable_bytes
                        .push(elapsed, probe::gauge(&metrics, "loggytracy_memtable_bytes"));
                    outcome
                        .part_count
                        .push(elapsed, probe::gauge(&metrics, "loggytracy_part_count"));
                    if let Some(healthy) = metrics.get("loggytracy_remote_healthy") {
                        outcome.health_samples += 1;
                        outcome.health_healthy += u64::from(*healthy >= 1.0);
                    }
                }
                // Loki's analogues, so the two runs report the same shape of
                // series: what is buffered in the ingester and how many
                // chunks it holds are its memtable and its part count.
                Target::Loki => {
                    outcome.memtable_bytes.push(
                        elapsed,
                        probe::sum_by_prefix(&metrics, "loki_ingester_memory_streams") as u64,
                    );
                    outcome.part_count.push(
                        elapsed,
                        probe::sum_by_prefix(&metrics, "loki_ingester_memory_chunks") as u64,
                    );
                }
                // VictoriaLogs' analogues: what is not yet on disk, and how
                // many parts hold what is.
                Target::VictoriaLogs => {
                    outcome.memtable_bytes.push(
                        elapsed,
                        probe::sum_by_prefix(&metrics, "vl_storage_inmemory_parts") as u64,
                    );
                    outcome.part_count.push(
                        elapsed,
                        probe::sum_by_prefix(&metrics, "vl_storage_file_parts") as u64,
                    );
                }
            },
            None => outcome.scrape_errors += 1,
        }
        tokio::time::sleep(interval).await;
    }
    outcome
}

/// Trace ingest, so the trace registry and the Tempo path are not idle while
/// the log path is loaded. One request in flight by design — it is a garnish
/// on the workload, not part of the measured rate — but its latency is still
/// taken from the intended send, so a stall shows.
async fn otlp_workload(cfg: Config, stop: Arc<AtomicBool>, deadline: Instant) -> OtlpOutcome {
    let mut outcome = OtlpOutcome::default();
    // Loki has no trace ingest, so a trace workload would be load one side
    // carries and the other does not.
    if cfg.target != Target::Loggytracy {
        return outcome;
    }
    let Some(interval) = cfg.otlp_interval() else {
        return outcome;
    };
    let Ok(mut client) = TraceServiceClient::connect(format!("http://{}", cfg.otlp_address)).await
    else {
        eprintln!(
            "warning: OTLP endpoint {} unavailable; trace ingest skipped",
            cfg.otlp_address
        );
        return outcome;
    };
    outcome.connected = true;
    let mut rng = loggytracy::corpus::Rng::new(cfg.seed ^ OTLP_SEED_SALT);
    let mut intended = Instant::now();
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        tokio::time::sleep_until(intended).await;
        let sent = Instant::now();
        let result = send_otlp(&mut client, &mut rng).await;
        let done = Instant::now();
        outcome.latency.record(
            duration_ms(sent.saturating_duration_since(intended)),
            duration_ms(done.saturating_duration_since(sent)),
        );
        match result {
            Ok(()) => outcome.sent += 1,
            Err(_) => outcome.errors += 1,
        }
        intended += interval;
    }
    outcome
}

async fn send_otlp(
    client: &mut TraceServiceClient<tonic::transport::Channel>,
    rng: &mut loggytracy::corpus::Rng,
) -> Result<(), String> {
    let now = unix_nanos();
    let trace_id = rng.next_u64().to_be_bytes().repeat(2);
    let span_id = rng.next_u64().to_be_bytes().to_vec();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id,
                    span_id,
                    name: "load-span".to_string(),
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
        .export(tonic::Request::new(request))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// What this corpus actually compresses to, through the engine's own writer.
///
/// The number is in the report because the retired documents recorded a 31.5x
/// ratio for `"x".repeat(n)` beside a 5.9x ratio for realistic lines and drew
/// disk-footprint conclusions from the first. A load result has to state which
/// data it was measuring.
fn measure_compression(corpus: &loggytracy::corpus::Corpus) -> Value {
    let dir = std::env::temp_dir().join(format!("loggytracy-load-corpus-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    if let Err(error) = std::fs::create_dir_all(&dir) {
        return json!({ "measured": false, "error": error.to_string() });
    }
    let line_bytes = corpus.line_bytes();
    let result = loggytracy::part::flush_rows(corpus.rows(), &dir, ROW_GROUP_SIZE);
    let value = match result {
        Ok(parts) => {
            let file_len = |path: std::path::PathBuf| {
                std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
            };
            let parquet_bytes: u64 = parts.iter().map(|part| file_len(part.data_path())).sum();
            let index_bytes: u64 = parts.iter().map(|part| file_len(part.index_path())).sum();
            json!({
                "measured": true,
                "rows": corpus.entry_count(),
                "line_bytes": line_bytes,
                "parquet_bytes": parquet_bytes,
                "index_bytes": index_bytes,
                "parquet_ratio": line_bytes as f64 / parquet_bytes.max(1) as f64,
            })
        }
        Err(error) => json!({ "measured": false, "error": error.to_string() }),
    };
    let _ = std::fs::remove_dir_all(&dir);
    value
}

struct ReportInputs<'a> {
    cfg: &'a Config,
    revision: String,
    machine_profile: String,
    compression: Value,
    corpus: &'a loggytracy::corpus::Corpus,
    elapsed_seconds: f64,
    push: PushOutcome,
    query: QueryOutcome,
    samples: SampleOutcome,
    otlp: OtlpOutcome,
    start_metrics: probe::Metrics,
    end_metrics: probe::Metrics,
    ended_on: &'static str,
}

fn build_report(inputs: ReportInputs<'_>) -> Value {
    let ReportInputs {
        cfg,
        revision,
        machine_profile,
        compression,
        corpus,
        elapsed_seconds,
        mut push,
        mut query,
        samples,
        mut otlp,
        start_metrics,
        end_metrics,
        ended_on,
    } = inputs;

    let delta = |name: &str| probe::counter_delta(&start_metrics, &end_metrics, name);
    let flush_success = delta("loggytracy_flush_success_total");
    let flush_errors = delta("loggytracy_flush_errors_total");
    let merge_success = delta("loggytracy_merge_success_total");
    let merge_errors = delta("loggytracy_merge_errors_total");
    let ingest_errors = delta("loggytracy_ingest_errors_total");
    let retention_success = delta("loggytracy_retention_success_total");
    let restore_success = delta("loggytracy_remote_restore_success_total");
    let restore_errors = delta("loggytracy_remote_restore_errors_total");

    // Throttled requests are on neither side of the error rate: a 429 is
    // neither work the server accepted nor work it failed at. It is reported
    // beside it instead, because a run that was refused most of what it
    // offered has not measured what it set out to.
    let attempted = push.accepted + push.errors + push.throttled;
    let error_rate = if push.accepted + push.errors == 0 {
        0.0
    } else {
        push.errors as f64 / (push.accepted + push.errors) as f64
    };
    let throttled_rate = if attempted == 0 {
        0.0
    } else {
        push.throttled as f64 / attempted as f64
    };

    let push_response_p95 = push.steady.response.quantile(0.95);
    let push_response_p99 = push.steady.response.quantile(0.99);
    let push_service_p99 = push.steady.service.quantile(0.99);
    let query_response_p95 = query.steady.response.quantile(0.95);

    let rss_peak = samples.vm_hwm_bytes;
    let rss_measured = rss_peak.is_some();

    let drain = wal_backlog_drains(
        &samples.wal_backlog,
        cfg.targets.wal_backlog_max_bytes,
        cfg.targets.min_backlog_samples,
    );
    // A wedged flush loop emits an error roughly every tick, so its errors
    // outnumber its successes; a healthy loop with injected transient faults
    // does not.
    let flush_progressing = flush_success > 0 && flush_errors <= flush_success && drain.drained;
    let remote_healthy_fraction = if samples.health_samples > 0 {
        samples.health_healthy as f64 / samples.health_samples as f64
    } else {
        0.0
    };
    let cache_healthy_end = probe::gauge(&end_metrics, "loggytracy_cache_healthy") == 1;

    let targets = json!({
        "push_response_p95_ms": target_row(
            push_response_p95,
            cfg.targets.push_response_p95_ms,
            "from the intended send; the service percentiles are beside it in push_latency_ms",
        ),
        "push_response_p99_ms": target_row(
            push_response_p99,
            cfg.targets.push_response_p99_ms,
            "",
        ),
        "query_response_p95_ms": target_row(
            query_response_p95,
            cfg.targets.query_response_p95_ms,
            "",
        ),
        "rss_peak_bytes": target_row(
            rss_peak.map(|bytes| bytes as f64),
            cfg.targets.rss_max_bytes as f64,
            "VmHWM of the server process, not a sampled maximum",
        ),
        "error_rate": target_row(Some(error_rate), cfg.targets.max_error_rate, "429 excluded"),
        "throttled_rate": target_row(
            Some(throttled_rate),
            cfg.targets.max_throttled_rate,
            "429 is backpressure working, but a run refused most of its offer measured a rate \
    nobody offered",
        ),
        "wal_backlog_peak_bytes": target_row(
            Some(samples.wal_backlog.peak() as f64),
            cfg.targets.wal_backlog_max_bytes as f64,
            "",
        ),
    });
    // A gate whose measurement is missing fails. `pass: null` is not a pass —
    // the correction in docs/LOAD_RESULTS.md §3 is exactly this rule, arrived
    // at after a peak RSS that had never been measured was written down as an
    // engine result.
    let numeric_pass = targets
        .as_object()
        .expect("targets is an object")
        .values()
        .all(|row| row["pass"] == json!(true));

    // Loki publishes none of the series the loggytracy gates are written
    // against, and a gate over a series that does not exist reads zero and
    // passes. So the Loki run is gated on Loki's own evidence instead — chiefly
    // `loki_discarded_samples_total`, which is where a rate limit, a rejected
    // old sample or an unordered write would show up. Every deviation this bed
    // makes from Loki's defaults exists to keep that counter at zero, and a
    // run where it is not zero is a misconfiguration on our side, not a result.
    let (behavioral, behavioral_pass) = match cfg.target {
        Target::Loggytracy => (
            json!({
                "no_ingest_errors": ingest_errors == 0,
                "remote_healthy_fraction": remote_healthy_fraction,
                "cache_healthy_end": cache_healthy_end,
                "flush_progressing": flush_progressing,
                "wal_backlog_drains": drain.drained,
                "wal_backlog_verdict_decided": drain.decided,
                "wal_backlog_reason": drain.reason,
                "wal_backlog": samples.wal_backlog.summary(),
                "flush_success_delta": flush_success,
                "recovered_flush_errors": flush_errors,
                "recovered_merge_errors": merge_errors,
                "merge_success_delta": merge_success,
                "restore_observed": restore_success > 0,
                "restore_errors": restore_errors,
                "restore_probe_rows": query.restore_rows,
                "restore_probes_with_rows": query.restore_probes_with_rows,
                "retention_observed": retention_success > 0,
            }),
            ingest_errors == 0
                && remote_healthy_fraction >= 0.95
                && cache_healthy_end
                && flush_progressing,
        ),
        Target::Loki => {
            let lines_received = probe::sum_delta(
                &start_metrics,
                &end_metrics,
                "loki_distributor_lines_received_total",
            );
            let discarded =
                probe::sum_delta(&start_metrics, &end_metrics, "loki_discarded_samples_total");
            (
                json!({
                    "lines_received": lines_received,
                    "discarded_samples": discarded,
                    "discarded_by_reason": probe::breakdown(
                        &end_metrics,
                        "loki_discarded_samples_total",
                        "reason",
                    ),
                    "chunks_flushed": probe::sum_delta(
                        &start_metrics,
                        &end_metrics,
                        "loki_ingester_chunks_flushed_total",
                    ),
                    "memory_chunks_end": probe::sum_by_prefix(&end_metrics, "loki_ingester_memory_chunks"),
                    "memory_streams_end": probe::sum_by_prefix(&end_metrics, "loki_ingester_memory_streams"),
                    "note": "a non-zero discard count is this bed misconfiguring Loki, not a Loki \
                result; the reason label says which limit did it",
                }),
                lines_received > 0 && discarded == 0,
            )
        }
        // Same rule as Loki's: a rejection here is this bed misconfiguring the
        // system rather than a result about it, so the gate is that nothing was
        // dropped and something arrived.
        Target::VictoriaLogs => {
            let rows = probe::sum_delta(&start_metrics, &end_metrics, "vl_rows_ingested_total");
            let dropped = probe::sum_delta(&start_metrics, &end_metrics, "vl_rows_dropped_total");
            (
                json!({
                    "rows_ingested": rows,
                    "rows_dropped": dropped,
                    "dropped_by_reason": probe::breakdown(
                        &end_metrics,
                        "vl_rows_dropped_total",
                        "reason",
                    ),
                    "inmemory_parts_end": probe::sum_by_prefix(&end_metrics, "vl_storage_inmemory_parts"),
                    "file_parts_end": probe::sum_by_prefix(&end_metrics, "vl_storage_file_parts"),
                    "note": "a non-zero drop count is this bed misconfiguring VictoriaLogs, not a \
                VictoriaLogs result",
                }),
                rows > 0 && dropped == 0,
            )
        }
    };

    let mut per_shape = serde_json::Map::new();
    for shape in QUERY_SHAPES {
        let name = shape.name();
        let mut latency = query.per_shape.remove(name).unwrap_or_default();
        per_shape.insert(
            name.to_string(),
            json!({
                "issued": query.shape_counts.get(name).copied().unwrap_or(0),
                "latency_ms": latency.summary(),
            }),
        );
    }

    let mut report = json!({
        "verdict": if behavioral_pass && numeric_pass {
            "PASS"
        } else if behavioral_pass {
            "PASS_BEHAVIORAL_ONLY"
        } else {
            "FAIL"
        },
        "numeric_pass": numeric_pass,
        "behavioral_pass": behavioral_pass,
        "targets": targets,
        "behavioral": behavioral,
        "run": {
            "target": cfg.target.name(),
            "phase": "load",
            "tier": cfg.tier,
            "build_revision": revision,
            "machine_profile": machine_profile,
            "seed": cfg.seed,
            "elapsed_seconds": elapsed_seconds,
            "ended_on": ended_on,
            "pacing": if cfg.push_interval().is_some() { "open_loop" } else { "closed_loop" },
        },
        "ingest": {
            "offered_eps": cfg.target_eps,
            "attempted_eps": push.events_offered as f64 / elapsed_seconds,
            "achieved_eps": push.events_accepted as f64 / elapsed_seconds,
            "events_accepted": push.events_accepted,
            "events_offered": push.events_offered,
            "pushes_accepted": push.accepted,
            "pushes_throttled": push.throttled,
            "pushes_failed": push.errors,
            "throttled_rate": throttled_rate,
            "error_rate": error_rate,
            "statuses": push.statuses.iter().map(|(k, v)| (k.to_string(), *v)).collect::<BTreeMap<_, _>>(),
            "first_error": push.first_error,
            "tcp_connections_opened": push.connects,
            "wire_bytes": push.wire_bytes,
            "line_bytes": push.line_bytes,
            "streams_per_push_mean": if push.accepted + push.throttled + push.errors > 0 {
                json!(push.streams_sent as f64 / (push.accepted + push.throttled + push.errors) as f64)
            } else {
                Value::Null
            },
            "out_of_order_entries": push.out_of_order_entries,
            "max_lateness_ms": push.max_lateness_ms,
        },
        "push_latency_ms": push.steady.summary(),
        "push_warmup_latency_ms": push.warmup.summary(),
        "push_throttled_latency_ms": push.throttled_latency.summary(),
        "queries": {
            "answered": query.answered,
            "errors": query.errors,
            "throttled": query.throttled,
            "rows_returned": query.rows_returned,
            "restore_probes": query.restore_probes,
            "achieved_qps": query.answered as f64 / elapsed_seconds,
            "statuses": query.statuses.iter().map(|(k, v)| (k.to_string(), *v)).collect::<BTreeMap<_, _>>(),
            "first_error": query.first_error,
            "tcp_connections_opened": query.connects,
        },
        "query_latency_ms": query.steady.summary(),
        "query_by_shape": Value::Object(per_shape),
    });

    // Assembled separately: one `json!` literal holding all of this stops the
    // macro expanding at its recursion limit.
    report["memory"] = json!({
        "vm_hwm_bytes": rss_peak,
        "measured": rss_measured,
        "error": samples.rss_error,
        "vm_rss": samples.rss.summary(),
        "vm_rss_samples": samples.rss.points(),
        // A cgroup peak includes the page cache the cgroup's own writes
        // created, which is reclaimable and is not the process's footprint.
        // Both systems write a WAL and then large data files, so both carry
        // hundreds of megabytes of it. The anonymous series is the part that
        // cannot be reclaimed and is what an OOM kill is decided on.
        "anon_peak_bytes": samples.anon_peak_bytes,
        "anon": samples.anon.summary(),
        "file_bytes_end": samples.file_bytes_end,
    });
    report["gauges"] = json!({
        "memtable_bytes": samples.memtable_bytes.summary(),
        "part_count": samples.part_count.summary(),
        "wal_backlog_bytes": samples.wal_backlog.summary(),
        "scrape_errors": samples.scrape_errors,
        "terminal": {
            "memtable_bytes": probe::gauge(&end_metrics, "loggytracy_memtable_bytes"),
            "memtable_entries": probe::gauge(&end_metrics, "loggytracy_memtable_entries"),
            "part_count": probe::gauge(&end_metrics, "loggytracy_part_count"),
            "part_bytes": probe::gauge(&end_metrics, "loggytracy_part_bytes"),
            "trace_part_count": probe::gauge(&end_metrics, "loggytracy_trace_part_count"),
            "merge_debt_parts": probe::gauge(&end_metrics, "loggytracy_merge_debt_parts"),
            "part_tenant_segments": probe::gauge(&end_metrics, "loggytracy_part_tenant_segments"),
            "part_sidecar_resident_bytes": probe::gauge(
                &end_metrics,
                "loggytracy_part_sidecar_resident_bytes",
            ),
            "part_meta_bytes": probe::gauge(&end_metrics, "loggytracy_part_meta_bytes"),
        },
    });
    report["object_store_operations"] = json!({
        "puts": probe::object_store_op_delta(&start_metrics, &end_metrics, "put"),
        "gets": probe::object_store_op_delta(&start_metrics, &end_metrics, "get"),
        "deletes": probe::object_store_op_delta(&start_metrics, &end_metrics, "delete"),
        "lists": probe::object_store_op_delta(&start_metrics, &end_metrics, "list"),
        "copies": probe::object_store_op_delta(&start_metrics, &end_metrics, "copy"),
        "listed_objects": delta("loggytracy_object_store_listed_objects_total"),
        "cycles": {
            "flush": flush_success,
            "merge": merge_success,
            "retention": retention_success,
        },
    });
    report["traces"] = json!({
        "connected": otlp.connected,
        "sent": otlp.sent,
        "errors": otlp.errors,
        "latency_ms": otlp.latency.summary(),
    });
    report["corpus"] = json!({
        "streams": corpus.streams.len(),
        "tenants": corpus.tenant_ids.len(),
        "rows": corpus.entry_count(),
        "distinct_label_bytes": corpus.distinct_label_bytes(),
        "line_bytes": corpus.line_bytes(),
        "compression": compression,
        "wire_snappy_ratio": if push.wire_bytes > 0 {
            json!(push.line_bytes as f64 / push.wire_bytes as f64)
        } else {
            Value::Null
        },
        "source": "loggytracy::corpus, the generator benches/ measures",
    });
    report["coordinated_omission"] = json!({
        "push_response_p99_ms": push_response_p99,
        "push_service_p99_ms": push_service_p99,
        "gap_p99_ms": match (push_response_p99, push_service_p99) {
            (Some(response), Some(service)) => json!(response - service),
            _ => Value::Null,
        },
        "queueing_delay_ms": push.steady.queueing.summary(),
        "note": "a large gap means the harness could not issue on schedule, so the service \
    percentiles describe a lower rate than the one offered. The floor is the harness's own: the \
    timer wheel and a channel handoff put roughly a millisecond under every queueing delay, so a \
    queueing p50 sitting at that floor is this process, not the server.",
    });
    report["config"] = serde_json::to_value(cfg).expect("config serialization");
    report["server_environment"] = json!(config::server_environment());
    report
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn unix_seconds() -> i64 {
    (unix_nanos() / 1_000_000_000) as i64
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
