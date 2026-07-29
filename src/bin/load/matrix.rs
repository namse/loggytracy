//! The two phases the comparison needs that a load run cannot give it.
//!
//! A paced load run sends whatever it managed to send, at wall-clock
//! timestamps. Two runs of it against two systems therefore produce two
//! *different* datasets, and any row-level comparison between them would be
//! comparing arrival timing rather than engines. So the query comparison runs
//! on its own dataset:
//!
//! * **`seed`** pushes a fixed corpus at fixed log timestamps, driven from an
//!   anchor both runs are given. Same seed, same rows, same bytes, same
//!   timestamps — the two systems provably hold the same entries.
//! * **`matrix`** times the four query shapes over that dataset, cold and
//!   warm, and records a digest of every answer so the two runs can be checked
//!   for returning the same rows. A fast wrong answer is not a win, so the
//!   digest is the part of this file that matters most.
//!
//! Neither phase paces, and neither reports a throughput. They are latency and
//! correctness instruments; the rate axis belongs to `Phase::Load`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::{Config, Verify};
use crate::http::{Client, Request};
use crate::stats::Series;
use loggytracy::corpus::{APPS, Corpus, CorpusSpec, LEVELS, PHRASES, STATUSES};
use loggytracy::memtable::LogEntry;

/// Keeps the verification dataset's identifiers distinct from the load
/// corpus's while staying a function of the run seed.
const VERIFY_SEED_SALT: u64 = 0x1e_4f_a1_ce;

const PUSH_PATH: &str = "/loki/api/v1/push";
const PUSH_CONTENT_TYPE: &str = "application/x-protobuf";

/// The four shapes `docs/VISION.md` names, in the order the claim is argued
/// in: two that both systems index for, then the one the claim rests on, then
/// the aggregation that should read one column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    LabelOnly,
    LineFilter,
    JsonField,
    Rate,
}

pub const SHAPES: [Shape; 4] = [
    Shape::LabelOnly,
    Shape::LineFilter,
    Shape::JsonField,
    Shape::Rate,
];

impl Shape {
    pub fn name(self) -> &'static str {
        match self {
            Shape::LabelOnly => "label_only",
            Shape::LineFilter => "line_filter",
            Shape::JsonField => "json_field",
            Shape::Rate => "rate",
        }
    }
}

/// The dataset both systems are seeded with.
///
/// One tenant, so the query phase reads only these rows however much a load
/// phase left behind; a fixed `start_ts_ns`, so the window a query asks for is
/// an absolute instant rather than "the last minute" of two different runs;
/// and `out_of_order: false`, because the arrival-order axis belongs to the
/// load phase and a jittered duplicate here would be a row-equality failure
/// that says nothing about either engine.
pub fn verify_corpus(cfg: &Config) -> Corpus {
    loggytracy::corpus::generate(&CorpusSpec {
        seed: cfg.seed ^ VERIFY_SEED_SALT,
        tenants: 1,
        streams: cfg.verify.streams,
        labels_per_stream: cfg.verify.labels_per_stream,
        rows: cfg.verify.rows,
        tenant_prefix: cfg.verify.tenant_prefix.clone(),
        plain_weight: cfg.plain_weight,
        json_weight: cfg.json_weight,
        logfmt_weight: cfg.logfmt_weight,
        metadata_pairs: cfg.metadata_pairs,
        start_ts_ns: cfg.verify.anchor_ns,
        step_ns: cfg.verify.step_ns,
        out_of_order: false,
    })
}

/// Anchors that are not `> 0` are refused rather than defaulted.
///
/// A default would be derived from this process's clock, and the two runs of a
/// comparison start minutes apart — the datasets would differ by exactly that
/// gap, which is the failure this whole phase exists to prevent, and it would
/// show up as a row-equality mismatch nobody could explain.
pub fn require_anchor(verify: &Verify) -> Result<(), String> {
    if verify.anchor_ns > 0 {
        return Ok(());
    }
    Err(
        "LOGGYTRACY_LOAD_VERIFY_ANCHOR_NS must be set to the same value for both runs of a \
comparison; without it each run would seed a different dataset"
            .to_string(),
    )
}

pub struct SeedOutcome {
    pub pushes: u64,
    pub rows: u64,
    pub line_bytes: u64,
    pub wire_bytes: u64,
    pub retries: u64,
    pub errors: u64,
    pub statuses: BTreeMap<u16, u64>,
    pub first_error: Option<String>,
    pub elapsed_seconds: f64,
}

struct SeedBody {
    bytes: Vec<u8>,
    rows: usize,
    line_bytes: usize,
}

/// One stream per push, `entries_per_push` entries at a time.
///
/// Chunking by stream rather than mixing them keeps the body an agent-shaped
/// one and, more importantly, keeps the push sequence a pure function of the
/// corpus: the same bytes leave this process in the same order for both
/// systems.
fn seed_bodies(corpus: &Corpus, entries_per_push: usize) -> Vec<SeedBody> {
    let mut bodies = Vec::new();
    for stream in &corpus.streams {
        for chunk in stream.entries.chunks(entries_per_push) {
            let line_bytes: usize = chunk.iter().map(|entry| entry.line.len()).sum();
            let batch: Vec<(loggytracy::memtable::Labels, Vec<LogEntry>)> =
                vec![(stream.labels.clone(), chunk.to_vec())];
            let encoded = loggytracy::proto::encode_push_request(&batch);
            bodies.push(SeedBody {
                bytes: snap::raw::Encoder::new()
                    .compress_vec(&encoded)
                    .expect("snappy encoding must work"),
                rows: chunk.len(),
                line_bytes,
            });
        }
    }
    bodies
}

pub async fn run_seed(cfg: &Config, corpus: &Corpus) -> SeedOutcome {
    let tenant = corpus.tenant_ids[0].as_str().to_string();
    let bodies = Arc::new(Mutex::new(seed_bodies(corpus, cfg.verify.entries_per_push)));
    let start = Instant::now();

    let workers: Vec<_> = (0..cfg.verify.push_connections)
        .map(|_| {
            let bodies = bodies.clone();
            let tenant = tenant.clone();
            let address = cfg.http_address.clone();
            let timeout = cfg.request_timeout();
            tokio::spawn(async move {
                let mut client = Client::new(&address, timeout);
                let mut outcome = SeedOutcome {
                    pushes: 0,
                    rows: 0,
                    line_bytes: 0,
                    wire_bytes: 0,
                    retries: 0,
                    errors: 0,
                    statuses: BTreeMap::new(),
                    first_error: None,
                    elapsed_seconds: 0.0,
                };
                loop {
                    let Some(body) = bodies.lock().await.pop() else {
                        break;
                    };
                    // Seeding is not a measurement, so a refusal is waited out
                    // rather than recorded: the dataset has to land in full on
                    // both sides or the comparison has nothing to stand on.
                    for attempt in 0..60 {
                        let result = client
                            .request(&Request {
                                method: "POST",
                                path: PUSH_PATH,
                                body: &body.bytes,
                                content_type: PUSH_CONTENT_TYPE,
                                tenant: Some(&tenant),
                            })
                            .await;
                        match result {
                            Ok(response) => {
                                *outcome.statuses.entry(response.status).or_default() += 1;
                                if response.status == 204 || response.status == 200 {
                                    outcome.pushes += 1;
                                    outcome.rows += body.rows as u64;
                                    outcome.line_bytes += body.line_bytes as u64;
                                    outcome.wire_bytes += body.bytes.len() as u64;
                                    break;
                                }
                                if response.status == 429 && attempt < 59 {
                                    outcome.retries += 1;
                                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                                    continue;
                                }
                                outcome.errors += 1;
                                outcome.first_error.get_or_insert_with(|| {
                                    format!(
                                        "{}: {}",
                                        response.status,
                                        String::from_utf8_lossy(&response.body)
                                            .chars()
                                            .take(300)
                                            .collect::<String>()
                                    )
                                });
                                break;
                            }
                            Err(error) => {
                                if attempt < 59 {
                                    outcome.retries += 1;
                                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                                    continue;
                                }
                                outcome.errors += 1;
                                outcome.first_error.get_or_insert(error);
                                break;
                            }
                        }
                    }
                }
                outcome
            })
        })
        .collect();

    let mut total = SeedOutcome {
        pushes: 0,
        rows: 0,
        line_bytes: 0,
        wire_bytes: 0,
        retries: 0,
        errors: 0,
        statuses: BTreeMap::new(),
        first_error: None,
        elapsed_seconds: 0.0,
    };
    for worker in workers {
        if let Ok(outcome) = worker.await {
            total.pushes += outcome.pushes;
            total.rows += outcome.rows;
            total.line_bytes += outcome.line_bytes;
            total.wire_bytes += outcome.wire_bytes;
            total.retries += outcome.retries;
            total.errors += outcome.errors;
            for (status, count) in outcome.statuses {
                *total.statuses.entry(status).or_default() += count;
            }
            total.first_error = total.first_error.take().or(outcome.first_error);
        }
    }
    total.elapsed_seconds = start.elapsed().as_secs_f64();
    total
}

pub struct Query {
    pub id: String,
    pub shape: Shape,
    pub expression: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub path: String,
}

/// `apps x sub-windows` queries per shape, built from the corpus so every one
/// of them selects rows that exist.
///
/// The sub-window is what makes a cold measurement cold: a time range nothing
/// has asked for cannot come out of either system's result cache, and both
/// systems have one.
pub fn build_queries(cfg: &Config, corpus: &Corpus) -> Vec<Query> {
    let apps: Vec<String> = {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for stream in &corpus.streams {
            if let Some(app) = stream.labels.get("app") {
                seen.insert(app.clone());
            }
        }
        if seen.is_empty() {
            seen.insert(APPS[0].to_string());
        }
        seen.into_iter().collect()
    };
    let span = cfg.verify_span_ns();
    let windows = cfg.verify.windows as i64;
    let mut queries = Vec::new();
    for shape in SHAPES {
        for (app_index, app) in apps.iter().enumerate() {
            for window in 0..cfg.verify.windows {
                let start_ns = cfg.verify.anchor_ns + span * window as i64 / windows;
                let end_ns = cfg.verify.anchor_ns + span * (window as i64 + 1) / windows;
                let selector = format!("{{app=\"{app}\"}}");
                let variant = app_index * cfg.verify.windows + window;
                let expression = match shape {
                    Shape::LabelOnly => selector,
                    Shape::LineFilter => {
                        format!("{selector} |= \"{}\"", PHRASES[variant % PHRASES.len()])
                    }
                    Shape::JsonField => {
                        if variant.is_multiple_of(2) {
                            format!(
                                "{selector} | json | status=\"{}\"",
                                STATUSES[variant % STATUSES.len()]
                            )
                        } else {
                            format!(
                                "{selector} | json | level=\"{}\"",
                                LEVELS[variant % LEVELS.len()]
                            )
                        }
                    }
                    // `sum(rate(...))` rather than a bare `rate(...)`, and the
                    // reason is a measured difference rather than a stylistic
                    // one: Loki promotes structured metadata into a metric's
                    // identity, so a bare `rate()` over this corpus returns one
                    // series per `trace_id` on Loki and one per stream on
                    // loggytracy. That is neither the same amount of work nor a
                    // comparable answer. Summed, both systems have to produce
                    // the same number, which is what the row-equality check is
                    // for. The unsummed difference is reported in
                    // `docs/COMPARISON.md` rather than hidden here.
                    Shape::Rate => format!("sum(rate({selector}[{}]))", cfg.verify.range),
                };
                let encoded = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("query", &expression)
                    .append_pair("start", &start_ns.to_string())
                    .append_pair("end", &end_ns.to_string())
                    .append_pair("step", &cfg.verify.step_seconds.to_string())
                    .append_pair("limit", &cfg.verify.limit.to_string())
                    .append_pair("direction", "backward")
                    .finish();
                queries.push(Query {
                    id: format!("{}/{app}/w{window}", shape.name()),
                    shape,
                    expression,
                    start_ns,
                    end_ns,
                    path: format!("/loki/api/v1/query_range?{encoded}"),
                });
            }
        }
    }
    queries
}

/// What a response contained, reduced to something two runs can be compared
/// on.
pub struct Answer {
    pub kind: String,
    pub rows: u64,
    pub series: u64,
    /// Order-independent digest of the rows. Two systems that returned the
    /// same rows in a different order agree here; two that returned different
    /// rows do not, however similar their counts.
    pub digest: String,
    /// Label names the result carried. Recorded separately from the digest
    /// because Loki's `| json` stage promotes extracted fields to stream
    /// labels and a difference there is a presentation difference, not a
    /// wrong answer.
    pub label_keys: Vec<String>,
    pub sample: Vec<String>,
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn canonical_labels(value: &Value) -> String {
    let Some(map) = value.as_object() else {
        return String::new();
    };
    map.iter()
        .map(|(name, value)| format!("{name}={}", value.as_str().unwrap_or_default()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Metric samples are compared at six decimals.
///
/// Both systems compute `rate()` in f64 and print it themselves, so the last
/// bits of the two decimal renderings are not a property either engine
/// promises. Six places is far finer than any difference that would matter and
/// far coarser than the printing.
fn canonical_sample(value: &Value) -> String {
    match value {
        Value::String(text) => text
            .parse::<f64>()
            .map(|number| format!("{number:.6}"))
            .unwrap_or_else(|_| text.clone()),
        Value::Number(number) => number
            .as_f64()
            .map(|number| format!("{number:.6}"))
            .unwrap_or_else(|| number.to_string()),
        other => other.to_string(),
    }
}

pub fn digest_response(body: &[u8]) -> Result<Answer, String> {
    let parsed: Value =
        serde_json::from_slice(body).map_err(|error| format!("response is not JSON: {error}"))?;
    let kind = parsed["data"]["resultType"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let entries = parsed["data"]["result"]
        .as_array()
        .ok_or_else(|| "response has no data.result array".to_string())?;

    let mut lines: Vec<String> = Vec::new();
    let mut label_keys: BTreeSet<String> = BTreeSet::new();
    for entry in entries {
        let labels = if kind == "matrix" || kind == "vector" {
            &entry["metric"]
        } else {
            &entry["stream"]
        };
        if let Some(map) = labels.as_object() {
            label_keys.extend(map.keys().cloned());
        }
        let Some(values) = entry["values"].as_array() else {
            continue;
        };
        for value in values {
            let Some(pair) = value.as_array() else {
                continue;
            };
            let (first, second) = (
                pair.first().unwrap_or(&Value::Null),
                pair.get(1).unwrap_or(&Value::Null),
            );
            if kind == "matrix" || kind == "vector" {
                lines.push(format!(
                    "{}\u{1}{}\u{1}{}",
                    canonical_labels(labels),
                    canonical_sample(first),
                    canonical_sample(second),
                ));
            } else {
                // Stream labels are deliberately out of the row digest: see
                // `Answer::label_keys`.
                lines.push(format!(
                    "{}\u{1}{}",
                    first.as_str().unwrap_or_default(),
                    second.as_str().unwrap_or_default(),
                ));
            }
        }
    }
    lines.sort();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for line in &lines {
        hash ^= fnv1a64(line.as_bytes());
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let sample = lines
        .iter()
        .take(3)
        .map(|line| line.chars().take(140).collect::<String>())
        .collect();
    Ok(Answer {
        kind,
        rows: lines.len() as u64,
        series: entries.len() as u64,
        digest: format!("{hash:016x}"),
        label_keys: label_keys.into_iter().collect(),
        sample,
    })
}

struct Timing {
    cold_ms: f64,
    warm_ms: Vec<f64>,
    answer: Option<Answer>,
    warm_digests_agree: bool,
    status: u16,
    error: Option<String>,
}

async fn issue(
    client: &mut Client,
    query: &Query,
    tenant: &str,
) -> (f64, u16, Vec<u8>, Option<String>) {
    let sent = Instant::now();
    let result = client
        .request(&Request {
            method: "GET",
            path: &query.path,
            body: &[],
            content_type: "",
            tenant: Some(tenant),
        })
        .await;
    let elapsed = sent.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok(response) if response.status == 200 => (elapsed, 200, response.body, None),
        Ok(response) => {
            let detail = String::from_utf8_lossy(&response.body)
                .chars()
                .take(300)
                .collect::<String>();
            (elapsed, response.status, Vec::new(), Some(detail))
        }
        Err(error) => (elapsed, 0, Vec::new(), Some(error)),
    }
}

/// Cold pass over every query, then the warm repeats.
///
/// The passes are separated rather than interleaved so that "cold" means what
/// it says: by the time a query is repeated, every other query of every shape
/// has already run, so nothing about the second issue is the first one still
/// being resident in a way the first was not.
pub async fn run_matrix(cfg: &Config, corpus: &Corpus) -> Value {
    let tenant = corpus.tenant_ids[0].as_str().to_string();
    let queries = build_queries(cfg, corpus);
    let mut client = Client::new(&cfg.http_address, cfg.request_timeout());
    let mut timings: Vec<Timing> = Vec::with_capacity(queries.len());

    for query in &queries {
        let (elapsed, status, body, error) = issue(&mut client, query, &tenant).await;
        let answer = if status == 200 {
            match digest_response(&body) {
                Ok(answer) => Some(answer),
                Err(reason) => {
                    timings.push(Timing {
                        cold_ms: elapsed,
                        warm_ms: Vec::new(),
                        answer: None,
                        warm_digests_agree: false,
                        status,
                        error: Some(reason),
                    });
                    continue;
                }
            }
        } else {
            None
        };
        timings.push(Timing {
            cold_ms: elapsed,
            warm_ms: Vec::new(),
            answer,
            warm_digests_agree: true,
            status,
            error,
        });
    }

    for _ in 0..cfg.verify.repeats {
        for (index, query) in queries.iter().enumerate() {
            let (elapsed, status, body, error) = issue(&mut client, query, &tenant).await;
            let timing = &mut timings[index];
            timing.warm_ms.push(elapsed);
            if status != 200 {
                timing.error.get_or_insert_with(|| {
                    error.unwrap_or_else(|| format!("warm repeat answered {status}"))
                });
                timing.warm_digests_agree = false;
                continue;
            }
            // A system whose repeat of the same query over the same fixed
            // window returns a different answer has a cache or a scan bound
            // that is not deterministic, and the cold/warm split would be
            // measuring that rather than caching.
            match (digest_response(&body), timing.answer.as_ref()) {
                (Ok(repeat), Some(first)) if repeat.digest != first.digest => {
                    timing.warm_digests_agree = false;
                }
                (Err(reason), _) => {
                    timing.error.get_or_insert(reason);
                    timing.warm_digests_agree = false;
                }
                _ => {}
            }
        }
    }

    let mut per_shape = serde_json::Map::new();
    for shape in SHAPES {
        let mut cold = Series::default();
        let mut warm = Series::default();
        let mut rows = 0u64;
        let mut errors = 0u64;
        let mut unstable = 0u64;
        let mut example = String::new();
        for (query, timing) in queries.iter().zip(&timings) {
            if query.shape != shape {
                continue;
            }
            if example.is_empty() {
                example = query.expression.clone();
            }
            if timing.error.is_some() || timing.answer.is_none() {
                errors += 1;
                continue;
            }
            cold.push(timing.cold_ms);
            for value in &timing.warm_ms {
                warm.push(*value);
            }
            rows += timing.answer.as_ref().map_or(0, |answer| answer.rows);
            unstable += u64::from(!timing.warm_digests_agree);
        }
        per_shape.insert(
            shape.name().to_string(),
            json!({
                "expression_example": example,
                "queries": queries.iter().filter(|query| query.shape == shape).count(),
                "errors": errors,
                "rows_returned_cold": rows,
                "warm_answers_differed": unstable,
                "cold_ms": cold.summary(),
                "warm_ms": warm.summary(),
            }),
        );
    }

    let answers: Vec<Value> = queries
        .iter()
        .zip(&timings)
        .map(|(query, timing)| {
            json!({
                "id": query.id,
                "shape": query.shape.name(),
                "expression": query.expression,
                "start_ns": query.start_ns,
                "end_ns": query.end_ns,
                "status": timing.status,
                "error": timing.error,
                "cold_ms": timing.cold_ms,
                "warm_ms": timing.warm_ms,
                "warm_digests_agree": timing.warm_digests_agree,
                "result_type": timing.answer.as_ref().map(|answer| answer.kind.clone()),
                "rows": timing.answer.as_ref().map(|answer| answer.rows),
                "series": timing.answer.as_ref().map(|answer| answer.series),
                "digest": timing.answer.as_ref().map(|answer| answer.digest.clone()),
                "label_keys": timing.answer.as_ref().map(|answer| answer.label_keys.clone()),
                "sample": timing.answer.as_ref().map(|answer| answer.sample.clone()),
            })
        })
        .collect();

    json!({
        "shapes": Value::Object(per_shape),
        "answers": answers,
        "queries_issued": queries.len() as u64 * (1 + cfg.verify.repeats as u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_row_digest_ignores_order_and_stream_labels_but_not_content() {
        let first = br#"{"data":{"resultType":"streams","result":[
            {"stream":{"app":"a"},"values":[["2","beta"],["1","alpha"]]}]}}"#;
        let second = br#"{"data":{"resultType":"streams","result":[
            {"stream":{"app":"a","level":"info"},"values":[["1","alpha"]]},
            {"stream":{"app":"a","level":"warn"},"values":[["2","beta"]]}]}}"#;
        let third = br#"{"data":{"resultType":"streams","result":[
            {"stream":{"app":"a"},"values":[["1","alpha"],["2","BETA"]]}]}}"#;

        let first = digest_response(first).expect("valid");
        let second = digest_response(second).expect("valid");
        let third = digest_response(third).expect("valid");
        assert_eq!(
            first.digest, second.digest,
            "the same rows split across streams in a different order are the same answer"
        );
        assert_ne!(
            first.digest, third.digest,
            "a changed line is a changed row"
        );
        assert_eq!(first.rows, 2);
        assert_eq!(first.label_keys, vec!["app".to_string()]);
        assert_eq!(
            second.label_keys,
            vec!["app".to_string(), "level".to_string()]
        );
    }

    #[test]
    fn matrix_samples_compare_at_six_decimals() {
        let loki = br#"{"data":{"resultType":"matrix","result":[
            {"metric":{"app":"a"},"values":[[1772000000,"0.0666666666666667"]]}]}}"#;
        let other = br#"{"data":{"resultType":"matrix","result":[
            {"metric":{"app":"a"},"values":[[1772000000,"0.06666666666666671"]]}]}}"#;
        let different = br#"{"data":{"resultType":"matrix","result":[
            {"metric":{"app":"a"},"values":[[1772000000,"0.07"]]}]}}"#;
        assert_eq!(
            digest_response(loki).expect("valid").digest,
            digest_response(other).expect("valid").digest
        );
        assert_ne!(
            digest_response(loki).expect("valid").digest,
            digest_response(different).expect("valid").digest
        );
    }

    #[test]
    fn a_body_that_is_not_a_loki_result_is_an_error_not_an_empty_answer() {
        assert!(digest_response(b"not json").is_err());
        assert!(digest_response(br#"{"status":"error"}"#).is_err());
    }

    #[test]
    fn an_unset_anchor_is_refused_because_two_runs_would_seed_different_data() {
        let mut verify = verify_for_test();
        verify.anchor_ns = 0;
        assert!(require_anchor(&verify).is_err());
        verify.anchor_ns = 1_772_000_000_000_000_000;
        assert!(require_anchor(&verify).is_ok());
    }

    fn verify_for_test() -> Verify {
        Verify {
            tenant_prefix: "verify-tenant".to_string(),
            rows: 1_000,
            streams: 8,
            labels_per_stream: 6,
            step_ns: 1_000_000,
            anchor_ns: 1_772_000_000_000_000_000,
            entries_per_push: 100,
            push_connections: 2,
            windows: 3,
            repeats: 2,
            limit: 100,
            range: "1m".to_string(),
            step_seconds: 10,
        }
    }
}
