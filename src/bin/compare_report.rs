//! Turns the comparison bed's result files into `docs/COMPARISON.md`.
//!
//! The document is generated rather than written because the last time this
//! repository published performance numbers, the prose and the artifacts
//! disagreed on the build revision and on the verdict, and one cited artifact
//! did not exist (`docs/VISION.md`, "The ruler comes before the work"). A
//! document that cannot be regenerated from the JSON that produced it is a
//! claim with no evidence behind it, whatever it says.
//!
//! Every number below is read out of a file `compare/run.sh` wrote. The prose
//! is fixed; the numbers, the ratios and the verdicts are not.
//!
//!     compare_report <results-dir> <output.md>

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::Value;

const SHAPES: [&str; 7] = [
    "label_only",
    "line_filter",
    "json_field",
    "json_field_rare",
    "metadata_rare",
    "trace_window",
    "rate",
];
const TARGETS: [&str; 3] = ["loggytracy", "loki", "victorialogs"];

/// Which digest a pair of systems can be held to.
///
/// The strict digest covers the timestamp, the line and every label with its
/// placement; it exists between the two systems whose responses share the
/// Loki shape. VictoriaLogs answers with every field it holds for a row
/// rather than with what a pipeline produced, so any pair containing it is
/// compared on the reduced basis — the timestamp plus the query-named fields
/// (`matrix.rs`, `Query::basis_fields`). The report states which basis every
/// agreement number is on, because the two are not the same strength of
/// claim.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Basis {
    Strict,
    Reduced,
}

impl Basis {
    fn field(self) -> &'static str {
        match self {
            Basis::Strict => "digest",
            Basis::Reduced => "reduced_digest",
        }
    }
}

const PAIRS: [(&str, &str, Basis); 3] = [
    ("loggytracy", "loki", Basis::Strict),
    ("loggytracy", "victorialogs", Basis::Reduced),
    ("loki", "victorialogs", Basis::Reduced),
];

/// Per-shape agreement for one pair of systems: `(agreed, compared)`.
struct PairAgreement {
    left: &'static str,
    right: &'static str,
    basis: Basis,
    per_shape: BTreeMap<&'static str, (u64, u64)>,
}

impl PairAgreement {
    fn shape(&self, shape: &str) -> (u64, u64) {
        self.per_shape.get(shape).copied().unwrap_or((0, 0))
    }
    fn shape_agrees(&self, shape: &str) -> bool {
        let (same, all) = self.shape(shape);
        all > 0 && same == all
    }
}

fn indexed_answers(matrix: &BTreeMap<&str, Value>, target: &str) -> BTreeMap<String, Value> {
    matrix[target]["matrix"]["answers"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|answer| {
            answer["id"]
                .as_str()
                .map(|id| (id.to_string(), answer.clone()))
        })
        .collect()
}

fn compute_agreements(matrix: &BTreeMap<&str, Value>) -> Vec<PairAgreement> {
    PAIRS
        .iter()
        .map(|(left, right, basis)| {
            let left_answers = indexed_answers(matrix, left);
            let right_answers = indexed_answers(matrix, right);
            let mut per_shape: BTreeMap<&'static str, (u64, u64)> =
                SHAPES.iter().map(|shape| (*shape, (0u64, 0u64))).collect();
            for (id, one) in &left_answers {
                let Some(other) = right_answers.get(id) else {
                    continue;
                };
                let shape = one["shape"].as_str().unwrap_or("?");
                let Some(entry) = SHAPES
                    .iter()
                    .find(|name| **name == shape)
                    .and_then(|name| per_shape.get_mut(name))
                else {
                    continue;
                };
                entry.1 += 1;
                let field = basis.field();
                if one[field] == other[field] && !one[field].is_null() {
                    entry.0 += 1;
                }
            }
            PairAgreement {
                left,
                right,
                basis: *basis,
                per_shape,
            }
        })
        .collect()
}

/// A timing ratio is printed only over answers that agreed; a ratio over a
/// disagreement would be comparing the speeds of different answers, and the
/// three-way run that skipped this rule published exactly that table.
fn gated_ratio(
    agreements: &[PairAgreement],
    left: &str,
    right: &str,
    shape: &str,
    numerator: &Value,
    denominator: &Value,
) -> String {
    let Some(pair) = agreements
        .iter()
        .find(|pair| pair.left == left && pair.right == right)
    else {
        return "null".to_string();
    };
    if pair.shape_agrees(shape) {
        ratio(numerator, denominator)
    } else {
        let (same, all) = pair.shape(shape);
        format!("withheld ({}/{all} disagree)", all - same)
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().unwrap_or_else(|| {
        eprintln!("usage: compare_report <results-dir> <output.md>");
        std::process::exit(2);
    });
    let out = args.next().unwrap_or_else(|| {
        eprintln!("usage: compare_report <results-dir> <output.md>");
        std::process::exit(2);
    });
    let dir = Path::new(&dir);

    let bed = read_json(dir, "bed.json");
    let load: BTreeMap<&str, Value> = TARGETS
        .iter()
        .map(|target| (*target, read_json(dir, &format!("load_{target}.json"))))
        .collect();
    let seed: BTreeMap<&str, Value> = TARGETS
        .iter()
        .map(|target| (*target, read_json(dir, &format!("seed_{target}.json"))))
        .collect();
    let matrix: BTreeMap<&str, Value> = TARGETS
        .iter()
        .map(|target| (*target, read_json(dir, &format!("matrix_{target}.json"))))
        .collect();

    let agreements = compute_agreements(&matrix);

    let mut page = String::new();
    header(&mut page, &bed);
    reproduction(&mut page, &bed);
    what_was_compared(&mut page, &bed, dir);
    ingest_table(&mut page, &load);
    // Agreement is printed before any timing, and the timing tables refuse a
    // ratio for a shape whose answers disagreed. The order is the rule: a fast
    // wrong answer is not a win, so the reader meets the check before the
    // race.
    row_equality(&mut page, &matrix, &agreements);
    query_table(&mut page, &matrix, &agreements);
    query_limits(&mut page, &bed, dir, &agreements);
    memory_table(&mut page, &bed, &load);
    disk_table(&mut page, &bed, &load, &seed);
    object_store(&mut page, &bed);
    verdict(&mut page, &bed, &matrix, &load, &agreements);
    distrust(&mut page, &bed, &load, &matrix);
    configuration(&mut page, dir);

    if let Err(error) = std::fs::write(&out, page) {
        eprintln!("failed to write {out}: {error}");
        std::process::exit(1);
    }
}

/// The query matrix is run once per `limit`, and this is why.
///
/// The first published run used one limit, 20000, over windows holding about
/// 1250 matching rows — so no bound ever reached the scan, and the table could
/// not see a bounded executor at all. Reporting only the limit where a bound
/// binds would be choosing the flattering condition; reporting only the
/// original would keep measuring nothing. Both are here.
///
/// Emitted only when the bed ran more than one, so a single-limit run's
/// document is unchanged.
fn query_limits(page: &mut String, bed: &Value, dir: &Path, agreements: &[PairAgreement]) {
    let runs = match bed["matrix_runs"].as_array() {
        Some(runs) if runs.len() > 1 => runs,
        _ => return,
    };

    page.push_str(
        "## The same six shapes at each query limit\n\n\
A `limit` above the number of rows a window holds never reaches the scan, so it \
measures an engine that cannot stop early as if it were one that can. The rows \
below are the same queries over the same dataset, differing only in the bound \
Grafana would have sent.\n\n\
| shape | limit | loggytracy cold p50 | Loki cold p50 | VictoriaLogs cold p50 | lt / Loki | lt / VL | loggytracy lines read | Loki lines read | rows returned |\n\
|---|---|---|---|---|---|---|---|---|---|\n",
    );

    for shape in SHAPES {
        for run in runs {
            let limit = &run["limit"];
            let suffix = run["suffix"].as_str().unwrap_or("");
            let lt = read_json(dir, &format!("matrix_loggytracy{suffix}.json"));
            let lk = read_json(dir, &format!("matrix_loki{suffix}.json"));
            let vl = read_json(dir, &format!("matrix_victorialogs{suffix}.json"));
            let lt_shape = &lt["matrix"]["shapes"][shape]["cold_ms"];
            let lk_shape = &lk["matrix"]["shapes"][shape]["cold_ms"];
            let vl_shape = &vl["matrix"]["shapes"][shape]["cold_ms"];
            page.push_str(&format!(
                "| `{shape}` | {} | {} | {} | {} | **{}** | **{}** | {} | {} | {} |\n",
                num(limit),
                num(&lt_shape["p50_ms"]),
                num(&lk_shape["p50_ms"]),
                num(&vl_shape["p50_ms"]),
                gated_ratio(
                    agreements,
                    "loggytracy",
                    "loki",
                    shape,
                    &lt_shape["p50_ms"],
                    &lk_shape["p50_ms"]
                ),
                gated_ratio(
                    agreements,
                    "loggytracy",
                    "victorialogs",
                    shape,
                    &lt_shape["p50_ms"],
                    &vl_shape["p50_ms"]
                ),
                lines_read(&lt, shape),
                lines_read(&lk, shape),
                rows_returned(&lt, shape),
            ));
        }
    }

    page.push_str(
        "\nMilliseconds, and a ratio above `1.00x` means loggytracy took longer. \
\"Lines read\" is each system's own `data.stats.summary.totalLinesProcessed`, \
summed over the shape's answers — what the engine had to touch to produce the \
answer, which is the quantity pruning and early termination exist to reduce. \
It is reported rather than gated: the systems count it in their own terms, and \
VictoriaLogs does not report it at all. The agreement gate on the ratio columns \
is the primary run's; each limit's own agreement is in its artifact.\n\n",
    );
}

fn lines_read(matrix: &Value, shape: &str) -> String {
    let total: u64 = matrix["matrix"]["answers"]
        .as_array()
        .map(|answers| {
            answers
                .iter()
                .filter(|answer| answer["shape"].as_str() == Some(shape))
                .filter_map(|answer| answer["lines_processed"].as_u64())
                .sum()
        })
        .unwrap_or(0);
    if total == 0 {
        "not reported".to_string()
    } else {
        total.to_string()
    }
}

fn rows_returned(matrix: &Value, shape: &str) -> String {
    let total: u64 = matrix["matrix"]["answers"]
        .as_array()
        .map(|answers| {
            answers
                .iter()
                .filter(|answer| answer["shape"].as_str() == Some(shape))
                .filter_map(|answer| answer["rows"].as_u64())
                .sum()
        })
        .unwrap_or(0);
    total.to_string()
}

fn read_json(dir: &Path, name: &str) -> Value {
    let path = dir.join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        eprintln!("{}: {error}", path.display());
        std::process::exit(1);
    });
    serde_json::from_str(&text).unwrap_or_else(|error| {
        eprintln!("{}: {error}", path.display());
        std::process::exit(1);
    })
}

fn read_text(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap_or_else(|_| String::new())
}

/// A number the run did not produce prints as `null`, never as zero and never
/// as a dash that reads like "small". This is the M8 rule (`stats.rs`,
/// `min_samples_for`) applied to the document: a percentile the sample count
/// could not support is `null` here too, and `null` is not a pass.
fn num(value: &Value) -> String {
    match value {
        Value::Number(number) => match number.as_f64() {
            Some(number) if number.fract() == 0.0 && number.abs() < 1e15 => format!("{number:.0}"),
            Some(number) if number.abs() >= 100.0 => format!("{number:.0}"),
            Some(number) if number.abs() >= 10.0 => format!("{number:.1}"),
            Some(number) => format!("{number:.2}"),
            None => number.to_string(),
        },
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn f64_of(value: &Value) -> Option<f64> {
    value.as_f64()
}

fn ratio(numerator: &Value, denominator: &Value) -> String {
    match (f64_of(numerator), f64_of(denominator)) {
        (Some(a), Some(b)) if b > 0.0 => format!("{:.2}x", a / b),
        _ => "null".to_string(),
    }
}

fn mib(value: &Value) -> String {
    match f64_of(value) {
        Some(bytes) => format!("{:.1} MiB", bytes / (1024.0 * 1024.0)),
        None => "null".to_string(),
    }
}

fn header(page: &mut String, bed: &Value) {
    page.push_str(&format!(
        r#"# loggytracy against Loki and VictoriaLogs

**Generated by `compare/run.sh` on {}, from revision `{}` (`{}`). Do not edit
this file: it is regenerated from the result JSON in `target/compare/`, and the
last time this repository kept a hand-written performance document beside its
artifacts the two disagreed on both the build and the verdict.**

`docs/VISION.md` states the claim this bed exists to test, and states it as a
falsifiable one:

> At an equal container memory limit, on the same corpus and the same machine,
> loggytracy answers `{{...}} | field="value"` over structured metadata — the
> shape an OTLP attribute produces — in materially less time than Loki, which
> does not index it, and not materially worse than VictoriaLogs, which
> columnizes it, without giving up ingest throughput or disk footprint.

It also says what publishing it honestly requires: *"Publishing the comparison
means publishing it when it loses."* The verdict is below, computed from the
numbers rather than asserted — and no timing ratio in this document is printed
for a shape whose answers disagreed, because a fast wrong answer is not a win.

"#,
        bed["generated_at"].as_str().unwrap_or("unknown"),
        bed["revision"].as_str().unwrap_or("unknown"),
        bed["branch"].as_str().unwrap_or("unknown"),
    ));
}

fn reproduction(page: &mut String, bed: &Value) {
    page.push_str(&format!(
        r#"## Reproducing this

```
compare/run.sh
```

That builds the loggytracy image, brings all three systems up under
`compare/docker-compose.yml` at `{}` per container, runs every phase, and
rewrites this file. It takes minutes rather than hours; the run this document
was generated from settled for {} seconds between ingest and query.

There is deliberately no other way to run a three-way comparison. The first
three-way numbers came from an ad-hoc shell loop that bypassed this script and
with it the agreement check, and a timing table went out over answers nobody
had compared — two of its six shapes were not even answering the same
question. The script is the path, and the check is on the path.

The knobs are defaults, not assignments — `COMPARE_MEMORY`,
`COMPARE_MEMORY_LIMITS`, `COMPARE_LOAD_EPS`, `COMPARE_LOAD_EVENTS`,
`COMPARE_VERIFY_ROWS`, `COMPARE_MATRIX_REPEATS`, `COMPARE_SETTLE_SECONDS`,
`COMPARE_SEED` — so a reader can vary one without editing the script.

Every number below comes from the JSON in
[`artifacts/m9/`](artifacts/m9/), which the same run copied out of
`target/compare/` — so the artifacts and the document cannot disagree about
which run they describe. That is not a formality: of the numbers this
repository retired, one cited artifact did not exist and another disagreed with
the document citing it on both build revision and verdict.

"#,
        bed["memory_limit"].as_str().unwrap_or("?"),
        num(&bed["settle_seconds"]),
    ));
}

fn what_was_compared(page: &mut String, bed: &Value, dir: &Path) {
    let loki_build: Value =
        serde_json::from_str(&read_text(dir, "loki_buildinfo.json")).unwrap_or(Value::Null);
    page.push_str(&format!(
        r#"## What was compared

| | loggytracy | Loki | VictoriaLogs |
|---|---|---|---|
| build | `{}` (`{}`) | `{}` (`{}`) | `{}` |
| image | built from `Dockerfile` at that revision | `{}` | `{}` |
| storage | local filesystem, **no object store** | local filesystem (`common.storage.filesystem`) | local filesystem (`-storageDataPath`) |
| index | Parquet parts plus `index.bin` sidecars | TSDB, schema `v13`, 24h period | per-block columns plus its own indexdb |
| data model | stores the line, parses at query time | stores the line, parses at query time | stores the line, parses at query time (its ingest-time JSON parse is a property of its Loki push endpoint, which this bed no longer uses) |
| memory limit | {} | identical | identical (`mem_limit` and `memswap_limit`) |
| CPU limit | none | none | none |
| volume | `loggytracy-data` | `loki-data` | `victorialogs-data` |

Machine: {}. Docker {}, Compose {}.

The data-model row changed with the ingest protocol. Under Loki push,
VictoriaLogs parsed a JSON line into fields at write time and did not keep the
line; under OTLP it stores the body as `_msg` unparsed (measured, v1.52.0), so
a field the other two reach through `| json` it now reaches through
`| unpack_json` — the parser stage is paid at query time on all three, and the
`json_field` / `metadata_rare` pair separates parsed-line from attribute
storage on every system. What still differs is what a row *is*: VictoriaLogs
answers with every field it holds where the other two answer with what the
pipeline produced, which is why every pair containing it is compared on the
reduced basis below.

**Why loggytracy runs with no object store.** Loki's filesystem backend keeps
exactly one durable copy of a chunk on local disk, and so does VictoriaLogs.
Pointing loggytracy at a `file://` object store would make it keep a local part
*and* a remote copy of the same part, so the bytes-on-disk axis would be
measuring the bed rather than the engine. Local-only is the configuration that
makes the three comparable. It is also the configuration in which loggytracy's
object-store request counters are all zero, which is half of why that axis is
deferred below.

Everything else on the loggytracy side is the engine's own default. The
container sets three variables — the listen addresses and the data directory —
and nothing else. Its full environment and its startup log are at the end of
this document, beside Loki's config diff and VictoriaLogs' non-default flags.

"#,
        bed["revision"].as_str().unwrap_or("unknown"),
        bed["branch"].as_str().unwrap_or("unknown"),
        loki_build["version"].as_str().unwrap_or("unknown"),
        loki_build["revision"].as_str().unwrap_or("unknown"),
        bed["victorialogs_image"]
            .as_str()
            .unwrap_or("unknown")
            .rsplit(':')
            .next()
            .unwrap_or("unknown"),
        bed["loki_image"].as_str().unwrap_or("unknown"),
        bed["victorialogs_image"].as_str().unwrap_or("unknown"),
        bed["memory_limit"].as_str().unwrap_or("?"),
        bed["machine"].as_str().unwrap_or("unknown"),
        bed["docker"].as_str().unwrap_or("unknown"),
        bed["compose"].as_str().unwrap_or("unknown"),
    ));
}

fn ingest_table(page: &mut String, load: &BTreeMap<&str, Value>) {
    let row = |label: &str, pointer: &str| -> String {
        let mut cells = String::new();
        for target in TARGETS {
            cells.push_str(&format!(
                " {} |",
                num(load[target].pointer(pointer).unwrap_or(&Value::Null))
            ));
        }
        format!("| {label} |{cells}\n")
    };
    page.push_str(
        r#"## Ingest

Same corpus, same seed, same offered rate, same number of connections, same
wire format — every system ingests the identical OTLP protobuf body at its own
`/v1/logs` spelling, so these are the same bytes sent three times, and they are
the bytes the one intended consumer sends.
The runs are **sequential**, not concurrent, so no system's throughput is a
function of what the others were doing with the same twelve cores; the cost of
that choice is that a later run starts with a warmer page cache, and it is the
only asymmetry in this phase.

Every run stops at the same event target rather than after the same time, so
all three end up holding the same number of entries. `ended_on` says which
condition actually stopped each one: a run that says `duration_cap` did not
reach the target, and its data volume is smaller than the others'.

Latency is reported twice. **Service time** starts when the bytes went out;
**response time** starts when the pacer intended them to. Their gap is the
delay the offered rate could not be issued at — the correction M8 added, and
the one whose absence let the retired numbers report a healthy p99 beside an
achieved rate a sixth of the offered one.

"#,
    );
    page.push_str("| | loggytracy | Loki | VictoriaLogs |\n|---|---|---|---|\n");
    page.push_str(&row("offered eps", "/ingest/offered_eps"));
    page.push_str(&row("**achieved eps**", "/ingest/achieved_eps"));
    page.push_str(&row("events accepted", "/ingest/events_accepted"));
    page.push_str(&row("elapsed s", "/run/elapsed_seconds"));
    page.push_str(&row("stopped on", "/run/ended_on"));
    page.push_str(&row("pushes accepted", "/ingest/pushes_accepted"));
    page.push_str(&row("pushes throttled (429)", "/ingest/pushes_throttled"));
    page.push_str(&row("pushes failed", "/ingest/pushes_failed"));
    page.push_str(&row("error rate", "/ingest/error_rate"));
    page.push_str(&row("throttled rate", "/ingest/throttled_rate"));
    page.push_str(&row(
        "push service p50 ms",
        "/push_latency_ms/service/p50_ms",
    ));
    page.push_str(&row(
        "push service p95 ms",
        "/push_latency_ms/service/p95_ms",
    ));
    page.push_str(&row(
        "push service p99 ms",
        "/push_latency_ms/service/p99_ms",
    ));
    page.push_str(&row(
        "push response p50 ms",
        "/push_latency_ms/response/p50_ms",
    ));
    page.push_str(&row(
        "push response p95 ms",
        "/push_latency_ms/response/p95_ms",
    ));
    page.push_str(&row(
        "push response p99 ms",
        "/push_latency_ms/response/p99_ms",
    ));
    page.push_str(&row("latency samples", "/push_latency_ms/service/count"));
    page.push_str(&row("line bytes offered", "/ingest/line_bytes"));
    page.push_str(&row("wire bytes (OTLP protobuf)", "/ingest/wire_bytes"));
    page.push_str(&row(
        "TCP connections opened",
        "/ingest/tcp_connections_opened",
    ));

    let discarded = load["loki"]
        .pointer("/behavioral/discarded_samples")
        .unwrap_or(&Value::Null);
    let dropped = load["victorialogs"]
        .pointer("/behavioral/rows_dropped")
        .unwrap_or(&Value::Null);
    page.push_str(&format!(
        r#"
Loki discarded **{}** samples and VictoriaLogs dropped **{}** rows during their
runs. Every deviation this bed makes from either system's defaults exists to
keep those numbers at zero — a non-zero value would be this bed throttling a
system where loggytracy is unthrottled, which is a misconfiguration reported as
a loss, and that is the same defect as a rigged win.

Queries ran concurrently with ingest on every side at {} qps — in each
system's own query language, at its own endpoint — so reads contended with
writes; those latencies are not the query measurement and are not reported
here. The query measurement is below, taken on a quiescent system over a
dataset all three hold identically.

"#,
        num(discarded),
        num(dropped),
        num(load["loggytracy"]
            .pointer("/config/query_eps")
            .unwrap_or(&Value::Null)),
    ));
}

fn query_table(page: &mut String, matrix: &BTreeMap<&str, Value>, agreements: &[PairAgreement]) {
    page.push_str(&format!(
        r#"## The six query shapes

Every system was **restarted** after the settle and before this phase, so
"cold" is a process that has just started. Each shape is issued as
`apps x sub-windows` distinct queries over absolute time ranges of the
verification dataset; **cold** is the first issue of each, after every other
query of every other shape has already run, and **warm** is the {} repeats that
follow. All three cache — Loki has an embedded result cache on by default,
loggytracy has resident part sidecars, VictoriaLogs has its own caches, and the
page cache sits under everything — so reporting one number would hide which was
being measured.

The dataset is {} rows over {} streams at fixed timestamps, pushed identically
to every system (`src/bin/load/matrix.rs`). One request at a time, one
connection: this is a latency instrument, not a throughput one.

A ratio cell reading `withheld` is the agreement gate above doing its job: that
shape's answers disagreed, and the speed of a different answer is not a
measurement of anything.

"#,
        num(&matrix["loggytracy"]["config"]["verify"]["repeats"]),
        num(&matrix["loggytracy"]["verify"]["rows"]),
        num(&matrix["loggytracy"]["verify"]["streams"]),
    ));

    page.push_str(
        "| shape | pass | loggytracy p50 | Loki p50 | VictoriaLogs p50 | \
loggytracy p95 | Loki p95 | VictoriaLogs p95 | lt / Loki (p50) | lt / VL (p50) |\n\
|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for shape in SHAPES {
        for pass in ["cold_ms", "warm_ms"] {
            let lt = &matrix["loggytracy"]["matrix"]["shapes"][shape][pass];
            let lk = &matrix["loki"]["matrix"]["shapes"][shape][pass];
            let vl = &matrix["victorialogs"]["matrix"]["shapes"][shape][pass];
            page.push_str(&format!(
                "| `{shape}` | {} | {} | {} | {} | {} | {} | {} | **{}** | **{}** |\n",
                pass.trim_end_matches("_ms"),
                num(&lt["p50_ms"]),
                num(&lk["p50_ms"]),
                num(&vl["p50_ms"]),
                num(&lt["p95_ms"]),
                num(&lk["p95_ms"]),
                num(&vl["p95_ms"]),
                gated_ratio(
                    agreements,
                    "loggytracy",
                    "loki",
                    shape,
                    &lt["p50_ms"],
                    &lk["p50_ms"]
                ),
                gated_ratio(
                    agreements,
                    "loggytracy",
                    "victorialogs",
                    shape,
                    &lt["p50_ms"],
                    &vl["p50_ms"]
                ),
            ));
        }
    }
    page.push_str(
        "\nMilliseconds. A ratio above `1.00x` means loggytracy took longer. A `null` \
percentile is one the sample count could not support — `stats.rs::min_samples_for` \
refuses `p99` under 101 samples and `p95` under 21, and `null` is not a pass. p99 \
is in the artifacts.\n\n",
    );

    page.push_str("The expressions, one per shape (the LogQL side; the LogsQL translation is `logsql()` in `src/bin/load/matrix.rs`):\n\n");
    for shape in SHAPES {
        page.push_str(&format!(
            "* `{shape}` — `{}`\n",
            matrix["loggytracy"]["matrix"]["shapes"][shape]["expression_example"]
                .as_str()
                .unwrap_or("?"),
        ));
    }
    page.push_str(
        r#"
`rate` is `sum(rate(...))` rather than a bare `rate(...)`, and that is a
measured decision rather than a stylistic one. Loki promotes structured
metadata into a metric's identity, so a bare `rate()` over this corpus returns
one series per `trace_id` on Loki and one series per stream on loggytracy —
neither the same amount of work nor a comparable answer. Summed, all three have
to produce the same number, which is what the row-equality check tests. Its
window equals the query step, because that is the one configuration in which
LogQL's sliding window and LogsQL's tumbling `_time` buckets ask the same
question.

`json_field`, `json_field_rare` and `metadata_rare` are three different
questions to the two systems that parse at query time and **one question asked
three ways** to VictoriaLogs, which parsed at ingest. Read those rows together:
the spread between them on the loggytracy side is what the parser stage and
the storage each cost, and the absence of a spread on the VictoriaLogs side is
its design.

"#,
    );
}

fn row_equality(page: &mut String, matrix: &BTreeMap<&str, Value>, agreements: &[PairAgreement]) {
    let lt = indexed_answers(matrix, "loggytracy");
    let lk = indexed_answers(matrix, "loki");

    let mut mismatches: Vec<String> = Vec::new();
    // Disagreements are grouped by the label difference they carry, because the
    // interesting statement is "these 24 queries differ in the same way", not
    // the same paragraph twenty-four times.
    let mut label_groups: BTreeMap<(Vec<String>, Vec<String>), Vec<String>> = BTreeMap::new();
    let mut off_by_one = 0u64;
    for (id, left) in &lt {
        let Some(right) = lk.get(id) else { continue };
        if left["digest"] == right["digest"] && !left["digest"].is_null() {
            continue;
        }
        if left["rows"].as_u64() == right["rows"].as_u64().map(|rows| rows + 1) {
            off_by_one += 1;
        }
        let only_left = only_in(&left["label_keys"], &right["label_keys"]);
        let only_right = only_in(&right["label_keys"], &left["label_keys"]);
        mismatches.push(format!(
            "| `{id}` | {} | {} | `{}` | `{}` | {} |",
            num(&left["rows"]),
            num(&right["rows"]),
            left["digest"].as_str().unwrap_or("null"),
            right["digest"].as_str().unwrap_or("null"),
            if only_left.is_empty() && only_right.is_empty() {
                "no".to_string()
            } else {
                format!("{} / {}", only_left.len(), only_right.len())
            },
        ));
        if !(only_left.is_empty() && only_right.is_empty()) {
            label_groups
                .entry((only_left, only_right))
                .or_default()
                .push(id.clone());
        }
    }
    let strict = &agreements[0];
    let total: u64 = strict.per_shape.values().map(|(_, all)| *all).sum();
    let agreed: u64 = strict.per_shape.values().map(|(same, _)| *same).sum();

    page.push_str(&format!(
        r#"## Do they return the same rows?

This is the check that matters more than any timing, because a fast wrong
answer is not a win — and **no timing ratio below is printed for a shape whose
answers disagreed here**. All three systems hold the same entries by
construction — the same generator, the same seed, the same fixed log
timestamps — so the same query over the same absolute window has one right
answer.

Two bases, because there are two kinds of pair. Between the two systems that
store the line — loggytracy and Loki — each response is reduced to an
order-independent **strict** digest over the whole answer: one record per
returned entry holding its timestamp, its line, and every label that entry
carried together with where the response put it. A response that returns the
right lines under the wrong stream is therefore not equal. For `sum(rate(...))`
the digest is one record per series identity — so an extra empty series cannot
hide — plus one per sample, compared at six decimals.

Any pair containing VictoriaLogs is compared on the **reduced** basis: each
row's nanosecond timestamp plus the values of the fields the query itself
named. VictoriaLogs returns every field it holds for a row where the other two
answer with what the pipeline produced, so a basis of "all fields" compares
the storage models and disagrees always and everywhere — an earlier checker
did exactly that, reporting 0/24 zeros that were its own. What every system
returns for the same row is the row's time and the fields the query
constrained, so that is the basis, and it is deliberately the weaker claim of
the two. Field names are canonicalized the way the promoting systems sanitize
them — VictoriaLogs answers under the dotted `service.name` it was sent while
loggytracy and Loki answer under the promoted `service_name` — and a metric
answer's remaining representation difference — LogsQL labels a `_time` bucket
by its start where LogQL labels the sample by its evaluation point — is
converted with the query's own step, not exempted.

The digest was over `(timestamp, line)` pairs alone until this run, which is how
a `| json` label difference was once reported as 24 of 24 agreed (`todo.md`,
"Open correctness defects"). **No placement is exempt from it.** One was, for one
run: labels the seed pushed as structured metadata were digested without their
placement, because Loki promoted them into a log response's stream labels while
loggytracy returned them in the third element of each `values` tuple. That was
the same defect the `| json` shape was open as — the same slot — and it is fixed
rather than declared, so both sides now answer with one flat stream label set and
a two-element tuple, and a regression back into that slot is a disagreement here.

Two exemptions remain, both by name rather than by placement, both reported
below with counts rather than left implicit. `detected_level` is **dropped**,
because Loki derives it at ingest from the line and nothing in this bed pushes
one; `service_name` is no longer exempt — the OTLP encoder pushes the corpus's
`app` as `service.name`, so it is data every system is held to. And
`__error_details__` is compared by **presence with its value normalized**: an
answer missing the label is a disagreement — 16 of 24 `json_field_rare`
answers once were exactly that — while its wording is each engine's own parser
internals and matching it would be matching Loki's JSON library.

Two known differences are left, and neither is normalized away by the digest.
The metric step grid is handled by the query: `align_to_step` snaps every window
boundary to a whole `step`, so Loki's absolute-multiple alignment and
loggytracy's step-from-`start` produce the same instants. And an *unaggregated*
metric query still differs in series identity — Loki promotes a row's structured
metadata and extracted fields into it, loggytracy groups by stream labels and by
whatever the query names in `by`/`without` — which is why the matrix asks for
`sum(rate(...))`, and which is reported as a difference rather than hidden.

**loggytracy and Loki agreed on {agreed} of {total} queries on the strict
basis.** Every pair, per shape:

| shape | loggytracy = Loki (strict) | loggytracy = VictoriaLogs (reduced) | Loki = VictoriaLogs (reduced) |
|---|---|---|---|
"#
    ));
    for shape in SHAPES {
        let mut row = format!("| `{shape}` |");
        for pair in agreements {
            let (same, all) = pair.shape(shape);
            row.push_str(&format!(" {same} / {all} |"));
        }
        row.push('\n');
        page.push_str(&row);
    }
    for pair in agreements {
        if pair.basis == Basis::Reduced {
            let mut disagreeing: Vec<String> = Vec::new();
            let left = indexed_answers(matrix, pair.left);
            let right = indexed_answers(matrix, pair.right);
            for (id, one) in &left {
                if let Some(other) = right.get(id)
                    && (one["reduced_digest"] != other["reduced_digest"]
                        || one["reduced_digest"].is_null())
                {
                    disagreeing.push(format!(
                        "`{id}` ({} against {} rows)",
                        num(&one["rows"]),
                        num(&other["rows"])
                    ));
                }
            }
            if !disagreeing.is_empty() {
                page.push_str(&format!(
                    "\n**{} against {} disagreed on {} quer{}** (reduced basis): {}{}\n",
                    pair.left,
                    pair.right,
                    disagreeing.len(),
                    if disagreeing.len() == 1 { "y" } else { "ies" },
                    disagreeing
                        .iter()
                        .take(10)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", "),
                    if disagreeing.len() > 10 {
                        format!(", and {} more", disagreeing.len() - 10)
                    } else {
                        String::new()
                    },
                ));
            }
        }
    }
    if mismatches.is_empty() {
        page.push_str("\nNo strict-basis mismatches between loggytracy and Loki.\n");
    } else {
        page.push_str(&format!(
            "\n**{} queries disagreed.** This is a correctness finding and it is \
reported before any timing conclusion is drawn from the same run. The last \
column counts label names only loggytracy had against label names only Loki \
had.\n\n\
| query | loggytracy rows | Loki rows | loggytracy digest | Loki digest | labels only one side had |\n\
|---|---|---|---|---|---|\n{}\n\n",
            mismatches.len(),
            mismatches
                .iter()
                .take(20)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n"),
        ));
        if off_by_one > 0 {
            page.push_str(&format!(
                r#"In {off_by_one} of the {} disagreements loggytracy returned **exactly one row
more** than Loki, and that one row is the entry whose timestamp equals the
window's `end`. Checked directly against both endpoints over the same window:
loggytracy's `query_range` treats `end` as **inclusive**, Loki treats it as
**exclusive**, and both include `start`. Loki's is the contract loggytracy's
endpoint claims to implement, so this is a Loki-compatibility defect on the
loggytracy side, found by this check and recorded in `todo.md`. It is invisible
whenever no entry lands exactly on a window boundary, which is why an unaligned
window never surfaced it.

"#,
                mismatches.len()
            ));
        }
        if !label_groups.is_empty() {
            page.push_str(
                "Which labels differed, grouped by the difference. `stream:` is a label \
the response put in the stream's label set, `entry:` one it put in the entry's \
structured-metadata object, and `metric:` one in a series' identity:\n\n",
            );
            for ((only_left, only_right), ids) in &label_groups {
                page.push_str(&format!(
                    "* **{} quer{}** — {}{}:\n    * only loggytracy: {}\n    * only Loki: {}\n",
                    ids.len(),
                    if ids.len() == 1 { "y" } else { "ies" },
                    ids.iter()
                        .take(3)
                        .map(|id| format!("`{id}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if ids.len() > 3 {
                        format!(" and {} more", ids.len() - 3)
                    } else {
                        String::new()
                    },
                    join_labels(only_left),
                    join_labels(only_right),
                ));
            }
            page.push('\n');
        }
    }
    let vl = indexed_answers(matrix, "victorialogs");
    let all_answers: [(&str, &BTreeMap<String, Value>); 3] =
        [("loggytracy", &lt), ("Loki", &lk), ("VictoriaLogs", &vl)];
    page.push_str(&dropped_labels_note(&all_answers));
    page.push_str(&ordering_note(&all_answers));
    page.push_str(
        "\n`data.stats` is recorded per answer and deliberately outside the digest: it \
reports how much each engine had to read to produce the answer, which is the \
thing they are supposed to differ on, not part of the answer. VictoriaLogs \
reports no such counter, and its \"lines read\" cells below say so rather than \
printing zero.\n\n",
    );
}

/// Label names in `left`'s array that `right`'s does not have.
fn only_in(left: &Value, right: &Value) -> Vec<String> {
    let right: Vec<&str> = right
        .as_array()
        .map(|names| names.iter().filter_map(|name| name.as_str()).collect())
        .unwrap_or_default();
    left.as_array()
        .map(|names| {
            names
                .iter()
                .filter_map(|name| name.as_str())
                .filter(|name| !right.contains(name))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn join_labels(names: &[String]) -> String {
    if names.is_empty() {
        return "none".to_string();
    }
    names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The declared exemption, stated with the count of answers it applied to.
///
/// A digest that is narrower than it claims is the failure mode this whole
/// section exists to avoid, so what was left out is published beside what was
/// checked rather than only in the code.
fn dropped_labels_note(all: &[(&str, &BTreeMap<String, Value>)]) -> String {
    let mut note = String::new();
    for (target, answers) in all {
        let mut names: BTreeMap<String, u64> = BTreeMap::new();
        for answer in answers.values() {
            for name in answer["dropped_label_keys"]
                .as_array()
                .map(|names| names.as_slice())
                .unwrap_or_default()
            {
                if let Some(name) = name.as_str() {
                    *names.entry(name.to_string()).or_default() += 1;
                }
            }
        }
        if names.is_empty() {
            continue;
        }
        note.push_str(&format!(
            "* **{target}** returned {}, dropped from the digest by declaration.\n",
            names
                .iter()
                .map(|(name, count)| format!("`{name}` on {count} answers"))
                .collect::<Vec<_>>()
                .join(", "),
        ));
    }
    if note.is_empty() {
        return "\nNeither side returned a label the digest drops.\n".to_string();
    }
    format!("\nThe declared exemption, as it applied to this run:\n\n{note}")
}

/// The digest is order-independent on purpose, so the order a response came
/// back in has to be checked as its own fact.
fn ordering_note(all: &[(&str, &BTreeMap<String, Value>)]) -> String {
    let mut out = Vec::new();
    for (target, answers) in all {
        let offenders: Vec<&String> = answers
            .iter()
            .filter(|(_, answer)| answer["ordered"] == Value::Bool(false))
            .map(|(id, _)| id)
            .collect();
        if !offenders.is_empty() {
            out.push(format!(
                "* **{target}** answered {} quer{} outside the order it was asked for \
(`direction=backward` for logs, ascending time for a metric range), starting with \
`{}`.\n",
                offenders.len(),
                if offenders.len() == 1 { "y" } else { "ies" },
                offenders[0],
            ));
        }
    }
    if out.is_empty() {
        return "\nEvery answer on both sides came back in the order the query asked for; \
the digest itself is order-independent, so this is checked separately.\n"
            .to_string();
    }
    format!(
        "\nResponse order, checked separately because the digest is order-independent:\n\n{}",
        out.join("")
    )
}

fn memory_table(page: &mut String, bed: &Value, load: &BTreeMap<&str, Value>) {
    let peak = &bed["peak_bytes"];
    let attempts = bed["memory_limit_attempts"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if attempts.len() > 1 {
        page.push_str(
            r#"## The memory limit the comparison could actually run at

The bed sweeps container memory limits and runs the rest of the pipeline at the
first one **every** system survives. A limit where one of them is killed is not
a failed setup to be retried past quietly; it is an ingest result at that
limit, and it is the first row of this comparison.

| limit | loggytracy survived / OOM | Loki survived / OOM | VictoriaLogs survived / OOM |
|---|---|---|---|
"#,
        );
        for attempt in &attempts {
            let mut row = format!("| {} |", num(&attempt["limit"]));
            for target in TARGETS {
                row.push_str(&format!(
                    " {} / {} |",
                    num(&attempt[format!("{target}_survived")]),
                    num(&attempt[format!("{target}_oom_killed")]),
                ));
            }
            row.push('\n');
            page.push_str(&row);
        }
        page.push_str(&format!(
            r#"
The per-limit ingest results are kept as `load_<system>_<limit>.json` in
`target/compare/`. Everything after this section ran at **{}**.

"#,
            bed["memory_limit"].as_str().unwrap_or("?"),
        ));
    }
    let anon = |target: &str| -> String {
        mib(load[target]
            .pointer("/memory/anon_peak_bytes")
            .unwrap_or(&Value::Null))
    };
    page.push_str(&format!(
        r#"## Peak memory

cgroup v2 `memory.peak` per container: the kernel's own high-water mark for the
cgroup, which is the analogue of the `VmHWM` the harness reads for a local
process and the only figure comparable between a Rust process and two Go ones,
whose RSS is a statement about when their garbage collectors last ran.

The containers are restarted between the two phases, so each phase's peak is
measured against a fresh cgroup.

**`memory.peak` is not the process's footprint on its own.** A cgroup's memory
accounting includes the page cache its own file I/O created, and every system
here writes a write-ahead log or journal and then large data files, so all
carry reclaimable cache inside their limit. That was measured here, not
assumed: an ingest-only run drove `memory.peak` to exactly the 2 GiB limit
without being killed, while the same run with the query workload on was killed.
So the anonymous high-water mark is reported beside it — sampled from the
cgroup's `memory.stat` during the ingest phase — and it is the number an OOM
kill is actually decided on.

| | loggytracy | Loki | VictoriaLogs |
|---|---|---|---|
| limit | {} | identical | identical |
| `memory.peak` during ingest | {} | {} | {} |
| **anonymous peak during ingest** | {} | {} | {} |
| `memory.peak` during queries | {} | {} | {} |
| OOM-killed | {} | {} | {} |

"#,
        mib(&bed["memory_limit_bytes"]),
        mib(&peak["loggytracy"]["ingest"]),
        mib(&peak["loki"]["ingest"]),
        mib(&peak["victorialogs"]["ingest"]),
        anon("loggytracy"),
        anon("loki"),
        anon("victorialogs"),
        mib(&peak["loggytracy"]["query"]),
        mib(&peak["loki"]["query"]),
        mib(&peak["victorialogs"]["query"]),
        num(&peak["loggytracy"]["oom_killed"]),
        num(&peak["loki"]["oom_killed"]),
        num(&peak["victorialogs"]["oom_killed"]),
    ));
}

/// The write-ahead log's share of a system's volume.
///
/// Taken from the `du -sb <dir>/*` breakdown that `run.sh` recorded, which is
/// the raw measurement, rather than from a second reading: a component is
/// write-ahead log when its name says so, and everything else is settled data.
fn wal_bytes(bed: &Value, target: &str) -> f64 {
    bed["disk_bytes"][target]["breakdown"]
        .as_str()
        .unwrap_or("")
        .split(',')
        .filter_map(|entry| entry.rsplit_once(':'))
        .filter(|(path, _)| {
            let name = path.rsplit('/').next().unwrap_or(path);
            name.starts_with("wal") || name.starts_with("journal")
        })
        .filter_map(|(_, bytes)| bytes.parse::<f64>().ok())
        .sum()
}

fn disk_table(
    page: &mut String,
    bed: &Value,
    load: &BTreeMap<&str, Value>,
    seed: &BTreeMap<&str, Value>,
) {
    let disk = &bed["disk_bytes"];
    let ingested = |target: &str| -> f64 {
        f64_of(&load[target]["ingest"]["line_bytes"]).unwrap_or(0.0)
            + f64_of(&seed[target]["seed"]["line_bytes"]).unwrap_or(0.0)
    };
    let wal_of = |target: &str| wal_bytes(bed, target);
    let per_gb = |bytes: f64, source: f64| -> String {
        if source <= 0.0 {
            return "null".to_string();
        }
        format!(
            "{:.0} MiB / GB",
            (bytes / source) * (1_000_000_000.0 / (1024.0 * 1024.0))
        )
    };
    let settled = |target: &str| f64_of(&disk[target]["settled"]).unwrap_or(0.0);
    let bytes = |value: f64| mib(&Value::from(value));

    let mut rows = String::new();
    let mut push_row = |label: &str, cell: &dyn Fn(&str) -> String| {
        rows.push_str(&format!("| {label} |"));
        for target in TARGETS {
            rows.push_str(&format!(" {} |", cell(target)));
        }
        rows.push('\n');
    };
    push_row("line bytes ingested", &|target| {
        format!("{:.0}", ingested(target))
    });
    push_row("total on disk, settled", &|target| {
        mib(&disk[target]["settled"])
    });
    push_row("of which write-ahead log", &|target| bytes(wal_of(target)));
    push_row("settled data (total minus WAL)", &|target| {
        bytes(settled(target) - wal_of(target))
    });
    push_row("**total per GB ingested**", &|target| {
        per_gb(settled(target), ingested(target))
    });
    push_row("**settled data per GB ingested**", &|target| {
        per_gb(settled(target) - wal_of(target), ingested(target))
    });
    push_row("total after the query phase", &|target| {
        mib(&disk[target]["after_queries"])
    });

    page.push_str(&format!(
        r#"## Bytes on disk

Measured per volume with `du -sb` inside each container, after the settle and
before the query phase, so every system had the same chance to flush, cut
chunks and compact.

| | loggytracy | Loki | VictoriaLogs |
|---|---|---|---|
{rows}
Breakdown:

* loggytracy — `{}`
* Loki — `{}`
* VictoriaLogs — `{}`

**The total row and the settled-data row say different things and the second is
the fairer one.**
loggytracy compacts its write-ahead log only when an object store is
configured — `flush.rs:219` passes `remote_cache.is_some()` as the `compact`
flag, so in the local-only mode this bed runs it in, the checkpoint offset
advances and the file never shrinks. Everything ever ingested is still in it,
uncompressed, because the WAL stores the decompressed protobuf rather than the
client's snappy (`todo.md`, M11). Loki truncates its ingester WAL on flush, so
its WAL row is kilobytes.

That difference is a **property of the configuration this bed chose**, not of
the engine as it is meant to be deployed, and reporting only the total would be
charging loggytracy for the bed's decision — the same defect as a rigged win,
pointed the other way. The settled-data row is the like-for-like number: parts
and sidecars against chunks and TSDB index. The total row is what an operator
running local-only actually gets, and it is not small.

The settle was {} seconds. Loki's chunks were flushed explicitly first, because
Loki holds a chunk in its ingester until it has been idle for `chunk_idle_period`
(30 minutes) or has reached `max_chunk_age` (2 hours), and neither happens
inside a run that takes minutes; without the flush its disk number would be a
fact about the run's length. VictoriaLogs was told to flush its in-memory parts
for the same reason — its rows are not even *searchable* before that flush.
loggytracy flushes every five seconds at its default and needed no equivalent.

"#,
        disk["loggytracy"]["breakdown"].as_str().unwrap_or(""),
        disk["loki"]["breakdown"].as_str().unwrap_or(""),
        disk["victorialogs"]["breakdown"].as_str().unwrap_or(""),
        num(&bed["settle_seconds"]),
    ));
}

fn object_store(page: &mut String, _bed: &Value) {
    page.push_str(
        r#"## Object-store request counts: deferred, and why

The M9 brief lists object-store request counts as a fourth axis. **It is
deferred, deliberately, and not for want of a MinIO image.**

On a filesystem backend the two counters do not count the same thing.
loggytracy's `loggytracy_object_store_operations_total` counts calls into the
`object_store` crate, and with no object store configured — which is how it has
to run here, so that it keeps one durable copy of a part rather than two — it
counts zero. Loki's filesystem chunk client emits no request counter at all.
Two zeroes are not a comparison.

Making the axis real means putting **both** systems on MinIO, and that is a
different experiment rather than an extra column on this one: it changes
loggytracy's storage path end to end (restores, the local cache, the manifest
CAS preflight that refuses startup when conditional writes are not enforced)
and Loki's too (its chunk client, its shipper, its index gateway path). Both
would need their own settling and their own validation before any number off
them could be trusted, and running it half-configured would produce exactly the
kind of number this milestone exists to stop publishing.

What is already known and does not need this bed: loggytracy's counts are
structural and pinned by a test — four PUTs per part plus one GET and one PUT
per manifest commit, four DELETEs per retirement, two LISTs per orphan sweep
(`docs/LOAD_RESULTS.md` §9). What is missing is Loki's side of the same
arithmetic, and it stays missing until both run on the same object store.

"#,
    );
}

fn verdict(
    page: &mut String,
    bed: &Value,
    matrix: &BTreeMap<&str, Value>,
    load: &BTreeMap<&str, Value>,
    agreements: &[PairAgreement],
) {
    let shape = |name: &str, pass: &str, target: &str| -> Option<f64> {
        f64_of(&matrix[target]["matrix"]["shapes"][name][pass]["p50_ms"])
    };
    let pair_agrees = |left: &str, right: &str, name: &str| -> bool {
        agreements
            .iter()
            .find(|pair| pair.left == left && pair.right == right)
            .is_some_and(|pair| pair.shape_agrees(name))
    };
    let describe = |name: &str, pass: &str, other: &str, other_label: &str| -> String {
        if !pair_agrees("loggytracy", other, name) {
            return format!("**withheld** — the {name} answers disagree with {other_label}");
        }
        match (shape(name, pass, "loggytracy"), shape(name, pass, other)) {
            (Some(lt), Some(them)) if them > 0.0 => {
                let factor = lt / them;
                if factor < 0.9 {
                    format!(
                        "loggytracy is {:.2}x faster ({lt:.1} ms against {them:.1} ms)",
                        1.0 / factor
                    )
                } else if factor > 1.1 {
                    format!("loggytracy is {factor:.2}x slower ({lt:.1} ms against {them:.1} ms)")
                } else {
                    format!("within 10% ({lt:.1} ms against {them:.1} ms)")
                }
            }
            _ => "not measured".to_string(),
        }
    };
    let ingest_of = |target: &str| f64_of(&load[target]["ingest"]["achieved_eps"]).unwrap_or(0.0);
    let disk_line = {
        let data_of = |target: &str| {
            f64_of(&bed["disk_bytes"][target]["settled"]).unwrap_or(0.0) - wal_bytes(bed, target)
        };
        let lt = data_of("loggytracy");
        let lk = data_of("loki");
        let vl = data_of("victorialogs");
        if lk > 0.0 && vl > 0.0 {
            format!(
                "loggytracy's settled data is {:.2}x Loki's and {:.2}x VictoriaLogs' \
(write-ahead logs excluded on every side)",
                lt / lk,
                lt / vl,
            )
        } else {
            "not measured".to_string()
        }
    };
    // The claim's two halves, each decided only over agreeing answers:
    // materially faster than Loki (under 0.9x) and not materially worse than
    // VictoriaLogs (under 1.1x), on the shape an OTLP attribute produces.
    let against = |other: &str, threshold: f64, pass: &str| -> Option<bool> {
        if !pair_agrees("loggytracy", other, "metadata_rare") {
            return None;
        }
        shape("metadata_rare", pass, "loggytracy")
            .zip(shape("metadata_rare", pass, other))
            .map(|(lt, them)| lt < them * threshold)
    };
    let verdict_line = |pass: &str| -> String {
        match (
            against("loki", 0.9, pass),
            against("victorialogs", 1.1, pass),
        ) {
            (Some(true), Some(true)) => "**holds** — materially faster than Loki and not \
materially worse than VictoriaLogs"
                .to_string(),
            (Some(loki_ok), Some(vl_ok)) => format!(
                "**does not hold** — {} Loki's side, {} the VictoriaLogs side",
                if loki_ok { "survives" } else { "fails" },
                if vl_ok { "survives" } else { "fails" },
            ),
            _ => "**could not be decided** — the answers disagreed, and a verdict over \
disagreeing answers is the mistake this report refuses"
                .to_string(),
        }
    };

    page.push_str(&format!(
        r#"## The verdict on the claim

The claim is about one shape: `{{...}} | field="value"` over **structured
metadata**, with no parser stage — what an OTLP attribute produces. The three
systems genuinely differ here by design: loggytracy indexes structured metadata
into a per-row-group bloom, Loki stores it without indexing it, VictoriaLogs
turns it into a column. The claim has two halves and both must hold: materially
less time than Loki (below 0.9x), not materially worse than VictoriaLogs
(below 1.1x).

* `metadata_rare`, cold: {}
* `metadata_rare`, warm: {}
* against Loki, cold: {}
* against Loki, warm: {}
* against VictoriaLogs, cold: {}
* against VictoriaLogs, warm: {}
* ingest: loggytracy achieved {:.0} eps against Loki's {:.0} and VictoriaLogs' {:.0}
* disk: {}

The previous claim's shape stays measured beside it: `json_field` cold is {}
against Loki and {} against VictoriaLogs. That claim was measured, lost, and
replaced — moving the target does not retract the loss, and the row is here so
the reader can see it.

"#,
        verdict_line("cold_ms"),
        verdict_line("warm_ms"),
        describe("metadata_rare", "cold_ms", "loki", "Loki"),
        describe("metadata_rare", "warm_ms", "loki", "Loki"),
        describe("metadata_rare", "cold_ms", "victorialogs", "VictoriaLogs"),
        describe("metadata_rare", "warm_ms", "victorialogs", "VictoriaLogs"),
        ingest_of("loggytracy"),
        ingest_of("loki"),
        ingest_of("victorialogs"),
        disk_line,
        describe("json_field", "cold_ms", "loki", "Loki"),
        describe("json_field", "cold_ms", "victorialogs", "VictoriaLogs"),
    ));
}

fn distrust(
    page: &mut String,
    bed: &Value,
    load: &BTreeMap<&str, Value>,
    matrix: &BTreeMap<&str, Value>,
) {
    let mut items: Vec<String> = Vec::new();

    items.push(
        "**Loki answers this window from memory and loggytracy answers it from disk.** \
Loki keeps flushed chunks resident and `query_ingesters_within` is three hours \
by default, so every query in the matrix hits the ingester's in-memory chunks \
as well as the store. loggytracy has no equivalent: after a restart its rows \
are in Parquet parts and it reads them. Both are each system's default \
behaviour and neither was changed, but it means the query columns are not \
'two engines reading the same medium'. A comparison over a window older than \
Loki's ingester retention would answer this, and it needs a run measured in \
hours rather than minutes."
            .to_string(),
    );

    if let Some(attempts) = bed["memory_limit_attempts"].as_array()
        && attempts.len() > 1
    {
        items.push(format!(
            "**The published run is at {}, not at the {} the bed asks for first.** \
loggytracy was OOM-killed at the lower limit and Loki was not, so the query, \
disk and memory columns are all taken at a limit loggytracy needed and Loki \
did not. That is the single largest thing this comparison found, it is not a \
caveat about the measurement but a result, and it means the claim's phrase \
'at an equal container memory limit' is satisfied here only by raising the \
limit until the losing side fits.",
            num(&bed["memory_limit"]),
            num(&attempts[0]["limit"]),
        ));
    }

    let wal_share = {
        let total = f64_of(&bed["disk_bytes"]["loggytracy"]["settled"]).unwrap_or(0.0);
        if total > 0.0 {
            wal_bytes(bed, "loggytracy") / total
        } else {
            0.0
        }
    };
    if wal_share > 0.25 {
        items.push(format!(
            "**{:.0}% of loggytracy's disk number is a write-ahead log the bed's own \
configuration stops it from compacting.** `flush.rs:219` compacts the WAL only \
when an object store is configured, and this bed runs loggytracy local-only so \
that it keeps one durable copy of a part rather than two. The two choices are \
in tension and I did not find a configuration that avoids both; the table \
reports the total and the WAL-excluded figure separately rather than picking \
one. The WAL-excluded row is the one to compare engines on, and the total is \
the one an operator running local-only actually gets.",
            wal_share * 100.0
        ));
    }

    items.push(format!(
        "**The ingest runs are sequential, and loggytracy went first.** A later run \
starts with a warmer page cache and a machine that has just finished doing \
work. Running them concurrently would remove that and introduce a worse \
problem — each system's throughput would depend on the others' — so this is a \
chosen bias rather than an overlooked one, and its direction favours the \
systems that ran later ({} / {} / {} achieved eps in run order).",
        num(&load["loggytracy"]["ingest"]["achieved_eps"]),
        num(&load["loki"]["ingest"]["achieved_eps"]),
        num(&load["victorialogs"]["ingest"]["achieved_eps"]),
    ));

    items.push(
        "**\"Cold\" means a restarted process, not a cold page cache.** Dropping the \
host page cache needs root, so both systems' data files may still be in it \
when the cold pass runs. Both get exactly the same treatment, so the \
comparison is fair, but neither cold column is a cold-storage number."
            .to_string(),
    );

    items.push(format!(
        "**Loki's compaction interval was moved from 10 minutes to 1 minute.** \
Without it the index would still be uncompacted when a run that takes minutes \
ends, and Loki's disk number would be an artefact of the run length rather \
than a property of the engine. It costs Loki CPU and memory during the {}-second \
settle, which is charged to its memory peak.",
        num(&bed["settle_seconds"]),
    ));

    items.push(
        "**The verification dataset is small relative to a real window.** It is \
sized so a full run takes minutes; a pruning advantage that only appears at a \
much larger part count is not visible here, and neither is a scan cost that \
only hurts at one. The shape of the curve is not measured, only one point on \
it."
        .to_string(),
    );

    let ended: Vec<(&str, &str)> = TARGETS
        .iter()
        .map(|target| {
            (
                *target,
                load[target]["run"]["ended_on"].as_str().unwrap_or(""),
            )
        })
        .collect();
    if ended.iter().any(|(_, how)| *how != ended[0].1) {
        items.push(format!(
            "**The ingest runs did not all stop the same way** — {}. They therefore \
hold different amounts of data, and every axis downstream of that (disk, \
query, memory) is comparing different corpora sizes. Treat the disk and query \
columns as indicative only.",
            ended
                .iter()
                .map(|(target, how)| format!("{target} on `{how}`"))
                .collect::<Vec<_>>()
                .join(", "),
        ));
    }

    for target in TARGETS {
        let errors: u64 = SHAPES
            .iter()
            .filter_map(|shape| matrix[target]["matrix"]["shapes"][shape]["errors"].as_u64())
            .sum();
        if errors > 0 {
            items.push(format!(
                "**{errors} queries errored on {target}** and are excluded from its \
percentiles, so those percentiles describe the queries that succeeded rather \
than the workload that was offered."
            ));
        }
        let out_of_order: u64 = SHAPES
            .iter()
            .filter_map(|shape| {
                matrix[target]["matrix"]["shapes"][shape]["answers_out_of_requested_order"].as_u64()
            })
            .sum();
        if out_of_order > 0 {
            items.push(format!(
                "**{out_of_order} answers on {target} came back in an order the query did \
not ask for.** The row digest is order-independent, so this does not show up as \
a disagreement, but `direction=backward` is part of the contract a Logs panel \
relies on."
            ));
        }
        let unstable: u64 = SHAPES
            .iter()
            .filter_map(|shape| {
                matrix[target]["matrix"]["shapes"][shape]["warm_answers_differed"].as_u64()
            })
            .sum();
        if unstable > 0 {
            items.push(format!(
                "**{unstable} queries on {target} returned a different answer when \
repeated** over the same fixed window on a system nothing was writing to. That \
is a determinism problem, and it undermines the row-equality result for those \
queries."
            ));
        }
    }

    page.push_str("## What I do not trust about these numbers\n\n");
    for item in items {
        page.push_str(&format!("* {item}\n\n"));
    }
}

fn configuration(page: &mut String, dir: &Path) {
    let diff = read_text(dir, "loki_config.diff");
    let env = read_text(dir, "loggytracy_env.txt");
    let startup = read_text(dir, "loggytracy_startup.log");
    page.push_str(&format!(
        r#"## Configuration, in full

Publish enough that a reader can find the bias. Both systems' complete
configuration is below, and the raw dumps are in `target/compare/`
(`loki_config.yaml`, `loki_config_defaults.yaml`, `loggytracy_startup.log`).

### Loki: every deviation from its own defaults

This is `GET /config` against the running process, diffed against
`GET /config?mode=defaults` from the same process — Loki's own report, not this
repository's claim about it. (`?mode=diff` answers `unsupported type <nil>` in
3.3.2, so the diff is taken here.) Entries that only assign a path, a ring
store or a replication factor are consequences of `common.path_prefix` and of
running one process; the ones that matter are `ingestion_rate_mb`,
`per_stream_rate_limit`, `max_global_streams_per_user`, `max_query_series`,
`max_entries_limit_per_query`, `compaction_interval` and `log_level`, and
`compare/loki-config.yaml` gives the reason for each beside it.

Deliberately left at Loki's default, and checked rather than assumed:
`reject_old_samples_max_age` (1w, the same window as loggytracy's
`max_timestamp_age`), `unordered_writes` (true), `allow_structured_metadata`
(true on v13), `max_streams_per_user` (0), `max_line_size`, the query frontend's
splitting and its embedded result cache, and `querier.max_concurrent`.

```diff
{diff}```

### loggytracy: the container's whole configuration surface

```
{env}```

Everything else is `src/config.rs`'s default. Its startup log, which prints the
derived budgets:

```
{}```
"#,
        startup.lines().take(40).collect::<Vec<_>>().join("\n") + "\n",
    ));
}
