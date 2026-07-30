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

const SHAPES: [&str; 4] = ["label_only", "line_filter", "json_field", "rate"];
const TARGETS: [&str; 2] = ["loggytracy", "loki"];

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

    let mut page = String::new();
    header(&mut page, &bed);
    reproduction(&mut page, &bed);
    what_was_compared(&mut page, &bed, dir);
    ingest_table(&mut page, &load);
    query_table(&mut page, &matrix);
    query_limits(&mut page, &bed, dir);
    row_equality(&mut page, &matrix);
    memory_table(&mut page, &bed, &load);
    disk_table(&mut page, &bed, &load, &seed);
    object_store(&mut page, &bed);
    verdict(&mut page, &bed, &matrix, &load);
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
fn query_limits(page: &mut String, bed: &Value, dir: &Path) {
    let runs = match bed["matrix_runs"].as_array() {
        Some(runs) if runs.len() > 1 => runs,
        _ => return,
    };

    page.push_str(
        "## The same four shapes at each query limit\n\n\
A `limit` above the number of rows a window holds never reaches the scan, so it \
measures an engine that cannot stop early as if it were one that can. The rows \
below are the same queries over the same dataset, differing only in the bound \
Grafana would have sent.\n\n\
| shape | limit | loggytracy cold p50 | Loki cold p50 | ratio | loggytracy lines read | Loki lines read | rows returned |\n\
|---|---|---|---|---|---|---|---|\n",
    );

    for shape in SHAPES {
        for run in runs {
            let limit = &run["limit"];
            let suffix = run["suffix"].as_str().unwrap_or("");
            let lt = read_json(dir, &format!("matrix_loggytracy{suffix}.json"));
            let lk = read_json(dir, &format!("matrix_loki{suffix}.json"));
            let lt_shape = &lt["matrix"]["shapes"][shape]["cold_ms"];
            let lk_shape = &lk["matrix"]["shapes"][shape]["cold_ms"];
            page.push_str(&format!(
                "| `{shape}` | {} | {} | {} | **{}** | {} | {} | {} |\n",
                num(limit),
                num(&lt_shape["p50_ms"]),
                num(&lk_shape["p50_ms"]),
                ratio(&lt_shape["p50_ms"], &lk_shape["p50_ms"]),
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
It is reported rather than gated: the two count it in their own terms.\n\n",
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
        r#"# loggytracy against Loki

**Generated by `compare/run.sh` on {}, from revision `{}` (`{}`). Do not edit
this file: it is regenerated from the result JSON in `target/compare/`, and the
last time this repository kept a hand-written performance document beside its
artifacts the two disagreed on both the build and the verdict.**

`docs/VISION.md` states the claim this bed exists to test, and states it as a
falsifiable one:

> At an equal container memory limit, on the same corpus and the same machine,
> loggytracy answers `{{...}} | json | field="value"` over a window Loki must
> scan in materially less time, without giving up ingest throughput or disk
> footprint.

It also says what publishing it honestly requires: *"Publishing the comparison
means publishing it when it loses."* The verdict is below, computed from the
numbers rather than asserted.

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

That builds the loggytracy image, brings both systems up under
`compare/docker-compose.yml` at `{}` per container, runs every phase, and
rewrites this file. It takes minutes rather than hours; the run this document
was generated from settled for {} seconds between ingest and query.

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

| | loggytracy | Loki |
|---|---|---|
| build | `{}` (`{}`) | `{}` (`{}`) |
| image | built from `Dockerfile` at that revision | `{}` |
| storage | local filesystem, **no object store** | local filesystem (`common.storage.filesystem`) |
| index | Parquet parts plus `index.bin` sidecars | TSDB, schema `v13`, 24h period |
| container memory limit | {} | {} (identical, `mem_limit` and `memswap_limit`) |
| CPU limit | none | none |
| volume | `loggytracy-data` | `loki-data` |

Machine: {}. Docker {}, Compose {}.

**Why loggytracy runs with no object store.** Loki's filesystem backend keeps
exactly one durable copy of a chunk on local disk. Pointing loggytracy at a
`file://` object store would make it keep a local part *and* a remote copy of
the same part, so the bytes-on-disk axis would be measuring the bed rather than
the engine. Local-only is the configuration that makes the two comparable. It
is also the configuration in which loggytracy's object-store request counters
are all zero, which is half of why that axis is deferred below.

Everything else on the loggytracy side is the engine's own default. The
container sets three variables — the listen addresses and the data directory —
and nothing else. Its full environment and its startup log are at the end of
this document.

"#,
        bed["revision"].as_str().unwrap_or("unknown"),
        bed["branch"].as_str().unwrap_or("unknown"),
        loki_build["version"].as_str().unwrap_or("unknown"),
        loki_build["revision"].as_str().unwrap_or("unknown"),
        bed["loki_image"].as_str().unwrap_or("unknown"),
        bed["memory_limit"].as_str().unwrap_or("?"),
        bed["memory_limit"].as_str().unwrap_or("?"),
        bed["machine"].as_str().unwrap_or("unknown"),
        bed["docker"].as_str().unwrap_or("unknown"),
        bed["compose"].as_str().unwrap_or("unknown"),
    ));
}

fn ingest_table(page: &mut String, load: &BTreeMap<&str, Value>) {
    let lt = &load["loggytracy"];
    let lk = &load["loki"];
    let row = |label: &str, pointer: &str| -> String {
        format!(
            "| {label} | {} | {} |\n",
            num(lt.pointer(pointer).unwrap_or(&Value::Null)),
            num(lk.pointer(pointer).unwrap_or(&Value::Null)),
        )
    };
    page.push_str(
        r#"## Ingest

Same corpus, same seed, same offered rate, same number of connections, same
wire format — loggytracy's push endpoint is Loki's, so these are the same bytes
sent twice. The two runs are **sequential**, not concurrent, so neither
system's throughput is a function of what the other was doing with the same
twelve cores; the cost of that choice is that the second run started with a
warmer page cache, and it is the only asymmetry in this phase.

Both runs stop at the same event target rather than after the same time, so the
two systems end up holding the same number of entries. `ended_on` says which
condition actually stopped each one: a run that says `duration_cap` did not
reach the target, and its data volume is smaller than the other's.

Latency is reported twice. **Service time** starts when the bytes went out;
**response time** starts when the pacer intended them to. Their gap is the
delay the offered rate could not be issued at — the correction M8 added, and
the one whose absence let the retired numbers report a healthy p99 beside an
achieved rate a sixth of the offered one.

"#,
    );
    page.push_str("| | loggytracy | Loki |\n|---|---|---|\n");
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
    page.push_str(&row("wire bytes (snappy)", "/ingest/wire_bytes"));
    page.push_str(&row(
        "TCP connections opened",
        "/ingest/tcp_connections_opened",
    ));

    let discarded = lk
        .pointer("/behavioral/discarded_samples")
        .unwrap_or(&Value::Null);
    page.push_str(&format!(
        r#"
Loki discarded **{}** samples during its run. Every deviation this bed makes
from Loki's defaults exists to keep that number at zero — a non-zero value
would be this bed throttling Loki where loggytracy is unthrottled, which is a
misconfiguration reported as a loss, and that is the same defect as a rigged
win. The breakdown by reason is `behavioral.discarded_by_reason` in
`target/compare/load_loki.json`.

Queries ran concurrently with ingest on both sides at {} qps, so reads
contended with writes; those latencies are not the query measurement and are
not reported here. The query measurement is the next section, taken on a
quiescent system over a dataset both hold identically.

"#,
        num(discarded),
        num(lt.pointer("/config/query_eps").unwrap_or(&Value::Null)),
    ));
}

fn query_table(page: &mut String, matrix: &BTreeMap<&str, Value>) {
    page.push_str(&format!(
        r#"## The four query shapes

Both systems were **restarted** after the settle and before this phase, so
"cold" is a process that has just started. Each shape is issued as
`apps x sub-windows` distinct queries over absolute time ranges of the
verification dataset; **cold** is the first issue of each, after every other
query of every other shape has already run, and **warm** is the {} repeats that
follow. Both systems cache — Loki has an embedded result cache on by default,
loggytracy has resident part sidecars and the page cache under both — so
reporting one number would hide which was being measured.

The dataset is {} rows over {} streams at fixed timestamps, pushed identically
to both systems (`src/bin/load/matrix.rs`). One request at a time, one
connection: this is a latency instrument, not a throughput one.

"#,
        num(&matrix["loggytracy"]["config"]["verify"]["repeats"]),
        num(&matrix["loggytracy"]["verify"]["rows"]),
        num(&matrix["loggytracy"]["verify"]["streams"]),
    ));

    page.push_str(
        "| shape | pass | loggytracy p50 | Loki p50 | loggytracy p95 | Loki p95 | \
loggytracy p99 | Loki p99 | samples | loggytracy / Loki (p50) |\n\
|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for shape in SHAPES {
        for pass in ["cold_ms", "warm_ms"] {
            let lt = &matrix["loggytracy"]["matrix"]["shapes"][shape][pass];
            let lk = &matrix["loki"]["matrix"]["shapes"][shape][pass];
            page.push_str(&format!(
                "| `{shape}` | {} | {} | {} | {} | {} | {} | {} | {} / {} | **{}** |\n",
                pass.trim_end_matches("_ms"),
                num(&lt["p50_ms"]),
                num(&lk["p50_ms"]),
                num(&lt["p95_ms"]),
                num(&lk["p95_ms"]),
                num(&lt["p99_ms"]),
                num(&lk["p99_ms"]),
                num(&lt["count"]),
                num(&lk["count"]),
                ratio(&lt["p50_ms"], &lk["p50_ms"]),
            ));
        }
    }
    page.push_str(
        "\nMilliseconds. A ratio above `1.00x` means loggytracy took longer. A `null` \
percentile is one the sample count could not support — `stats.rs::min_samples_for` \
refuses `p99` under 101 samples and `p95` under 21, and `null` is not a pass.\n\n",
    );

    page.push_str("The expressions, one per shape:\n\n");
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
neither the same amount of work nor a comparable answer. Summed, both have to
produce the same number, which is what the row-equality check tests.

"#,
    );
}

fn row_equality(page: &mut String, matrix: &BTreeMap<&str, Value>) {
    let index = |target: &str| -> BTreeMap<String, Value> {
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
    };
    let lt = index("loggytracy");
    let lk = index("loki");

    let mut per_shape: BTreeMap<&str, (u64, u64)> =
        SHAPES.iter().map(|shape| (*shape, (0u64, 0u64))).collect();
    let mut mismatches: Vec<String> = Vec::new();
    // Disagreements are grouped by the label difference they carry, because the
    // interesting statement is "these 24 queries differ in the same way", not
    // the same paragraph twenty-four times.
    let mut label_groups: BTreeMap<(Vec<String>, Vec<String>), Vec<String>> = BTreeMap::new();
    let mut off_by_one = 0u64;
    for (id, left) in &lt {
        let Some(right) = lk.get(id) else { continue };
        let shape = left["shape"].as_str().unwrap_or("?");
        let entry = per_shape
            .entry(
                SHAPES
                    .iter()
                    .find(|name| **name == shape)
                    .copied()
                    .unwrap_or("?"),
            )
            .or_insert((0, 0));
        entry.1 += 1;
        if left["digest"] == right["digest"] && !left["digest"].is_null() {
            entry.0 += 1;
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
    let total: u64 = per_shape.values().map(|(_, all)| *all).sum();
    let agreed: u64 = per_shape.values().map(|(same, _)| *same).sum();

    page.push_str(&format!(
        r#"## Do they return the same rows?

This is the check that matters more than any timing, because a fast wrong
answer is not a win. Both systems hold the same entries by construction — the
same generator, the same seed, the same fixed log timestamps — so the same
query over the same absolute window has one right answer, and each response is
reduced to an order-independent digest over **the whole answer**: one record per
returned entry holding its timestamp, its line, and every label that entry
carried together with where the response put it. A response that returns the
right lines under the wrong stream is therefore not equal. For `sum(rate(...))`
the digest is one record per series identity — so an extra empty series cannot
hide — plus one per sample, compared at six decimals.

The digest was over `(timestamp, line)` pairs alone until this run, which is how
a `| json` label difference was once reported as 24 of 24 agreed (`todo.md`,
"Open correctness defects"). **No placement is exempt from it.** One was, for one
run: labels the seed pushed as structured metadata were digested without their
placement, because Loki promoted them into a log response's stream labels while
loggytracy returned them in the third element of each `values` tuple. That was
the same defect the `| json` shape was open as — the same slot — and it is fixed
rather than declared, so both sides now answer with one flat stream label set and
a two-element tuple, and a regression back into that slot is a disagreement here.

One exemption remains, and it is by name rather than by placement:
`detected_level` and `service_name` are **dropped**, because Loki derives them at
ingest from the line and the stream and nothing in this bed pushes either. Every
answer records which of them it carried, and that is reported below rather than
left implicit.

Two known differences are left, and neither is normalized away by the digest.
The metric step grid is handled by the query: `align_to_step` snaps every window
boundary to a whole `step`, so Loki's absolute-multiple alignment and
loggytracy's step-from-`start` produce the same instants. And an *unaggregated*
metric query still differs in series identity — Loki promotes a row's structured
metadata and extracted fields into it, loggytracy groups by stream labels and by
whatever the query names in `by`/`without` — which is why the matrix asks for
`sum(rate(...))`, and which is reported as a difference rather than hidden.

**{agreed} of {total} queries agreed.**

| shape | agreed |
|---|---|
"#
    ));
    for shape in SHAPES {
        let (same, all) = per_shape.get(shape).copied().unwrap_or((0, 0));
        page.push_str(&format!("| `{shape}` | {same} / {all} |\n"));
    }
    if mismatches.is_empty() {
        page.push_str("\nNo mismatches.\n");
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
    page.push_str(&dropped_labels_note(&lt, &lk));
    page.push_str(&ordering_note(&lt, &lk));
    page.push_str(
        "\n`data.stats` is recorded per answer and deliberately outside the digest: it \
reports how much each engine had to read to produce the answer, which is the \
thing the two are supposed to differ on, not part of the answer.\n\n",
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
fn dropped_labels_note(lt: &BTreeMap<String, Value>, lk: &BTreeMap<String, Value>) -> String {
    let mut note = String::new();
    for (target, answers) in [("loggytracy", lt), ("Loki", lk)] {
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
fn ordering_note(lt: &BTreeMap<String, Value>, lk: &BTreeMap<String, Value>) -> String {
    let mut out = Vec::new();
    for (target, answers) in [("loggytracy", lt), ("Loki", lk)] {
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
first one **both** systems survive. A limit where one of them is killed is not
a failed setup to be retried past quietly; it is an ingest result at that
limit, and it is the first row of this comparison.

| limit | loggytracy survived | OOM-killed | Loki survived | OOM-killed |
|---|---|---|---|---|
"#,
        );
        for attempt in &attempts {
            page.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                num(&attempt["limit"]),
                num(&attempt["loggytracy_survived"]),
                num(&attempt["loggytracy_oom_killed"]),
                num(&attempt["loki_survived"]),
                num(&attempt["loki_oom_killed"]),
            ));
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
process and the only figure comparable between a Rust process and a Go one,
whose RSS is a statement about when its garbage collector last ran.

The containers are restarted between the two phases, so each phase's peak is
measured against a fresh cgroup.

**`memory.peak` is not the process's footprint on its own.** A cgroup's memory
accounting includes the page cache its own file I/O created, and both systems
write a write-ahead log and then large data files, so both carry hundreds of
megabytes of reclaimable cache inside their limit. That was measured here, not
assumed: an ingest-only run drove `memory.peak` to exactly the 2 GiB limit
without being killed, while the same run with the query workload on was killed.
So the anonymous high-water mark is reported beside it — sampled from the
cgroup's `memory.stat` during the ingest phase — and it is the number an OOM
kill is actually decided on.

| | loggytracy | Loki |
|---|---|---|
| limit | {} | {} |
| `memory.peak` during ingest | {} | {} |
| **anonymous peak during ingest** | {} | {} |
| `memory.peak` during queries | {} | {} |
| OOM-killed | {} | {} |

"#,
        mib(&bed["memory_limit_bytes"]),
        mib(&bed["memory_limit_bytes"]),
        mib(&peak["loggytracy"]["ingest"]),
        mib(&peak["loki"]["ingest"]),
        anon("loggytracy"),
        anon("loki"),
        mib(&peak["loggytracy"]["query"]),
        mib(&peak["loki"]["query"]),
        num(&peak["loggytracy"]["oom_killed"]),
        num(&peak["loki"]["oom_killed"]),
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

    page.push_str(&format!(
        r#"## Bytes on disk

Measured per volume with `du -sb` inside each container, after the settle and
before the query phase, so both had the same chance to flush, cut chunks and
compact.

| | loggytracy | Loki |
|---|---|---|
| line bytes ingested | {:.0} | {:.0} |
| total on disk, settled | {} | {} |
| of which write-ahead log | {} | {} |
| settled data (total minus WAL) | {} | {} |
| **total per GB ingested** | {} | {} |
| **settled data per GB ingested** | {} | {} |
| total after the query phase | {} | {} |

Breakdown:

* loggytracy — `{}`
* Loki — `{}`

**The two rows say different things and the second is the fairer one.**
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
fact about the run's length. loggytracy flushes every five seconds at its
default and needed no equivalent.

"#,
        ingested("loggytracy"),
        ingested("loki"),
        mib(&disk["loggytracy"]["settled"]),
        mib(&disk["loki"]["settled"]),
        bytes(wal_of("loggytracy")),
        bytes(wal_of("loki")),
        bytes(settled("loggytracy") - wal_of("loggytracy")),
        bytes(settled("loki") - wal_of("loki")),
        per_gb(settled("loggytracy"), ingested("loggytracy")),
        per_gb(settled("loki"), ingested("loki")),
        per_gb(
            settled("loggytracy") - wal_of("loggytracy"),
            ingested("loggytracy")
        ),
        per_gb(settled("loki") - wal_of("loki"), ingested("loki")),
        mib(&disk["loggytracy"]["after_queries"]),
        mib(&disk["loki"]["after_queries"]),
        disk["loggytracy"]["breakdown"].as_str().unwrap_or(""),
        disk["loki"]["breakdown"].as_str().unwrap_or(""),
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
) {
    let shape = |name: &str, pass: &str, target: &str| -> Option<f64> {
        f64_of(&matrix[target]["matrix"]["shapes"][name][pass]["p50_ms"])
    };
    let describe = |name: &str, pass: &str| -> String {
        match (shape(name, pass, "loggytracy"), shape(name, pass, "loki")) {
            (Some(lt), Some(lk)) if lk > 0.0 => {
                let factor = lt / lk;
                if factor < 0.9 {
                    format!(
                        "loggytracy is {:.2}x faster ({lt:.1} ms against {lk:.1} ms)",
                        1.0 / factor
                    )
                } else if factor > 1.1 {
                    format!("loggytracy is {factor:.2}x slower ({lt:.1} ms against {lk:.1} ms)")
                } else {
                    format!("within 10% ({lt:.1} ms against {lk:.1} ms)")
                }
            }
            _ => "not measured".to_string(),
        }
    };
    let ingest_lt = f64_of(&load["loggytracy"]["ingest"]["achieved_eps"]).unwrap_or(0.0);
    let ingest_lk = f64_of(&load["loki"]["ingest"]["achieved_eps"]).unwrap_or(0.0);
    let disk_line = {
        let lt = f64_of(&bed["disk_bytes"]["loggytracy"]["settled"]).unwrap_or(0.0);
        let lk = f64_of(&bed["disk_bytes"]["loki"]["settled"]).unwrap_or(0.0);
        let lt_data = lt - wal_bytes(bed, "loggytracy");
        let lk_data = lk - wal_bytes(bed, "loki");
        if lk > 0.0 && lk_data > 0.0 {
            format!(
                "loggytracy holds {:.2}x Loki's total on disk, and {:.2}x once each \
system's write-ahead log is taken out",
                lt / lk,
                lt_data / lk_data,
            )
        } else {
            "not measured".to_string()
        }
    };
    let claim_cold = shape("json_field", "cold_ms", "loggytracy")
        .zip(shape("json_field", "cold_ms", "loki"))
        .map(|(lt, lk)| lt < lk * 0.9);
    let claim_warm = shape("json_field", "warm_ms", "loggytracy")
        .zip(shape("json_field", "warm_ms", "loki"))
        .map(|(lt, lk)| lt < lk * 0.9);

    page.push_str(&format!(
        r#"## The verdict on the claim

The claim is about one shape: `{{...}} | json | field="value"`, where Loki has
to run its parser over every line in the window and loggytracy has columnized
and bloomed those fields at ingest.

* `json_field`, cold: **{}**
* `json_field`, warm: **{}**
* `line_filter`, cold: {}
* `label_only`, cold: {}
* `rate`, cold: {}
* ingest: loggytracy achieved {ingest_lt:.0} eps against Loki's {ingest_lk:.0}
* disk: {}

**{}**

{}

"#,
        describe("json_field", "cold_ms"),
        describe("json_field", "warm_ms"),
        describe("line_filter", "cold_ms"),
        describe("label_only", "cold_ms"),
        describe("rate", "cold_ms"),
        disk_line,
        match (claim_cold, claim_warm) {
            (Some(true), Some(true)) =>
                "The claim survives this run on both the cold and the warm pass.",
            (Some(true), Some(false)) | (Some(false), Some(true)) =>
                "The claim survives on one pass and not the other, which is not \
'materially less time' — it is a result that depends on which number you quote.",
            (Some(false), Some(false)) =>
                "The claim does not survive this run. loggytracy loses its own headline \
query.",
            _ => "The claim could not be decided from this run; the measurement is missing.",
        },
        match (claim_cold, claim_warm) {
            (Some(false), Some(false)) => {
                r#"That is a valid and valuable result and it is not tuned around. It is
also the result M8's own benchmarks predicted: loggytracy has no projection
pushdown, parses every line twice when writing JSON parts, and sets its scan
limit to `usize::MAX` for exactly this shape (`query/execution.rs:102-106`),
because a limit cannot be applied before the pipeline runs. So
`| json | field=` materializes every matching row in the window and then throws
almost all of it away. `docs/VISION.md` invariant III names that as the worst
violation of the three and the fix as a streaming executor with a bounded
top-K heap. This document is the measurement that says how much it is worth,
and nothing about the engine was changed to make the number smaller."#
            }
            _ => {
                r#"The numbers above are the whole argument; the sections below say what is
wrong with them. No loggytracy setting was changed from its default to produce
this run, and the Loki deviations are listed in full with their reasons."#
            }
        },
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
        "**The two ingest runs are sequential, and loggytracy went first.** Loki's \
run started with a warmer page cache and a machine that had just finished \
doing work. Running them concurrently would remove that and introduce a worse \
problem — each system's throughput would depend on the other's — so this is a \
chosen bias rather than an overlooked one, and its direction favours Loki on \
ingest ({} against {} achieved eps).",
        num(&load["loggytracy"]["ingest"]["achieved_eps"]),
        num(&load["loki"]["ingest"]["achieved_eps"]),
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

    let ended_lt = load["loggytracy"]["run"]["ended_on"].as_str().unwrap_or("");
    let ended_lk = load["loki"]["run"]["ended_on"].as_str().unwrap_or("");
    if ended_lt != ended_lk {
        items.push(format!(
            "**The two ingest runs did not stop the same way** — loggytracy on \
`{ended_lt}`, Loki on `{ended_lk}`. They therefore hold different amounts of \
data, and every axis downstream of that (disk, query, memory) is comparing two \
different corpora sizes. Treat the disk and query columns as indicative only."
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
