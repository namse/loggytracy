//! LogQL parse and per-row pipeline evaluation.
//!
//! `| json | field="x"` is the shape the claim in `docs/VISION.md` rests on
//! and the shape `query/execution.rs:102-106` currently answers by setting the
//! scan limit to `usize::MAX`, so its per-row cost is the multiplier on
//! everything that limit fails to bound.

#[path = "corpus/mod.rs"]
#[allow(dead_code)]
mod corpus;

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use loggytracy::logql;
use loggytracy::memtable::LogEntry;

use corpus::{CorpusSpec, Shape};

/// Criterion warms up for three seconds per bench function by default,
/// which across this suite is minutes of warm-up alone. These are a
/// regression gate, not a distribution.
const WARM_UP: Duration = Duration::from_millis(500);
const QUERIES: [(&str, &str); 6] = [
    ("label_only", r#"{app="api-gateway",env="prod"}"#),
    ("line_filter", r#"{app="api-gateway"} |= "timeout""#),
    ("line_regex", r#"{app="api-gateway"} |~ "error.*timeout""#),
    ("json_field", r#"{app="api-gateway"} | json | status="500""#),
    (
        "logfmt_field",
        r#"{app="api-gateway"} | logfmt | level="error""#,
    ),
    (
        "metric",
        r#"sum by (app) (rate({app="api-gateway"} | json | status="500" [5m]))"#,
    ),
];

fn bench_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("logql/parse");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(1));
    for (name, query) in QUERIES {
        group.bench_with_input(BenchmarkId::from_parameter(name), &query, |b, query| {
            b.iter(|| logql::parse_expr(query).expect("bench query parses"));
        });
    }
    group.finish();
}

fn bench_eval(c: &mut Criterion) {
    let mut group = c.benchmark_group("logql/eval");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for (name, shape, query) in [
        (
            "line_filter",
            Shape::Plain,
            r#"{app="api-gateway"} |= "timeout""#,
        ),
        (
            "line_regex",
            Shape::Plain,
            r#"{app="api-gateway"} |~ "error.*timeout""#,
        ),
        (
            "json_field",
            Shape::Json,
            r#"{app="api-gateway"} | json | status="500""#,
        ),
        (
            "logfmt_field",
            Shape::Logfmt,
            r#"{app="api-gateway"} | logfmt | level="error""#,
        ),
        (
            "json_line_format",
            Shape::Json,
            r#"{app="api-gateway"} | json | line_format "{{.status}} {{.user_id}}""#,
        ),
    ] {
        let corpus = corpus::generate(&CorpusSpec::default().rows(10_000).streams(16).only(shape));
        let pairs = corpus.labelled_entries();
        let parsed = logql::parse(query).expect("bench query parses");
        group.throughput(Throughput::Elements(pairs.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &pairs, |b, pairs| {
            b.iter(|| {
                let mut matched = 0usize;
                for (labels, entry) in pairs {
                    // The executor evaluates against a query-local copy, so
                    // the clone is part of the per-row cost rather than setup.
                    let mut scratch: LogEntry = (*entry).clone();
                    if parsed
                        .process_entry_with_labels_cancellable(labels, &mut scratch, None)
                        .unwrap_or(false)
                    {
                        matched += 1;
                    }
                }
                matched
            });
        });
    }
    group.finish();
}

/// What the reader can turn into row-group pruning before any page is read.
fn bench_exact_field_predicates(c: &mut Criterion) {
    let parsed = logql::parse(r#"{app="api-gateway"} | json | status="500" | user_id="u-1""#)
        .expect("bench query parses");
    let mut group = c.benchmark_group("logql/exact_field_predicates");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(1));
    group.bench_function("json_two_fields", |b| {
        b.iter(|| parsed.exact_field_predicates());
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_parse,
    bench_eval,
    bench_exact_field_predicates
);
criterion_main!(benches);
