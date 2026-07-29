//! MemTable insert and the memtable query path.
//!
//! `scan_memtable_stream` (`src/memtable.rs:145-156`) collects and sorts the
//! whole stream on every query before it looks at the time range, so the query
//! sweep varies entries-per-stream and the cardinality sweep varies how many
//! streams a matcher has to walk past.

#[path = "corpus/mod.rs"]
#[allow(dead_code)]
mod corpus;

use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use loggytracy::logql::{LabelMatcher, LineFilter, MatcherOp};
use loggytracy::memtable::MemTable;

use corpus::CorpusSpec;

/// Criterion warms up for three seconds per bench function by default,
/// which across this suite is minutes of warm-up alone. These are a
/// regression gate, not a distribution.
const WARM_UP: Duration = Duration::from_millis(500);
fn fill(memtable: &MemTable, corpus: &corpus::Corpus) {
    for stream in &corpus.streams {
        memtable.insert(
            stream.tenant.clone(),
            stream.labels.clone(),
            stream.entries.clone(),
        );
    }
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/insert");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for streams in [1usize, 64, 4096] {
        let corpus = corpus::generate(&CorpusSpec::default().rows(20_000).streams(streams));
        let batches = corpus::push_batches(&corpus);
        let tenant = corpus.tenant().clone();
        group.throughput(Throughput::Elements(corpus.entry_count() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(streams),
            &batches,
            |b, batches| {
                b.iter_batched(
                    || batches.clone(),
                    |batches| {
                        let memtable = MemTable::new();
                        for (labels, entries) in batches {
                            memtable.insert(tenant.clone(), labels, entries);
                        }
                        memtable
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

/// The sort-the-whole-stream path. Rows per stream is the axis because the
/// sort is O(n log n) in it and the query's own limit does not bound it.
fn bench_query_stream_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/query_stream_depth");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for rows_per_stream in [100usize, 2_000, 50_000] {
        let corpus = corpus::generate(
            &CorpusSpec::default()
                .rows(rows_per_stream * 4)
                .streams(4)
                .out_of_order(true),
        );
        let memtable = MemTable::new();
        fill(&memtable, &corpus);
        let tenant = corpus.tenant().clone();
        let start = corpus.min_ts_ns();
        let end = corpus.max_ts_ns();
        group.throughput(Throughput::Elements(corpus.entry_count() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows_per_stream),
            &rows_per_stream,
            |b, _| {
                b.iter(|| memtable.query(&tenant, &[], &[], start, end, 100, false));
            },
        );
    }
    group.finish();
}

/// Label matching across a cardinality sweep. One hardcoded label set is what
/// the old harness had, so nothing ever measured the walk past the streams a
/// matcher rejects.
fn bench_query_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/query_cardinality");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for streams in [1usize, 256, 8_192] {
        let corpus = corpus::generate(
            &CorpusSpec::default()
                .rows(50_000)
                .streams(streams)
                .labels_per_stream(6)
                .out_of_order(true),
        );
        let memtable = MemTable::new();
        fill(&memtable, &corpus);
        let tenant = corpus.tenant().clone();
        let matcher =
            LabelMatcher::new("app".to_string(), MatcherOp::Eq, corpus.label_value("app"))
                .expect("bench matcher is well formed");
        let start = corpus.min_ts_ns();
        let end = corpus.max_ts_ns();
        group.bench_with_input(BenchmarkId::from_parameter(streams), &streams, |b, _| {
            b.iter(|| {
                memtable.query(
                    &tenant,
                    std::slice::from_ref(&matcher),
                    &[],
                    start,
                    end,
                    100,
                    false,
                )
            });
        });
    }
    group.finish();
}

fn bench_query_line_filter(c: &mut Criterion) {
    let corpus = corpus::generate(
        &CorpusSpec::default()
            .rows(50_000)
            .streams(64)
            .out_of_order(true),
    );
    let memtable = MemTable::new();
    fill(&memtable, &corpus);
    let tenant = corpus.tenant().clone();
    let start = corpus.min_ts_ns();
    let end = corpus.max_ts_ns();
    let filters = vec![LineFilter::Contains("timeout".to_string())];

    let mut group = c.benchmark_group("memtable/query_line_filter");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2))
        .throughput(Throughput::Elements(corpus.entry_count() as u64));
    group.bench_function("contains", |b| {
        b.iter(|| memtable.query(&tenant, &[], &filters, start, end, 100, false));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_insert,
    bench_query_stream_depth,
    bench_query_cardinality,
    bench_query_line_filter
);
criterion_main!(benches);
