//! Flush-path row materialization: `rows_from_snapshot` and `Row::from_entry`.
//!
//! `Row::from_entry` cloned the whole label `BTreeMap` per row, which
//! `docs/VISION.md` II named as the largest single term in the repository. The
//! cost was labels-per-stream x rows, so both are swept, and the number that had
//! to move was bytes-allocated per row rather than nanoseconds — so this binary
//! carries the counting allocator and prints that table before the timings.
//!
//! `Row::labels` is now `Arc<Labels>`, so the table's job changes from watching
//! the amplification to holding it down: `bytes/row`, `allocs/row` and
//! `peak live` must stay flat across the label sweep, and the `label sets` note
//! must stay equal to the stream count. A per-row clone reappearing anywhere
//! makes all four move together.

#[path = "corpus/mod.rs"]
#[allow(dead_code)]
mod corpus;

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use loggytracy::part::{Row, rows_from_snapshot};

use corpus::CorpusSpec;

/// Criterion warms up for three seconds per bench function by default,
/// which across this suite is minutes of warm-up alone. These are a
/// regression gate, not a distribution.
const WARM_UP: Duration = Duration::from_millis(500);
#[global_allocator]
static ALLOCATOR: corpus::alloc::CountingAllocator = corpus::alloc::CountingAllocator;

const ROWS: usize = 20_000;
const LABEL_SWEEP: [usize; 3] = [2, 5, 10];
const STREAM_SWEEP: [usize; 3] = [1, 256, 8_192];

fn bench_rows_from_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("rows/from_snapshot");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(3));
    for labels_per_stream in LABEL_SWEEP {
        for streams in STREAM_SWEEP {
            let corpus = corpus::generate(
                &CorpusSpec::default()
                    .rows(ROWS)
                    .streams(streams)
                    .labels_per_stream(labels_per_stream)
                    .out_of_order(true),
            );
            let snapshot = corpus.snapshot();
            group.throughput(Throughput::Elements(corpus.entry_count() as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("labels={labels_per_stream}"), streams),
                &snapshot,
                |b, snapshot| b.iter(|| rows_from_snapshot(snapshot)),
            );
        }
    }
    group.finish();
}

/// One row, without the surrounding sort, so a change in the clone itself is
/// separable from a change in `sort_rows`.
fn bench_row_from_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("rows/from_entry");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for labels_per_stream in LABEL_SWEEP {
        let corpus = corpus::generate(
            &CorpusSpec::default()
                .rows(4_096)
                .streams(64)
                .labels_per_stream(labels_per_stream),
        );
        let pairs: Vec<_> = corpus.labelled_entries();
        let tenant = corpus.tenant().clone();
        group.throughput(Throughput::Elements(pairs.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(labels_per_stream),
            &pairs,
            |b, pairs| {
                b.iter(|| {
                    let mut rows = Vec::with_capacity(pairs.len());
                    for (labels, entry) in pairs {
                        rows.push(Row::from_entry(&tenant, labels, entry));
                    }
                    rows
                });
            },
        );
    }
    group.finish();
}

/// The memory half of the same sweep.
///
/// `peak live` is the whole `Vec<Row>` plus the sort's scratch. `label sets`
/// counts the distinct label-set allocations the rows point at, which is the
/// direct measurement of sharing: one per stream is what `Arc<Labels>` buys,
/// and one per row is what it replaced.
fn report_allocations() {
    corpus::alloc::header("rows_from_snapshot (flush-path materialization)");
    for labels_per_stream in LABEL_SWEEP {
        for streams in STREAM_SWEEP {
            let corpus = corpus::generate(
                &CorpusSpec::default()
                    .rows(ROWS)
                    .streams(streams)
                    .labels_per_stream(labels_per_stream)
                    .out_of_order(true),
            );
            let snapshot = corpus.snapshot();
            let entries = corpus.entry_count();
            let distinct_label_bytes = corpus.distinct_label_bytes();
            let (rows, stats) = corpus::alloc::measure(|| rows_from_snapshot(&snapshot));
            corpus::alloc::row(
                &format!("labels={labels_per_stream} streams={streams}"),
                entries,
                &stats,
                &format!(
                    "label sets {} for {entries} rows ({} label bytes held once)",
                    distinct_label_sets(&rows),
                    distinct_label_bytes,
                ),
            );
        }
    }
    corpus::alloc::footer();
}

/// How many label-set allocations the whole `Vec<Row>` points at.
///
/// Counted by address rather than by value: two equal label sets that are two
/// allocations are two live copies, which is exactly the thing being measured,
/// and comparing by value would report them as one.
fn distinct_label_sets(rows: &[Row]) -> usize {
    rows.iter()
        .map(|row| std::sync::Arc::as_ptr(&row.labels))
        .collect::<std::collections::HashSet<_>>()
        .len()
}

fn benches(c: &mut Criterion) {
    report_allocations();
    bench_rows_from_snapshot(c);
    bench_row_from_entry(c);
}

criterion_group!(rows, benches);
criterion_main!(rows);
