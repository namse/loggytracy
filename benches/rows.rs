//! Flush-path row materialization: `rows_from_snapshot` and `Row::from_entry`.
//!
//! `Row::from_entry` (`src/part/mod.rs:302`) clones the whole label `BTreeMap`
//! per row, which `docs/VISION.md` II names as the largest single term in the
//! repository. The cost is labels-per-stream x rows, so both are swept, and
//! the number that has to move when `Arc<Labels>` lands is bytes-allocated per
//! row rather than nanoseconds — so this binary carries the counting
//! allocator and prints that table before the timings.

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
/// `peak live` is the whole `Vec<Row>` plus the sort's scratch, and
/// `bytes/row` is what a shared label set is supposed to remove: at ten labels
/// the per-row clone is most of it, and at two labels it is not.
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
            let cloned_label_bytes = label_bytes_in_rows(&rows);
            corpus::alloc::row(
                &format!("labels={labels_per_stream} streams={streams}"),
                entries,
                &stats,
                &format!(
                    "label bytes {}x ({} held once, {} in rows)",
                    cloned_label_bytes / distinct_label_bytes.max(1),
                    distinct_label_bytes,
                    cloned_label_bytes,
                ),
            );
        }
    }
    corpus::alloc::footer();
}

fn label_bytes_in_rows(rows: &[Row]) -> u64 {
    rows.iter()
        .map(|row| {
            row.labels
                .iter()
                .map(|(name, value)| (name.len() + value.len()) as u64)
                .sum::<u64>()
        })
        .sum()
}

fn benches(c: &mut Criterion) {
    report_allocations();
    bench_rows_from_snapshot(c);
    bench_row_from_entry(c);
}

criterion_group!(rows, benches);
criterion_main!(rows);
