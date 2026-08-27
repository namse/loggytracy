#[path = "corpus.rs"]
mod corpus;

use std::sync::Arc;

use collecty::queue::{Queue, QueueLimits, Record};
use collecty::signal::Signal;
use collecty::wire;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn scratch(label: &str) -> std::path::PathBuf {
    let dir =
        std::path::PathBuf::from("/tmp").join(format!("cy-bench-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    dir
}

fn appending(criterion: &mut Criterion) {
    let export = corpus::export_bytes(256);
    let frame = wire::compress_record(Signal::Logs, &export, wire::ZSTD_LEVEL).expect("a frame");
    let dir = scratch("append");
    let queue = Queue::open(
        &dir,
        QueueLimits {
            max_bytes: 8 * 1024 * 1024 * 1024,
            max_segment_bytes: 64 * 1024 * 1024,
        },
    )
    .expect("a queue");

    let mut group = criterion.benchmark_group("queue/append");
    group.throughput(Throughput::Bytes(frame.len() as u64));
    group.bench_function("one-record", |bencher| {
        bencher.iter(|| {
            queue
                .append(&Record {
                    frame: frame.clone(),
                    plain_len: (wire::RECORD_HEADER_BYTES + export.len()) as u32,
                })
                .expect("an append")
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

fn batching(criterion: &mut Criterion) {
    let export = corpus::export_bytes(64);
    let frame = wire::compress_record(Signal::Logs, &export, wire::ZSTD_LEVEL).expect("a frame");
    let dir = scratch("batch");
    let queue = Arc::new(
        Queue::open(
            &dir,
            QueueLimits {
                max_bytes: 8 * 1024 * 1024 * 1024,
                max_segment_bytes: 256 * 1024 * 1024,
            },
        )
        .expect("a queue"),
    );
    for _ in 0..4096 {
        queue
            .append(&Record {
                frame: frame.clone(),
                plain_len: export.len() as u32,
            })
            .expect("an append");
    }

    let mut group = criterion.benchmark_group("queue/read-batch");
    group.throughput(Throughput::Bytes((frame.len() * 256) as u64));
    group.bench_function("256-records", |bencher| {
        bencher.iter(|| {
            let batch = queue
                .read_batch(usize::MAX, 256)
                .expect("a batch")
                .expect("records");
            std::hint::black_box(batch.frames.len())
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, appending, batching);
criterion_main!(benches);
