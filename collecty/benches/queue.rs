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
            ..QueueLimits::default()
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
                })
                .expect("an append")
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reading a closed segment into the bytes that go on the wire.
fn sealing(criterion: &mut Criterion) {
    let export = corpus::export_bytes(64);
    let frame = wire::compress_record(Signal::Logs, &export, wire::ZSTD_LEVEL).expect("a frame");
    let dir = scratch("segment");
    let queue = Arc::new(
        Queue::open(
            &dir,
            QueueLimits {
                max_bytes: 8 * 1024 * 1024 * 1024,
                max_segment_bytes: 256 * 1024 * 1024,
                max_segment_age: std::time::Duration::from_nanos(1),
            },
        )
        .expect("a queue"),
    );
    for _ in 0..256 {
        queue
            .append(&Record {
                frame: frame.clone(),
            })
            .expect("an append");
    }
    queue.seal_if_due().expect("a seal");
    let seq = queue.oldest_sealed().expect("a closed segment");

    let mut group = criterion.benchmark_group("queue/read-segment");
    group.throughput(Throughput::Bytes((frame.len() * 256) as u64));
    group.bench_function("256-records", |bencher| {
        bencher.iter(|| {
            let sealed = queue.read_segment(seq).expect("a segment");
            std::hint::black_box(sealed.frames.len())
        })
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, appending, sealing);
criterion_main!(benches);
