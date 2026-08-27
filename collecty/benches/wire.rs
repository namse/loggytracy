#[path = "corpus.rs"]
mod corpus;

use collecty::wire;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

fn compression(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("wire/compress");
    for records in [16usize, 256, 4096] {
        let export = corpus::export_bytes(records);
        group.throughput(Throughput::Bytes(export.len() as u64));
        group.bench_function(format!("{records}-records"), |bencher| {
            bencher.iter(|| wire::compress(std::hint::black_box(&export), wire::ZSTD_LEVEL))
        });
    }
    group.finish();
}

fn levels(criterion: &mut Criterion) {
    let export = corpus::export_bytes(256);
    let mut group = criterion.benchmark_group("wire/level");
    group.throughput(Throughput::Bytes(export.len() as u64));
    for level in [1i32, 3, 6, 12] {
        group.bench_function(format!("level-{level}"), |bencher| {
            bencher.iter(|| wire::compress(std::hint::black_box(&export), level))
        });
    }
    group.finish();
}

fn batch_decompression(criterion: &mut Criterion) {
    let export = corpus::export_bytes(64);
    let mut frames = Vec::new();
    let mut plain = 0;
    for _ in 0..64 {
        plain += export.len();
        frames.extend_from_slice(&wire::compress(&export, wire::ZSTD_LEVEL).expect("a frame"));
    }

    let mut group = criterion.benchmark_group("wire/decompress-batch");
    group.throughput(Throughput::Bytes(plain as u64));
    group.bench_function("64-frames", |bencher| {
        bencher.iter(|| wire::decompress_concatenated(std::hint::black_box(&frames), plain))
    });
    group.finish();
}

criterion_group!(benches, compression, levels, batch_decompression);
criterion_main!(benches);
