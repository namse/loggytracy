//! Bloom construction and lookup, plus the parser pass that sizes the
//! exact-field filter.
//!
//! `encode_blooms` (`src/part/format.rs:318-383`) runs the JSON and logfmt
//! parsers over every line **twice** per row group: once at `:335-341` to size
//! the filter and again at `:360-366` to fill it. `encode_blooms` itself and
//! `logql::indexed_parser_fields` are private to the crate, so the pass is
//! measured through the public LogQL pipeline instead — the doubled cost is
//! two of `blooms/parse_pass`, and `part/write` in `part.rs` shows it landing
//! in a real flush.

#[path = "corpus/mod.rs"]
#[allow(dead_code)]
mod corpus;

use std::collections::BTreeSet;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use loggytracy::bloom::{BloomFilter, trigrams};
use loggytracy::logql;
use loggytracy::memtable::LogEntry;

use corpus::{CorpusSpec, Shape};

/// Criterion warms up for three seconds per bench function by default,
/// which across this suite is minutes of warm-up alone. These are a
/// regression gate, not a distribution.
const WARM_UP: Duration = Duration::from_millis(500);
/// The same wire shape `encode_exact_field_token` produces — magic, scope
/// byte, and length-prefixed name and value. That function is private, so the
/// bench reproduces the *size* of the token it inserts rather than calling it;
/// what is being measured is `BloomFilter::insert` at a realistic key length.
fn field_token(name: &str, value: &str) -> Vec<u8> {
    let mut token = Vec::with_capacity(9 + name.len() + value.len());
    token.extend_from_slice(b"FEQ1");
    token.push(0);
    token.extend_from_slice(&(name.len() as u32).to_le_bytes());
    token.extend_from_slice(name.as_bytes());
    token.extend_from_slice(&(value.len() as u32).to_le_bytes());
    token.extend_from_slice(value.as_bytes());
    token
}

/// Exactly what `encode_blooms` does for the line filter: unique trigrams for
/// the row group, then one filter sized to that count.
fn build_line_bloom(lines: &[&str]) -> BloomFilter {
    let mut unique: BTreeSet<[u8; 3]> = BTreeSet::new();
    for line in lines {
        for trigram in trigrams(line) {
            unique.insert(trigram);
        }
    }
    let mut bloom = BloomFilter::with_capacity(unique.len().max(1), 0.01);
    for trigram in &unique {
        bloom.insert(trigram);
    }
    bloom
}

fn bench_line_bloom_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom/build_line");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    // A row group is `row_group_size` rows, so the sweep brackets the default.
    for rows in [1_000usize, 10_000] {
        let corpus = corpus::generate(&CorpusSpec::default().rows(rows).streams(16));
        let lines = corpus.lines();
        group.throughput(Throughput::Bytes(corpus.line_bytes()));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &lines, |b, lines| {
            b.iter(|| build_line_bloom(lines));
        });
    }
    group.finish();
}

fn bench_exact_field_build(c: &mut Criterion) {
    let corpus = corpus::generate(
        &CorpusSpec::default()
            .rows(10_000)
            .streams(16)
            .metadata_pairs(3),
    );
    let tokens: Vec<Vec<u8>> = corpus
        .entries()
        .iter()
        .flat_map(|entry| entry.structured_metadata.iter())
        .map(|(name, value)| field_token(name, value))
        .collect();

    let mut group = c.benchmark_group("bloom/build_exact_fields");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2))
        .throughput(Throughput::Elements(tokens.len() as u64));
    group.bench_function("insert", |b| {
        b.iter(|| {
            let mut bloom = BloomFilter::with_capacity(tokens.len(), 0.01);
            for token in &tokens {
                bloom.insert(token);
            }
            bloom
        });
    });
    group.finish();
}

fn bench_lookup(c: &mut Criterion) {
    let corpus = corpus::generate(&CorpusSpec::default().rows(10_000).streams(16));
    let lines = corpus.lines();
    let bloom = build_line_bloom(&lines);
    let tokens: Vec<Vec<u8>> = corpus
        .entries()
        .iter()
        .flat_map(|entry| entry.structured_metadata.iter())
        .map(|(name, value)| field_token(name, value))
        .collect();
    let mut exact = BloomFilter::with_capacity(tokens.len(), 0.01);
    for token in &tokens {
        exact.insert(token);
    }

    let mut group = c.benchmark_group("bloom/lookup");
    group
        .sample_size(20)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    // The pruning decision is per row group, so what matters is one call, and
    // the miss is the case pruning exists for.
    group.bench_function("substr_hit", |b| {
        b.iter(|| bloom.might_contain_substr("connection reset"));
    });
    group.bench_function("substr_miss", |b| {
        b.iter(|| bloom.might_contain_substr("zzqqxx-never-written"));
    });
    group.bench_function("exact_field_hit", |b| {
        b.iter(|| exact.contains(&tokens[0]));
    });
    group.bench_function("exact_field_miss", |b| {
        let absent = field_token("trace_id", "ffffffffffffffffffffffffffffffff");
        b.iter(|| exact.contains(&absent));
    });
    group.finish();
}

/// One parser pass over a row group's lines. `encode_blooms` runs two.
fn bench_parse_pass(c: &mut Criterion) {
    let mut group = c.benchmark_group("blooms/parse_pass");
    group
        .sample_size(10)
        .warm_up_time(WARM_UP)
        .measurement_time(Duration::from_secs(2));
    for (name, shape, query) in [
        ("json", Shape::Json, "{app=\"api-gateway\"} | json"),
        ("logfmt", Shape::Logfmt, "{app=\"api-gateway\"} | logfmt"),
    ] {
        let corpus = corpus::generate(&CorpusSpec::default().rows(5_000).streams(8).only(shape));
        let pairs = corpus.labelled_entries();
        let parsed = logql::parse(query).expect("bench query parses");
        group.throughput(Throughput::Elements(pairs.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &pairs, |b, pairs| {
            b.iter(|| {
                let mut matched = 0usize;
                for (labels, entry) in pairs {
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

criterion_group!(
    benches,
    bench_line_bloom_build,
    bench_exact_field_build,
    bench_lookup,
    bench_parse_pass
);
criterion_main!(benches);
