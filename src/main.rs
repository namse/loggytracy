use std::sync::Arc;

use loggytracy::config::Config;

#[cfg(feature = "memprof")]
#[global_allocator]
static ALLOCATOR: loggytracy::memprof::ProfilingAllocator = loggytracy::memprof::ProfilingAllocator;

/// jemalloc, because the soak measured glibc out (todo.md, 2026-08-09): with
/// every gauged resident flat, glibc-retained free crept until the 2 GiB kill
/// — 4.05 hours even with the fixed thresholds, the arena cap and a
/// `malloc_trim` timer, because a free chunk on a page it shares with a live
/// one can never be returned. jemalloc's decay-based purge returns freed
/// runs continuously, which is the property the competitors get from the Go
/// runtime for free. The memprof build keeps its instrumented wrapper over
/// glibc — its job is live-byte attribution, and the glibc tuning stays
/// active there.
#[cfg(not(feature = "memprof"))]
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // Before the runtime exists: M_ARENA_MAX only bounds arenas not yet
    // created, and tokio's workers each create one on first contention.
    // Under jemalloc this is a no-op and logs `applied = false`.
    let malloc_tuned = loggytracy::malloc_tuning::apply_from_env();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build the tokio runtime")
        .block_on(run(malloc_tuned));
}

async fn run(malloc_tuned: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("loggytracy=info,warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    tracing::info!(
        applied = malloc_tuned,
        "glibc malloc tuning (LOGGYTRACY_MALLOC_TUNING=off to disable)"
    );

    let config = Arc::new(
        Config::from_env().unwrap_or_else(|error| panic!("invalid configuration: {error}")),
    );
    config.log_memory_budget();
    loggytracy::run(config).await;
}
