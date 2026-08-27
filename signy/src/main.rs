use std::sync::Arc;

use signy::config::Config;

#[cfg(feature = "memprof")]
#[global_allocator]
static ALLOCATOR: signy::memprof::ProfilingAllocator = signy::memprof::ProfilingAllocator;

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

// jemalloc runs at its own defaults, and that is a measured choice: a
// five-way 600 s sweep on the soak rig (todo.md, 2026-08-09) read
// `background_thread:true` and `dirty_decay_ms:30000` inside the defaults'
// run-to-run spread on every axis, and the apparent throughput gap against
// glibc dissolved once 600 s runs were compared with 600 s runs — both
// allocators throttle ~7% in the cold-start transient and recover by the
// hour mark. An operator A/B reaches jemalloc through `_RJEM_MALLOC_CONF`
// (the `MALLOC_CONF` name is not consulted in this prefixed build).

fn main() {
    // Before the runtime exists: M_ARENA_MAX only bounds arenas not yet
    // created, and tokio's workers each create one on first contention.
    // Under jemalloc this is a no-op and logs `applied = false`.
    let malloc_tuned = signy::malloc_tuning::apply_from_env();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build the tokio runtime")
        .block_on(run(malloc_tuned));
}

async fn run(malloc_tuned: bool) {
    let config = Arc::new(
        Config::from_env().unwrap_or_else(|error| panic!("invalid configuration: {error}")),
    );
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("signy=info,warn"));
    match config.log_format {
        signy::config::LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        signy::config::LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_current_span(false)
            .with_env_filter(filter)
            .init(),
    }
    tracing::info!(
        applied = malloc_tuned,
        "glibc malloc tuning (SIGNY_MALLOC_TUNING=off to disable)"
    );
    config.log_memory_budget();
    signy::run(config).await;
}
