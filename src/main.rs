use std::sync::Arc;

use loggytracy::config::Config;

#[cfg(feature = "memprof")]
#[global_allocator]
static ALLOCATOR: loggytracy::memprof::ProfilingAllocator = loggytracy::memprof::ProfilingAllocator;

fn main() {
    // Before the runtime exists: M_ARENA_MAX only bounds arenas not yet
    // created, and tokio's workers each create one on first contention.
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
