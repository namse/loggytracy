use std::sync::Arc;

use loggytracy::config::Config;

#[cfg(feature = "memprof")]
#[global_allocator]
static ALLOCATOR: loggytracy::memprof::ProfilingAllocator = loggytracy::memprof::ProfilingAllocator;

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("loggytracy=info,warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = Arc::new(
        Config::from_env().unwrap_or_else(|error| panic!("invalid configuration: {error}")),
    );
    config.log_memory_budget();
    loggytracy::run(config).await;
}
