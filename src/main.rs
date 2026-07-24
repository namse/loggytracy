mod app_state;
mod bloom;
mod config;
mod flush;
mod ingest;
mod journal;
mod logql;
mod memtable;
mod merge;
mod metrics;
mod object_storage;
mod part;
mod part_registry;
mod proto;
mod query;
mod remote_lifecycle;
mod retention;
mod router;
mod runtime_error;
mod shutdown;
mod startup;
mod tempo;
#[cfg(test)]
mod test_support;
mod trace;
mod trace_ingest;
mod trace_part;
mod trace_registry;

use std::sync::Arc;

pub use app_state::{AppState, AppStateDependencies};
use config::Config;
pub use router::build_router;
pub use startup::recover;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("loggytracy=debug,info")
        .init();

    let config = Arc::new(
        Config::from_env().unwrap_or_else(|error| panic!("invalid configuration: {error}")),
    );
    startup::run(config).await;
}

#[cfg(test)]
mod tests {
    include!("tests/e2e.rs");
    include!("tests/shutdown_rehearsal.rs");
}
