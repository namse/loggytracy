mod admin;
mod app_state;
mod backpressure;
pub mod bloom;
pub mod clock;
pub mod config;
// Shared with `benches/` and `bin/load`. One generator, so a load result and a
// bench result describe the same bytes; see the module doc.
pub mod corpus;
mod delete_requests;
mod flush;
mod ingest;
pub mod journal;
mod log_ingest;
pub mod log_scan;
pub mod logql;
pub mod memprof;
pub mod memtable;
mod merge;
mod metrics;
pub mod object_storage;
mod otlp_http;
mod otlp_log;
pub mod part;
pub mod part_registry;
pub mod proto;
mod query;
mod remote_lifecycle;
mod retention;
mod router;
mod runtime_error;
mod shutdown;
mod startup;
mod tempo;
pub mod tenant;
pub mod tenant_policy;
mod tenant_quota;
pub mod test_support;
mod trace;
mod trace_ingest;
mod trace_part;
pub mod trace_registry;

pub use app_state::{AppState, AppStateDependencies};
pub use router::build_router;
pub use startup::{recover, run};

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use std::sync::Arc;

    include!("tests/e2e.rs");
    include!("tests/shutdown_rehearsal.rs");
}
