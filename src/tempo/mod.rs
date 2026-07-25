use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::AppState;
use crate::tenant::TenantId;
use crate::trace::{TraceSpan, canonical_trace_id};
use crate::trace_registry::TraceRegistry;

const MAX_TRACE_SPANS: usize = 100_000;
const MAX_TRACE_SEARCH_LIMIT: usize = 1_000;

include!("scan.rs");
include!("handlers.rs");
include!("response.rs");
include!("tags.rs");

fn client_error(error: String) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error)
}

fn internal_error(error: String) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error)
}

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
