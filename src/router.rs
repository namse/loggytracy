use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};

use crate::{AppState, ingest, query, tempo};

pub fn build_router(state: Arc<AppState>) -> Router {
    // Without this the push route silently inherits axum's 2 MiB default, so an
    // Alloy tuned to larger batches gets an unexplained 413 with no knob to
    // turn. The handler enforces the same bound for a precise error message.
    let push_body_limit = DefaultBodyLimit::max(state.config.max_push_bytes);
    Router::new()
        .route("/loki/api/v1/push", post(ingest::push))
        .layer(push_body_limit)
        .route("/loki/api/v1/query_range", get(query::query_range))
        .route("/loki/api/v1/query", get(query::query))
        .route("/loki/api/v1/series", get(query::series))
        .route("/loki/api/v1/labels", get(query::labels))
        .route("/loki/api/v1/label/{name}/values", get(query::label_values))
        .route("/loki/api/v1/status/buildinfo", get(query::buildinfo))
        .route("/loki/api/v1/index/stats", get(query::index_stats))
        .route("/metrics", get(query::metrics))
        .route("/api/traces/{trace_id}", get(tempo::trace_by_id))
        .route("/api/search", get(tempo::search))
        .route("/api/search/tags", get(tempo::search_tags))
        .route(
            "/api/search/tag/{tag}/values",
            get(tempo::search_tag_values),
        )
        .route("/ready", get(query::ready))
        .with_state(state)
}
