use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};

use crate::{AppState, admin, ingest, query, tempo};

pub fn build_router(state: Arc<AppState>) -> Router {
    // Without this the push route silently inherits axum's 2 MiB default, so an
    // Alloy tuned to larger batches gets an unexplained 413 with no knob to
    // turn. The handler enforces the same bound for a precise error message.
    let push_body_limit = DefaultBodyLimit::max(state.config.max_push_bytes);
    let router = Router::new()
        .route("/loki/api/v1/push", post(ingest::push))
        .layer(push_body_limit)
        .route("/loki/api/v1/query_range", get(query::query_range))
        .route("/loki/api/v1/tail", get(query::tail))
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
        .route("/ready", get(query::ready));
    // Without a token there is no per-tenant retention at all, so the admin
    // surface does not exist rather than existing unauthenticated.
    let router = match state.config.tenant_policy_token {
        Some(_) => router.merge(admin_router()),
        None => router,
    };
    router.with_state(state)
}

fn admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/loggytracy/api/v1/admin/tenants/{tenant}/retention",
            put(admin::put_retention)
                .get(admin::get_retention)
                .delete(admin::delete_retention),
        )
        .layer(DefaultBodyLimit::max(admin::MAX_ADMIN_BODY_BYTES))
}
