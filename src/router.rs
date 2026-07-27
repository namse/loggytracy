use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};

use crate::{AppState, admin, ingest, otlp_http, query, tempo};

pub fn build_router(state: Arc<AppState>) -> Router {
    // Each ingest route carries its own body limit, so they are merged as
    // separate routers rather than layered onto one. `Router::layer` applies to
    // every route registered before it, which would leave the effective limit
    // of a route depending on where in this chain it happens to sit.
    let router = Router::new()
        .merge(ingest_router(state.config.max_push_bytes))
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

/// The write routes, each with the body limit it enforces.
///
/// Without an explicit limit these inherit axum's 2 MiB default, so an Alloy
/// tuned to larger batches gets an unexplained 413 with no knob to turn. Each
/// handler enforces the same bound again for a precise error message.
fn ingest_router(max_push_bytes: usize) -> Router<Arc<AppState>> {
    let loki = Router::new()
        .route("/loki/api/v1/push", post(ingest::push))
        .layer(DefaultBodyLimit::max(max_push_bytes));
    // OTLP over HTTP, on the same listener as the Loki API. A collector
    // configured with `otlphttp` is at least as common as one using gRPC, and
    // it is the only option behind a proxy that does not carry gRPC. The limit
    // matches what the gRPC services accept, so a collector sees one size
    // whichever transport it picks.
    let otlp = Router::new()
        .route("/v1/logs", post(otlp_http::logs))
        .route("/v1/traces", post(otlp_http::traces))
        .layer(DefaultBodyLimit::max(otlp_http::MAX_OTLP_HTTP_BODY_BYTES));
    loki.merge(otlp)
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
