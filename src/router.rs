use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};

use crate::{AppState, admin, otlp_http, query, tempo};

pub fn build_router(state: Arc<AppState>) -> Router {
    // The ingest routes carry their own body limit, so they are merged as a
    // separate router rather than layered onto this one. `Router::layer`
    // applies to every route registered before it, which would leave the
    // effective limit of a route depending on where in this chain it sits.
    let router = Router::new()
        .merge(ingest_router())
        .route("/loki/api/v1/query_range", get(query::query_range))
        .route("/loki/api/v1/tail", get(query::tail))
        .route("/loki/api/v1/query", get(query::query))
        .route("/loki/api/v1/series", get(query::series))
        .route("/loki/api/v1/labels", get(query::labels))
        .route("/loki/api/v1/label/{name}/values", get(query::label_values))
        .route("/loki/api/v1/status/buildinfo", get(query::buildinfo))
        .route("/loki/api/v1/index/stats", get(query::index_stats))
        .route("/loki/api/v1/index/volume", get(query::index_volume))
        .route(
            "/loki/api/v1/index/volume_range",
            get(query::index_volume_range),
        )
        .route("/loki/api/v1/format_query", get(query::format_query))
        .route("/loki/api/v1/detected_labels", get(query::detected_labels))
        .route("/loki/api/v1/detected_fields", get(query::detected_fields))
        .route("/loki/api/v1/patterns", get(query::patterns))
        .route(
            "/loki/api/v1/delete",
            post(query::submit_delete_request)
                .get(query::list_delete_requests)
                .delete(query::cancel_delete_request),
        )
        .route("/metrics", get(query::metrics))
        .route("/api/traces/{trace_id}", get(tempo::trace_by_id))
        .route("/api/search", get(tempo::search))
        .route("/api/search/tags", get(tempo::search_tags))
        .route(
            "/api/search/tag/{tag}/values",
            get(tempo::search_tag_values),
        )
        // The current Grafana Tempo datasource tries the v2 tag APIs first and
        // falls back to v1, so without these every tag lookup pays a failed
        // request before the one that works.
        .route("/api/v2/search/tags", get(tempo::search_tags_v2))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(tempo::search_tag_values_v2),
        )
        .route("/api/echo", get(tempo::echo))
        .route("/ready", get(query::ready));
    // Without a token there is no per-tenant retention at all, so the admin
    // surface does not exist rather than existing unauthenticated.
    let router = match state.config.tenant_policy_token {
        Some(_) => router.merge(admin_router()),
        None => router,
    };
    router.with_state(state)
}

/// The write routes, with the body limit they enforce.
///
/// Ingest is OTLP only — the Loki push route was removed with the rest of
/// that ingest (`todo.md`, "Next — OTLP only"); the Loki **query** API above
/// stays, because Grafana reads through it. Without an explicit limit these
/// routes inherit axum's 2 MiB default, so a collector tuned to larger
/// batches would get an unexplained 413 with no knob to turn. The limit
/// matches what the gRPC services accept, so a collector sees one size
/// whichever transport it picks.
fn ingest_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/logs", post(otlp_http::logs))
        .route("/v1/traces", post(otlp_http::traces))
        .layer(DefaultBodyLimit::max(otlp_http::MAX_OTLP_HTTP_BODY_BYTES))
}

fn admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/loggytracy/api/v1/admin/tenants/{tenant}/retention",
            put(admin::put_retention)
                .get(admin::get_retention)
                .delete(admin::delete_retention),
        )
        .route(
            "/loggytracy/api/v1/admin/tenants/{tenant}/usage",
            get(admin::get_usage),
        )
        .layer(DefaultBodyLimit::max(admin::MAX_ADMIN_BODY_BYTES))
}
