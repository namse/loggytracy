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
        .merge(ingest_router(state.clone()))
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
    // The admin routes carry no authentication of their own: loggytracy is
    // not built to be reachable from the outside network, and assumes every
    // request arrives through a secured channel.
    router.merge(admin_router()).with_state(state)
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
fn ingest_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/logs", post(otlp_http::logs))
        .route("/v1/traces", post(otlp_http::traces))
        .layer(DefaultBodyLimit::max(otlp_http::MAX_OTLP_HTTP_BODY_BYTES))
        // Outside the body limit deliberately, so it runs *before* the body is
        // collected. A handler takes `body: Bytes`, which means axum has already
        // put the whole thing in the heap by the time handler code could look at
        // it — an admission check there would be counting memory it had already
        // spent. Here the only thing read is the header.
        .layer(axum::middleware::from_fn_with_state(
            state,
            admit_inflight_body,
        ))
}

/// Charge a push against the in-flight body ceiling for as long as it runs.
///
/// `Content-Length` is what a collector sends and what axum will buffer, so it
/// is the charge. A body without one is chunked, and its size is unknowable
/// until it has been read — exactly the case a bound exists for — so it is
/// charged the ceiling a single request may reach. The guard lives across
/// `next.run`, which is the whole request including the response write.
async fn admit_inflight_body(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let declared = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(otlp_http::MAX_OTLP_HTTP_BODY_BYTES as u64);
    let _permit = match state.ingest_gate.admit_body(declared) {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };
    next.run(request).await
}

fn admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/loggytracy/api/v1/admin/tenants", get(admin::list_tenants))
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
