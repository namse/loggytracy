use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};

use crate::{AppState, admin, otlp_http, query};

pub fn build_router(state: Arc<AppState>) -> Router {
    // The ingest routes carry their own body limit, so they are merged as a
    // separate router rather than layered onto this one. `Router::layer`
    // applies to every route registered before it, which would leave the
    // effective limit of a route depending on where in this chain it sits.
    // The read surface is the first-party API alone: the Loki and Tempo
    // compatibility routes went with the read-path decision (issue #3), and
    // the trace routes below are their first-party replacement.
    let router = Router::new()
        .merge(ingest_router(state.clone()))
        .route("/metrics", get(query::metrics))
        .route("/loggytracy/api/v1/logs", get(query::logs))
        .route(
            "/loggytracy/api/v1/logs/histogram",
            get(query::logs_histogram),
        )
        .route(
            "/loggytracy/api/v1/logs/attributes",
            get(query::logs_attributes),
        )
        .route(
            "/loggytracy/api/v1/logs/attributes/{key}/values",
            get(query::logs_attribute_values),
        )
        .route("/loggytracy/api/v1/logs/tail", get(query::logs_tail))
        // Moved from /loki/api/v1/delete with the selector grammar: the old
        // route and the new one could not share one parser, and one parser is
        // the point.
        .route(
            "/loggytracy/api/v1/logs/delete",
            post(query::submit_delete_request)
                .get(query::list_delete_requests)
                .delete(query::cancel_delete_request),
        )
        .route("/loggytracy/api/v1/traces", get(query::traces_search))
        .route(
            "/loggytracy/api/v1/traces/{trace_id}",
            get(query::trace_by_id),
        )
        .route(
            "/loggytracy/api/v1/traces/attributes",
            get(query::traces_attributes),
        )
        .route(
            "/loggytracy/api/v1/traces/attributes/{key}/values",
            get(query::traces_attribute_values),
        )
        .route("/ready", get(query::ready))
        .fallback(query::api_fallback);
    // The admin routes carry no authentication of their own: loggytracy is
    // not built to be reachable from the outside network, and assumes every
    // request arrives through a secured channel.
    router.merge(admin_router()).with_state(state)
}

/// The write routes, with the body limit they enforce.
///
/// Ingest is OTLP only — the Loki push route was removed with the rest of
/// that ingest (`todo.md`, "Next — OTLP only"). Without an explicit limit
/// these routes inherit axum's 2 MiB default, so a collector tuned to larger
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
