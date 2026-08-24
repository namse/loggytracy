use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::tenant::TenantId;
use crate::tenant_policy::PolicyError;

/// Admin request bodies are a single field. The limit exists to keep an
/// unauthenticated caller from buffering anything meaningful before the token
/// is checked.
pub const MAX_ADMIN_BODY_BYTES: usize = 4 * 1024;

#[derive(Deserialize)]
struct RetentionRequest {
    retention: String,
    /// Optional so a control plane that only manages retention keeps working
    /// unchanged. Omitting it clears any rate previously pushed for the
    /// tenant — the body is the whole policy, not a patch of it.
    #[serde(default)]
    ingest_rate: Option<String>,
    /// The read side of the same policy. Optional for the same reason.
    #[serde(default)]
    query_rate: Option<String>,
    /// Distinct log streams the tenant may hold at once.
    #[serde(default)]
    max_streams: Option<u64>,
    /// Bytes the tenant may keep stored, as a size such as `10GiB`. Optional
    /// for the same reason as the rates.
    #[serde(default)]
    max_stored_bytes: Option<String>,
}

#[derive(Serialize)]
pub struct RetentionResponse {
    tenant: String,
    retention: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingest_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_streams: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_stored_bytes: Option<String>,
    updated_at: String,
}

#[derive(Serialize)]
pub struct TenantListResponse {
    tenants: Vec<RetentionResponse>,
}

/// `PUT …/tenants/{tenant}/retention` — the control plane's only way to change
/// retention, and the only way a tenant is onboarded at all: the pushed
/// policies are the tenant registry, so the moment this answers 200 the
/// tenant's requests are served. Answered only once the policy is durable, so
/// the caller's retry loop terminates on a real guarantee rather than on an
/// in-memory write.
pub async fn put_retention(
    State(state): State<Arc<AppState>>,
    Path(raw_tenant): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<RetentionResponse>, (StatusCode, String)> {
    authorize(&state, &headers)?;
    let tenant = parse_tenant_for_change(&state, &raw_tenant)?;
    let request: RetentionRequest = serde_json::from_slice(&body).map_err(|error| {
        state.tenant_policy.record_rejected_push();
        (
            StatusCode::BAD_REQUEST,
            format!("invalid retention request body: {error}"),
        )
    })?;
    let view = state
        .tenant_policy
        .push(
            &tenant,
            &request.retention,
            request.ingest_rate.as_deref(),
            request.query_rate.as_deref(),
            request.max_streams,
            request.max_stored_bytes.as_deref(),
        )
        .await
        .map_err(into_http)?;
    tracing::info!(
        %tenant,
        retention = %view.retention,
        ingest_rate = view.ingest_rate.as_deref().unwrap_or("unset"),
        query_rate = view.query_rate.as_deref().unwrap_or("unset"),
        "tenant policy updated"
    );
    Ok(Json(RetentionResponse {
        tenant: tenant.as_str().to_string(),
        retention: view.retention,
        ingest_rate: view.ingest_rate,
        query_rate: view.query_rate,
        max_streams: view.max_streams,
        max_stored_bytes: view.max_stored_bytes,
        updated_at: rfc3339(view.updated_at),
    }))
}

pub async fn get_retention(
    State(state): State<Arc<AppState>>,
    Path(raw_tenant): Path<String>,
    headers: HeaderMap,
) -> Result<Json<RetentionResponse>, (StatusCode, String)> {
    authorize(&state, &headers)?;
    let tenant = parse_tenant(&raw_tenant)?;
    let view = state.tenant_policy.view(&tenant).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("no retention policy for tenant {tenant}"),
        )
    })?;
    Ok(Json(RetentionResponse {
        tenant: tenant.as_str().to_string(),
        retention: view.retention,
        ingest_rate: view.ingest_rate,
        query_rate: view.query_rate,
        max_streams: view.max_streams,
        max_stored_bytes: view.max_stored_bytes,
        updated_at: rfc3339(view.updated_at),
    }))
}

/// `GET …/admin/tenants` — every tenant this instance serves, with the policy
/// each one was pushed. The pushed policies are the tenant registry, so this
/// is the control plane's reconciliation read: what it believes it onboarded
/// against what this instance actually holds.
pub async fn list_tenants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<TenantListResponse>, (StatusCode, String)> {
    authorize(&state, &headers)?;
    // The routes are only mounted with a token, so the policy is enabled and
    // the snapshot exists; an empty instance still answers with an empty list.
    let tenants = state
        .tenant_policy
        .snapshot()
        .map(|policies| {
            policies
                .views()
                .map(|(tenant, view)| RetentionResponse {
                    tenant: tenant.as_str().to_string(),
                    retention: view.retention,
                    ingest_rate: view.ingest_rate,
                    query_rate: view.query_rate,
                    max_streams: view.max_streams,
                    max_stored_bytes: view.max_stored_bytes,
                    updated_at: rfc3339(view.updated_at),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Json(TenantListResponse { tenants }))
}

/// `DELETE` returns the tenant to *unknown*: its data is kept forever, and —
/// because the pushed policies are the tenant registry — its requests are
/// refused from here on. Deleting the data is `retention: "0"`, pushed before
/// this.
pub async fn delete_retention(
    State(state): State<Arc<AppState>>,
    Path(raw_tenant): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize(&state, &headers)?;
    let tenant = parse_tenant_for_change(&state, &raw_tenant)?;
    state
        .tenant_policy
        .remove(&tenant)
        .await
        .map_err(into_http)?;
    tracing::info!(%tenant, "tenant retention policy removed; the tenant keeps its data");
    Ok(StatusCode::OK)
}

/// `GET …/tenants/{tenant}/usage` — what one tenant is currently costing this
/// instance.
///
/// Deliberately here rather than as labels on `/metrics`. That endpoint is
/// unauthenticated and process-wide by design, and a label per tenant would
/// multiply every series by the tenant count — on a workload whose whole point
/// is many small tenants, that is the cardinality problem this engine bounds
/// everywhere else. The reader that actually needs per-tenant numbers is the
/// control plane, which is already authenticated here and already asks per
/// tenant.
pub async fn get_usage(
    State(state): State<Arc<AppState>>,
    Path(raw_tenant): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize(&state, &headers)?;
    let tenant = parse_tenant(&raw_tenant)?;

    let window = crate::part::MetadataWindow {
        start_ns: i64::MIN,
        end_ns: i64::MAX,
    };
    let on_disk = state.parts.stats(&tenant, window);
    let buffered = state.memtable.stats(&tenant, window);
    // What a storage plan charges for: the tenant's own extents in the shared
    // objects, logs and traces together. Read from the registries' running
    // census rather than recomputed, so this endpoint costs the same whether
    // the tenant has one part or ten thousand.
    let stored_bytes = state
        .parts
        .tenant_stored_bytes(&tenant)
        .saturating_add(state.trace_parts.tenant_stored_bytes(&tenant));
    // The union, for the same reason the limit uses it: a flushed stream lives
    // in both until the buffer is cleared.
    let streams = state
        .memtable
        .tenant_streams(&tenant)
        .iter()
        .filter(|labels| {
            !state
                .parts
                .contains_stream(&tenant, crate::part_registry::stream_key(labels))
        })
        .count()
        + state.parts.tenant_stream_count(&tenant);
    let (ingest_budget, query_budget) = state.tenant_quota.budget_snapshot(&tenant);

    Ok(Json(serde_json::json!({
        "tenant": tenant.as_str(),
        "streams": streams,
        "max_streams": state.tenant_quota.max_streams_for(&tenant),
        "parts": state.parts.tenant_part_count(&tenant),
        "entries": on_disk.entries + buffered.entries,
        "bytes": on_disk.bytes + buffered.bytes,
        // `bytes` above counts logs, including what is buffered and therefore
        // not yet stored anywhere. `stored_bytes` is the durable total the
        // limit is compared against, so a control plane showing a customer
        // their usage reads this one and nothing else.
        "stored_bytes": stored_bytes,
        "max_stored_bytes": state.tenant_quota.max_stored_bytes_for(&tenant),
        // Remaining tokens, not consumption: the control plane holds the
        // monthly ledger, and what an instance can answer is how close this
        // tenant is to being refused right now.
        "ingest_budget_bytes": ingest_budget,
        "query_budget_bytes": query_budget,
    })))
}

/// The tenant id arrives in the request path and ends up in an object key, so
/// it goes through the same allowlist as an ingest header rather than a
/// path-specific check.
fn parse_tenant(raw: &str) -> Result<TenantId, (StatusCode, String)> {
    TenantId::parse(raw).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid tenant id: {error}"),
        )
    })
}

/// A rejected id on a method that meant to change the policy. `GET` reads, so
/// a malformed id there is a bad read, not a rejected push.
fn parse_tenant_for_change(state: &AppState, raw: &str) -> Result<TenantId, (StatusCode, String)> {
    parse_tenant(raw).inspect_err(|_| state.tenant_policy.record_rejected_push())
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    // The routes are only mounted when a token is configured, so a missing one
    // here is unreachable rather than a way in.
    let Some(expected) = state.config.tenant_policy_token.as_deref() else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "per-tenant retention is not enabled".to_string(),
        ));
    };
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");
    if secret_matches(provided.as_bytes(), expected.as_bytes()) {
        return Ok(());
    }
    state.tenant_policy.record_unauthorized();
    Err((
        StatusCode::UNAUTHORIZED,
        "invalid or missing bearer token".to_string(),
    ))
}

/// Compares without an early return, so neither the token's length nor the
/// position of the first mismatch is observable in the response time.
fn secret_matches(provided: &[u8], expected: &[u8]) -> bool {
    let mut difference = u8::from(provided.len() != expected.len());
    for index in 0..provided.len().max(expected.len()) {
        let left = provided.get(index).copied().unwrap_or(0);
        let right = expected.get(index).copied().unwrap_or(0);
        difference |= left ^ right;
    }
    difference == 0
}

fn into_http(error: PolicyError) -> (StatusCode, String) {
    match error {
        // Nothing was stored, so retrying the same request cannot help.
        PolicyError::Invalid(message) => (StatusCode::BAD_REQUEST, message),
        // Nothing was applied either. The control plane owns the retry, which
        // is what bounds the exposure of a delayed upgrade to a retry rather
        // than to an outage.
        PolicyError::Persist(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
    }
}

fn rfc3339(at: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at).to_rfc3339()
}

#[cfg(test)]
mod tests {
    include!("tests/admin.rs");
}
