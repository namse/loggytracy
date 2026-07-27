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
}

#[derive(Serialize)]
pub struct RetentionResponse {
    tenant: String,
    retention: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingest_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_rate: Option<String>,
    updated_at: String,
}

/// `PUT …/tenants/{tenant}/retention` — the control plane's only way to change
/// retention. Answered only once the policy is durable, so the caller's retry
/// loop terminates on a real guarantee rather than on an in-memory write.
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
        updated_at: rfc3339(view.updated_at),
    }))
}

/// `DELETE` returns the tenant to *unknown*, which keeps its data forever. It
/// is not tenant deletion — that is `retention: "0"`.
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
