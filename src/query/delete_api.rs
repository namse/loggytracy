/// The HTTP surface of the deletion requests. What a request means, and why it
/// both hides now and removes later, is in [`crate::delete_requests`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSubmitParams {
    pub query: String,
    pub start: String,
    pub end: Option<String>,
    /// Accepted for compatibility and ignored: it exists in Loki to split one
    /// request into chunks its compactor can schedule, which is a property of
    /// how that deletion is executed rather than of what was asked for.
    #[allow(dead_code)]
    pub max_interval: Option<String>,
}

/// `POST delete` — hide a selector's lines now, remove them at the next rewrite.
pub async fn submit_delete_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<DeleteSubmitParams>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let start_ns = crate::query::parse_time_ns(&params.start)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid start: {error}")))?;
    let now_ns = state.clock.now_ns();
    let end_ns = match params.end.as_deref() {
        Some(raw) => crate::query::parse_time_ns(raw)
            .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid end: {error}")))?,
        // Loki's default: everything from `start` up to the moment the request
        // was made. Leaving it open-ended instead would make the request cover
        // lines that had not been written when it was submitted.
        None => now_ns,
    };
    state
        .delete_requests
        .submit(&tenant, &params.query, start_ns, end_ns, now_ns)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(delete_error_response)
}

/// `GET delete` — the tenant's requests and what has happened to each.
pub async fn list_delete_requests(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let requests: Vec<serde_json::Value> = state
        .delete_requests
        .list(&tenant)
        .iter()
        .map(crate::delete_requests::DeleteRequest::to_json)
        .collect();
    Ok(Json(serde_json::Value::Array(requests)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteCancelParams {
    pub request_id: String,
    /// Loki uses this to cancel a partially-processed request. Here a request
    /// is either hiding rows or has had them rewritten away, and the second is
    /// refused whatever this says, so there is nothing for it to force.
    #[allow(dead_code)]
    pub force: Option<bool>,
}

/// `DELETE delete` — withdraw a request whose rows are still only hidden.
pub async fn cancel_delete_request(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<DeleteCancelParams>,
) -> Result<StatusCode, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    state
        .delete_requests
        .cancel(&tenant, &params.request_id)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(delete_error_response)
}

fn delete_error_response(
    error: crate::delete_requests::DeleteRequestError,
) -> (StatusCode, String) {
    use crate::delete_requests::{DeleteRequestError, MAX_DELETE_REQUESTS_PER_TENANT};
    match error {
        DeleteRequestError::Invalid(message) => (StatusCode::BAD_REQUEST, message),
        DeleteRequestError::TooMany => (
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "a tenant may hold {MAX_DELETE_REQUESTS_PER_TENANT} deletion requests at once; \
cancel a processed one before submitting another"
            ),
        ),
        DeleteRequestError::NotFound => (
            StatusCode::NOT_FOUND,
            "no such deletion request for this tenant".to_string(),
        ),
        // The request is not durable, so it must not be reported as accepted.
        DeleteRequestError::Storage(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
    }
}
