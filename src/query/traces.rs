/// The first-party trace endpoints (`docs/QUERY_API.md`). Same contract as
/// the log surface: NDJSON out, refusals that teach, and the whole answer is
/// collected before the first byte is written.
pub async fn trace_by_id(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<axum::response::Response, ApiError> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(ApiError::from_tenant)?;
    if raw.as_deref().is_some_and(|raw| !raw.is_empty()) {
        return Err(ApiError::bad_request(
            "the trace-by-id endpoint takes no parameters: \
GET /loggytracy/api/v1/traces/{trace_id} returns every span retention still holds — \
see docs/QUERY_API.md"
                .to_string(),
        ));
    }
    let trace_id = crate::trace::canonical_trace_id(&trace_id).map_err(ApiError::bad_request)?;

    let _slot = state.tenant_quota.begin_query(&tenant).map_err(|error| {
        ApiError::from_engine(format!("{TENANT_QUOTA_PREFIX}{}", error.message))
    })?;
    let metrics = state.metrics.clone();
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let started = std::time::Instant::now();
    let result = scan_trace_spans(state, tenant, TraceScanTarget::ById(trace_id.clone())).await;
    metrics.observe_query(crate::metrics::QueryEndpoint::TraceById, started.elapsed());
    let mut outcome = match result {
        Ok(outcome) => {
            metrics
                .query_success
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            outcome
        }
        Err(error) => {
            metrics
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(error);
        }
    };
    metrics
        .query_scanned_rows
        .fetch_add(outcome.spans.len() as u64, std::sync::atomic::Ordering::Relaxed);
    metrics
        .query_scanned_bytes
        .fetch_add(outcome.estimated_bytes, std::sync::atomic::Ordering::Relaxed);

    // Retention holds span by span here rather than clamping a window: the
    // route has no window, and a trace straddling the floor should show the
    // part of itself the tenant is still entitled to see.
    if let Some(floor_ns) = retention_floor_ns {
        outcome.spans.retain(|span| span.start_time_ns >= floor_ns);
    }
    if outcome.spans.is_empty() {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!(
                "trace {trace_id} was not found: no spans remain above the tenant's retention \
floor — it may have expired or never existed here"
            ),
        ));
    }
    let body = trace_span_rows_ndjson(&outcome.spans);
    Ok(ndjson_response(
        body,
        outcome.spans.len() as u64,
        outcome.estimated_bytes,
    ))
}

#[derive(Serialize)]
struct TraceSpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    name: String,
    kind: &'static str,
    service: String,
    status: String,
    /// Nanoseconds as strings, like every timestamp this API emits: the
    /// values exceed 2^53, past which a JSON number silently loses precision
    /// in every JavaScript consumer.
    start: String,
    end: String,
    duration: String,
    attributes: std::collections::BTreeMap<String, String>,
    events: Vec<TraceEventRow>,
}

#[derive(Serialize)]
struct TraceEventRow {
    timestamp: String,
    name: String,
    attributes: std::collections::BTreeMap<String, String>,
}

fn trace_span_rows_ndjson(spans: &[crate::trace::TraceSpan]) -> String {
    let mut body = String::new();
    for span in spans {
        let row = trace_span_row(span);
        body.push_str(&serde_json::to_string(&row).expect("a span row serializes infallibly"));
        body.push('\n');
    }
    body
}

fn trace_span_row(span: &crate::trace::TraceSpan) -> TraceSpanRow {
    TraceSpanRow {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: hex_id(&span.span.parent_span_id),
        name: span.span.name.clone(),
        kind: span_kind_label(span.span.kind),
        service: span.service_name().unwrap_or_default().to_string(),
        status: span
            .tag_value("status")
            .expect("status is an intrinsic and always answers"),
        start: span.start_time_ns.to_string(),
        end: span.end_time_ns.to_string(),
        duration: span.duration_ns().to_string(),
        attributes: merged_attributes(span),
        events: span
            .span
            .events
            .iter()
            .map(|event| TraceEventRow {
                timestamp: event.time_unix_nano.to_string(),
                name: event.name.clone(),
                attributes: attribute_map(&event.attributes),
            })
            .collect(),
    }
}

fn hex_id(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn span_kind_label(kind: i32) -> &'static str {
    use opentelemetry_proto::tonic::trace::v1::span::SpanKind;
    match SpanKind::try_from(kind) {
        Ok(SpanKind::Internal) => "internal",
        Ok(SpanKind::Server) => "server",
        Ok(SpanKind::Client) => "client",
        Ok(SpanKind::Producer) => "producer",
        Ok(SpanKind::Consumer) => "consumer",
        _ => "unspecified",
    }
}

/// Resource attributes first, span attributes overwriting same-named keys —
/// the shadowing order `tag_value` already prefers, stated in QUERY_API.md.
fn merged_attributes(
    span: &crate::trace::TraceSpan,
) -> std::collections::BTreeMap<String, String> {
    let mut attributes = span
        .resource
        .as_ref()
        .map(|resource| attribute_map(&resource.attributes))
        .unwrap_or_default();
    for attribute in &span.span.attributes {
        if let Some(value) = &attribute.value {
            attributes.insert(
                attribute.key.clone(),
                crate::trace::attr_display_string(value),
            );
        }
    }
    attributes
}

fn attribute_map(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> std::collections::BTreeMap<String, String> {
    attributes
        .iter()
        .filter_map(|attribute| {
            attribute.value.as_ref().map(|value| {
                (
                    attribute.key.clone(),
                    crate::trace::attr_display_string(value),
                )
            })
        })
        .collect()
}
