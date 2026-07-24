pub async fn trace_by_id(
    State(state): State<Arc<AppState>>,
    Path(trace_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let trace_id =
        canonical_trace_id(&trace_id).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let spans = query_trace(&state, &trace_id).await?;
    if spans.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("trace {trace_id} was not found"),
        ));
    }
    Ok(Json(tempo_trace_response(spans)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchParams {
    pub tags: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(20);
    let max_search_limit = state
        .config
        .max_trace_search_limit
        .min(MAX_TRACE_SEARCH_LIMIT);
    if limit == 0 || limit > max_search_limit {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("limit must be between 1 and {max_search_limit}"),
        ));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64;
    let default_end = now;
    let default_start = state
        .config
        .max_query_range
        .map(|range| {
            range
                .as_nanos()
                .min(i64::MAX as u128) as i64
        })
        .map(|range| default_end.saturating_sub(range))
        .unwrap_or(i64::MIN);
    let start = params
        .start
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or(default_start);
    let end = params
        .end
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or(default_end);
    if start > end {
        return Err((
            StatusCode::BAD_REQUEST,
            "search start must not be after end".to_string(),
        ));
    }
    crate::query::validate_query_range(&state.config, start, end).map_err(client_error)?;
    let tags = parse_tags(params.tags.as_deref()).map_err(client_error)?;
    let min_duration = params
        .min_duration
        .as_deref()
        .map(parse_duration_ns)
        .transpose()
        .map_err(client_error)?;
    let max_duration = params
        .max_duration
        .as_deref()
        .map(parse_duration_ns)
        .transpose()
        .map_err(client_error)?;

    let guard = pin_all_trace_parts(&state).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        None,
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    let mut traces: BTreeMap<String, Vec<TraceSpan>> = BTreeMap::new();
    for span in spans {
        traces.entry(span.trace_id.clone()).or_default().push(span);
    }

    let mut results = Vec::new();
    for (trace_id, mut trace_spans) in traces {
        trace_spans.sort_by_key(|span| span.start_time_ns);
        let root = trace_spans
            .iter()
            .find(|span| span.span.parent_span_id.is_empty())
            .unwrap_or(&trace_spans[0]);
        let start_time = trace_spans
            .iter()
            .map(|span| span.start_time_ns)
            .min()
            .unwrap_or(root.start_time_ns);
        let end_time = trace_spans
            .iter()
            .map(|span| span.end_time_ns)
            .max()
            .unwrap_or(root.end_time_ns);
        if start_time < start || start_time > end {
            continue;
        }
        let duration = end_time.saturating_sub(start_time);
        if min_duration.is_some_and(|minimum| duration < minimum)
            || max_duration.is_some_and(|maximum| duration > maximum)
            || !tags.iter().all(|(name, value)| {
                trace_spans
                    .iter()
                    .any(|span| tag_matches(span, name, value))
            })
        {
            continue;
        }
        results.push(serde_json::json!({
            "traceID": trace_id,
            "rootServiceName": root.service_name().unwrap_or(""),
            "rootTraceName": root.span.name,
            "startTimeUnixNano": start_time.to_string(),
            "durationMs": (duration as f64) / 1_000_000.0,
        }));
        if results.len() == limit {
            break;
        }
    }
    Ok(Json(serde_json::json!({ "traces": results })))
}

pub async fn search_tags(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let guard = pin_all_trace_parts(&state).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        None,
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    let tags = collect_tags(&spans);
    let (resource_tags, span_tags) = collect_scoped_tags(&spans);
    Ok(Json(serde_json::json!({
        "tags": tags,
        "scopes": [
            { "name": "resource", "tags": resource_tags },
            { "name": "span", "tags": span_tags },
            { "name": "intrinsic", "tags": ["duration", "name", "status"] },
        ],
    })))
}

pub async fn search_tag_values(
    State(state): State<Arc<AppState>>,
    Path(tag): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let guard = pin_all_trace_parts(&state).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        None,
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    let values: BTreeSet<String> = spans
        .iter()
        .filter_map(|span| span.tag_value(&tag))
        .collect();
    Ok(Json(serde_json::json!({ "tag": tag, "values": values })))
}

