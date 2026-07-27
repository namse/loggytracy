pub async fn trace_by_id(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let trace_id =
        canonical_trace_id(&trace_id).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let mut spans = query_trace(&state, &tenant, &trace_id).await?;
    // A trace lookup carries no range, so retention is applied to the spans
    // themselves instead of to a clamped window.
    if let Some(floor_ns) = state.tenant_policy.query_floor_ns(&tenant) {
        spans.retain(|span| span.start_time_ns >= floor_ns);
    }
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
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
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
    let now = state.clock.now_ns();
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
    let start = match state.tenant_policy.query_floor_ns(&tenant) {
        Some(floor_ns) => start.max(floor_ns),
        None => start,
    };
    if start > end {
        return Ok(Json(serde_json::json!({ "traces": [] })));
    }
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

    // A trace matches when any of its spans overlaps the window.
    //
    // This used to require the trace's *earliest* span to fall inside it,
    // which is both narrower than Tempo — a request that began before the
    // window and was still running inside it is exactly what an operator
    // searches for — and impossible to prune for, because that earliest span
    // can sit in a row group the window never touches. Overlap is the rule the
    // row-group bounds already answer, so the scan reads only the row groups
    // that can contribute and the restore set narrows with it. Every trace the
    // old rule returned still matches: its earliest span was in the window, so
    // the trace overlaps it.
    let guard = pin_all_trace_parts(&state, &tenant, Some((start, end))).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        tenant.clone(),
        None,
        Some((start, end)),
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
        // Summarized from the spans that overlap the window, not from the
        // whole trace: reading the rest would mean restoring the parts the
        // window was pruned to avoid. A trace opened from these results is
        // fetched by id and shows its full extent there.
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
    headers: HeaderMap,
    Query(params): Query<TagParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(range) = tag_range(&state, &tenant, &params)? else {
        return Ok(Json(empty_tags()));
    };
    let guard = pin_all_trace_parts(&state, &tenant, Some(range)).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        tenant.clone(),
        None,
        Some(range),
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    let tags = params.truncate(collect_tags(&spans));
    let (resource_tags, span_tags) = collect_scoped_tags(&spans);
    let resource_tags = params.truncate(resource_tags);
    let span_tags = params.truncate(span_tags);
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
    headers: HeaderMap,
    Path(tag): Path<String>,
    Query(params): Query<TagParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(range) = tag_range(&state, &tenant, &params)? else {
        return Ok(Json(serde_json::json!({ "tag": tag, "values": [] })));
    };
    let guard = pin_all_trace_parts(&state, &tenant, Some(range)).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        tenant.clone(),
        None,
        Some(range),
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    let values: BTreeSet<String> = spans
        .iter()
        .filter_map(|span| span.tag_value(&tag))
        .collect();
    let values = params.truncate(values);
    Ok(Json(serde_json::json!({ "tag": tag, "values": values })))
}


/// The window a tag lookup covers.
///
/// Grafana sends `start`/`end` on every tag call. Answering from the whole
/// history both returns tags that do not exist in the range and restores every
/// part the tenant ever wrote to do it — one dropdown, the entire archive.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagParams {
    pub start: Option<String>,
    pub end: Option<String>,
    /// Accepted and ignored. Grafana sends it on tag calls and
    /// `deny_unknown_fields` would turn that into a 400.
    #[serde(default, rename = "q")]
    #[allow(dead_code)]
    pub query: Option<String>,
    /// How many names or values the client wants back.
    pub limit: Option<usize>,
}

impl TagParams {
    /// Truncate a tag result to what the client asked for. The window already
    /// bounds the work; this only bounds the response.
    fn truncate<T>(&self, values: impl IntoIterator<Item = T>) -> Vec<T> {
        let mut values: Vec<T> = values.into_iter().collect();
        if let Some(limit) = self.limit {
            values.truncate(limit);
        }
        values
    }
}

/// Resolve the tag window, or `None` when retention has already emptied it.
///
/// The retention floor folds in here, so one bound expresses both what the
/// client asked for and what the tenant is still entitled to — the same shape
/// the log metadata endpoints settled on.
fn tag_range(
    state: &AppState,
    tenant: &TenantId,
    params: &TagParams,
) -> Result<Option<(i64, i64)>, (StatusCode, String)> {
    let end = params
        .end
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or_else(|| state.clock.now_ns());
    let start = params
        .start
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or(i64::MIN);
    if start > end {
        return Err((
            StatusCode::BAD_REQUEST,
            "tag search start must not be after end".to_string(),
        ));
    }
    let start = match state.tenant_policy.query_floor_ns(tenant) {
        Some(floor_ns) => start.max(floor_ns),
        None => start,
    };
    Ok((start <= end).then_some((start, end)))
}

fn empty_tags() -> serde_json::Value {
    serde_json::json!({
        "tags": [],
        "scopes": [
            { "name": "resource", "tags": [] },
            { "name": "span", "tags": [] },
            { "name": "intrinsic", "tags": ["duration", "name", "status"] },
        ],
    })
}
