/// The Loki endpoints Grafana Explore calls beyond query and metadata.
///
/// Each is expressed in terms of machinery that already exists rather than
/// given its own scan. That is not only economy: a second path to the same data
/// is a second place for the retention clamp, the tenant scope and the resource
/// limits to be forgotten.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeParams {
    pub query: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
    pub limit: Option<usize>,
    /// Attributes to aggregate the volume by. Empty means one total series:
    /// with no stream concept there is no per-stream breakdown.
    #[serde(rename = "targetLabels")]
    pub target_labels: Option<String>,
    /// Accepted and not acted on separately: with no `targetLabels` the result
    /// is already per series, and with them it is already by label.
    #[serde(rename = "aggregateBy")]
    #[allow(dead_code)]
    pub aggregate_by: Option<String>,
}

/// `index/volume` — bytes over the window, as an instant vector; broken down
/// by `targetLabels` when given, one total series otherwise.
pub async fn index_volume(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<VolumeParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    volume_response(state, headers, params, false).await
}

/// `index/volume_range` — the same, bucketed, as a matrix. This is what draws
/// the histogram above Explore's results.
pub async fn index_volume_range(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<VolumeParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    volume_response(state, headers, params, true).await
}

async fn volume_response(
    state: Arc<AppState>,
    headers: HeaderMap,
    params: VolumeParams,
    ranged: bool,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    let now_ns = state.clock.now_ns();
    let end_ns = match params.end.as_deref() {
        Some(raw) => parse_time_ns(raw).map_err(|error| (StatusCode::BAD_REQUEST, error))?,
        None => now_ns,
    };
    let start_ns = match params.start.as_deref() {
        Some(raw) => parse_time_ns(raw).map_err(|error| (StatusCode::BAD_REQUEST, error))?,
        None => end_ns.saturating_sub(3_600 * 1_000_000_000),
    };
    if start_ns > end_ns {
        return Err((
            StatusCode::BAD_REQUEST,
            "volume start must not be after end".to_string(),
        ));
    }
    validate_query_range(&state.config, start_ns, end_ns)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    let step_ns = match (ranged, params.step.as_deref()) {
        (true, step) => parse_step_ns(step).map_err(|error| (StatusCode::BAD_REQUEST, error))?,
        // An instant volume is one bucket covering the whole window.
        (false, _) => (end_ns - start_ns).max(1),
    };

    // Expressed as LogQL and handed to the metric evaluator rather than given
    // its own scan. `bytes_over_time` is the quantity volume asks for, so the
    // endpoint reduces to a query this engine already answers — including its
    // scan budgets, its retention clamp and its tenant scope.
    let selector = selector_of(&params.query)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let window = format!("{}ms", (step_ns / 1_000_000).max(1));
    let target_labels = parse_target_labels(params.target_labels.as_deref());
    let expression = if target_labels.is_empty() {
        format!("bytes_over_time({selector}[{window}])")
    } else {
        format!(
            "sum by ({}) (bytes_over_time({selector}[{window}]))",
            target_labels.join(",")
        )
    };
    let logql::QueryExpr::Metric(expr) = logql::parse_expr(&expression).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("volume query is not expressible: {error}"),
        )
    })?
    else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "volume built a non-metric expression".to_string(),
        ));
    };
    crate::query::validate_metric_lookback(&state.config, &expr)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;

    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let start_ns = clamp_to_retention(start_ns, retention_floor_ns);
    if start_ns > end_ns {
        return Ok(Json(empty_volume(ranged)));
    }

    let times = if ranged {
        evaluation_times(start_ns, end_ns, step_ns)
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?
    } else {
        vec![end_ns]
    };
    let max_points = state
        .config
        .max_metric_evaluation_points
        .min(MAX_METRIC_EVALUATION_POINTS);
    if times.len() > max_points {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("volume query exceeds the maximum of {max_points} evaluation points"),
        ));
    }
    // Clamping the range start is not enough: the first bucket still looks back
    // past it. Raise the scan start too, so no expired row reaches the sum.
    let scan_start_override =
        retention_floor_ns.map(|floor_ns| floor_ns.max(start_ns.saturating_sub(step_ns)));
    let execution = run_metric_query_with_stats(
        state,
        tenant,
        expr,
        times,
        scan_start_override,
        crate::metrics::QueryEndpoint::Volume,
    )
    .await
        .map_err(|error| (metric_error_status(&error), error))?;

    let mut series = execution.series;
    // Loki's `limit` on volume is a cap on returned series, applied to the
    // largest ones — an unbounded label set would otherwise answer a histogram
    // request with a series per stream.
    if let Some(limit) = params.limit {
        series.sort_by(|left, right| {
            total_of(right)
                .partial_cmp(&total_of(left))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        series.truncate(limit);
    }

    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "resultType": if ranged { "matrix" } else { "vector" },
            "result": metric_series_json(series, !ranged),
        }
    })))
}

fn empty_volume(ranged: bool) -> serde_json::Value {
    serde_json::json!({
        "status": "success",
        "data": {
            "resultType": if ranged { "matrix" } else { "vector" },
            "result": [],
        }
    })
}

fn parse_target_labels(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

/// The stream selector of a query, without its pipeline.
///
/// Volume asks how many bytes a selector matched, so a line filter or parser
/// stage in the request is not part of the question. Reparsing the selector
/// from the AST rather than string-slicing the request keeps a malformed tail
/// from reaching the expression this builds.
fn selector_of(query: &str) -> Result<String, String> {
    let parsed = logql::parse_expr(query).map_err(|error| format!("LogQL parse error: {error}"))?;
    let matchers = match &parsed {
        logql::QueryExpr::Logs(logs) => &logs.matchers,
        logql::QueryExpr::Metric(expr) => &expr.log_query().matchers,
    };
    let rendered: Vec<String> = matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.op {
                logql::MatcherOp::Eq => "=",
                logql::MatcherOp::Neq => "!=",
                logql::MatcherOp::Re => "=~",
                logql::MatcherOp::NRe => "!~",
            };
            format!(
                "{}{op}\"{}\"",
                matcher.name,
                matcher.value.replace('\\', "\\\\").replace('"', "\\\"")
            )
        })
        .collect();
    Ok(format!("{{{}}}", rendered.join(",")))
}

fn total_of(series: &MetricSeries) -> f64 {
    series.samples.iter().map(|(_, value)| *value).sum()
}

/// `format_query` — validate the query and return it.
///
/// Grafana's format button calls this. It **does not rewrite the query**, and
/// that is deliberate: rendering the whole LogQL surface back to text
/// faithfully is a second grammar to keep in step with the parser, and a
/// formatter that silently turns a query into a different one is worse than a
/// button that leaves it alone. What this does give is the other half of what
/// the button is for — an invalid query is a 400 with the parse error, rather
/// than a rewrite that hides it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatQueryParams {
    pub query: String,
}

pub async fn format_query(
    Query(params): Query<FormatQueryParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let parsed = logql::parse_expr(&params.query)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {error}")))?;
    let _ = parsed;
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": params.query.trim(),
    })))
}

/// `detected_labels` — the labels present in the window with their
/// cardinality, which is what Grafana 11+ builds its filter sidebar from.
pub async fn detected_labels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<crate::query::MetadataParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &params).await? else {
        return Ok(Json(serde_json::json!({ "detectedLabels": [] })));
    };
    // Values come from a bounded sample now that no value index exists —
    // the same shape `detected_fields` always had.
    let mut per_name: std::collections::BTreeMap<String, BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for name in state.memtable.label_names(&tenant, guard.window) {
        for value in state.memtable.label_values(&tenant, &name, guard.window) {
            per_name.entry(name.clone()).or_default().insert(value);
        }
    }
    guard.check_deadline()?;
    for metadata in state
        .parts
        .sample_metadata(
            &tenant,
            guard.window,
            METADATA_SAMPLE_ROWS,
        )
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?
    {
        for (name, value) in metadata {
            per_name.entry(name).or_default().insert(value);
        }
    }

    let detected: Vec<serde_json::Value> = per_name
        .into_iter()
        .map(|(name, values)| {
            serde_json::json!({
                "label": name,
                "cardinality": values.len(),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "detectedLabels": detected })))
}

/// `detected_fields` — the structured-metadata keys carried by lines in the
/// window, with their cardinality and a type guess.
///
/// Bounded by a sample rather than a full scan. The answer feeds a UI hint, so
/// reading the whole window to be exhaustive would spend a query's budget on a
/// list of field names.
pub async fn detected_fields(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<DetectedFieldsParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    let metadata_params = crate::query::MetadataParams {
        start: params.start.clone(),
        end: params.end.clone(),
        query: None,
    };
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &metadata_params).await? else {
        return Ok(Json(serde_json::json!({ "fields": [] })));
    };
    let query = match params.query.as_deref() {
        Some(raw) => match logql::parse_expr(raw).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("LogQL parse error: {error}"),
            )
        })? {
            logql::QueryExpr::Logs(logs) => logs,
            logql::QueryExpr::Metric(expr) => expr.log_query().clone(),
        },
        None => logql::LogQuery {
            matchers: Vec::new(),
            line_filters: Vec::new(),
            stages: Vec::new(),
        },
    };
    let sample_limit = params
        .line_limit
        .unwrap_or(DETECTED_FIELDS_SAMPLE)
        .min(DETECTED_FIELDS_SAMPLE);

    let execution = run_unified_query_with_stats(
        state.clone(),
        tenant,
        query,
        // As `patterns`: a sample of the rows in the log window, so it uses the
        // log window's boundary.
        crate::part::QueryTimeRange::half_open(guard.window.start_ns, guard.window.end_ns),
        sample_limit,
        false,
        Some(sample_limit),
        crate::metrics::QueryEndpoint::DetectedFields,
    )
    .await
    .map_err(|error| (metric_error_status(&error), error))?;

    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for stream in &execution.results {
        for entry in &stream.entries {
            for (name, value) in &entry.structured_metadata {
                values
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
            }
        }
    }
    let fields: Vec<serde_json::Value> = values
        .into_iter()
        .map(|(name, seen)| {
            serde_json::json!({
                "label": name,
                "type": field_type_of(&seen),
                "cardinality": seen.len(),
                "parsers": ["structuredMetadata"],
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "fields": fields })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectedFieldsParams {
    pub query: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(rename = "lineLimit")]
    pub line_limit: Option<usize>,
    #[serde(rename = "fieldName")]
    #[allow(dead_code)]
    pub field_name: Option<String>,
    #[allow(dead_code)]
    pub step: Option<String>,
    #[allow(dead_code)]
    pub limit: Option<usize>,
}

const DETECTED_FIELDS_SAMPLE: usize = 1_000;

/// A type for a field, from the values actually seen. Reported as a hint, so
/// the conservative answer when the values disagree is `string`.
fn field_type_of(values: &BTreeSet<String>) -> &'static str {
    if values.is_empty() {
        return "string";
    }
    if values.iter().all(|value| value.parse::<i64>().is_ok()) {
        return "int";
    }
    if values.iter().all(|value| value.parse::<f64>().is_ok()) {
        return "float";
    }
    if values
        .iter()
        .all(|value| matches!(value.as_str(), "true" | "false"))
    {
        return "boolean";
    }
    "string"
}
