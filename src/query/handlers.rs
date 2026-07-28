pub async fn query_range(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let parsed = logql::parse_expr(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = state.clock.now_ns();

    let end_ns = match params.end.as_deref() {
        Some(s) => parse_time_ns(s).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => now_ns,
    };
    let start_ns = match params.start.as_deref() {
        Some(s) => parse_time_ns(s).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => match &parsed {
            logql::QueryExpr::Logs(_) => state
                .config
                .max_query_range
                .map(duration_to_i64_ns)
                .map(|range| end_ns.saturating_sub(range))
                .unwrap_or(i64::MIN),
            logql::QueryExpr::Metric(expr) => end_ns.saturating_sub(expr.lookback_ns()),
        },
    };

    if start_ns > end_ns {
        return Err((
            StatusCode::BAD_REQUEST,
            "query start must not be after end".to_string(),
        ));
    }
    validate_query_range(&state.config, start_ns, end_ns)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if let logql::QueryExpr::Metric(expr) = &parsed {
        crate::query::validate_metric_lookback(&state.config, expr)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    }

    // Retention is enforced logically here, before any scan. The range is
    // validated against the request the client made and clamped afterwards, so
    // a downgrade hides data immediately instead of turning valid queries into
    // errors. `None` leaves the range untouched: an unknown tenant, or a
    // control plane that has not answered yet, must never break reads.
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let start_ns = clamp_to_retention(start_ns, retention_floor_ns);
    if start_ns > end_ns {
        return Ok(Json(empty_query_range_response(&parsed)));
    }

    let forward = parse_direction(&params.direction).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let max_log_limit = state.config.max_log_limit.min(MAX_LOG_LIMIT);
    let max_scan_rows = state.config.max_query_scan_rows.min(MAX_LOG_SCAN_ROWS);
    let max_metric_points = state
        .config
        .max_metric_evaluation_points
        .min(MAX_METRIC_EVALUATION_POINTS);
    let (result_type, result, total_lines_processed) = match parsed {
        logql::QueryExpr::Logs(parsed) => {
            let limit = parse_limit(params.limit, max_log_limit)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let execution = run_unified_query_with_stats(
                state,
                tenant,
                parsed,
                part::QueryTimeRange::half_open(start_ns, end_ns),
                limit,
                forward,
                Some(max_scan_rows),
                crate::metrics::QueryEndpoint::QueryRange,
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "streams",
                crate::query::ResultPayload::Streams(build_stream_data(execution.results, forward)),
                execution.scanned_rows,
            )
        }
        logql::QueryExpr::Metric(expr) => {
            let step_ns =
                parse_step_ns(params.step.as_deref()).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let times = evaluation_times(start_ns, end_ns, step_ns)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            if times.len() > max_metric_points {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "metric query exceeds the maximum of {max_metric_points} evaluation points"
                    ),
                ));
            }
            // Clamping the range start is not enough for a metric query: the
            // first evaluation point still looks back past it. Raise the scan
            // start too, so no expired row can reach the evaluator.
            let scan_start_override = retention_floor_ns
                .map(|floor_ns| floor_ns.max(start_ns.saturating_sub(expr.lookback_ns())));
            let execution =
                run_metric_query_with_stats(
                    state,
                    tenant,
                    expr,
                    times,
                    scan_start_override,
                    crate::metrics::QueryEndpoint::QueryRange,
                )
                    .await
                    .map_err(|e| (metric_error_status(&e), e))?;
            (
                "matrix",
                crate::query::ResultPayload::Value(metric_series_json(execution.series, false)),
                execution.scanned_rows,
            )
        }
    };

    Ok(Json(LokiResponse {
        status: "success",
        data: QueryRangeData {
            result_type,
            result,
            stats: Stats {
                summary: StatsSummary {
                    total_lines_processed,
                },
            },
        },
    }))
}

pub async fn query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let parsed = logql::parse_expr(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = state.clock.now_ns();

    let end_ns = match params.time.as_deref() {
        Some(t) => parse_time_ns(t).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => now_ns,
    };

    let query_start_ns = match &parsed {
        logql::QueryExpr::Logs(_) => state
            .config
            .max_query_range
            .map(duration_to_i64_ns)
            .map(|range| end_ns.saturating_sub(range))
            .unwrap_or(i64::MIN),
        logql::QueryExpr::Metric(expr) => end_ns.saturating_sub(expr.lookback_ns()),
    };
    validate_query_range(&state.config, query_start_ns, end_ns)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if let logql::QueryExpr::Metric(expr) = &parsed {
        crate::query::validate_metric_lookback(&state.config, expr)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    }

    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let query_start_ns = clamp_to_retention(query_start_ns, retention_floor_ns);
    if query_start_ns > end_ns {
        return Ok(Json(empty_query_response(&parsed)));
    }

    let forward = parse_direction(&params.direction).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let max_log_limit = state.config.max_log_limit.min(MAX_LOG_LIMIT);
    let max_scan_rows = state.config.max_query_scan_rows.min(MAX_LOG_SCAN_ROWS);

    let (result_type, result, total_lines_processed) = match parsed {
        logql::QueryExpr::Logs(parsed) => {
            let limit = parse_limit(params.limit, max_log_limit)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let execution = run_unified_query_with_stats(
                state,
                tenant,
                parsed,
                part::QueryTimeRange::half_open(query_start_ns, end_ns),
                limit,
                forward,
                Some(max_scan_rows),
                crate::metrics::QueryEndpoint::Query,
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "streams",
                crate::query::ResultPayload::Streams(build_stream_data(execution.results, forward)),
                execution.scanned_rows,
            )
        }
        logql::QueryExpr::Metric(expr) => {
            let scan_start_override = retention_floor_ns
                .map(|floor_ns| floor_ns.max(end_ns.saturating_sub(expr.lookback_ns())));
            let execution = run_metric_query_with_stats(
                state,
                tenant,
                expr,
                vec![end_ns],
                scan_start_override,
                crate::metrics::QueryEndpoint::Query,
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "vector",
                crate::query::ResultPayload::Value(metric_series_json(execution.series, true)),
                execution.scanned_rows,
            )
        }
    };

    Ok(Json(LokiResponse {
        status: "success",
        data: QueryRangeData {
            result_type,
            result,
            stats: Stats {
                summary: StatsSummary {
                    total_lines_processed,
                },
            },
        },
    }))
}

/// `effective_start = max(requested_start, now - retention(tenant))`.
fn clamp_to_retention(start_ns: i64, retention_floor_ns: Option<i64>) -> i64 {
    match retention_floor_ns {
        Some(floor_ns) => start_ns.max(floor_ns),
        None => start_ns,
    }
}

/// A range query whose whole window is older than the tenant's retention. The
/// data may still be on disk waiting for the next merge, but it is no longer
/// the tenant's to read.
fn empty_query_range_response(parsed: &logql::QueryExpr) -> LokiResponse<QueryRangeData> {
    empty_response(match parsed {
        logql::QueryExpr::Logs(_) => "streams",
        logql::QueryExpr::Metric(_) => "matrix",
    })
}

fn empty_query_response(parsed: &logql::QueryExpr) -> LokiResponse<QueryRangeData> {
    empty_response(match parsed {
        logql::QueryExpr::Logs(_) => "streams",
        logql::QueryExpr::Metric(_) => "vector",
    })
}

fn empty_response(result_type: &'static str) -> LokiResponse<QueryRangeData> {
    LokiResponse {
        status: "success",
        data: QueryRangeData {
            result_type,
            result: crate::query::ResultPayload::Value(serde_json::Value::Array(Vec::new())),
            stats: Stats {
                summary: StatsSummary {
                    total_lines_processed: 0,
                },
            },
        },
    }
}

fn duration_to_i64_ns(duration: std::time::Duration) -> i64 {
    duration
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

/// What every metadata endpoint has to acquire before it touches a registry.
///
/// These four used to have none of it: no concurrency bound, no deadline, and
/// no time range, so each call read every part of every tenant it was allowed
/// to see. They share the log scan semaphore rather than getting their own,
/// because they compete for the same thing a log query does — the part readers.
struct MetadataGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _occupancy: crate::metrics::ScanOccupancy,
    window: crate::part::MetadataWindow,
    deadline: std::time::Instant,
}

impl MetadataGuard {
    async fn acquire(
        state: &Arc<AppState>,
        tenant: &crate::tenant::TenantId,
        params: &crate::query::MetadataParams,
    ) -> Result<Option<Self>, (StatusCode, String)> {
        let window = metadata_window(state, params)?.clamped_to(state.tenant_policy.query_floor_ns(tenant));
        // An empty window is a valid question with an empty answer, not an
        // error: a tenant whose retention already passed the requested range
        // asks this on every dashboard refresh.
        if window.is_empty() {
            return Ok(None);
        }
        let permit = match state.query_scan_semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "query semaphore closed".to_string(),
                ));
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                let queued_at = std::time::Instant::now();
                let permit = state
                    .query_scan_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "query semaphore closed".to_string(),
                        )
                    })?;
                state.metrics.record_scan_queue_wait(queued_at.elapsed());
                permit
            }
        };
        Ok(Some(Self {
            _permit: permit,
            _occupancy: crate::metrics::ScanOccupancy::enter(state.metrics.clone()),
            window,
            deadline: std::time::Instant::now() + state.config.max_query_runtime,
        }))
    }

    /// Checked between units of work rather than enforced by a timer: these
    /// lookups are synchronous walks, so the only place a deadline can act is
    /// where the walk yields control back.
    fn check_deadline(&self) -> Result<(), (StatusCode, String)> {
        if std::time::Instant::now() > self.deadline {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "metadata query exceeded its time budget".to_string(),
            ));
        }
        Ok(())
    }
}

/// `series` takes its query string raw so it can read repeated `match[]`, so
/// the time bounds have to come out of the same string rather than from an
/// extractor.
fn metadata_params_from_raw(
    raw: &Option<String>,
) -> Result<crate::query::MetadataParams, (StatusCode, String)> {
    let Some(query) = raw else {
        return Ok(crate::query::MetadataParams::default());
    };
    let mut params = crate::query::MetadataParams::default();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "start" => params.start = Some(value.into_owned()),
            "end" => params.end = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(params)
}

fn metadata_window(
    state: &Arc<AppState>,
    params: &crate::query::MetadataParams,
) -> Result<crate::part::MetadataWindow, (StatusCode, String)> {
    let now_ns = state.clock.now_ns();
    let end_ns = match params.end.as_deref() {
        Some(raw) => crate::query::parse_time_ns(raw).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => now_ns,
    };
    // Absent `start` means the configured maximum range back from `end`, not
    // all of history: an unbounded default here is what made these endpoints
    // read every part.
    let start_ns = match params.start.as_deref() {
        Some(raw) => crate::query::parse_time_ns(raw).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => state
            .config
            .max_query_range
            .map(duration_to_i64_ns)
            .map(|range| end_ns.saturating_sub(range))
            .unwrap_or(i64::MIN),
    };
    if start_ns > end_ns {
        return Err((
            StatusCode::BAD_REQUEST,
            "start must not be after end".to_string(),
        ));
    }
    validate_query_range(&state.config, start_ns, end_ns)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    Ok(crate::part::MetadataWindow { start_ns, end_ns })
}

pub async fn labels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<crate::query::MetadataParams>,
) -> Result<Json<LokiResponse<Vec<String>>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &params).await? else {
        return Ok(Json(LokiResponse {
            status: "success",
            data: Vec::new(),
        }));
    };
    let mut names = std::collections::BTreeSet::new();
    for n in state.memtable.label_names(&tenant, guard.window) {
        names.insert(n);
    }
    guard.check_deadline()?;
    for n in state.parts.label_names(&tenant, guard.window) {
        names.insert(n);
    }
    Ok(Json(LokiResponse {
        status: "success",
        data: names.into_iter().collect(),
    }))
}

/// The matchers `label/{name}/values?query=` filters by, or `None` when the
/// caller sent no selector.
///
/// A line filter is refused rather than dropped. Label values come from stream
/// labels, so no filter over line content can narrow them without a scan this
/// endpoint does not run — and answering a query that carries one as though it
/// did not is the silent approximation this engine refuses elsewhere. Grafana
/// sends a selector here, so the refusal is for a caller that hand-wrote
/// something this cannot honour.
fn label_values_filter(
    params: &crate::query::MetadataParams,
) -> Result<Option<Vec<crate::logql::LabelMatcher>>, (StatusCode, String)> {
    let Some(selector) = params.query.as_deref().map(str::trim).filter(|q| !q.is_empty()) else {
        return Ok(None);
    };
    let parsed = crate::logql::parse(selector).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            format!("LogQL parse error: {error}"),
        )
    })?;
    if !parsed.line_filters.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "label values can be filtered by a stream selector but not by a line filter: \
             label values come from stream labels, which no filter over line content narrows"
                .to_string(),
        ));
    }
    Ok(Some(parsed.matchers))
}

pub async fn label_values(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<crate::query::MetadataParams>,
) -> Result<Json<LokiResponse<Vec<String>>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &params).await? else {
        return Ok(Json(LokiResponse {
            status: "success",
            data: Vec::new(),
        }));
    };
    let mut values = std::collections::BTreeSet::new();
    match label_values_filter(&params)? {
        // Loki filters this endpoint's values by a stream selector, and a
        // dropdown built without it offers values that belong to another
        // stream and return nothing when clicked.
        Some(matchers) => {
            for labels in state.memtable.series(&tenant, &matchers, guard.window) {
                if let Some(value) = labels.get(&name) {
                    values.insert(value.clone());
                }
            }
            guard.check_deadline()?;
            for labels in state.parts.series(&tenant, &matchers, guard.window) {
                if let Some(value) = labels.get(&name) {
                    values.insert(value.clone());
                }
            }
        }
        None => {
            for v in state.memtable.label_values(&tenant, &name, guard.window) {
                values.insert(v);
            }
            guard.check_deadline()?;
            for v in state.parts.label_values(&tenant, &name, guard.window) {
                values.insert(v);
            }
        }
    }
    Ok(Json(LokiResponse {
        status: "success",
        data: values.into_iter().collect(),
    }))
}

pub async fn ready(
    State(state): State<Arc<AppState>>,
) -> Result<&'static str, (StatusCode, String)> {
    if state.shutdown.is_fenced() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "fenced by a newer writer; this instance no longer owns the object-store prefix"
                .to_string(),
        ));
    }
    if state.shutdown.is_draining() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "draining for shutdown: force_flush_complete={}, pending_flush_bytes={}",
                state.shutdown.is_flush_complete(),
                state.shutdown.pending_flush_bytes(),
            ),
        ));
    }
    let mut unavailable = Vec::new();
    if !state.journal.is_healthy() {
        unavailable.push("journal writer");
    }
    if !state.flush_healthy.load(Ordering::Acquire) {
        unavailable.push("flush worker");
    }
    if !state.merge_healthy.load(Ordering::Acquire) {
        unavailable.push("merge worker");
    }
    if !state.retention_healthy.load(Ordering::Acquire) {
        unavailable.push("retention worker");
    }
    if !state.otlp_healthy.load(Ordering::Acquire) {
        unavailable.push("OTLP gRPC server");
    }
    if let Some(cache) = &state.remote_cache {
        if !cache.is_remote_healthy() {
            unavailable.push("object store");
        }
        if !cache.is_cache_healthy() {
            unavailable.push("local cache");
        }
    }

    if unavailable.is_empty() {
        Ok("ready")
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{} unavailable", unavailable.join(", ")),
        ))
    }
}

pub async fn buildinfo() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "success",
        "data": {
            "version": env!("CARGO_PKG_VERSION"),
            "revision": build_revision(),
            "branch": option_env!("LOGGYTRACY_BUILD_BRANCH").unwrap_or("unknown"),
            "goVersion": "n/a"
        }
    }))
}

pub async fn index_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<crate::query::MetadataParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &params).await? else {
        return Ok(Json(serde_json::json!({
            "status": "success",
            "data": { "streams": 0, "entries": 0, "bytes": 0 }
        })));
    };
    let mem = state.memtable.stats(&tenant, guard.window);
    guard.check_deadline()?;
    let disk = state.parts.stats(&tenant, guard.window);
    guard.check_deadline()?;
    let stream_count = distinct_stream_count(&state, &tenant, guard.window);
    Ok(Json(serde_json::json!({
        "status": "success",
        "data": {
            "streams": stream_count,
            "entries": mem.entries + disk.entries,
            "bytes": mem.bytes + disk.bytes
        }
    })))
}

/// Operator-facing Prometheus scrape. These gauges are process-wide by
/// design, so they use the global accessors rather than a tenant scope.
pub async fn metrics(State(state): State<Arc<AppState>>) -> String {
    let mem = state.memtable.global_stats();
    let disk = state.parts.global_stats();
    let remote_healthy = state
        .remote_cache
        .as_ref()
        .is_none_or(|cache| cache.is_remote_healthy());
    let cache_healthy = state
        .remote_cache
        .as_ref()
        .is_none_or(|cache| cache.is_cache_healthy());
    let wal_backlog_bytes = state.journal.wal_backlog_bytes();
    // Both of these are published by the workers that already hold the
    // snapshots they describe. A scrape must not be able to ask for a walk of
    // every part's tenant index.
    let merge_debt_parts = m_merge_debt(&state);
    // Read rather than computed: the registry maintains these as its set
    // changes, so they are current whether or not any worker has ticked.
    let layout = state.parts.layout_totals();
    let policy = tenant_policy_gauges(&state);
    let m = &state.metrics;
    let mut body = format!(
        "# TYPE loggytracy_memtable_entries gauge\n\
loggytracy_memtable_entries {}\n\
# TYPE loggytracy_memtable_bytes gauge\n\
loggytracy_memtable_bytes {}\n\
# TYPE loggytracy_part_entries gauge\n\
loggytracy_part_entries {}\n\
# TYPE loggytracy_part_bytes gauge\n\
loggytracy_part_bytes {}\n\
# TYPE loggytracy_part_count gauge\n\
loggytracy_part_count {}\n\
# TYPE loggytracy_trace_part_count gauge\n\
loggytracy_trace_part_count {}\n\
# TYPE loggytracy_remote_healthy gauge\n\
loggytracy_remote_healthy {}\n\
# HELP loggytracy_remote_consecutive_failures Object-store failures since the last success. The health flag hides these below its threshold, so this is where a degrading store shows before it is declared down.\n\
# TYPE loggytracy_remote_consecutive_failures gauge\n\
loggytracy_remote_consecutive_failures {}\n\
# TYPE loggytracy_cache_healthy gauge\n\
loggytracy_cache_healthy {}\n\
# TYPE loggytracy_wal_backlog_bytes gauge\n\
loggytracy_wal_backlog_bytes {}\n\
# HELP loggytracy_inflight_push_bytes Request bodies admitted and not yet answered, bounded by max_inflight_push_bytes. Counted at admission because a body is already resident by the time a handler sees it.\n\
# TYPE loggytracy_inflight_push_bytes gauge\n\
loggytracy_inflight_push_bytes {}\n\
# TYPE loggytracy_merge_debt_parts gauge\n\
loggytracy_merge_debt_parts {}\n\
# HELP loggytracy_part_tenant_segments (tenant, part) pairs. The shared-part layout spends a row group, two blooms and a metadata segment per pair.\n\
# TYPE loggytracy_part_tenant_segments gauge\n\
loggytracy_part_tenant_segments {}\n\
# HELP loggytracy_part_sidecar_resident_bytes Bloom and stream-index bytes resident for open parts. The bloom half is bounded by sidecar_cache_max_bytes and evicted LRU; the stream-index half stays resident per part.\n\
# TYPE loggytracy_part_sidecar_resident_bytes gauge\n\
loggytracy_part_sidecar_resident_bytes {}\n\
# HELP loggytracy_row_group_cache_bytes Decoded row groups held for reuse across scans, bounded by row_group_cache_max_bytes.\n\
# TYPE loggytracy_row_group_cache_bytes gauge\n\
loggytracy_row_group_cache_bytes {}\n\
# HELP loggytracy_part_meta_bytes Total meta.json across parts, which startup parses before serving.\n\
# TYPE loggytracy_part_meta_bytes gauge\n\
loggytracy_part_meta_bytes {}\n\
# TYPE loggytracy_ingest_requests_total counter\n\
loggytracy_ingest_requests_total {}\n\
# TYPE loggytracy_ingest_errors_total counter\n\
loggytracy_ingest_errors_total {}\n\
# TYPE loggytracy_ingest_throttled_total counter\n\
loggytracy_ingest_throttled_total {}\n\
# HELP loggytracy_ingest_quota_rejected_total Requests refused for exceeding the tenant's own ingest rate, as opposed to this instance being behind.\n\
# TYPE loggytracy_ingest_quota_rejected_total counter\n\
loggytracy_ingest_quota_rejected_total {}\n\
# HELP loggytracy_query_quota_rejected_total Queries refused by the tenant's own read quota, as opposed to queries this instance failed to answer.\n\
# TYPE loggytracy_query_quota_rejected_total counter\n\
loggytracy_query_quota_rejected_total {}\n\
# HELP loggytracy_stream_limit_rejected_total Writes refused for creating a stream past the tenant's limit. Far more often a client minting label values than a plan being outgrown.\n\
# TYPE loggytracy_stream_limit_rejected_total counter\n\
loggytracy_stream_limit_rejected_total {}\n\
# HELP loggytracy_wal_replayed_records Records this process replayed from the WAL at startup. Non-zero means the previous run did not shut down cleanly.\n\
# TYPE loggytracy_wal_replayed_records gauge\n\
loggytracy_wal_replayed_records {}\n\
# HELP loggytracy_wal_replayed_entries Log entries in those records — the upper bound on how many lines this restart may have duplicated.\n\
# TYPE loggytracy_wal_replayed_entries gauge\n\
loggytracy_wal_replayed_entries {}\n\
# TYPE loggytracy_memtable_buffered_bytes gauge\n\
loggytracy_memtable_buffered_bytes {}\n\
# TYPE loggytracy_flush_success_total counter\n\
loggytracy_flush_success_total {}\n\
# TYPE loggytracy_flush_errors_total counter\n\
loggytracy_flush_errors_total {}\n\
# TYPE loggytracy_merge_success_total counter\n\
loggytracy_merge_success_total {}\n\
# TYPE loggytracy_merge_errors_total counter\n\
loggytracy_merge_errors_total {}\n\
# TYPE loggytracy_merge_inputs_changed_total counter\n\
loggytracy_merge_inputs_changed_total {}\n\
# TYPE loggytracy_retention_success_total counter\n\
loggytracy_retention_success_total {}\n\
# TYPE loggytracy_retention_errors_total counter\n\
loggytracy_retention_errors_total {}\n\
# TYPE loggytracy_retention_expired_rows_dropped_total counter\n\
loggytracy_retention_expired_rows_dropped_total {}\n\
# TYPE loggytracy_retention_parts_rewritten_total counter\n\
loggytracy_retention_parts_rewritten_total {}\n\
# TYPE loggytracy_retention_rewrite_skipped_total counter\n\
loggytracy_retention_rewrite_skipped_total {}\n\
# TYPE loggytracy_tenant_policy_push_accepted_total counter\n\
loggytracy_tenant_policy_push_accepted_total {}\n\
# TYPE loggytracy_tenant_policy_push_rejected_total counter\n\
loggytracy_tenant_policy_push_rejected_total {}\n\
# TYPE loggytracy_tenant_policy_push_persist_errors_total counter\n\
loggytracy_tenant_policy_push_persist_errors_total {}\n\
# TYPE loggytracy_tenant_policy_admin_unauthorized_total counter\n\
loggytracy_tenant_policy_admin_unauthorized_total {}\n\
# TYPE loggytracy_tenant_policy_known_tenants gauge\n\
loggytracy_tenant_policy_known_tenants {}\n\
# TYPE loggytracy_tenant_policy_infinite_tenants gauge\n\
loggytracy_tenant_policy_infinite_tenants {}\n\
# TYPE loggytracy_tenant_policy_unknown_tenants gauge\n\
loggytracy_tenant_policy_unknown_tenants {}\n\
# TYPE loggytracy_tenant_policy_last_push_age_seconds gauge\n\
loggytracy_tenant_policy_last_push_age_seconds {}\n\
# TYPE loggytracy_query_success_total counter\n\
loggytracy_query_success_total {}\n\
# TYPE loggytracy_query_errors_total counter\n\
loggytracy_query_errors_total {}\n\
# TYPE loggytracy_query_scanned_rows_total counter\n\
loggytracy_query_scanned_rows_total {}\n\
# TYPE loggytracy_query_scanned_bytes_total counter\n\
loggytracy_query_scanned_bytes_total {}\n\
# TYPE loggytracy_query_latency_ns_total counter\n\
loggytracy_query_latency_ns_total {}\n\
# HELP loggytracy_query_scans_in_flight Scans holding a scheduler permit right now, out of max_concurrent_query_scans.\n\
# TYPE loggytracy_query_scans_in_flight gauge\n\
loggytracy_query_scans_in_flight {}\n\
# HELP loggytracy_query_scans_in_flight_peak High-water mark of that since start. The memory budget's largest term is max_concurrent_query_scans x max_query_memory_bytes, and this is how far into it a run actually reached — a sampled gauge cannot see a burst that fills the scheduler and drains between two scrapes.\n\
# TYPE loggytracy_query_scans_in_flight_peak gauge\n\
loggytracy_query_scans_in_flight_peak {}\n\
# HELP loggytracy_query_scans_queued_total Scans that found every slot taken and waited. Nonzero is proof the concurrency limit bound, which the peak alone only suggests.\n\
# TYPE loggytracy_query_scans_queued_total counter\n\
loggytracy_query_scans_queued_total {}\n\
# TYPE loggytracy_query_scan_queue_wait_ns_total counter\n\
loggytracy_query_scan_queue_wait_ns_total {}\n\
# TYPE loggytracy_remote_restore_success_total counter\n\
loggytracy_remote_restore_success_total {}\n\
# TYPE loggytracy_remote_restore_errors_total counter\n\
loggytracy_remote_restore_errors_total {}\n\
# TYPE loggytracy_remote_restore_latency_ns_total counter\n\
loggytracy_remote_restore_latency_ns_total {}\n\
# TYPE loggytracy_cache_evictions_total counter\n\
loggytracy_cache_evictions_total {}\n\
# TYPE loggytracy_drain_in_progress gauge\n\
loggytracy_drain_in_progress {}\n\
# TYPE loggytracy_pending_flush_bytes gauge\n\
loggytracy_pending_flush_bytes {}\n\
# TYPE loggytracy_force_flush_complete gauge\n\
loggytracy_force_flush_complete {}\n\
# HELP loggytracy_build_info Build identity, always 1. Join on it to attribute a series to a revision.\n\
# TYPE loggytracy_build_info gauge\n\
loggytracy_build_info{{version=\"{}\",revision=\"{}\"}} 1\n\
# HELP loggytracy_query_latency_ms Query latency by the endpoint the query arrived at. The cumulative _ns_total counters only ever yielded a mean; every target is written as p95/p99, so use histogram_quantile on this, and sum by (le) across endpoints for the whole read path.\n\
# TYPE loggytracy_query_latency_ms histogram\n\
{}\
# HELP loggytracy_remote_restore_latency_ms Object-store restore latency, the cost of a cache miss.\n\
# TYPE loggytracy_remote_restore_latency_ms histogram\n\
{}",
        mem.entries,
        mem.bytes,
        disk.entries,
        disk.bytes,
        state.parts.part_count(),
        state.trace_parts.part_count(),
        remote_healthy as u8,
        state
            .remote_cache
            .as_ref()
            .map(|cache| cache.consecutive_remote_failures())
            .unwrap_or(0),
        cache_healthy as u8,
        wal_backlog_bytes,
        state.ingest_gate.inflight_body_bytes(),
        merge_debt_parts,
        layout.tenant_segments,
        layout
            .sidecar_resident_bytes
            .saturating_add(crate::part::bloom_cache_bytes()),
        crate::part::row_group_cache_bytes(),
        layout.meta_bytes,
        m.ingest_requests.load(Ordering::Relaxed),
        m.ingest_errors.load(Ordering::Relaxed),
        m.ingest_throttled.load(Ordering::Relaxed),
        m.ingest_quota_rejected.load(Ordering::Relaxed),
        m.query_quota_rejected.load(Ordering::Relaxed),
        m.stream_limit_rejected.load(Ordering::Relaxed),
        m.wal_replayed_records.load(Ordering::Relaxed),
        m.wal_replayed_entries.load(Ordering::Relaxed),
        state.ingest_gate.buffered_bytes(),
        m.flush_success.load(Ordering::Relaxed),
        m.flush_errors.load(Ordering::Relaxed),
        m.merge_success.load(Ordering::Relaxed),
        m.merge_errors.load(Ordering::Relaxed),
        m.merge_inputs_changed.load(Ordering::Relaxed),
        m.retention_success.load(Ordering::Relaxed),
        m.retention_errors.load(Ordering::Relaxed),
        m.retention_expired_rows_dropped.load(Ordering::Relaxed),
        m.retention_parts_rewritten.load(Ordering::Relaxed),
        m.retention_rewrite_skipped.load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_accepted
            .load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_rejected
            .load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .push_persist_errors
            .load(Ordering::Relaxed),
        state
            .tenant_policy
            .metrics
            .admin_unauthorized
            .load(Ordering::Relaxed),
        policy.known_tenants,
        policy.infinite_tenants,
        policy.unknown_tenants,
        policy.last_push_age_seconds,
        m.query_success.load(Ordering::Relaxed),
        m.query_errors.load(Ordering::Relaxed),
        m.query_scanned_rows.load(Ordering::Relaxed),
        m.query_scanned_bytes.load(Ordering::Relaxed),
        m.query_latency_ns.load(Ordering::Relaxed),
        m.query_scans_in_flight.load(Ordering::Relaxed),
        m.query_scans_in_flight_peak.load(Ordering::Relaxed),
        m.query_scans_queued.load(Ordering::Relaxed),
        m.query_scan_queue_wait_ns.load(Ordering::Relaxed),
        m.remote_restore_success.load(Ordering::Relaxed),
        m.remote_restore_errors.load(Ordering::Relaxed),
        m.remote_restore_latency_ns.load(Ordering::Relaxed),
        m.cache_evictions.load(Ordering::Relaxed),
        state.shutdown.is_draining() as u8,
        state.shutdown.pending_flush_bytes(),
        state.shutdown.is_flush_complete() as u8,
        env!("CARGO_PKG_VERSION"),
        build_revision(),
        crate::metrics::QueryEndpoint::ALL
            .iter()
            .map(|endpoint| {
                m.query_latency[*endpoint as usize].render_labeled(
                    "loggytracy_query_latency_ms",
                    &format!("endpoint=\"{}\"", endpoint.label()),
                )
            })
            .collect::<String>(),
        m.remote_restore_latency
            .render("loggytracy_remote_restore_latency_ms"),
    );
    body.push_str(&object_store_operation_metrics(&state));
    body.push_str(&restore_economics_metrics());
    body.push_str(&delete_request_metrics(&state));
    body.push_str(&journal_writer_metrics(&state));
    body.push_str(&crate::memprof::render());
    body
}

/// Where an accepted push's server-side time went.
///
/// Every push in the process is written by one task, so these four phases are
/// the whole of it and they are additive: queue, write, fsync, insert. The
/// question they exist to answer is which of them the push tail is made of —
/// a p50 of 12 ms beside a p95 that moves between 40 and 106 ms with nothing
/// but the client's connection count (`todo.md`, 2026-08-12) is a queue, and
/// until these there was no number in the process that could say so.
fn journal_writer_metrics(state: &AppState) -> String {
    let metrics = state.journal.metrics();
    let mut out = String::new();
    out.push_str(
        "# HELP loggytracy_journal_batches_total Batches the writer task wrote, one fsync each.\n\
# TYPE loggytracy_journal_batches_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_journal_batches_total {}\n",
        metrics.batches.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP loggytracy_journal_batched_records_total Appends carried by those batches. Divided by the batches, the number of pushes sharing each fsync.\n\
# TYPE loggytracy_journal_batched_records_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_journal_batched_records_total {}\n",
        metrics.batched_records.load(Ordering::Relaxed)
    ));
    out.push_str(
        &metrics
            .append_queue_wait
            .render("loggytracy_journal_append_queue_wait_ms"),
    );
    out.push_str(&metrics.batch_write.render("loggytracy_journal_write_ms"));
    out.push_str(&metrics.batch_fsync.render("loggytracy_journal_fsync_ms"));
    out.push_str(&metrics.batch_insert.render("loggytracy_journal_insert_ms"));
    out.push_str(
        &metrics
            .checkpoint
            .render("loggytracy_journal_checkpoint_ms"),
    );
    out.push_str(&flush_phase_metrics(state));
    out.push_str(
        "# HELP loggytracy_query_memory_exhausted_total Queries refused because this instance's query memory pool had no room. Distinct from the tenant read quota, which says a tenant asked for more than it was sold, and from a scan-limit refusal, which says the query was too broad: this one says the instance ran out of room for work it was willing to do, and is the read side's counterpart to ingest_throttled.\n\
# TYPE loggytracy_query_memory_exhausted_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_query_memory_exhausted_total {}\n",
        state.query_memory_pool.exhausted()
    ));
    out
}

/// Where a flush pass's time goes.
///
/// The companion to the journal writer's phases, and the more consequential of
/// the two: the rate ladder of 2026-08-13 put this engine's capacity ceiling
/// here rather than in the WAL. A pass that takes longer than the memtable
/// takes to refill *is* the ceiling, and before these the only evidence of it
/// reaching one was a `429` arriving at a client.
fn flush_phase_metrics(state: &AppState) -> String {
    let flush = &state.metrics.flush;
    let mut out = String::new();
    out.push_str(
        "# HELP loggytracy_flush_rows_total Rows written into parts by the flush loop.\n\
# TYPE loggytracy_flush_rows_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_flush_rows_total {}\n",
        flush.rows.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP loggytracy_flush_parts_total Parts those passes produced. Divided by the rows, the part size the chunker is choosing.\n\
# TYPE loggytracy_flush_parts_total counter\n",
    );
    out.push_str(&format!(
        "loggytracy_flush_parts_total {}\n",
        flush.parts.load(Ordering::Relaxed)
    ));
    out.push_str(
        &flush
            .checkpoint_wait
            .render("loggytracy_flush_checkpoint_wait_ms"),
    );
    out.push_str(&flush.build.render("loggytracy_flush_build_ms"));
    out.push_str(&flush.open.render("loggytracy_flush_open_ms"));
    out.push_str(&flush.visibility.render("loggytracy_flush_visibility_ms"));
    out.push_str(&flush.advance_checkpoint.render("loggytracy_flush_advance_ms"));
    // The build phase's own four, counted per part rather than per pass because
    // a flush cuts its snapshot into chunks. Only the flush path observes them;
    // a merge rewrite runs the same code and is deliberately excluded.
    let build = &crate::part::FLUSH_BUILD;
    out.push_str(&build.sort.render("loggytracy_flush_build_sort_ms"));
    out.push_str(&build.parse.render("loggytracy_flush_build_parse_ms"));
    out.push_str(&build.write.render("loggytracy_flush_build_write_ms"));
    out.push_str(&build.parquet.render("loggytracy_flush_build_parquet_ms"));
    out.push_str(&build.index.render("loggytracy_flush_build_index_ms"));
    out.push_str(&build.meta.render("loggytracy_flush_build_meta_ms"));
    out.push_str(&build.commit.render("loggytracy_flush_build_commit_ms"));
    out
}

/// Deletion is the one operation here that destroys data on request, so how
/// many were accepted, how many were refused, and how many rows are being
/// hidden are all things an operator has to be able to see without asking a
/// tenant.
fn delete_request_metrics(state: &AppState) -> String {
    let metrics = &state.delete_requests.metrics;
    format!(
        "# TYPE loggytracy_delete_requests_accepted_total counter\n\
loggytracy_delete_requests_accepted_total {}\n\
# HELP loggytracy_delete_requests_rejected_total Submissions refused for exceeding the per-tenant limit. Each outstanding request is a predicate every scan for that tenant evaluates per row.\n\
# TYPE loggytracy_delete_requests_rejected_total counter\n\
loggytracy_delete_requests_rejected_total {}\n\
# TYPE loggytracy_delete_requests_cancelled_total counter\n\
loggytracy_delete_requests_cancelled_total {}\n\
# HELP loggytracy_delete_hidden_rows_total Rows a scan dropped because a deletion request covered them. Stops growing for a request once a rewrite has removed its bytes.\n\
# TYPE loggytracy_delete_hidden_rows_total counter\n\
loggytracy_delete_hidden_rows_total {}\n",
        metrics.accepted.load(Ordering::Relaxed),
        metrics.rejected.load(Ordering::Relaxed),
        metrics.cancelled.load(Ordering::Relaxed),
        metrics.hidden_rows.load(Ordering::Relaxed),
    )
}

/// The two numbers that decide the sign of "add Parquet range reads": what a
/// selective download would cost in requests, and what the whole-object
/// download earns by leaving a reusable copy behind. See
/// [`crate::restore_meter`] for why those two and not the byte total.
fn restore_economics_metrics() -> String {
    let meter = crate::restore_meter::global().snapshot();
    format!(
        "# HELP loggytracy_query_part_scans_total Query scans that read a part body. A rewrite is excluded: it reads what it was told to.\n\
# TYPE loggytracy_query_part_scans_total counter\n\
loggytracy_query_part_scans_total {}\n\
# HELP loggytracy_query_row_groups_total Row groups in the parts those scans read, by how far selection narrowed them. `present` is the whole part a restore downloads, `tenant` is the querying tenant's segment, `selected` is what the scan read.\n\
# TYPE loggytracy_query_row_groups_total counter\n\
loggytracy_query_row_groups_total{{stage=\"present\"}} {}\n\
loggytracy_query_row_groups_total{{stage=\"tenant\"}} {}\n\
loggytracy_query_row_groups_total{{stage=\"selected\"}} {}\n\
# HELP loggytracy_query_selected_runs_total Contiguous runs among the selected row groups. Column chunks of a row group are contiguous and the log path projects every column, so a run is one byte range: this plus one footer read is what a selective download would issue where a whole restore issues one GET.\n\
# TYPE loggytracy_query_selected_runs_total counter\n\
loggytracy_query_selected_runs_total {}\n\
# HELP loggytracy_restore_first_scan_total The same three numbers over the first scan of each restored body alone. That scan is the query the download was issued for, so its selection is the one a selective download would have applied; the aggregates above mix it with scans of bodies that were never downloaded.\n\
# TYPE loggytracy_restore_first_scan_total counter\n\
loggytracy_restore_first_scan_total{{stage=\"parts\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"present\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"selected\"}} {}\n\
loggytracy_restore_first_scan_total{{stage=\"runs\"}} {}\n\
# HELP loggytracy_restored_body_scans_total Query scans served by a body that was downloaded whole after eviction and is still on disk. Divided by the restore count, this is how much later work one over-fetch prepaid.\n\
# TYPE loggytracy_restored_body_scans_total counter\n\
loggytracy_restored_body_scans_total {}\n\
# HELP loggytracy_restored_bodies_total Bodies restored, and how many of them eviction has since taken. A restore still resident has not finished earning.\n\
# TYPE loggytracy_restored_bodies_total counter\n\
loggytracy_restored_bodies_total{{state=\"restored\"}} {}\n\
loggytracy_restored_bodies_total{{state=\"retired\"}} {}\n\
# HELP loggytracy_restored_tenant_slices_total Distinct (restored body, querying tenant) pairs. A whole restore costs one GET however many tenants read it; a selective download serves one slice, so this is how many it would have taken.\n\
# TYPE loggytracy_restored_tenant_slices_total counter\n\
loggytracy_restored_tenant_slices_total {}\n",
        meter.part_scans,
        meter.row_groups_present,
        meter.row_groups_tenant,
        meter.row_groups_selected,
        meter.selected_runs,
        meter.first_scan_parts,
        meter.first_scan_row_groups_present,
        meter.first_scan_row_groups_selected,
        meter.first_scan_runs,
        meter.restored_scans,
        meter.restores,
        meter.restored_retired,
        meter.restored_tenant_slices,
    )
}

/// The cost model of this design is operation counts, not bytes. R2 bills per
/// request, and the whole shared-part layout exists because per-tenant objects
/// multiplied that count. These are the numbers to divide by flush, merge and
/// retention cycles to get the per-cycle cost, and they measure the same
/// locally as they do against a paid backend.
fn object_store_operation_metrics(state: &AppState) -> String {
    let Some(counts) = state
        .remote_cache
        .as_ref()
        .map(|cache| cache.storage.operation_counts())
    else {
        return String::new();
    };
    format!(
        "# HELP loggytracy_object_store_operations_total Object-store requests issued, by kind. Which kinds are billed how is the backend's policy; how many of each this engine issues is not.\n\
# TYPE loggytracy_object_store_operations_total counter\n\
loggytracy_object_store_operations_total{{kind=\"put\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"put_multipart\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"get\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"delete\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"list\"}} {}\n\
loggytracy_object_store_operations_total{{kind=\"copy\"}} {}\n\
# HELP loggytracy_object_store_listed_objects_total Objects the listings returned. A backend pages a listing, so its request count follows from this and the page size rather than from the list count.\n\
# TYPE loggytracy_object_store_listed_objects_total counter\n\
loggytracy_object_store_listed_objects_total {}\n\
# HELP loggytracy_object_store_ranged_gets_total GETs that asked for a byte range rather than a whole object. Zero means every restore moves the whole part, including the rows belonging to other tenants of a shared part.\n\
# TYPE loggytracy_object_store_ranged_gets_total counter\n\
loggytracy_object_store_ranged_gets_total {}\n\
# HELP loggytracy_object_store_bytes_total Bytes moved to and from the object store. Read bytes are what the responses agreed to return, not what a caller consumed.\n\
# TYPE loggytracy_object_store_bytes_total counter\n\
loggytracy_object_store_bytes_total{{direction=\"get\"}} {}\n\
loggytracy_object_store_bytes_total{{direction=\"put\"}} {}\n\
# HELP loggytracy_object_store_bytes_by_kind_total The same bytes split by what was read or written. A part restore and a manifest rewrite are both bytes and only one of them is a part; the totals above cannot tell them apart.\n\
# TYPE loggytracy_object_store_bytes_by_kind_total counter\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"manifest\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"trace_part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"get\",kind=\"other\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"manifest\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"trace_part\"}} {}\n\
loggytracy_object_store_bytes_by_kind_total{{direction=\"put\",kind=\"other\"}} {}\n",
        counts.puts,
        counts.multipart_puts,
        counts.gets,
        counts.deletes,
        counts.lists,
        counts.copies,
        counts.listed_objects,
        counts.ranged_gets,
        counts.get_bytes,
        counts.put_bytes,
        counts.get_bytes_by_kind.manifest,
        counts.get_bytes_by_kind.part,
        counts.get_bytes_by_kind.trace_part,
        counts.get_bytes_by_kind.other,
        counts.put_bytes_by_kind.manifest,
        counts.put_bytes_by_kind.part,
        counts.put_bytes_by_kind.trace_part,
        counts.put_bytes_by_kind.other,
    )
}

/// The revision this binary was built from, or `unknown` when the build did
/// not supply one. Without it a scraped series cannot be attributed to code,
/// which is the first question asked when two deployments behave differently.
pub fn build_revision() -> &'static str {
    option_env!("LOGGYTRACY_BUILD_REVISION").unwrap_or("unknown")
}

fn m_merge_debt(state: &AppState) -> u64 {
    state
        .metrics
        .merge_debt_parts
        .load(std::sync::atomic::Ordering::Relaxed)
}

struct TenantPolicyGauges {
    known_tenants: usize,
    infinite_tenants: usize,
    unknown_tenants: u64,
    last_push_age_seconds: u64,
}

/// The policy map is small and in memory, so its two counts are computed here.
/// The unknown-tenant count is not: it walks every part's tenant index, so the
/// retention worker publishes it and this reads what that worker last saw.
fn tenant_policy_gauges(state: &AppState) -> TenantPolicyGauges {
    let Some(snapshot) = state.tenant_policy.snapshot() else {
        return TenantPolicyGauges {
            known_tenants: 0,
            infinite_tenants: 0,
            unknown_tenants: 0,
            last_push_age_seconds: 0,
        };
    };
    TenantPolicyGauges {
        known_tenants: snapshot.tenant_count(),
        infinite_tenants: snapshot.infinite_tenant_count(),
        unknown_tenants: state
            .metrics
            .unknown_tenants
            .load(std::sync::atomic::Ordering::Relaxed),
        last_push_age_seconds: snapshot
            .newest_push_age(state.clock.now())
            .as_secs(),
    }
}

fn extract_match_params(raw: &Option<String>) -> Vec<String> {
    let Some(q) = raw else {
        return Vec::new();
    };
    url::form_urlencoded::parse(q.as_bytes())
        .filter(|(key, _)| key == "match[]" || key == "match")
        .map(|(_, value)| value.into_owned())
        .collect()
}

pub async fn series(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
) -> Result<Json<LokiResponse<Vec<HashMap<String, String>>>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(crate::tenant::TenantError::into_http)?;
    // `series` is the one metadata endpoint whose cost the client chooses:
    // every `match[]` is another full pass. The cap bounds that multiplier,
    // and the guard bounds each pass.
    let matchers = extract_match_params(&raw);
    if matchers.len() > state.config.max_series_matchers {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "series request has {} match[] selectors, exceeding the maximum of {}",
                matchers.len(),
                state.config.max_series_matchers
            ),
        ));
    }
    let params = metadata_params_from_raw(&raw)?;
    let Some(guard) = MetadataGuard::acquire(&state, &tenant, &params).await? else {
        return Ok(Json(LokiResponse {
            status: "success",
            data: Vec::new(),
        }));
    };
    let mut all_series: Vec<Labels> = Vec::new();
    for matcher_str in &matchers {
        guard.check_deadline()?;
        let parsed = logql::parse(matcher_str)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;
        all_series.extend(state.memtable.series(&tenant, &parsed.matchers, guard.window));
        all_series.extend(state.parts.series(&tenant, &parsed.matchers, guard.window));
    }
    all_series.sort();
    all_series.dedup();

    let data = all_series
        .into_iter()
        .map(|labels| labels.into_iter().collect())
        .collect();

    Ok(Json(LokiResponse {
        status: "success",
        data,
    }))
}
