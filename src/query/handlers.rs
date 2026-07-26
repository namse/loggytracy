pub async fn query_range(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let parsed = logql::parse_expr(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

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
                start_ns,
                end_ns,
                limit,
                forward,
                Some(max_scan_rows),
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "streams",
                serde_json::to_value(build_stream_data(execution.results)).unwrap(),
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
                run_metric_query_with_stats(state, tenant, expr, times, scan_start_override)
                    .await
                    .map_err(|e| (metric_error_status(&e), e))?;
            (
                "matrix",
                metric_series_json(execution.series, false),
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
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let parsed = logql::parse_expr(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

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
                query_start_ns,
                end_ns,
                limit,
                forward,
                Some(max_scan_rows),
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "streams",
                serde_json::to_value(build_stream_data(execution.results)).unwrap(),
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
            )
            .await
            .map_err(|e| (metric_error_status(&e), e))?;
            (
                "vector",
                metric_series_json(execution.series, true),
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
            result: serde_json::Value::Array(Vec::new()),
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

pub async fn labels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<LokiResponse<Vec<String>>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let mut names = std::collections::BTreeSet::new();
    for n in state.memtable.label_names(&tenant, retention_floor_ns) {
        names.insert(n);
    }
    for n in state.parts.label_names(&tenant, retention_floor_ns) {
        names.insert(n);
    }
    Ok(Json(LokiResponse {
        status: "success",
        data: names.into_iter().collect(),
    }))
}

pub async fn label_values(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<LokiResponse<Vec<String>>>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let mut values = std::collections::BTreeSet::new();
    for v in state.memtable.label_values(&tenant, &name, retention_floor_ns) {
        values.insert(v);
    }
    for v in state.parts.label_values(&tenant, &name, retention_floor_ns) {
        values.insert(v);
    }
    Ok(Json(LokiResponse {
        status: "success",
        data: values.into_iter().collect(),
    }))
}

pub async fn ready(
    State(state): State<Arc<AppState>>,
) -> Result<&'static str, (StatusCode, String)> {
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
            "revision": "unknown",
            "branch": "main",
            "goVersion": "n/a"
        }
    }))
}

pub async fn index_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let tenant = crate::tenant::from_headers(&headers, &state.config)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let mem = state.memtable.stats(&tenant, retention_floor_ns);
    let disk = state.parts.stats(&tenant, retention_floor_ns);
    let stream_count = distinct_stream_count(&state, &tenant, retention_floor_ns);
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
    let wal_bytes = std::fs::metadata(state.journal.wal_path())
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let checkpoint = crate::journal::read_checkpoint(state.journal.ckpt_path()).unwrap_or(0);
    let wal_backlog_bytes = wal_bytes.saturating_sub(checkpoint);
    let merge_debt_parts = crate::merge::merge_debt_part_count(&state.parts, &state.config);
    let policy = tenant_policy_gauges(&state);
    let m = &state.metrics;
    format!(
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
# TYPE loggytracy_cache_healthy gauge\n\
loggytracy_cache_healthy {}\n\
# TYPE loggytracy_wal_backlog_bytes gauge\n\
loggytracy_wal_backlog_bytes {}\n\
# TYPE loggytracy_merge_debt_parts gauge\n\
loggytracy_merge_debt_parts {}\n\
# TYPE loggytracy_ingest_requests_total counter\n\
loggytracy_ingest_requests_total {}\n\
# TYPE loggytracy_ingest_errors_total counter\n\
loggytracy_ingest_errors_total {}\n\
# TYPE loggytracy_flush_success_total counter\n\
loggytracy_flush_success_total {}\n\
# TYPE loggytracy_flush_errors_total counter\n\
loggytracy_flush_errors_total {}\n\
# TYPE loggytracy_merge_success_total counter\n\
loggytracy_merge_success_total {}\n\
# TYPE loggytracy_merge_errors_total counter\n\
loggytracy_merge_errors_total {}\n\
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
loggytracy_force_flush_complete {}\n",
        mem.entries,
        mem.bytes,
        disk.entries,
        disk.bytes,
        state.parts.part_count(),
        state.trace_parts.part_count(),
        remote_healthy as u8,
        cache_healthy as u8,
        wal_backlog_bytes,
        merge_debt_parts,
        m.ingest_requests.load(Ordering::Relaxed),
        m.ingest_errors.load(Ordering::Relaxed),
        m.flush_success.load(Ordering::Relaxed),
        m.flush_errors.load(Ordering::Relaxed),
        m.merge_success.load(Ordering::Relaxed),
        m.merge_errors.load(Ordering::Relaxed),
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
        policy.known_tenants,
        policy.infinite_tenants,
        policy.unknown_tenants,
        policy.last_push_age_seconds,
        m.query_success.load(Ordering::Relaxed),
        m.query_errors.load(Ordering::Relaxed),
        m.query_scanned_rows.load(Ordering::Relaxed),
        m.query_scanned_bytes.load(Ordering::Relaxed),
        m.query_latency_ns.load(Ordering::Relaxed),
        m.remote_restore_success.load(Ordering::Relaxed),
        m.remote_restore_errors.load(Ordering::Relaxed),
        m.remote_restore_latency_ns.load(Ordering::Relaxed),
        m.cache_evictions.load(Ordering::Relaxed),
        state.shutdown.is_draining() as u8,
        state.shutdown.pending_flush_bytes(),
        state.shutdown.is_flush_complete() as u8,
    )
}

struct TenantPolicyGauges {
    known_tenants: usize,
    infinite_tenants: usize,
    unknown_tenants: usize,
    last_push_age_seconds: u64,
}

/// `"infinite"` and *absent* keep the same data, so a control plane that
/// silently drops a tenant is only visible as a rising unknown-tenant gauge
/// rather than as invisible unbounded storage.
///
/// Both memtables are counted alongside the parts. A tenant that has just
/// started pushing owns no `meta.json` segment until its first flush, and that
/// is exactly the window in which the control plane is most likely to have
/// missed it — waiting for a flush to make it visible would hide the newest
/// omissions for as long as they are newest.
fn tenant_policy_gauges(state: &AppState) -> TenantPolicyGauges {
    let Some(snapshot) = state.tenant_policy.snapshot() else {
        return TenantPolicyGauges {
            known_tenants: 0,
            infinite_tenants: 0,
            unknown_tenants: 0,
            last_push_age_seconds: 0,
        };
    };
    let mut unknown: std::collections::BTreeSet<crate::tenant::TenantId> =
        std::collections::BTreeSet::new();
    let mut note = |tenant: &crate::tenant::TenantId| {
        if snapshot.retention(tenant).is_none() {
            unknown.insert(tenant.clone());
        }
    };
    for reader in state.parts.snapshot() {
        for segment in &reader.meta().tenants {
            note(&segment.tenant);
        }
    }
    for reader in state.trace_parts.snapshot() {
        for segment in &reader.part().meta.tenants {
            note(&segment.tenant);
        }
    }
    for tenant in state.memtable.tenants() {
        note(&tenant);
    }
    for tenant in state.journal.trace_memtable().tenants() {
        note(&tenant);
    }
    TenantPolicyGauges {
        known_tenants: snapshot.tenant_count(),
        infinite_tenants: snapshot.infinite_tenant_count(),
        unknown_tenants: unknown.len(),
        last_push_age_seconds: snapshot.newest_push_age(std::time::SystemTime::now()).as_secs(),
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
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let retention_floor_ns = state.tenant_policy.query_floor_ns(&tenant);
    let matchers = extract_match_params(&raw);
    let mut all_series: Vec<Labels> = Vec::new();
    for matcher_str in &matchers {
        let parsed = logql::parse(matcher_str)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;
        all_series.extend(
            state
                .memtable
                .series(&tenant, &parsed.matchers, retention_floor_ns),
        );
        all_series.extend(
            state
                .parts
                .series(&tenant, &parsed.matchers, retention_floor_ns),
        );
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
