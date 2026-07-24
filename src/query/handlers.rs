pub async fn query_range(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
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
            let execution = run_metric_query_with_stats(state, expr, times, None)
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
    Query(params): Query<QueryParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
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

    let forward = parse_direction(&params.direction).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let max_log_limit = state.config.max_log_limit.min(MAX_LOG_LIMIT);
    let max_scan_rows = state.config.max_query_scan_rows.min(MAX_LOG_SCAN_ROWS);

    let (result_type, result, total_lines_processed) = match parsed {
        logql::QueryExpr::Logs(parsed) => {
            let limit = parse_limit(params.limit, max_log_limit)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let execution = run_unified_query_with_stats(
                state,
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
            let execution = run_metric_query_with_stats(state, expr, vec![end_ns], None)
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

fn duration_to_i64_ns(duration: std::time::Duration) -> i64 {
    duration
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

pub async fn labels(State(state): State<Arc<AppState>>) -> Json<LokiResponse<Vec<String>>> {
    let mut names = std::collections::BTreeSet::new();
    for n in state.memtable.label_names() {
        names.insert(n);
    }
    for n in state.parts.label_names() {
        names.insert(n);
    }
    Json(LokiResponse {
        status: "success",
        data: names.into_iter().collect(),
    })
}

pub async fn label_values(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<LokiResponse<Vec<String>>> {
    let mut values = std::collections::BTreeSet::new();
    for v in state.memtable.label_values(&name) {
        values.insert(v);
    }
    for v in state.parts.label_values(&name) {
        values.insert(v);
    }
    Json(LokiResponse {
        status: "success",
        data: values.into_iter().collect(),
    })
}

pub async fn ready(
    State(state): State<Arc<AppState>>,
) -> Result<&'static str, (StatusCode, String)> {
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

pub async fn index_stats(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mem = state.memtable.stats();
    let disk = state.parts.stats();
    let stream_count = distinct_stream_count(&state);
    Json(serde_json::json!({
        "status": "success",
        "data": {
            "streams": stream_count,
            "entries": mem.entries + disk.entries,
            "bytes": mem.bytes + disk.bytes
        }
    }))
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> String {
    let mem = state.memtable.stats();
    let disk = state.parts.stats();
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
loggytracy_cache_evictions_total {}\n",
        mem.entries,
        mem.bytes,
        disk.entries,
        disk.bytes,
        state.parts.part_count(),
        state.trace_parts.part_count(),
        remote_healthy as u8,
        cache_healthy as u8,
        wal_backlog_bytes,
        m.ingest_requests.load(Ordering::Relaxed),
        m.ingest_errors.load(Ordering::Relaxed),
        m.flush_success.load(Ordering::Relaxed),
        m.flush_errors.load(Ordering::Relaxed),
        m.merge_success.load(Ordering::Relaxed),
        m.merge_errors.load(Ordering::Relaxed),
        m.retention_success.load(Ordering::Relaxed),
        m.retention_errors.load(Ordering::Relaxed),
        m.query_success.load(Ordering::Relaxed),
        m.query_errors.load(Ordering::Relaxed),
        m.query_scanned_rows.load(Ordering::Relaxed),
        m.query_scanned_bytes.load(Ordering::Relaxed),
        m.query_latency_ns.load(Ordering::Relaxed),
        m.remote_restore_success.load(Ordering::Relaxed),
        m.remote_restore_errors.load(Ordering::Relaxed),
        m.remote_restore_latency_ns.load(Ordering::Relaxed),
        m.cache_evictions.load(Ordering::Relaxed),
    )
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
    RawQuery(raw): RawQuery,
) -> Result<Json<LokiResponse<Vec<HashMap<String, String>>>>, (StatusCode, String)> {
    let matchers = extract_match_params(&raw);
    let mut all_series: Vec<Labels> = Vec::new();
    for matcher_str in &matchers {
        let parsed = logql::parse(matcher_str)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;
        all_series.extend(state.memtable.series(&parsed.matchers));
        all_series.extend(state.parts.series(&parsed.matchers));
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
