use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::AppState;
use crate::logql::{self};
use crate::memtable::{Labels, LogEntry, StreamResult};
use crate::part;

const MAX_METRIC_EVALUATION_POINTS: usize = 10_000;
const MAX_METRIC_ROWS: usize = 1_000_000;
const MAX_METRIC_SERIES: usize = 100_000;
const MAX_METRIC_SAMPLES: usize = 5_000_000;
const MAX_LOG_LIMIT: usize = 100_000;
const MAX_LOG_SCAN_ROWS: usize = 5_000_000;
const MAX_CONCURRENT_QUERY_SCANS: usize = 8;
const MAX_CONCURRENT_METRIC_EVALUATIONS: usize = 4;
const MAX_QUERY_RUNTIME: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_RESTORE_RUNTIME: std::time::Duration = std::time::Duration::from_secs(25);

fn query_scan_semaphore() -> Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_QUERY_SCANS)))
        .clone()
}

fn metric_evaluation_semaphore() -> Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_METRIC_EVALUATIONS)))
        .clone()
}

fn metric_error_status(error: &str) -> StatusCode {
    if error.starts_with("metric query exceeds") || error.starts_with("query exceeds") {
        StatusCode::BAD_REQUEST
    } else if error == "query timed out"
        || error == "metric query timed out"
        || error == "object store restore timed out"
    {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

#[derive(Serialize)]
pub struct LokiResponse<T: Serialize> {
    pub status: &'static str,
    pub data: T,
}

#[derive(Serialize)]
pub struct QueryRangeData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: serde_json::Value,
    pub stats: Stats,
}

#[derive(Serialize)]
pub struct StreamData {
    pub stream: HashMap<String, String>,
    pub values: Vec<serde_json::Value>,
}

#[derive(Serialize)]
pub struct Stats {
    pub summary: StatsSummary,
}

#[derive(Serialize)]
pub struct StatsSummary {
    #[serde(rename = "totalLinesProcessed")]
    pub total_lines_processed: u64,
}

#[derive(serde::Deserialize)]
pub struct QueryRangeParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
    step: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct QueryParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

pub(crate) fn parse_time_ns(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty timestamp".to_string());
    }
    let numeric_digits = s.strip_prefix('-').unwrap_or(s);
    if !numeric_digits.is_empty() && numeric_digits.bytes().all(|b| b.is_ascii_digit()) {
        let n: i64 = s
            .parse()
            .map_err(|e| format!("invalid timestamp '{}': {}", s, e))?;
        let multiplier = match numeric_digits.len() {
            1..=10 => 1_000_000_000i64,
            13 => 1_000_000i64,
            16 => 1_000i64,
            19 => 1i64,
            _ => {
                return Err(format!(
                    "unsupported numeric timestamp precision (expected seconds, milliseconds, microseconds, or nanoseconds): '{}'",
                    s
                ));
            }
        };
        n.checked_mul(multiplier)
            .ok_or_else(|| format!("timestamp '{}' is out of range", s))
    } else if s.contains(['.', 'e', 'E'])
        && s.bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        logql::decimal_seconds_to_ns(s).map_err(|_| {
            format!(
                "invalid decimal Unix timestamp '{}': expected finite seconds with nanosecond precision",
                s
            )
        })
    } else {
        let dt = chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| format!("invalid RFC3339 timestamp '{}': {}", s, e))?;
        dt.timestamp_nanos_opt()
            .ok_or_else(|| format!("timestamp '{}' out of range", s))
    }
}

fn build_stream_data(results: Vec<StreamResult>) -> Vec<StreamData> {
    results
        .into_iter()
        .map(|r| StreamData {
            stream: r.labels.into_iter().collect(),
            values: r
                .entries
                .iter()
                .map(|e| {
                    let mut arr = vec![
                        serde_json::Value::String(e.timestamp_ns.to_string()),
                        serde_json::Value::String(e.line.clone()),
                    ];
                    if !e.structured_metadata.is_empty() {
                        let mut map = serde_json::Map::new();
                        for (k, v) in &e.structured_metadata {
                            map.insert(k.clone(), serde_json::Value::String(v.clone()));
                        }
                        arr.push(serde_json::Value::Object(map));
                    }
                    serde_json::Value::Array(arr)
                })
                .collect(),
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct MetricSeries {
    labels: Labels,
    samples: Vec<(i64, f64)>,
}

fn metric_series_json(series: Vec<MetricSeries>, instant: bool) -> serde_json::Value {
    serde_json::Value::Array(
        series
            .into_iter()
            .map(|series| {
                let metric: serde_json::Map<String, serde_json::Value> = series
                    .labels
                    .into_iter()
                    .map(|(name, value)| (name, serde_json::Value::String(value)))
                    .collect();
                let mut object = serde_json::Map::new();
                object.insert("metric".to_string(), serde_json::Value::Object(metric));
                if instant {
                    let (timestamp_ns, value) = series.samples.into_iter().next().unwrap();
                    object.insert("value".to_string(), metric_sample_json(timestamp_ns, value));
                } else {
                    object.insert(
                        "values".to_string(),
                        serde_json::Value::Array(
                            series
                                .samples
                                .into_iter()
                                .map(|(timestamp_ns, value)| {
                                    metric_sample_json(timestamp_ns, value)
                                })
                                .collect(),
                        ),
                    );
                }
                serde_json::Value::Object(object)
            })
            .collect(),
    )
}

fn metric_sample_json(timestamp_ns: i64, value: f64) -> serde_json::Value {
    serde_json::Value::Array(vec![
        timestamp_seconds_json(timestamp_ns),
        serde_json::Value::String(format_metric_value(value)),
    ])
}

fn timestamp_seconds_json(timestamp_ns: i64) -> serde_json::Value {
    let magnitude = i128::from(timestamp_ns).unsigned_abs();
    let seconds = magnitude / 1_000_000_000;
    let nanos = magnitude % 1_000_000_000;
    let sign = if timestamp_ns < 0 { "-" } else { "" };
    let text = if nanos == 0 {
        format!("{sign}{seconds}")
    } else {
        let fraction = format!("{nanos:09}").trim_end_matches('0').to_string();
        format!("{sign}{seconds}.{fraction}")
    };
    serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))
}

fn format_metric_value(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        value.to_string()
    }
}

fn parse_direction(direction: &Option<String>) -> Result<bool, String> {
    match direction.as_deref() {
        None => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("forward") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("backward") => Ok(false),
        Some(value) => Err(format!(
            "invalid direction '{value}': expected 'forward' or 'backward'"
        )),
    }
}

fn parse_limit(limit: Option<usize>) -> Result<usize, String> {
    let limit = limit.unwrap_or(100);
    if limit > MAX_LOG_LIMIT {
        return Err(format!(
            "query limit exceeds the maximum of {MAX_LOG_LIMIT}"
        ));
    }
    Ok(limit)
}

fn distinct_stream_count(state: &AppState) -> usize {
    let mut streams = std::collections::BTreeSet::new();
    streams.extend(state.memtable.series(&[]));
    streams.extend(state.parts.series(&[]));
    streams.len()
}

#[cfg(test)]
fn unified_query(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(unified_query_with_stats(state, parsed, start_ns, end_ns, limit, forward, None)?.results)
}

struct QueryExecution {
    results: Vec<StreamResult>,
    scanned_rows: u64,
}

#[cfg(test)]
fn unified_query_with_stats(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    unified_query_with_stats_cancellable(
        state,
        parsed,
        start_ns,
        end_ns,
        limit,
        forward,
        scan_budget,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn unified_query_with_stats_cancellable(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Option<&AtomicBool>,
) -> Result<QueryExecution, String> {
    let mut all: Vec<(Labels, LogEntry)> = Vec::new();
    let mut scanned_rows = 0u64;

    // Pipeline predicates run after storage scans. Do not let the API log
    // limit truncate raw rows before a json/logfmt/field stage has evaluated.
    let normal_scan_limit = if parsed.stages.len() == parsed.line_filters.len() {
        limit
    } else {
        usize::MAX
    };
    let scan_limit = scan_budget.map(|budget| budget.saturating_add(1));

    let memtable_result = state.memtable.query_with_scan_limit(
        &parsed.matchers,
        &parsed.line_filters,
        start_ns,
        end_ns,
        normal_scan_limit,
        forward,
        scan_limit,
        cancellation,
    );
    scanned_rows = scanned_rows.saturating_add(memtable_result.scanned_rows as u64);
    for sr in memtable_result.results {
        for mut e in sr.entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("query timed out".to_string());
            }
            if parsed.process_entry_with_labels_cancellable(&sr.labels, &mut e, cancellation)? {
                all.push((sr.labels.clone(), e));
            }
        }
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if let Some(budget) = scan_budget
        && scanned_rows > budget as u64
    {
        return Err(format!(
            "query exceeds the maximum of {budget} scanned rows"
        ));
    }

    let exact_fields = parsed.exact_field_predicates();
    let part_scan_limit = scan_limit.map(|budget| budget.saturating_sub(scanned_rows as usize));
    let part_result = state.parts.query_with_exact_field_pruning_and_scan_limit(
        &parsed.matchers,
        crate::part::ExactFieldPruning::new(&parsed.line_filters, &exact_fields),
        start_ns,
        end_ns,
        normal_scan_limit,
        forward,
        part_scan_limit,
        cancellation,
    )?;
    scanned_rows = scanned_rows.saturating_add(part_result.scanned_rows as u64);
    for sr in part_result.results {
        for mut e in sr.entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("query timed out".to_string());
            }
            if parsed.process_entry_with_labels_cancellable(&sr.labels, &mut e, cancellation)? {
                all.push((sr.labels.clone(), e));
            }
        }
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if let Some(budget) = scan_budget
        && scanned_rows > budget as u64
    {
        return Err(format!(
            "query exceeds the maximum of {budget} scanned rows"
        ));
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if forward {
        all.sort_by_key(|e| e.1.timestamp_ns);
    } else {
        all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
    }
    all.truncate(limit);

    Ok(QueryExecution {
        results: part::group_by_labels(all),
        scanned_rows,
    })
}

#[cfg(test)]
async fn run_unified_query(
    state: Arc<AppState>,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(
        run_unified_query_with_stats(state, parsed, start_ns, end_ns, limit, forward, None)
            .await?
            .results,
    )
}

async fn run_unified_query_with_stats(
    state: Arc<AppState>,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    let cancellation = Arc::new(AtomicBool::new(false));
    run_unified_query_with_stats_cancellable(
        state,
        parsed,
        start_ns,
        end_ns,
        limit,
        forward,
        scan_budget,
        cancellation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_unified_query_with_stats_cancellable(
    state: Arc<AppState>,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
) -> Result<QueryExecution, String> {
    let scan_permit =
        tokio::time::timeout(MAX_QUERY_RUNTIME, query_scan_semaphore().acquire_owned())
            .await
            .map_err(|_| "query timed out".to_string())?
            .map_err(|_| "query scan scheduler is closed".to_string())?;
    let part_guard = tokio::time::timeout(
        MAX_QUERY_RUNTIME,
        pin_query_parts(&state, &parsed, start_ns, end_ns),
    )
    .await
    .map_err(|_| "query timed out".to_string())??;
    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        // Keep the scheduler permit until the blocking task actually exits;
        // cancelling the request must not admit an unbounded second scan while
        // the first task is still consuming CPU and memory.
        let _scan_permit = scan_permit;
        let _part_guard = part_guard;
        unified_query_with_stats_cancellable(
            &state,
            &parsed,
            start_ns,
            end_ns,
            limit,
            forward,
            scan_budget,
            Some(task_cancellation.as_ref()),
        )
    });
    match tokio::time::timeout(MAX_QUERY_RUNTIME, &mut task).await {
        Ok(result) => result.map_err(|error| format!("query task failed: {error}"))?,
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            Err("query timed out".to_string())
        }
    }
}

async fn pin_query_parts(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, String> {
    pin_query_parts_with_gap_hook(state, parsed, start_ns, end_ns, || Ok(())).await
}

async fn pin_query_parts_with_gap_hook<F>(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    after_read_guard: F,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, String>
where
    F: FnOnce() -> Result<(), String>,
{
    let operation_lock = state.parts.operation_lock();
    let read_guard = operation_lock.clone().read_owned().await;
    let Some(remote) = &state.remote_cache else {
        return Ok(read_guard);
    };
    let exact_fields = parsed.exact_field_predicates();
    let required = state.parts.candidate_part_ids_with_exact_fields(
        &parsed.matchers,
        &parsed.line_filters,
        &exact_fields,
        start_ns,
        end_ns,
    );
    let missing = state.parts.missing_data_ids(&required);
    if missing.is_empty() {
        return Ok(read_guard);
    }
    drop(read_guard);
    after_read_guard()?;

    let write_guard = operation_lock.write_owned().await;
    // A merge may have replaced the registry generation while this query was
    // queued for the exclusive guard. Re-plan against the protected current
    // generation instead of restoring only IDs captured before the gap.
    let required = state.parts.candidate_part_ids_with_exact_fields(
        &parsed.matchers,
        &parsed.line_filters,
        &exact_fields,
        start_ns,
        end_ns,
    );
    let missing = state.parts.missing_data_ids(&required);
    if !missing.is_empty() {
        let epoch = remote.remote_operation_epoch();
        match tokio::time::timeout(
            MAX_RESTORE_RUNTIME,
            remote.storage.restore_parts(&remote.parts_root, &missing),
        )
        .await
        {
            Ok(Ok(())) => remote.mark_remote_healthy_since(epoch),
            Ok(Err(error)) => {
                remote.mark_remote_unhealthy();
                return Err(error);
            }
            Err(_) => {
                remote.mark_remote_unhealthy();
                return Err("object store restore timed out".to_string());
            }
        }
    }
    Ok(tokio::sync::OwnedRwLockWriteGuard::downgrade(write_guard))
}

fn parse_step_ns(step: Option<&str>) -> Result<i64, String> {
    let Some(step) = step else {
        return Ok(1_000_000_000);
    };
    let step = step.trim();
    let value = match step.parse::<f64>() {
        Ok(seconds) if seconds.is_finite() => {
            let ns = seconds * 1_000_000_000.0;
            if !ns.is_finite() || ns > i64::MAX as f64 {
                return Err(format!("step '{step}' is out of range"));
            }
            ns.round() as i64
        }
        Ok(_) => return Err(format!("step '{step}' must be finite")),
        Err(_) => logql::parse_duration_ns(step)?,
    };
    if value <= 0 {
        return Err("step must be greater than zero".to_string());
    }
    Ok(value)
}

#[cfg(test)]
async fn run_metric_query(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
) -> Result<Vec<MetricSeries>, String> {
    Ok(
        run_metric_query_with_stats(state, expr, evaluation_times, None)
            .await?
            .series,
    )
}

struct MetricQueryResult {
    series: Vec<MetricSeries>,
    scanned_rows: u64,
}

async fn run_metric_query_with_stats(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
) -> Result<MetricQueryResult, String> {
    let cancellation = Arc::new(AtomicBool::new(false));
    run_metric_query_with_stats_cancellable(
        state,
        expr,
        evaluation_times,
        scan_start_override,
        cancellation,
    )
    .await
}

async fn run_metric_query_with_stats_cancellable(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
    cancellation: Arc<AtomicBool>,
) -> Result<MetricQueryResult, String> {
    let evaluation_permit = tokio::time::timeout(
        MAX_QUERY_RUNTIME,
        metric_evaluation_semaphore().acquire_owned(),
    )
    .await
    .map_err(|_| "metric query timed out".to_string())?
    .map_err(|_| "metric evaluation scheduler is closed".to_string())?;
    let first = *evaluation_times
        .first()
        .ok_or("metric query has no evaluation timestamps")?;
    let end = *evaluation_times.last().unwrap();
    let scan_start =
        scan_start_override.unwrap_or_else(|| first.saturating_sub(expr.lookback_ns()));
    let query = expr.log_query().clone();
    let execution = run_unified_query_with_stats_cancellable(
        state,
        query,
        scan_start,
        end,
        MAX_METRIC_ROWS.saturating_add(1),
        true,
        Some(MAX_METRIC_ROWS),
        cancellation.clone(),
    )
    .await?;
    let row_count: usize = execution
        .results
        .iter()
        .map(|stream| stream.entries.len())
        .sum();
    if row_count > MAX_METRIC_ROWS {
        return Err(format!(
            "metric query exceeds the maximum of {MAX_METRIC_ROWS} scanned rows"
        ));
    }
    let streams = execution.results;
    let scanned_rows = execution.scanned_rows;
    let grouping_fields = expr.grouping_fields();
    let mut entries = Vec::new();
    for stream in streams {
        for entry in stream.entries {
            if cancellation.load(Ordering::Acquire) {
                return Err("metric query timed out".to_string());
            }
            let mut labels = stream.labels.clone();
            // Pipeline-extracted fields are query-local structured metadata
            // at this point. Promote them into the metric label set so range
            // functions and aggregations can distinguish entries such as
            // `level=info` and `level=error` from the same stream.
            for (name, value) in &entry.structured_metadata {
                // Pipeline fields must not replace original stream labels.
                // A colliding extraction has already been renamed by
                // process_entry_with_labels.
                if grouping_fields.contains(name) {
                    labels.entry(name.clone()).or_insert_with(|| value.clone());
                }
            }
            entries.push((labels, entry));
        }
    }

    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _evaluation_permit = evaluation_permit;
        evaluate_metric_stream(
            &expr,
            &entries,
            &evaluation_times,
            Some(task_cancellation.as_ref()),
        )
    });
    let output = match tokio::time::timeout(MAX_QUERY_RUNTIME, &mut task).await {
        Ok(result) => {
            result.map_err(|error| format!("metric evaluation task failed: {error}"))??
        }
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            return Err("metric query timed out".to_string());
        }
    };
    Ok(MetricQueryResult {
        series: output
            .into_iter()
            .map(|(labels, samples)| MetricSeries { labels, samples })
            .collect(),
        scanned_rows,
    })
}

#[cfg(test)]
fn evaluate_metric_at(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    timestamp_ns: i64,
) -> Vec<(Labels, f64)> {
    evaluate_metric_all(expr, entries, &[timestamp_ns])
        .expect("test metric evaluation must remain within resource limits")
        .into_iter()
        .next()
        .unwrap_or_default()
}

#[cfg(test)]
fn ensure_metric_series_limit(values: &[Vec<(Labels, f64)>]) -> Result<(), String> {
    if values
        .iter()
        .any(|at_time| at_time.len() > MAX_METRIC_SERIES)
    {
        return Err(format!(
            "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
        ));
    }
    let samples: usize = values.iter().map(Vec::len).sum();
    if samples > MAX_METRIC_SAMPLES {
        return Err(format!(
            "metric query exceeds the maximum of {MAX_METRIC_SAMPLES} output samples"
        ));
    }
    Ok(())
}

/// Evaluate all timestamps in one pass over each label series. The previous
/// implementation rescanned every entry for every evaluation point, turning a
/// bounded metric query into O(points * rows). Sliding window cursors reduce
/// the range-function work to O(rows + points * series), while the explicit
/// output-sample cap still bounds the materialized result.
#[cfg(test)]
fn evaluate_metric_all(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    evaluation_times: &[i64],
) -> Result<Vec<Vec<(Labels, f64)>>, String> {
    let values = match expr {
        logql::MetricExpr::Range {
            function, range_ns, ..
        } => {
            let mut by_labels: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
            for (labels, entry) in entries {
                let increment = match function {
                    logql::RangeFunction::BytesOverTime => entry.line.len() as f64,
                    logql::RangeFunction::Rate | logql::RangeFunction::CountOverTime => 1.0,
                };
                by_labels
                    .entry(labels.clone())
                    .or_default()
                    .push((entry.timestamp_ns, increment));
            }
            for events in by_labels.values_mut() {
                events.sort_unstable_by_key(|(timestamp_ns, _)| *timestamp_ns);
            }

            let mut output: Vec<Vec<(Labels, f64)>> =
                (0..evaluation_times.len()).map(|_| Vec::new()).collect();
            let seconds = *range_ns as f64 / 1_000_000_000.0;
            for (labels, events) in by_labels {
                let mut left = 0usize;
                let mut right = 0usize;
                let mut active_count = 0usize;
                let mut active_value = 0.0f64;
                for (index, &evaluation_ns) in evaluation_times.iter().enumerate() {
                    while right < events.len() && events[right].0 <= evaluation_ns {
                        active_count += 1;
                        active_value += events[right].1;
                        right += 1;
                    }
                    if let Some(window_start) = evaluation_ns.checked_sub(*range_ns) {
                        while left < right && events[left].0 <= window_start {
                            active_count -= 1;
                            active_value -= events[left].1;
                            left += 1;
                        }
                    }
                    if active_count > 0 {
                        let value = if matches!(function, logql::RangeFunction::Rate) {
                            active_value / seconds
                        } else {
                            active_value
                        };
                        output[index].push((labels.clone(), value));
                        if output[index].len() > MAX_METRIC_SERIES {
                            return Err(format!(
                                "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
                            ));
                        }
                    }
                }
            }
            output
        }
        logql::MetricExpr::Aggregate { op, by, expr } => {
            let inner = evaluate_metric_all(expr, entries, evaluation_times)?;
            let mut output = Vec::with_capacity(inner.len());
            for values in inner {
                let mut grouped: BTreeMap<Labels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in values {
                    let group = match by {
                        Some(names) => names
                            .iter()
                            .filter_map(|name| {
                                labels.get(name).map(|value| (name.clone(), value.clone()))
                            })
                            .collect(),
                        None => Labels::new(),
                    };
                    let aggregate = grouped.entry(group).or_insert((value, 0));
                    if aggregate.1 > 0 {
                        aggregate.0 = match op {
                            logql::AggregateOp::Sum | logql::AggregateOp::Avg => {
                                aggregate.0 + value
                            }
                            logql::AggregateOp::Min => aggregate.0.min(value),
                            logql::AggregateOp::Max => aggregate.0.max(value),
                        };
                    }
                    aggregate.1 += 1;
                }
                output.push(
                    grouped
                        .into_iter()
                        .map(|(labels, (mut value, count))| {
                            if matches!(op, logql::AggregateOp::Avg) {
                                value /= count as f64;
                            }
                            (labels, value)
                        })
                        .collect(),
                );
            }
            output
        }
        logql::MetricExpr::TopK { k, expr } => {
            let mut output = evaluate_metric_all(expr, entries, evaluation_times)?;
            for values in &mut output {
                values.sort_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                values.truncate(*k);
            }
            output
        }
    };
    ensure_metric_series_limit(&values)?;
    Ok(values)
}

struct MetricRangeSeries {
    events: Vec<(i64, f64)>,
    prefix: Vec<f64>,
}

struct MetricEvaluator {
    function: logql::RangeFunction,
    range_ns: i64,
    by_labels: BTreeMap<Labels, MetricRangeSeries>,
}

impl MetricEvaluator {
    fn new(
        expr: &logql::MetricExpr,
        entries: &[(Labels, LogEntry)],
        cancellation: Option<&AtomicBool>,
    ) -> Result<Self, String> {
        let (function, range_ns) = metric_range_spec(expr);
        let mut events_by_labels: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
        for (labels, entry) in entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("metric query timed out".to_string());
            }
            let increment = match function {
                logql::RangeFunction::BytesOverTime => entry.line.len() as f64,
                logql::RangeFunction::Rate | logql::RangeFunction::CountOverTime => 1.0,
            };
            if !events_by_labels.contains_key(labels) && events_by_labels.len() == MAX_METRIC_SERIES
            {
                return Err(format!(
                    "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
                ));
            }
            events_by_labels
                .entry(labels.clone())
                .or_default()
                .push((entry.timestamp_ns, increment));
        }

        let mut by_labels = BTreeMap::new();
        for (labels, mut events) in events_by_labels {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("metric query timed out".to_string());
            }
            events.sort_unstable_by_key(|(timestamp_ns, _)| *timestamp_ns);
            let mut prefix = Vec::with_capacity(events.len() + 1);
            prefix.push(0.0);
            for &(_, value) in &events {
                if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    return Err("metric query timed out".to_string());
                }
                prefix.push(prefix.last().copied().unwrap_or_default() + value);
            }
            by_labels.insert(labels, MetricRangeSeries { events, prefix });
        }
        Ok(Self {
            function,
            range_ns,
            by_labels,
        })
    }

    fn evaluate_at(
        &self,
        expr: &logql::MetricExpr,
        evaluation_ns: i64,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Vec<(Labels, f64)>, String> {
        let mut values = match expr {
            logql::MetricExpr::Range { .. } => {
                let seconds = self.range_ns as f64 / 1_000_000_000.0;
                let window_start = evaluation_ns.checked_sub(self.range_ns);
                let mut values = Vec::new();
                for (labels, series) in &self.by_labels {
                    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        return Err("metric query timed out".to_string());
                    }
                    let right = series
                        .events
                        .partition_point(|(timestamp_ns, _)| *timestamp_ns <= evaluation_ns);
                    let left = window_start
                        .map(|start| {
                            series
                                .events
                                .partition_point(|(timestamp_ns, _)| *timestamp_ns <= start)
                        })
                        .unwrap_or(0);
                    if right > left {
                        let mut value = series.prefix[right] - series.prefix[left];
                        if matches!(self.function, logql::RangeFunction::Rate) {
                            value /= seconds;
                        }
                        values.push((labels.clone(), value));
                        if values.len() > MAX_METRIC_SERIES {
                            return Err(format!(
                                "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
                            ));
                        }
                    }
                }
                values
            }
            logql::MetricExpr::Aggregate { op, by, expr } => {
                let inner = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                let mut grouped: BTreeMap<Labels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in inner {
                    let group = match by {
                        Some(names) => names
                            .iter()
                            .filter_map(|name| {
                                labels.get(name).map(|value| (name.clone(), value.clone()))
                            })
                            .collect(),
                        None => Labels::new(),
                    };
                    let aggregate = grouped.entry(group).or_insert((value, 0));
                    if aggregate.1 > 0 {
                        aggregate.0 = match op {
                            logql::AggregateOp::Sum | logql::AggregateOp::Avg => {
                                aggregate.0 + value
                            }
                            logql::AggregateOp::Min => aggregate.0.min(value),
                            logql::AggregateOp::Max => aggregate.0.max(value),
                        };
                    }
                    aggregate.1 += 1;
                }
                grouped
                    .into_iter()
                    .map(|(labels, (mut value, count))| {
                        if matches!(op, logql::AggregateOp::Avg) {
                            value /= count as f64;
                        }
                        (labels, value)
                    })
                    .collect()
            }
            logql::MetricExpr::TopK { k, expr } => {
                let mut values = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                values.sort_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                values.truncate(*k);
                values
            }
        };
        if values.len() > MAX_METRIC_SERIES {
            return Err(format!(
                "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
            ));
        }
        Ok(std::mem::take(&mut values))
    }
}

fn metric_range_spec(expr: &logql::MetricExpr) -> (logql::RangeFunction, i64) {
    match expr {
        logql::MetricExpr::Range {
            function, range_ns, ..
        } => (*function, *range_ns),
        logql::MetricExpr::Aggregate { expr, .. } | logql::MetricExpr::TopK { expr, .. } => {
            metric_range_spec(expr)
        }
    }
}

fn evaluate_metric_stream(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
) -> Result<BTreeMap<Labels, Vec<(i64, f64)>>, String> {
    let evaluator = MetricEvaluator::new(expr, entries, cancellation)?;
    let mut output: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
    let mut sample_count = 0usize;
    for &timestamp_ns in evaluation_times {
        if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err("metric query timed out".to_string());
        }
        let values = evaluator.evaluate_at(expr, timestamp_ns, cancellation)?;
        sample_count = sample_count.saturating_add(values.len());
        if sample_count > MAX_METRIC_SAMPLES {
            return Err(format!(
                "metric query exceeds the maximum of {MAX_METRIC_SAMPLES} output samples"
            ));
        }
        for (labels, value) in values {
            if !output.contains_key(&labels) && output.len() == MAX_METRIC_SERIES {
                return Err(format!(
                    "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
                ));
            }
            output
                .entry(labels)
                .or_default()
                .push((timestamp_ns, value));
        }
    }
    Ok(output)
}

fn evaluation_times(start_ns: i64, end_ns: i64, step_ns: i64) -> Result<Vec<i64>, String> {
    if start_ns > end_ns {
        return Err("query start must not be after end".to_string());
    }
    if step_ns <= 0 {
        return Err("step must be greater than zero".to_string());
    }
    let mut times = Vec::new();
    let mut current = start_ns;
    loop {
        if times.len() == MAX_METRIC_EVALUATION_POINTS {
            return Err(format!(
                "metric query exceeds the maximum of {MAX_METRIC_EVALUATION_POINTS} evaluation points"
            ));
        }
        times.push(current);
        if current == end_ns {
            break;
        }
        let Some(next) = current.checked_add(step_ns) else {
            break;
        };
        if next > end_ns {
            break;
        }
        current = next;
    }
    Ok(times)
}

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
            logql::QueryExpr::Logs(_) => 0,
            logql::QueryExpr::Metric(expr) => end_ns.saturating_sub(expr.lookback_ns()),
        },
    };

    if start_ns > end_ns {
        return Err((
            StatusCode::BAD_REQUEST,
            "query start must not be after end".to_string(),
        ));
    }

    let forward = parse_direction(&params.direction).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (result_type, result, total_lines_processed) = match parsed {
        logql::QueryExpr::Logs(parsed) => {
            let limit = parse_limit(params.limit).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let execution = run_unified_query_with_stats(
                state,
                parsed,
                start_ns,
                end_ns,
                limit,
                forward,
                Some(MAX_LOG_SCAN_ROWS),
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

    let forward = parse_direction(&params.direction).map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let (result_type, result, total_lines_processed) = match parsed {
        logql::QueryExpr::Logs(parsed) => {
            let limit = parse_limit(params.limit).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            let execution = run_unified_query_with_stats(
                state,
                parsed,
                0,
                end_ns,
                limit,
                forward,
                Some(MAX_LOG_SCAN_ROWS),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::{LogEntry, MemTable};
    use crate::part::{self, Row};
    use crate::part_registry::PartRegistry;
    use tower::ServiceExt;

    fn temp_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "loggytracy-query-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn test_state(
        data_dir: &std::path::Path,
        memtable: Arc<MemTable>,
        parts: Arc<PartRegistry>,
        remote_cache: Option<Arc<crate::object_storage::RemoteCache>>,
    ) -> Arc<AppState> {
        let config = Config {
            data_dir: data_dir.to_path_buf(),
            ..Config::default()
        };
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts,
            trace_parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache,
        })
    }

    #[test]
    fn extracts_repeated_match_params() {
        let raw =
            Some("match%5B%5D=%7Bapp%3D%22a%22%7D&match%5B%5D=%7Bapp%3D%22b%22%7D".to_string());
        let v = extract_match_params(&raw);
        assert_eq!(v, vec![r#"{app="a"}"#, r#"{app="b"}"#]);
    }

    #[test]
    fn extracts_single_match_param() {
        let raw = Some("match%5B%5D=%7Bjob%3D%22x%22%7D".to_string());
        let v = extract_match_params(&raw);
        assert_eq!(v, vec![r#"{job="x"}"#]);
    }

    #[test]
    fn empty_when_no_match() {
        assert!(extract_match_params(&None).is_empty());
        assert!(extract_match_params(&Some("foo=bar".to_string())).is_empty());
    }

    #[tokio::test]
    async fn http_query_range_decodes_url_encoded_logql() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels,
            vec![LogEntry {
                timestamp_ns: 5_000_000_000,
                line: "hello from http".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let request = axum::http::Request::builder()
            .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22hello%22&start=4&end=6&limit=10&direction=forward")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = crate::build_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["resultType"], "streams");
        assert_eq!(json["data"]["result"][0]["values"][0][1], "hello from http");
        assert_eq!(json["data"]["stats"]["summary"]["totalLinesProcessed"], 1);
    }

    #[tokio::test]
    async fn omitted_metric_start_scans_the_first_lookback_window() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels,
            vec![LogEntry {
                timestamp_ns: 20_000_000_000,
                line: "inside first lookback".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let response = query_range(
            State(state),
            Query(QueryRangeParams {
                query: "count_over_time({}[60s])".to_string(),
                start: None,
                end: Some("100".to_string()),
                limit: None,
                direction: None,
                step: Some("60".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(response.data.result[0]["values"][0][0], 40);
        assert_eq!(response.data.result[0]["values"][0][1], "1");
    }

    #[tokio::test]
    async fn query_scan_stats_count_rows_before_applying_output_limit() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels,
            (0..3)
                .map(|timestamp_ns| LogEntry {
                    timestamp_ns,
                    line: format!("line-{timestamp_ns}"),
                    structured_metadata: Vec::new(),
                })
                .collect(),
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);
        let parsed = logql::parse("{}").unwrap();
        let execution =
            run_unified_query_with_stats(state.clone(), parsed.clone(), 0, 2, 1, true, None)
                .await
                .unwrap();
        assert_eq!(execution.results[0].entries.len(), 1);
        assert_eq!(execution.scanned_rows, 3);

        let error = match unified_query_with_stats_cancellable(
            &state,
            &parsed,
            0,
            2,
            1,
            true,
            Some(2),
            None,
        ) {
            Ok(_) => panic!("scan budget should reject the third physical row"),
            Err(error) => error,
        };
        assert!(error.contains("2 scanned rows"));
    }

    #[test]
    fn parse_time_ns_unix_seconds() {
        assert_eq!(parse_time_ns("0").unwrap(), 0);
        assert_eq!(
            parse_time_ns("1700000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_negative_unix_seconds() {
        assert_eq!(
            parse_time_ns("-1700000000").unwrap(),
            -1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_unix_nanos() {
        assert_eq!(
            parse_time_ns("1700000000000000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_unix_millis_and_micros() {
        assert_eq!(
            parse_time_ns("1700000000000").unwrap(),
            1_700_000_000_000_000_000
        );
        assert_eq!(
            parse_time_ns("1700000000000000").unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn parse_time_ns_rfc3339() {
        let ns = parse_time_ns("2023-11-14T22:13:20Z").unwrap();
        assert_eq!(ns, 1_700_000_000_000_000_000);
    }

    #[test]
    fn parse_time_ns_rfc3339_with_nanos() {
        let ns = parse_time_ns("2023-11-14T22:13:20.123456789Z").unwrap();
        assert_eq!(ns, 1_700_000_000_123_456_789);
    }

    #[test]
    fn parse_time_ns_invalid() {
        assert!(parse_time_ns("not-a-time").is_err());
        assert!(parse_time_ns("").is_err());
    }

    #[test]
    fn parse_time_ns_accepts_exact_decimal_unix_seconds() {
        assert_eq!(
            parse_time_ns("1700000000.000000001").unwrap(),
            1_700_000_000_000_000_001
        );
        assert_eq!(parse_time_ns("1.5e0").unwrap(), 1_500_000_000);
        assert!(parse_time_ns("1.0000000001").is_err());
    }

    #[test]
    fn metric_timestamps_keep_nanosecond_precision_in_json() {
        assert_eq!(
            timestamp_seconds_json(1_700_000_000_000_000_001).to_string(),
            "1700000000.000000001"
        );
        assert_eq!(timestamp_seconds_json(-1).to_string(), "-0.000000001");
    }

    #[test]
    fn query_input_limits_and_direction_are_validated() {
        assert_eq!(parse_limit(None).unwrap(), 100);
        assert_eq!(parse_limit(Some(MAX_LOG_LIMIT)).unwrap(), MAX_LOG_LIMIT);
        assert!(parse_limit(Some(MAX_LOG_LIMIT + 1)).is_err());
        assert!(parse_direction(&Some("sideways".to_string())).is_err());
        assert!(parse_direction(&Some("FORWARD".to_string())).unwrap());
        assert!(!parse_direction(&Some("backward".to_string())).unwrap());
    }

    #[tokio::test]
    async fn distinct_stream_count_deduplicates_memtable_and_parts() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let labels: Labels = [("app".to_string(), "same-stream".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 2,
                line: "in memory".to_string(),
                structured_metadata: Vec::new(),
            }],
        );
        let parts_root = data_dir.join("parts");
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        timestamp_ns: 1,
                        labels,
                        line: "on disk".to_string(),
                        structured_metadata: Vec::new(),
                    }],
                    &parts_root,
                    config.row_group_size,
                )
                .unwrap(),
            )
            .unwrap();
        let journal = Arc::new(Journal::spawn(&config, memtable.clone()).unwrap());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = AppState {
            memtable,
            journal,
            parts,
            trace_parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: None,
        };

        assert_eq!(distinct_stream_count(&state), 1);
    }

    #[tokio::test]
    async fn readiness_reflects_background_worker_health() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let state = Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts: Arc::new(PartRegistry::new()),
            trace_parts: Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: None,
        });

        assert_eq!(ready(State(state.clone())).await.unwrap(), "ready");

        state.flush_healthy.store(false, Ordering::Release);
        state.merge_healthy.store(false, Ordering::Release);
        state.otlp_healthy.store(false, Ordering::Release);
        let error = ready(State(state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(error.1.contains("flush worker"));
        assert!(error.1.contains("merge worker"));
        assert!(error.1.contains("OTLP gRPC server"));
    }

    #[tokio::test]
    async fn readiness_reflects_remote_storage_health() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let remote = Arc::new(crate::object_storage::RemoteCache::new(
            Arc::new(crate::object_storage::ObjectStorage::in_memory()),
            data_dir.join("parts"),
        ));
        remote.mark_remote_unhealthy();
        remote.mark_cache_healthy();
        let state = Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts: Arc::new(PartRegistry::new()),
            trace_parts: Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: Some(remote.clone()),
        });

        let error = ready(State(state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(error.1.contains("object store"));

        remote.mark_remote_healthy();
        remote.mark_cache_unhealthy();
        let state = Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, Arc::new(MemTable::new())).unwrap()),
            memtable: Arc::new(MemTable::new()),
            parts: Arc::new(PartRegistry::new()),
            trace_parts: Arc::new(crate::trace_registry::TraceRegistry::standalone()),
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: Some(remote),
        });
        let error = ready(State(state)).await.unwrap_err();
        assert!(error.1.contains("local cache"));
    }

    #[tokio::test]
    async fn query_restores_evicted_part_from_object_store() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        let local_parts = part::flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels,
                line: "restored after eviction".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let other_labels: Labels = [("app".to_string(), "other".to_string())]
            .into_iter()
            .collect();
        let other_parts = part::flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_001,
                labels: other_labels,
                line: "must remain remote".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let other_data_path = other_parts[0].data_path();
        let mut published_parts = local_parts.clone();
        published_parts.extend(other_parts);
        storage.publish(&published_parts, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_ids())
            .unwrap();
        assert!(parts.has_missing_cache_files());

        let memtable = Arc::new(MemTable::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts,
            trace_parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        });
        let parsed = logql::parse(r#"{app="remote"}"#).unwrap();
        let result = run_unified_query(state, parsed, i64::MIN, i64::MAX, 10, true)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].entries[0].line, "restored after eviction");
        assert!(
            !other_data_path.exists(),
            "query restored an unrelated part"
        );
    }

    #[tokio::test]
    async fn query_replans_restore_after_registry_changes_in_lock_gap() {
        let data_dir = temp_dir();
        let config = Config {
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        let old = part::flush_rows(
            vec![Row {
                timestamp_ns: 1,
                labels: labels.clone(),
                line: "old generation".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());

        let new = part::flush_rows(
            vec![Row {
                timestamp_ns: 2,
                labels,
                line: "new generation".to_string(),
                structured_metadata: Vec::new(),
            }],
            &parts_root,
            config.row_group_size,
        )
        .unwrap();
        let manifest = storage
            .publish(&new, &[old[0].meta.id.clone()])
            .await
            .unwrap();
        let eligible = [old[0].meta.id.clone(), new[0].meta.id.clone()]
            .into_iter()
            .collect();
        storage.evict_cache(&parts_root, 0, &eligible).unwrap();

        let memtable = Arc::new(MemTable::new());
        let trace_parts = Arc::new(crate::trace_registry::TraceRegistry::new(
            parts.operation_lock(),
        ));
        let state = AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts: parts.clone(),
            trace_parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage,
                parts_root.clone(),
            ))),
        };
        let parsed = logql::parse(r#"{app="remote"}"#).unwrap();
        let guard = pin_query_parts_with_gap_hook(&state, &parsed, i64::MIN, i64::MAX, || {
            parts.reload_from_manifest(&parts_root, &manifest)
        })
        .await
        .unwrap();
        let result = unified_query(&state, &parsed, i64::MIN, i64::MAX, 10, true).unwrap();
        drop(guard);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].entries[0].line, "new generation");
        assert!(new[0].data_path().exists());
    }

    #[tokio::test]
    async fn structured_filters_match_identically_in_memtable_and_parts() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let line = r#"{"code":503,"elapsed":"250ms","json":"ok","level":"error"}"#;
        let metadata = vec![
            ("trace_id".to_string(), "abc".to_string()),
            ("logfmt".to_string(), "value".to_string()),
        ];
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 10,
                line: line.to_string(),
                structured_metadata: metadata.clone(),
            }],
        );
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        timestamp_ns: 20,
                        labels,
                        line: line.to_string(),
                        structured_metadata: metadata,
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);
        let parsed = logql::parse(
            r#"{app="api"} | app="api" | trace_id="abc" | json | level=~"err.*" | code>=500 | elapsed<1s"#,
        )
        .unwrap();

        let result = run_unified_query(state.clone(), parsed, 0, 30, 10, true)
            .await
            .unwrap();
        let timestamps: Vec<_> = result[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect();
        assert_eq!(timestamps, vec![10, 20]);

        for query in [r#"{} | json | json="ok""#, r#"{} | logfmt="value""#] {
            let result =
                run_unified_query(state.clone(), logql::parse(query).unwrap(), 0, 30, 10, true)
                    .await
                    .unwrap();
            let timestamps: Vec<_> = result[0]
                .entries
                .iter()
                .map(|entry| entry.timestamp_ns)
                .collect();
            assert_eq!(timestamps, vec![10, 20]);
        }
    }

    #[tokio::test]
    async fn synthesized_extracted_field_never_false_negative_prunes_parts() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let metadata = vec![
            ("foo".to_string(), "x".to_string()),
            ("foo_extracted".to_string(), "y".to_string()),
        ];
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels.clone(),
            vec![LogEntry {
                timestamp_ns: 10,
                line: r#"{"foo":"z"}"#.to_string(),
                structured_metadata: metadata.clone(),
            }],
        );
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        timestamp_ns: 20,
                        labels,
                        line: r#"{"foo":"z"}"#.to_string(),
                        structured_metadata: metadata,
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, memtable, parts, None);
        let parsed = logql::parse(r#"{} | json | foo_extracted_2="z""#).unwrap();

        let result = run_unified_query(state, parsed, 0, 30, 10, true)
            .await
            .unwrap();
        let timestamps: Vec<_> = result[0]
            .entries
            .iter()
            .map(|entry| entry.timestamp_ns)
            .collect();
        assert_eq!(timestamps, vec![10, 20]);
    }

    #[test]
    fn metric_windows_are_left_open_and_aggregations_recompute_each_step() {
        let a: Labels = [("app".to_string(), "a".to_string())].into_iter().collect();
        let b: Labels = [("app".to_string(), "b".to_string())].into_iter().collect();
        let entries = vec![
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 5_000_000_000,
                    line: "boundary".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 6_000_000_000,
                    line: "a1".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                a.clone(),
                LogEntry {
                    timestamp_ns: 10_000_000_000,
                    line: "a2".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 8_000_000_000,
                    line: "b1".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 13_000_000_000,
                    line: "b2".into(),
                    structured_metadata: vec![],
                },
            ),
            (
                b.clone(),
                LogEntry {
                    timestamp_ns: 14_000_000_000,
                    line: "b3".into(),
                    structured_metadata: vec![],
                },
            ),
        ];
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"topk(1, sum by (app) (count_over_time({}[5s])))"#).unwrap()
        else {
            panic!("expected metric")
        };

        let at_ten = evaluate_metric_at(&expr, &entries, 10_000_000_000);
        assert_eq!(at_ten, vec![(a, 2.0)], "entry at t-range must be excluded");
        let at_fifteen = evaluate_metric_at(&expr, &entries, 15_000_000_000);
        assert_eq!(
            at_fifteen,
            vec![(b, 2.0)],
            "topk must be recomputed per step"
        );

        for (name, expected) in [("sum", 3.0), ("avg", 1.5), ("min", 1.0), ("max", 2.0)] {
            let logql::QueryExpr::Metric(aggregate) =
                logql::parse_expr(&format!("{name}(count_over_time({{}}[5s]))")).unwrap()
            else {
                panic!("expected metric")
            };
            assert_eq!(
                evaluate_metric_at(&aggregate, &entries, 10_000_000_000),
                vec![(Labels::new(), expected)]
            );
        }
    }

    #[test]
    fn metric_evaluation_stops_when_timeout_cancellation_is_requested() {
        let expr = match logql::parse_expr("count_over_time({}[1m])") {
            Ok(logql::QueryExpr::Metric(expr)) => expr,
            _ => panic!("expected metric expression"),
        };
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let entries = vec![(
            labels,
            LogEntry {
                timestamp_ns: 1,
                line: "line".to_string(),
                structured_metadata: Vec::new(),
            },
        )];
        let cancelled = AtomicBool::new(true);
        let result = evaluate_metric_stream(&expr, &entries, &[1], Some(&cancelled));
        assert_eq!(result.unwrap_err(), "metric query timed out");
    }

    #[tokio::test]
    async fn metric_window_includes_i64_min_when_mathematical_start_underflows() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "oldest".to_string())]
            .into_iter()
            .collect();
        let parts = Arc::new(PartRegistry::new());
        parts
            .register(
                part::flush_rows(
                    vec![Row {
                        timestamp_ns: i64::MIN,
                        labels: labels.clone(),
                        line: "oldest".to_string(),
                        structured_metadata: vec![],
                    }],
                    &data_dir.join("parts"),
                    1,
                )
                .unwrap(),
            )
            .unwrap();
        let state = test_state(&data_dir, Arc::new(MemTable::new()), parts, None);
        let logql::QueryExpr::Metric(expr) = logql::parse_expr("count_over_time({}[5ns])").unwrap()
        else {
            panic!("expected metric")
        };

        let result = run_metric_query(state, expr, vec![i64::MIN + 1])
            .await
            .unwrap();
        assert_eq!(result[0].labels, labels);
        assert_eq!(result[0].samples, vec![(i64::MIN + 1, 1.0)]);
    }

    #[tokio::test]
    async fn metric_query_ignores_log_limit_and_returns_matrix_or_vector() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels,
            (1..=3)
                .map(|second| LogEntry {
                    timestamp_ns: second * 1_000_000_000,
                    line: "xx".to_string(),
                    structured_metadata: vec![],
                })
                .collect(),
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let range = query_range(
            State(state.clone()),
            Query(QueryRangeParams {
                query: "bytes_over_time({}[5s])".to_string(),
                start: Some("5".to_string()),
                end: Some("5".to_string()),
                limit: Some(1),
                direction: None,
                step: Some("1".to_string()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(range.data.result_type, "matrix");
        assert_eq!(range.data.result[0]["values"][0][1], "6");

        let instant = query(
            State(state),
            Query(QueryParams {
                query: "count_over_time({}[5s])".to_string(),
                time: Some("5".to_string()),
                limit: Some(1),
                direction: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(instant.data.result_type, "vector");
        assert_eq!(instant.data.result[0]["value"][1], "3");
    }

    #[tokio::test]
    async fn metric_grouping_uses_extracted_fields_and_parser_errors() {
        let data_dir = temp_dir();
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let memtable = Arc::new(MemTable::new());
        memtable.insert(
            labels,
            vec![
                LogEntry {
                    timestamp_ns: 6_000_000_000,
                    line: r#"{"level":"info","app":"line"}"#.to_string(),
                    structured_metadata: vec![],
                },
                LogEntry {
                    timestamp_ns: 7_000_000_000,
                    line: r#"{"level":"error"}"#.to_string(),
                    structured_metadata: vec![],
                },
                LogEntry {
                    timestamp_ns: 8_000_000_000,
                    line: "not json".to_string(),
                    structured_metadata: vec![],
                },
            ],
        );
        let state = test_state(&data_dir, memtable, Arc::new(PartRegistry::new()), None);

        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"sum(count_over_time({app="api"} | json [5s])) by (level)"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        let result = run_metric_query(state.clone(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        let levels: Vec<_> = result
            .iter()
            .filter_map(|series| series.labels.get("level"))
            .cloned()
            .collect();
        assert_eq!(levels, vec!["error".to_string(), "info".to_string()]);

        let logql::QueryExpr::Metric(expr) = logql::parse_expr(
            r#"sum by (app_extracted) (count_over_time({app="api"} | json | app="api" [5s]))"#,
        )
        .unwrap() else {
            panic!("expected metric")
        };
        let result = run_metric_query(state.clone(), expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert!(
            result
                .iter()
                .any(|series| { series.labels.get("app_extracted") == Some(&"line".to_string()) })
        );

        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"sum by (__error__) (count_over_time({app="api"} | json [5s]))"#)
                .unwrap()
        else {
            panic!("expected metric")
        };
        let result = run_metric_query(state, expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert!(result.iter().any(|series| {
            series.labels.get(logql::PARSER_ERROR_FIELD) == Some(&"JSONParserErr".to_string())
                && series.samples == vec![(10_000_000_000, 1.0)]
        }));
    }

    #[tokio::test]
    async fn metric_lookback_restores_only_exact_field_candidates() {
        let data_dir = temp_dir();
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let wanted = part::flush_rows(
            vec![Row {
                timestamp_ns: 6_000_000_000,
                labels: labels.clone(),
                line: "wanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "one".to_string())],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        let unwanted = part::flush_rows(
            vec![Row {
                timestamp_ns: 7_000_000_000,
                labels,
                line: "unwanted".to_string(),
                structured_metadata: vec![("tenant".to_string(), "two".to_string())],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        let unwanted_path = unwanted[0].data_path();
        let mut published = wanted.clone();
        published.extend(unwanted.clone());
        storage.publish(&published, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_ids())
            .unwrap();
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        );
        let logql::QueryExpr::Metric(expr) =
            logql::parse_expr(r#"count_over_time({} | tenant="one" [5s])"#).unwrap()
        else {
            panic!("expected metric")
        };

        let result = run_metric_query(state, expr, vec![10_000_000_000])
            .await
            .unwrap();
        assert_eq!(result[0].samples, vec![(10_000_000_000, 1.0)]);
        assert!(
            wanted[0].data_path().exists(),
            "lookback part was not restored"
        );
        assert!(
            !unwanted_path.exists(),
            "exact field pruning should happen before remote restoration"
        );
    }

    #[tokio::test]
    async fn synthesized_extracted_field_restores_an_evicted_part_conservatively() {
        let data_dir = temp_dir();
        let parts_root = data_dir.join("parts");
        let storage = Arc::new(crate::object_storage::ObjectStorage::in_memory());
        let labels: Labels = [("app".to_string(), "api".to_string())]
            .into_iter()
            .collect();
        let flushed = part::flush_rows(
            vec![Row {
                timestamp_ns: 10,
                labels,
                line: r#"{"foo":"z"}"#.to_string(),
                structured_metadata: vec![
                    ("foo".to_string(), "x".to_string()),
                    ("foo_extracted".to_string(), "y".to_string()),
                ],
            }],
            &parts_root,
            1,
        )
        .unwrap();
        storage.publish(&flushed, &[]).await.unwrap();
        let parts = Arc::new(PartRegistry::load_from_disk(&parts_root).unwrap());
        storage
            .evict_cache(&parts_root, 0, &parts.part_ids())
            .unwrap();
        assert!(!flushed[0].data_path().exists());
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            parts,
            Some(Arc::new(crate::object_storage::RemoteCache::new(
                storage, parts_root,
            ))),
        );
        let parsed = logql::parse(r#"{} | json | foo_extracted_2="z""#).unwrap();

        let result = run_unified_query(state, parsed, 0, 20, 10, true)
            .await
            .unwrap();
        assert_eq!(result[0].entries[0].line, r#"{"foo":"z"}"#);
        assert!(flushed[0].data_path().exists());
    }

    #[tokio::test]
    async fn invalid_metric_step_and_range_are_client_errors() {
        let data_dir = temp_dir();
        let state = test_state(
            &data_dir,
            Arc::new(MemTable::new()),
            Arc::new(PartRegistry::new()),
            None,
        );
        let bad_step = query_range(
            State(state.clone()),
            Query(QueryRangeParams {
                query: "rate({}[5m])".to_string(),
                start: Some("5".to_string()),
                end: Some("10".to_string()),
                limit: None,
                direction: None,
                step: Some("0".to_string()),
            }),
        )
        .await;
        let Err(error) = bad_step else {
            panic!("zero step must fail")
        };
        assert_eq!(error.0, StatusCode::BAD_REQUEST);

        let bad_range = query_range(
            State(state),
            Query(QueryRangeParams {
                query: "rate({}[5m])".to_string(),
                start: Some("10".to_string()),
                end: Some("5".to_string()),
                limit: None,
                direction: None,
                step: Some("0".to_string()),
            }),
        )
        .await;
        let Err(error) = bad_range else {
            panic!("invalid query range must fail")
        };
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn metric_step_accepts_duration_decimal_and_scientific_seconds() {
        assert_eq!(parse_step_ns(Some("250ms")).unwrap(), 250_000_000);
        assert_eq!(parse_step_ns(Some("0.25")).unwrap(), 250_000_000);
        assert_eq!(parse_step_ns(Some("2.5e-1")).unwrap(), 250_000_000);
        assert!(parse_step_ns(Some("NaN")).is_err());
        assert!(parse_step_ns(Some("inf")).is_err());
    }

    #[test]
    fn metric_evaluation_points_are_bounded() {
        assert!(evaluation_times(0, MAX_METRIC_EVALUATION_POINTS as i64, 1).is_err());
        assert!(evaluation_times(0, (MAX_METRIC_EVALUATION_POINTS - 1) as i64, 1).is_ok());
    }
}
