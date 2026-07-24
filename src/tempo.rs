use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::AppState;
use crate::trace::{TraceSpan, canonical_trace_id};
use crate::trace_registry::TraceRegistry;

const MAX_TRACE_SPANS: usize = 100_000;
const MAX_TRACE_SEARCH_LIMIT: usize = 1_000;
const MAX_CONCURRENT_TRACE_SCANS: usize = 8;
const MAX_TRACE_QUERY_RUNTIME: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_TRACE_RESTORE_RUNTIME: std::time::Duration = std::time::Duration::from_secs(25);

fn trace_scan_semaphore() -> Arc<Semaphore> {
    static SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_TRACE_SCANS)))
        .clone()
}

async fn scan_trace_spans(
    guard: tokio::sync::OwnedRwLockReadGuard<()>,
    journal: Arc<crate::journal::Journal>,
    trace_parts: Arc<TraceRegistry>,
    trace_id: Option<String>,
) -> Result<Vec<TraceSpan>, (StatusCode, String)> {
    let scan_permit = tokio::time::timeout(
        MAX_TRACE_QUERY_RUNTIME,
        trace_scan_semaphore().acquire_owned(),
    )
    .await
    .map_err(|_| {
        (
            StatusCode::GATEWAY_TIMEOUT,
            "trace query timed out".to_string(),
        )
    })?
    .map_err(|error| internal_error(format!("trace scan scheduler is closed: {error}")))?;
    let cancellation = Arc::new(AtomicBool::new(false));
    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _scan_permit = scan_permit;
        let _guard = guard;
        let mut spans = match trace_id.as_deref() {
            Some(trace_id) => journal
                .trace_memtable()
                .query_trace_id_limited(trace_id, MAX_TRACE_SPANS)?,
            None => journal.trace_memtable().snapshot_limited(MAX_TRACE_SPANS)?,
        };
        let remaining = MAX_TRACE_SPANS.saturating_sub(spans.len());
        let part_spans = match trace_id.as_deref() {
            Some(trace_id) => {
                trace_parts.query_trace_id(trace_id, Some(remaining), Some(&task_cancellation))?
            }
            None => trace_parts.query_all(Some(remaining), Some(&task_cancellation))?,
        };
        spans.extend(part_spans);
        Ok::<_, String>(spans)
    });

    match tokio::time::timeout(MAX_TRACE_QUERY_RUNTIME, &mut task).await {
        Ok(Ok(Ok(spans))) => Ok(spans),
        Ok(Ok(Err(error))) => Err(trace_scan_error(error)),
        Ok(Err(error)) => Err(internal_error(format!("trace query task failed: {error}"))),
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            Err((
                StatusCode::GATEWAY_TIMEOUT,
                "trace query timed out".to_string(),
            ))
        }
    }
}

fn trace_scan_error(error: String) -> (StatusCode, String) {
    if error.starts_with("trace query exceeds") {
        (StatusCode::PAYLOAD_TOO_LARGE, error)
    } else {
        internal_error(error)
    }
}

async fn query_trace(
    state: &AppState,
    trace_id: &str,
) -> Result<Vec<TraceSpan>, (StatusCode, String)> {
    let guard = pin_trace_parts(state, trace_id).await?;
    let mut spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        Some(trace_id.to_string()),
    )
    .await?;
    spans.sort_by(|left, right| {
        left.start_time_ns
            .cmp(&right.start_time_ns)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
    if spans.len() > MAX_TRACE_SPANS {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("trace contains more than {MAX_TRACE_SPANS} spans"),
        ));
    }
    Ok(spans)
}

async fn pin_trace_parts(
    state: &AppState,
    trace_id: &str,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, (StatusCode, String)> {
    let operation_lock = state.parts.operation_lock();
    let read_guard = operation_lock.clone().read_owned().await;
    let Some(remote) = &state.remote_cache else {
        return Ok(read_guard);
    };
    let required = state.trace_parts.candidate_part_ids(trace_id);
    let missing = state.trace_parts.missing_data_ids(&required);
    if missing.is_empty() {
        return Ok(read_guard);
    }
    drop(read_guard);
    let write_guard = operation_lock.write_owned().await;
    let required = state.trace_parts.candidate_part_ids(trace_id);
    let missing = state.trace_parts.missing_data_ids(&required);
    if !missing.is_empty() {
        let epoch = remote.remote_operation_epoch();
        match tokio::time::timeout(
            MAX_TRACE_RESTORE_RUNTIME,
            remote
                .storage
                .restore_trace_parts(&remote.trace_parts_root(), &missing),
        )
        .await
        {
            Ok(Ok(())) => remote.mark_remote_healthy_since(epoch),
            Ok(Err(error)) => {
                remote.mark_remote_unhealthy();
                return Err(internal_error(error));
            }
            Err(_) => {
                remote.mark_remote_unhealthy();
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    "trace object store restore timed out".to_string(),
                ));
            }
        }
    }
    Ok(tokio::sync::OwnedRwLockWriteGuard::downgrade(write_guard))
}

async fn pin_all_trace_parts(
    state: &AppState,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, (StatusCode, String)> {
    let operation_lock = state.parts.operation_lock();
    let read_guard = operation_lock.clone().read_owned().await;
    let Some(remote) = &state.remote_cache else {
        return Ok(read_guard);
    };
    let required = state.trace_parts.part_ids();
    let missing = state.trace_parts.missing_data_ids(&required);
    if missing.is_empty() {
        return Ok(read_guard);
    }
    drop(read_guard);
    let write_guard = operation_lock.write_owned().await;
    let required = state.trace_parts.part_ids();
    let missing = state.trace_parts.missing_data_ids(&required);
    if !missing.is_empty() {
        let epoch = remote.remote_operation_epoch();
        match tokio::time::timeout(
            MAX_TRACE_RESTORE_RUNTIME,
            remote
                .storage
                .restore_trace_parts(&remote.trace_parts_root(), &missing),
        )
        .await
        {
            Ok(Ok(())) => remote.mark_remote_healthy_since(epoch),
            Ok(Err(error)) => {
                remote.mark_remote_unhealthy();
                return Err(internal_error(error));
            }
            Err(_) => {
                remote.mark_remote_unhealthy();
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    "trace object store restore timed out".to_string(),
                ));
            }
        }
    }
    Ok(tokio::sync::OwnedRwLockWriteGuard::downgrade(write_guard))
}

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
    if limit == 0 || limit > MAX_TRACE_SEARCH_LIMIT {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("limit must be between 1 and {MAX_TRACE_SEARCH_LIMIT}"),
        ));
    }
    let start = params
        .start
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or(i64::MIN);
    let end = params
        .end
        .as_deref()
        .map(crate::query::parse_time_ns)
        .transpose()
        .map_err(client_error)?
        .unwrap_or(i64::MAX);
    if start > end {
        return Err((
            StatusCode::BAD_REQUEST,
            "search start must not be after end".to_string(),
        ));
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

    let guard = pin_all_trace_parts(&state).await?;
    let spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        None,
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
    )
    .await?;
    let values: BTreeSet<String> = spans
        .iter()
        .filter_map(|span| span.tag_value(&tag))
        .collect();
    Ok(Json(serde_json::json!({ "tag": tag, "values": values })))
}

fn tempo_trace_response(spans: Vec<TraceSpan>) -> serde_json::Value {
    let mut batches: BTreeMap<
        String,
        (serde_json::Value, serde_json::Value, Vec<serde_json::Value>),
    > = BTreeMap::new();
    for span in spans {
        let resource = span
            .resource
            .as_ref()
            .and_then(|resource| serde_json::to_value(resource).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let scope = span
            .scope
            .as_ref()
            .and_then(|scope| serde_json::to_value(scope).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let key = serde_json::to_string(&(resource.clone(), scope.clone())).unwrap_or_default();
        let entry = batches
            .entry(key)
            .or_insert_with(|| (resource, scope, Vec::new()));
        entry
            .2
            .push(serde_json::to_value(span.span).unwrap_or_else(|_| serde_json::json!({})));
    }
    serde_json::json!({
        "batches": batches
            .into_values()
            .map(|(resource, scope, spans)| serde_json::json!({
                "resource": resource,
                "instrumentationLibrarySpans": [{
                    "instrumentationLibrary": scope,
                    "spans": spans,
                }],
            }))
            .collect::<Vec<_>>(),
    })
}

fn collect_tags(spans: &[TraceSpan]) -> BTreeSet<String> {
    let mut tags = BTreeSet::new();
    for span in spans {
        tags.insert("name".to_string());
        if span.service_name().is_some() {
            tags.insert("service.name".to_string());
        }
        for attribute in &span.span.attributes {
            tags.insert(attribute.key.clone());
        }
        if let Some(resource) = &span.resource {
            for attribute in &resource.attributes {
                tags.insert(attribute.key.clone());
            }
        }
    }
    tags
}

fn collect_scoped_tags(spans: &[TraceSpan]) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut resource_tags = BTreeSet::new();
    let mut span_tags = BTreeSet::new();
    for span in spans {
        span_tags.insert("name".to_string());
        for attribute in &span.span.attributes {
            span_tags.insert(attribute.key.clone());
        }
        if let Some(resource) = &span.resource {
            for attribute in &resource.attributes {
                resource_tags.insert(attribute.key.clone());
            }
        }
    }
    (resource_tags, span_tags)
}

fn parse_tags(raw: Option<&str>) -> Result<Vec<(String, String)>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let raw = raw.trim().trim_start_matches('{').trim_end_matches('}');
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    split_tag_tokens(raw)?
        .into_iter()
        .map(|token| {
            let (name, value) = token
                .split_once('=')
                .ok_or_else(|| format!("invalid tag filter {token:?}"))?;
            let name = name.trim().to_string();
            let value = value.trim().trim_matches('"').to_string();
            if name.is_empty() {
                return Err("tag name must not be empty".to_string());
            }
            Ok((name, value))
        })
        .collect()
}

fn split_tag_tokens(raw: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for character in raw.chars() {
        if character == '"' && !escaped {
            quoted = !quoted;
        }
        if (character == ',' || character.is_whitespace()) && !quoted {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
        } else {
            token.push(character);
        }
        escaped = character == '\\' && !escaped;
        if character != '\\' {
            escaped = false;
        }
    }
    if quoted {
        return Err("unterminated quoted tag value".to_string());
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    Ok(tokens)
}

fn tag_matches(span: &TraceSpan, name: &str, expected: &str) -> bool {
    if name == "duration" {
        if span.tag_value(name).as_deref() == Some(expected) {
            return true;
        }
        return parse_duration_ns(expected)
            .ok()
            .and_then(|duration| u64::try_from(duration).ok())
            .is_some_and(|duration| span.duration_ns() == duration);
    }
    span.tag_value(name).is_some_and(|actual| {
        actual == expected || (name == "status" && actual.eq_ignore_ascii_case(expected))
    })
}

fn parse_duration_ns(value: &str) -> Result<i64, String> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<f64>()
        && seconds.is_finite()
        && seconds >= 0.0
    {
        return (seconds * 1_000_000_000.0)
            .round()
            .to_string()
            .parse::<i64>()
            .map_err(|_| format!("duration {value:?} is out of range"));
    }
    crate::logql::parse_duration_ns(value)
}

fn client_error(error: String) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error)
}

fn internal_error(error: String) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::journal::Journal;
    use crate::memtable::MemTable;
    use crate::part_registry::PartRegistry;
    use crate::trace::{TraceMemTable, normalize_request};
    use crate::trace_registry::TraceRegistry;
    use axum::extract::Path;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn test_state() -> Arc<AppState> {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-tempo-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            Journal::spawn_with_traces(&config, Arc::new(MemTable::new()), trace_memtable.clone())
                .unwrap(),
        );
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(TraceRegistry::new(parts.operation_lock()));
        trace_memtable.insert(
            normalize_request(ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: None,
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![2; 8],
                            start_time_unix_nano: 100,
                            end_time_unix_nano: 250,
                            name: "GET_items".to_string(),
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .unwrap(),
        );
        Arc::new(AppState {
            memtable: Arc::new(MemTable::new()),
            journal,
            parts,
            trace_parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            otlp_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: None,
        })
    }

    #[tokio::test]
    async fn trace_by_id_returns_tempo_batches() {
        let response = trace_by_id(State(test_state()), Path("01".repeat(16)))
            .await
            .unwrap()
            .0;
        assert!(
            response
                .get("batches")
                .and_then(|value| value.as_array())
                .is_some()
        );
        assert_eq!(
            response["batches"][0]["instrumentationLibrarySpans"][0]["spans"][0]["name"],
            "GET_items"
        );
    }

    #[tokio::test]
    async fn trace_search_returns_trace_summary_and_rejects_bad_ids() {
        let response = search(
            State(test_state()),
            Query(SearchParams {
                tags: Some("name=GET_items".to_string()),
                start: None,
                end: None,
                limit: Some(10),
                min_duration: None,
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(response["traces"].as_array().unwrap().len(), 1);

        let error = trace_by_id(State(test_state()), Path("bad".to_string()))
            .await
            .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn trace_search_filters_on_child_but_summarizes_the_full_trace() {
        let state = test_state();
        state.journal.trace_memtable().insert(
            normalize_request(ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: None,
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![1; 16],
                            span_id: vec![3; 8],
                            parent_span_id: vec![2; 8],
                            start_time_unix_nano: 250,
                            end_time_unix_nano: 650,
                            name: "child".to_string(),
                            attributes: vec![KeyValue {
                                key: "http.route".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(
                                        "/items".to_string(),
                                    )),
                                }),
                                key_strindex: 0,
                            }],
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            })
            .unwrap(),
        );

        let response = search(
            State(state),
            Query(SearchParams {
                tags: Some("http.route=/items".to_string()),
                start: None,
                end: None,
                limit: Some(10),
                min_duration: Some("500ns".to_string()),
                max_duration: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let trace = &response["traces"][0];

        assert_eq!(trace["rootTraceName"], "GET_items");
        assert_eq!(trace["startTimeUnixNano"], "100");
        assert!((trace["durationMs"].as_f64().unwrap() - 0.00055).abs() < f64::EPSILON);
    }
}
