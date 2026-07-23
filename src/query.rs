use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use serde::Serialize;

use crate::AppState;
use crate::logql::{self};
use crate::memtable::{Labels, LogEntry, StreamResult};
use crate::part;

#[derive(Serialize)]
pub struct LokiResponse<T: Serialize> {
    pub status: &'static str,
    pub data: T,
}

#[derive(Serialize)]
pub struct QueryRangeData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<StreamData>,
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
}

#[derive(serde::Deserialize)]
pub struct QueryParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
}

fn parse_time_ns(s: &str) -> Result<i64, String> {
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

fn is_forward(direction: &Option<String>) -> bool {
    direction
        .as_deref()
        .map(|d| d.eq_ignore_ascii_case("forward"))
        .unwrap_or(false)
}

fn distinct_stream_count(state: &AppState) -> usize {
    let mut streams = std::collections::BTreeSet::new();
    streams.extend(state.memtable.series(&[]));
    streams.extend(state.parts.series(&[]));
    streams.len()
}

fn unified_query(
    state: &AppState,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    let mut all: Vec<(Labels, LogEntry)> = Vec::new();

    for sr in state.memtable.query(
        &parsed.matchers,
        &parsed.line_filters,
        start_ns,
        end_ns,
        limit,
        forward,
    ) {
        for e in sr.entries {
            all.push((sr.labels.clone(), e));
        }
    }

    for sr in state.parts.query(
        &parsed.matchers,
        &parsed.line_filters,
        start_ns,
        end_ns,
        limit,
        forward,
    )? {
        for e in sr.entries {
            all.push((sr.labels.clone(), e));
        }
    }

    if forward {
        all.sort_by_key(|e| e.1.timestamp_ns);
    } else {
        all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
    }
    all.truncate(limit);

    Ok(part::group_by_labels(all))
}

async fn run_unified_query(
    state: Arc<AppState>,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    let part_guard = pin_query_parts(&state, &parsed, start_ns, end_ns).await?;
    tokio::task::spawn_blocking(move || {
        let _part_guard = part_guard;
        unified_query(&state, &parsed, start_ns, end_ns, limit, forward)
    })
    .await
    .map_err(|error| format!("query task failed: {error}"))?
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
    let required = state
        .parts
        .candidate_part_ids(&parsed.matchers, start_ns, end_ns);
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
    let required = state
        .parts
        .candidate_part_ids(&parsed.matchers, start_ns, end_ns);
    let missing = state.parts.missing_data_ids(&required);
    if !missing.is_empty() {
        remote
            .storage
            .restore_parts(&remote.parts_root, &missing)
            .await?;
    }
    Ok(tokio::sync::OwnedRwLockWriteGuard::downgrade(write_guard))
}

pub async fn query_range(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
    let parsed = logql::parse(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let start_ns = match params.start.as_deref() {
        Some(s) => parse_time_ns(s).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => 0,
    };
    let end_ns = match params.end.as_deref() {
        Some(s) => parse_time_ns(s).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => now_ns,
    };

    let limit = params.limit.unwrap_or(100);
    let forward = is_forward(&params.direction);

    let results = run_unified_query(state, parsed, start_ns, end_ns, limit, forward)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let stream_data = build_stream_data(results);

    Ok(Json(LokiResponse {
        status: "success",
        data: QueryRangeData {
            result_type: "streams",
            result: stream_data,
            stats: Stats {
                summary: StatsSummary {
                    total_lines_processed: 0,
                },
            },
        },
    }))
}

pub async fn query(
    State(state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> Result<Json<LokiResponse<QueryRangeData>>, (StatusCode, String)> {
    let parsed = logql::parse(&params.query)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let end_ns = match params.time.as_deref() {
        Some(t) => parse_time_ns(t).map_err(|e| (StatusCode::BAD_REQUEST, e))?,
        None => now_ns,
    };

    let limit = params.limit.unwrap_or(100);
    let forward = is_forward(&params.direction);

    let results = run_unified_query(state, parsed, 0, end_ns, limit, forward)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let stream_data = build_stream_data(results);

    Ok(Json(LokiResponse {
        status: "success",
        data: QueryRangeData {
            result_type: "streams",
            result: stream_data,
            stats: Stats {
                summary: StatsSummary {
                    total_lines_processed: 0,
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

    fn temp_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "loggytracy-query-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
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
        let state = AppState {
            memtable,
            journal,
            parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
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
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            remote_cache: None,
        });

        assert_eq!(ready(State(state.clone())).await.unwrap(), "ready");

        state.flush_healthy.store(false, Ordering::Release);
        state.merge_healthy.store(false, Ordering::Release);
        let error = ready(State(state)).await.unwrap_err();
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(error.1.contains("flush worker"));
        assert!(error.1.contains("merge worker"));
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
        let state = Arc::new(AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts,
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
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
        let state = AppState {
            journal: Arc::new(Journal::spawn(&config, memtable.clone()).unwrap()),
            memtable,
            parts: parts.clone(),
            flush_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            merge_healthy: Arc::new(std::sync::atomic::AtomicBool::new(true)),
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
}
