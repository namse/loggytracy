use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, RawQuery, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;

use crate::logql;
use crate::memtable::StreamResult;
use crate::AppState;

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
    if s.bytes().all(|b| b.is_ascii_digit()) {
        let n: i64 = s
            .parse()
            .map_err(|e| format!("invalid timestamp '{}': {}", s, e))?;
        if s.len() >= 19 {
            Ok(n)
        } else {
            Ok(n.saturating_mul(1_000_000_000))
        }
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

    let results = state
        .memtable
        .query(&parsed.matchers, &parsed.line_filters, start_ns, end_ns, limit, forward);

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

    let results = state
        .memtable
        .query(&parsed.matchers, &parsed.line_filters, 0, end_ns, limit, forward);

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

pub async fn labels(
    State(state): State<Arc<AppState>>,
) -> Json<LokiResponse<Vec<String>>> {
    let names = state.memtable.label_names();
    Json(LokiResponse {
        status: "success",
        data: names,
    })
}

pub async fn label_values(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Json<LokiResponse<Vec<String>>> {
    let values = state.memtable.label_values(&name);
    Json(LokiResponse {
        status: "success",
        data: values,
    })
}

pub async fn ready() -> &'static str {
    "ready"
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
) -> Json<serde_json::Value> {
    let stats = state.memtable.stats();
    Json(serde_json::json!({
        "status": "success",
        "data": {
            "streams": stats.streams,
            "entries": stats.entries,
            "bytes": stats.bytes
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
    let mut all_series: Vec<crate::memtable::Labels> = Vec::new();
    for matcher_str in &matchers {
        let parsed = logql::parse(matcher_str)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("LogQL parse error: {}", e)))?;
        all_series.extend(state.memtable.series(&parsed.matchers));
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

    #[test]
    fn extracts_repeated_match_params() {
        let raw = Some("match%5B%5D=%7Bapp%3D%22a%22%7D&match%5B%5D=%7Bapp%3D%22b%22%7D".to_string());
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
        assert_eq!(parse_time_ns("1700000000").unwrap(), 1_700_000_000_000_000_000);
    }

    #[test]
    fn parse_time_ns_unix_nanos() {
        assert_eq!(
            parse_time_ns("1700000000000000000").unwrap(),
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
}
