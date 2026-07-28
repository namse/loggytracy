use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::logql::{self};
use crate::memtable::{Labels, LogEntry, SharedLabels, StreamResult};
use crate::part;
use crate::tenant::TenantId;

const MAX_METRIC_EVALUATION_POINTS: usize = 10_000;
const MAX_METRIC_SERIES: usize = 100_000;
const MAX_METRIC_SAMPLES: usize = 5_000_000;
const MAX_LOG_LIMIT: usize = 100_000;
const MAX_LOG_SCAN_ROWS: usize = 5_000_000;

/// Marks a refusal that belongs to the tenant's own quota rather than to the
/// query. Carried in the message because the scan path reports `String`, and
/// the distinction matters to the client: a 429 is worth retrying later and a
/// 400 never is.
pub(crate) const TENANT_QUOTA_PREFIX: &str = "tenant quota: ";

fn metric_error_status(error: &str) -> StatusCode {
    if error.starts_with(TENANT_QUOTA_PREFIX) {
        StatusCode::TOO_MANY_REQUESTS
    } else if error.starts_with(crate::query_memory::EXHAUSTED_PREFIX) {
        // This instance had no room, which is temporary by construction: the
        // pool refills as the queries holding it finish, and the same request a
        // moment later succeeds. It reached a client as `500` until 2026-08-13
        // — a working limit wearing a fault's code, which pages an operator for
        // a refusal and tells a client library to treat backoff as pointless.
        // The event is not thereby harmless, and it is not meant to disappear
        // into the throttling either: `QueryMemoryPool::exhausted` counts it on
        // its own so "this instance could not serve work it was willing to do"
        // stays visible after the client has been told the right thing.
        StatusCode::TOO_MANY_REQUESTS
    } else if error.starts_with("metric query exceeds") || error.starts_with("query exceeds") {
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
    pub result: ResultPayload,
    pub stats: Stats,
}

/// The log path's result serializes straight from the structs; routing it
/// through a `serde_json::Value` tree first cost a second full pass over
/// megabytes of response — `label_only` returns thousands of rows and the
/// tree was ~40% of its latency. Metric results are small; they keep the
/// `Value` convenience.
#[derive(Serialize)]
#[serde(untagged)]
pub enum ResultPayload {
    Streams(Vec<StreamData>),
    Value(serde_json::Value),
}

impl ResultPayload {
    /// The tree form, for tests that inspect rather than serialize; the
    /// response path never goes through this.
    #[cfg(test)]
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("result payloads serialize")
    }
}

#[derive(Serialize)]
pub struct StreamData {
    pub stream: StreamKey,
    /// `(timestamp, line)`, serialized as the wire's two-string array.
    pub values: Vec<(String, String)>,
}

/// A stream's label set on the wire, never materialized as one map.
///
/// The union of a row's stream labels and its structured metadata used to be
/// built per row — a deep clone of the label map, a `BTreeMap` insert per
/// metadata pair, then a second map for serialization — and grouped under a
/// `BTreeMap` whose key comparison walked the label pairs; with `trace_id`
/// both unique per row and last alphabetically, that walk compared every
/// label of every row against every probe. At thousands of returned rows
/// this was the response's dominant cost. The key now holds the label `Arc`
/// and the row's metadata as they are, and every consumer — equality,
/// hashing, serialization — reads the union through one sorted merge with
/// metadata shadowing same-named labels, which is the semantic the built
/// map had.
#[derive(Debug)]
pub struct StreamKey {
    labels: SharedLabels,
    /// Sorted by key, one entry per key (the row's last duplicate wins),
    /// pairs that merely repeat the label's own value dropped — so two rows
    /// whose unions are equal always compare equal however the union was
    /// split between labels and metadata.
    metadata: Vec<(String, String)>,
}

impl StreamKey {
    fn new(labels: SharedLabels, metadata: Vec<(String, String)>) -> Self {
        let mut normalized: Vec<(String, String)> = Vec::with_capacity(metadata.len());
        for (name, value) in metadata {
            match normalized
                .iter_mut()
                .find(|(existing, _)| *existing == name)
            {
                Some((_, slot)) => *slot = value,
                None => normalized.push((name, value)),
            }
        }
        normalized.sort_by(|a, b| a.0.cmp(&b.0));
        normalized.retain(|(name, value)| labels.get(name) != Some(value));
        Self {
            labels,
            metadata: normalized,
        }
    }

    /// The union, sorted by key, metadata shadowing labels.
    fn merged(&self) -> impl Iterator<Item = (&str, &str)> {
        let mut labels = self.labels.iter().peekable();
        let mut metadata = self.metadata.iter().peekable();
        std::iter::from_fn(move || match (labels.peek(), metadata.peek()) {
            (Some((ln, lv)), Some((mn, mv))) => match ln.as_str().cmp(mn.as_str()) {
                std::cmp::Ordering::Less => {
                    let out = (ln.as_str(), lv.as_str());
                    labels.next();
                    Some(out)
                }
                std::cmp::Ordering::Greater => {
                    let out = (mn.as_str(), mv.as_str());
                    metadata.next();
                    Some(out)
                }
                std::cmp::Ordering::Equal => {
                    labels.next();
                    let out = (mn.as_str(), mv.as_str());
                    metadata.next();
                    Some(out)
                }
            },
            (Some((ln, lv)), None) => {
                let out = (ln.as_str(), lv.as_str());
                labels.next();
                Some(out)
            }
            (None, Some((mn, mv))) => {
                let out = (mn.as_str(), mv.as_str());
                metadata.next();
                Some(out)
            }
            (None, None) => None,
        })
    }

    fn union_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (name, value) in self.merged() {
            name.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn union_eq(&self, other: &StreamKey) -> bool {
        self.merged().eq(other.merged())
    }

    #[cfg(test)]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.merged()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value)
    }

    #[cfg(test)]
    pub fn to_map(&self) -> BTreeMap<String, String> {
        self.merged()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }
}

impl Serialize for StreamKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in self.merged() {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
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

/// The `start`/`end` every Grafana metadata call sends and these endpoints
/// used to ignore.
#[derive(Default, serde::Deserialize)]
pub struct MetadataParams {
    pub start: Option<String>,
    pub end: Option<String>,
    /// Loki's stream-selector filter on `label/{name}/values`. Absent on the
    /// other metadata endpoints, which is why it is optional here rather than
    /// a second parameter type: `labels` and `index_stats` deserialize the
    /// same struct and simply never carry it.
    pub query: Option<String>,
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

/// A log response's rows, grouped the way Loki groups them.
///
/// Everything a row carries — its stream labels, the structured metadata the
/// push sent, and whatever the pipeline extracted — goes into the stream's
/// label set, and each `values` tuple is `[timestamp, line]`. This is measured
/// against `grafana/loki:3.3.2` rather than inferred: for a stream pushed with
/// `trace_id` metadata, `{app="probe"}` answers with `trace_id` among the
/// `stream` labels and a two-element tuple, and `{app="probe"} | json` adds the
/// extracted fields to the same set. The three-element tuple is Loki's
/// *opt-in* shape, requested with `X-Loki-Response-Encoding-Flags:
/// categorize-labels`, and there it is an object of categories
/// (`{"structuredMetadata": {...}, "parsed": {...}}`) rather than a flat map.
/// loggytracy returned the flat map unconditionally, so pushed metadata and
/// extracted fields both landed in a slot Loki's default response does not use
/// (`todo.md`, "Open correctness defects").
///
/// One output stream per distinct label set, which is why the grouping is
/// global rather than per input stream: two input streams whose labels a
/// `label_format` collapsed onto each other are one stream to Loki. The
/// direction has to be passed in because merging two inputs into one group
/// interleaves their rows, and the response order is part of the contract.
fn build_stream_data(results: Vec<StreamResult>, forward: bool) -> Vec<StreamData> {
    // Streams come out in first-occurrence order — the scan's order — rather
    // than sorted by label set as they once were. Nothing reads the stream
    // order: the comparison bed's digest is order-independent and its
    // ordering check is per stream, over the values. What *is* still the
    // contract is the grouping (one output stream per distinct union — two
    // input streams a `label_format` collapsed are one stream to Loki) and
    // each stream's values in query direction, timestamp-sorted, stable.
    let mut streams: Vec<(StreamKey, Vec<(i64, String)>)> = Vec::new();
    let mut index: HashMap<u64, Vec<usize>> = HashMap::new();
    for result in results {
        for entry in result.entries {
            let key = StreamKey::new(result.labels.clone(), entry.structured_metadata);
            let candidates = index.entry(key.union_hash()).or_default();
            match candidates
                .iter()
                .copied()
                .find(|&at| streams[at].0.union_eq(&key))
            {
                Some(at) => streams[at].1.push((entry.timestamp_ns, entry.line)),
                None => {
                    candidates.push(streams.len());
                    streams.push((key, vec![(entry.timestamp_ns, entry.line)]));
                }
            }
        }
    }
    streams
        .into_iter()
        .map(|(stream, mut rows)| {
            // Stable, so rows sharing a timestamp keep the order the scan
            // produced them in rather than being reshuffled by the grouping.
            if forward {
                rows.sort_by_key(|(timestamp_ns, _)| *timestamp_ns);
            } else {
                rows.sort_by_key(|(timestamp_ns, _)| std::cmp::Reverse(*timestamp_ns));
            }
            StreamData {
                stream,
                values: rows
                    .into_iter()
                    .map(|(timestamp_ns, line)| (timestamp_ns.to_string(), line))
                    .collect(),
            }
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
struct MetricSeries {
    labels: SharedLabels,
    samples: Vec<(i64, f64)>,
}

fn metric_series_json(series: Vec<MetricSeries>, instant: bool) -> serde_json::Value {
    serde_json::Value::Array(
        series
            .into_iter()
            .map(|series| {
                let metric: serde_json::Map<String, serde_json::Value> = series
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
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

fn parse_limit(limit: Option<usize>, max_limit: usize) -> Result<usize, String> {
    let limit = limit.unwrap_or(100);
    if limit > max_limit {
        return Err(format!("query limit exceeds the maximum of {max_limit}"));
    }
    Ok(limit)
}

fn distinct_stream_count(
    state: &AppState,
    tenant: &TenantId,
    window: crate::part::MetadataWindow,
) -> usize {
    let mut streams = std::collections::BTreeSet::new();
    streams.extend(state.memtable.series(tenant, &[], window));
    streams.extend(state.parts.series(tenant, &[], window));
    streams.len()
}

pub(crate) fn validate_query_range(
    config: &crate::config::Config,
    start_ns: i64,
    end_ns: i64,
) -> Result<(), String> {
    if start_ns > end_ns {
        return Err("query start must not be after end".to_string());
    }
    let Some(max_range) = config.max_query_range else {
        return Ok(());
    };
    let range_ns = (i128::from(end_ns) - i128::from(start_ns)) as u128;
    if range_ns > max_range.as_nanos() {
        return Err(format!(
            "query range exceeds the maximum of {} seconds",
            max_range.as_secs()
        ));
    }
    Ok(())
}

pub(crate) fn validate_metric_lookback(
    config: &crate::config::Config,
    expr: &logql::MetricExpr,
) -> Result<(), String> {
    if expr.lookback_ns() <= 0 {
        return Err("metric lookback must be greater than zero".to_string());
    }
    if let Some(max_range) = config.max_query_range
        && (expr.lookback_ns() as u128) > max_range.as_nanos()
    {
        return Err(format!(
            "metric lookback exceeds the maximum of {} seconds",
            max_range.as_secs()
        ));
    }
    Ok(())
}

fn estimated_query_memory_bytes(results: &[StreamResult]) -> u64 {
    results
        .iter()
        .flat_map(|stream| {
            stream.entries.iter().map(|entry| {
                let labels = stream
                    .labels
                    .iter()
                    .map(|(name, value)| name.len().saturating_add(value.len()))
                    .sum::<usize>();
                let metadata = entry
                    .structured_metadata
                    .iter()
                    .map(|(name, value)| name.len().saturating_add(value.len()))
                    .sum::<usize>();
                labels
                    .saturating_add(metadata)
                    .saturating_add(entry.line.len())
                    .saturating_add(std::mem::size_of::<LogEntry>())
            })
        })
        .try_fold(0u64, |total, bytes| total.checked_add(bytes as u64))
        .unwrap_or(u64::MAX)
}

include!("execution.rs");
include!("restore.rs");
include!("metrics.rs");
include!("handlers.rs");
include!("loki_extra.rs");
include!("patterns.rs");
include!("delete_api.rs");
include!("tail.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
