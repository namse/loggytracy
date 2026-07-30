//! What gets sent: push bodies built from `loggytracy::corpus`, and the query
//! shapes that read them back.
//!
//! The corpus is the library's, not a second one written here. The harness it
//! replaces had one hardcoded label set and lines padded with `"x".repeat(n)`
//! (`bin/load.rs:680,693` before the rewrite), which compressed 31.5x where
//! realistic lines compress about 5.9x, and left the stream index, label
//! matching and row-group selection untouched by every run.

use loggytracy::corpus::{Corpus, LEVELS, PHRASES, Rng, STATUSES};
use loggytracy::memtable::LogEntry;
use loggytracy::proto;

/// One push, ready for the wire.
pub struct PushBody {
    pub bytes: Vec<u8>,
    pub tenant: String,
    pub entries: usize,
    pub line_bytes: usize,
    pub encoded_bytes: usize,
    pub streams: usize,
    pub out_of_order_entries: usize,
    pub max_lateness_ms: u64,
}

pub struct ArrivalOrder {
    /// Milliseconds of backward jitter applied to every entry, which is what
    /// makes a batch unsorted on arrival.
    pub spread_ms: u64,
    /// Fraction of entries that arrive materially late, on top of the spread.
    pub late_fraction: f64,
    pub late_max_ms: u64,
}

pub struct PushGenerator {
    corpus: std::sync::Arc<Corpus>,
    rng: Rng,
    streams_by_tenant: Vec<Vec<usize>>,
    cursors: Vec<usize>,
    entries_per_push: usize,
    streams_per_push: usize,
    arrival: ArrivalOrder,
}

impl PushGenerator {
    pub fn new(
        corpus: std::sync::Arc<Corpus>,
        seed: u64,
        entries_per_push: usize,
        streams_per_push: usize,
        arrival: ArrivalOrder,
    ) -> Self {
        let mut streams_by_tenant = vec![Vec::new(); corpus.tenant_ids.len().max(1)];
        for (index, stream) in corpus.streams.iter().enumerate() {
            let tenant = corpus
                .tenant_ids
                .iter()
                .position(|id| id == &stream.tenant)
                .unwrap_or(0);
            streams_by_tenant[tenant].push(index);
        }
        let cursors = vec![0usize; corpus.streams.len()];
        Self {
            corpus,
            rng: Rng::new(seed ^ 0x9e37_79b9),
            streams_by_tenant,
            cursors,
            entries_per_push,
            streams_per_push,
            arrival,
        }
    }

    /// A push carries several streams of one tenant, because that is the shape
    /// an agent sends and because the tenant is a request header: one push
    /// cannot span two of them.
    pub fn next_body(&mut self, now_ns: i64) -> PushBody {
        let corpus = self.corpus.clone();
        let tenant_index = self.rng.below(self.streams_by_tenant.len());
        let candidate_count = self.streams_by_tenant[tenant_index].len();
        let stream_count = self.streams_per_push.min(candidate_count).max(1);
        let first = self.rng.below(candidate_count);
        let chosen: Vec<usize> = (0..stream_count)
            .map(|position| {
                self.streams_by_tenant[tenant_index][(first + position) % candidate_count]
            })
            .collect();

        let mut out_of_order_entries = 0usize;
        let mut max_lateness_ms = 0u64;
        let mut line_bytes = 0usize;
        let mut batch: Vec<(loggytracy::memtable::Labels, Vec<LogEntry>)> =
            Vec::with_capacity(stream_count);

        for (position, stream_index) in chosen.into_iter().enumerate() {
            let share = self.entries_per_push / stream_count
                + usize::from(position < self.entries_per_push % stream_count);
            if share == 0 {
                continue;
            }
            let stream = &corpus.streams[stream_index];
            if stream.entries.is_empty() {
                continue;
            }
            let mut entries = Vec::with_capacity(share);
            let mut previous_ts = i64::MIN;
            for _ in 0..share {
                let cursor = self.cursors[stream_index] % stream.entries.len();
                self.cursors[stream_index] = cursor + 1;
                let source = &stream.entries[cursor];
                let lateness_ms = self.lateness_ms();
                max_lateness_ms = max_lateness_ms.max(lateness_ms);
                let timestamp_ns = now_ns - (lateness_ms as i64) * 1_000_000;
                if timestamp_ns < previous_ts {
                    out_of_order_entries += 1;
                }
                previous_ts = timestamp_ns;
                line_bytes += source.line.len();
                entries.push(LogEntry {
                    timestamp_ns,
                    line: source.line.clone(),
                    structured_metadata: source.structured_metadata.clone(),
                });
            }
            batch.push(((*stream.labels).clone(), entries));
        }

        // The library's own encoder, so the bytes on the wire are the bytes
        // the WAL stores rather than a second implementation of the same
        // message.
        let encoded = proto::encode_push_request(&batch);
        let bytes = snap::raw::Encoder::new()
            .compress_vec(&encoded)
            .expect("snappy encoding must work");
        PushBody {
            tenant: corpus.tenant_ids[tenant_index].as_str().to_string(),
            entries: batch.iter().map(|(_, entries)| entries.len()).sum(),
            streams: batch.len(),
            line_bytes,
            encoded_bytes: encoded.len(),
            out_of_order_entries,
            max_lateness_ms,
            bytes,
        }
    }

    fn lateness_ms(&mut self) -> u64 {
        let mut lateness = self.rng.below(self.arrival.spread_ms as usize + 1) as u64;
        if self.arrival.late_fraction > 0.0 && self.rng.unit() < self.arrival.late_fraction {
            lateness += self.rng.below(self.arrival.late_max_ms as usize + 1) as u64;
        }
        lateness
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueryShape {
    /// Label matchers only — what a stream browser sends.
    LabelOnly,
    /// `|=` substring, which trigram blooms can prune.
    LineFilter,
    /// `| json | field="value"`, the shape `docs/VISION.md` stakes the claim
    /// on and the shape whose scan limit is `usize::MAX` today.
    JsonField,
    /// A range aggregation, which reads one column and throws the rest away.
    Rate,
    /// An old window, so a part evicted from the local cache has to be
    /// restored from the object store on the measured path.
    RestoreProbe,
}

pub const QUERY_SHAPES: [QueryShape; 5] = [
    QueryShape::LabelOnly,
    QueryShape::LineFilter,
    QueryShape::JsonField,
    QueryShape::Rate,
    QueryShape::RestoreProbe,
];

impl QueryShape {
    pub fn name(self) -> &'static str {
        match self {
            QueryShape::LabelOnly => "label_only",
            QueryShape::LineFilter => "line_filter",
            QueryShape::JsonField => "json_field",
            QueryShape::Rate => "rate",
            QueryShape::RestoreProbe => "restore_probe",
        }
    }
}

pub struct QueryPlan {
    pub shape: QueryShape,
    pub path: String,
    pub tenant: String,
    pub expression: String,
}

pub struct QueryGenerator {
    corpus: std::sync::Arc<Corpus>,
    rng: Rng,
    weights: [u32; 5],
    window_seconds: i64,
    restore_lookback_seconds: i64,
    limit: usize,
}

impl QueryGenerator {
    pub fn new(
        corpus: std::sync::Arc<Corpus>,
        seed: u64,
        weights: [u32; 5],
        window_seconds: i64,
        restore_lookback_seconds: i64,
        limit: usize,
    ) -> Self {
        Self {
            corpus,
            rng: Rng::new(seed ^ 0x2545_f491),
            weights,
            window_seconds,
            restore_lookback_seconds,
            limit,
        }
    }

    pub fn next_plan(&mut self, now_seconds: i64) -> QueryPlan {
        let shape = self.pick_shape();
        let stream = &self.corpus.streams[self.rng.below(self.corpus.streams.len())];
        let tenant = stream.tenant.as_str().to_string();
        // The selector comes off a stream that exists, so a query that returns
        // nothing means the engine did not find it rather than that the
        // harness asked for something never written.
        let app = stream
            .labels
            .get("app")
            .cloned()
            .unwrap_or_else(|| "api-gateway".to_string());
        let selector = format!("{{app=\"{app}\"}}");
        let expression = match shape {
            QueryShape::LabelOnly | QueryShape::RestoreProbe => selector,
            QueryShape::LineFilter => {
                let phrase = PHRASES[self.rng.below(PHRASES.len())];
                format!("{selector} |= \"{phrase}\"")
            }
            QueryShape::JsonField => {
                if self.rng.below(2) == 0 {
                    let status = STATUSES[self.rng.below(STATUSES.len())];
                    format!("{selector} | json | status=\"{status}\"")
                } else {
                    let level = LEVELS[self.rng.below(LEVELS.len())];
                    format!("{selector} | json | level=\"{level}\"")
                }
            }
            QueryShape::Rate => format!("rate({selector}[1m])"),
        };

        let (start, end, step, direction) = match shape {
            QueryShape::RestoreProbe => {
                let start = now_seconds - self.restore_lookback_seconds;
                (start, start + self.window_seconds, 60, "forward")
            }
            _ => (
                now_seconds - self.window_seconds,
                now_seconds,
                10,
                "backward",
            ),
        };
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", &expression)
            .append_pair("start", &start.to_string())
            .append_pair("end", &end.to_string())
            .append_pair("step", &step.to_string())
            .append_pair("limit", &self.limit.to_string())
            .append_pair("direction", direction)
            .finish();
        QueryPlan {
            shape,
            path: format!("/loki/api/v1/query_range?{query}"),
            tenant,
            expression,
        }
    }

    fn pick_shape(&mut self) -> QueryShape {
        let total: u32 = self.weights.iter().sum();
        if total == 0 {
            return QueryShape::LabelOnly;
        }
        let mut roll = (self.rng.next_u64() % total as u64) as u32;
        for (index, weight) in self.weights.iter().enumerate() {
            if roll < *weight {
                return QUERY_SHAPES[index];
            }
            roll -= weight;
        }
        QueryShape::LabelOnly
    }
}

/// Log lines a Loki `query_range` response returned, across every stream.
///
/// Deliberately tolerant: a body this cannot parse counts as zero rows rather
/// than failing the query, because the gate is the status code and this number
/// describes what came back. A restore probe that answers `200` over an empty
/// window is not the same event as one that answered from a restored part, and
/// only the row count tells them apart.
pub fn loki_result_rows(body: &[u8]) -> u64 {
    let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(body) else {
        return 0;
    };
    let result = &parsed["data"]["result"];
    let Some(entries) = result.as_array() else {
        return 0;
    };
    entries
        .iter()
        .map(|entry| entry["values"].as_array().map_or(0, Vec::len) as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use loggytracy::corpus::{self, CorpusSpec};
    use std::sync::Arc;

    fn corpus() -> Arc<Corpus> {
        Arc::new(corpus::generate(
            &CorpusSpec::default()
                .rows(2_000)
                .streams(16)
                .tenants(4)
                .tenant_prefix("load-tenant"),
        ))
    }

    #[test]
    fn counts_the_lines_a_loki_response_returned() {
        let body = br#"{"status":"success","data":{"resultType":"streams","result":[
            {"stream":{"app":"a"},"values":[["1","x"],["2","y"]]},
            {"stream":{"app":"b"},"values":[["3","z"]]}]}}"#;
        assert_eq!(loki_result_rows(body), 3);
    }

    #[test]
    fn an_empty_result_counts_zero_rows() {
        let body = br#"{"status":"success","data":{"resultType":"streams","result":[]}}"#;
        assert_eq!(loki_result_rows(body), 0);
        assert_eq!(loki_result_rows(b"not json"), 0);
    }

    /// Every stream in one push has to belong to the tenant in the header,
    /// because the header is per request and the server routes on it.
    #[test]
    fn a_push_carries_one_tenants_streams_and_the_requested_entry_count() {
        let corpus = corpus();
        let mut generator = PushGenerator::new(
            corpus.clone(),
            7,
            100,
            4,
            ArrivalOrder {
                spread_ms: 0,
                late_fraction: 0.0,
                late_max_ms: 0,
            },
        );
        for _ in 0..16 {
            let body = generator.next_body(1_772_000_000_000_000_000);
            assert_eq!(body.entries, 100);
            assert!(body.streams > 1, "a push should carry several streams");
            assert!(
                corpus
                    .tenant_ids
                    .iter()
                    .any(|id| id.as_str() == body.tenant)
            );
            assert!(!body.bytes.is_empty());
        }
    }

    #[test]
    fn arrival_order_knobs_produce_late_and_unsorted_entries() {
        let mut ordered = PushGenerator::new(
            corpus(),
            7,
            64,
            2,
            ArrivalOrder {
                spread_ms: 0,
                late_fraction: 0.0,
                late_max_ms: 0,
            },
        );
        let body = ordered.next_body(1_772_000_000_000_000_000);
        assert_eq!(body.out_of_order_entries, 0);
        assert_eq!(body.max_lateness_ms, 0);

        let mut jittered = PushGenerator::new(
            corpus(),
            7,
            64,
            2,
            ArrivalOrder {
                spread_ms: 500,
                late_fraction: 0.25,
                late_max_ms: 30_000,
            },
        );
        let mut out_of_order = 0;
        let mut lateness = 0;
        for _ in 0..8 {
            let body = jittered.next_body(1_772_000_000_000_000_000);
            out_of_order += body.out_of_order_entries;
            lateness = lateness.max(body.max_lateness_ms);
        }
        assert!(out_of_order > 0, "spread must break the arrival order");
        assert!(lateness > 500, "a late fraction must produce late entries");
    }

    #[test]
    fn every_query_shape_names_a_stream_that_exists() {
        let corpus = corpus();
        let mut generator = QueryGenerator::new(corpus.clone(), 11, [1, 1, 1, 1, 1], 300, 600, 100);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..400 {
            let plan = generator.next_plan(1_772_000_000);
            seen.insert(plan.shape.name());
            assert!(plan.path.starts_with("/loki/api/v1/query_range?query="));
            assert!(
                corpus
                    .tenant_ids
                    .iter()
                    .any(|id| id.as_str() == plan.tenant)
            );
            let app = plan
                .expression
                .split_once("app=\"")
                .and_then(|(_, rest)| rest.split_once('"'))
                .map(|(value, _)| value.to_string())
                .expect("every shape selects on app");
            assert!(
                corpus
                    .streams
                    .iter()
                    .any(|stream| stream.labels.get("app") == Some(&app))
            );
        }
        assert_eq!(seen.len(), QUERY_SHAPES.len(), "every shape must be drawn");
    }
}
