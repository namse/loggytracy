//! Every knob the run was given, in one struct that is also what the result
//! file records.
//!
//! A result that cannot be reproduced is a number without a claim attached, so
//! the report carries this whole struct verbatim alongside the build revision,
//! the seed, the machine profile and the server's own environment.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct Targets {
    /// Gated on the **response** percentiles: the service ones describe a rate
    /// that may never have been offered.
    pub push_response_p95_ms: f64,
    pub push_response_p99_ms: f64,
    pub query_response_p95_ms: f64,
    pub rss_max_bytes: u64,
    pub max_error_rate: f64,
    /// A 429 is the engine defending itself, so it is not an error — but a run
    /// that was refused most of what it offered has not measured the rate it
    /// claims to, and must not read as a clean pass.
    pub max_throttled_rate: f64,
    pub wal_backlog_max_bytes: u64,
    pub min_backlog_samples: usize,
}

/// Which system this run is driving.
///
/// The point of the comparison bed is that this is the *only* variable: the
/// corpus, the seed, the offered rate, the queries and the wire format are the
/// same on both sides, because loggytracy's HTTP surface is Loki-compatible by
/// design. What differs is a readiness check that means the same thing in two
/// vocabularies, two sets of `/metrics` names, and which behavioural gates can
/// be asked at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Loggytracy,
    Loki,
}

impl Target {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "loggytracy" => Ok(Target::Loggytracy),
            "loki" => Ok(Target::Loki),
            other => Err(format!(
                "LOGGYTRACY_LOAD_TARGET must be loggytracy or loki, got {other:?}"
            )),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Target::Loggytracy => "loggytracy",
            Target::Loki => "loki",
        }
    }
}

/// What this invocation does.
///
/// `load` is M8's run and is unchanged. `seed` and `matrix` exist because the
/// query comparison has to run on data both systems provably hold: a paced
/// ingest run sends whatever it managed to send, at wall-clock timestamps, so
/// two runs of it produce two different datasets and any row-level comparison
/// between them would be meaningless. `seed` pushes a fixed corpus at fixed
/// timestamps, so both systems end up holding byte-identical entries, and
/// `matrix` then times and compares queries over that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    Load,
    Seed,
    Matrix,
}

impl Phase {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "load" => Ok(Phase::Load),
            "seed" => Ok(Phase::Seed),
            "matrix" => Ok(Phase::Matrix),
            other => Err(format!(
                "LOGGYTRACY_LOAD_PHASE must be load, seed or matrix, got {other:?}"
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Config {
    pub target: Target,
    pub phase: Phase,
    pub http_address: String,
    pub otlp_address: String,
    pub tier: String,
    pub seed: u64,
    pub duration_seconds: u64,
    pub warmup_seconds: u64,
    pub target_events: u64,
    pub request_timeout_seconds: u64,
    pub sample_interval_ms: u64,
    pub server_pid: Option<u32>,
    /// cgroup v2 directory of the server's container, when the server is not a
    /// sibling process. Takes precedence over `server_pid`.
    pub cgroup_path: Option<String>,
    pub result_path: Option<String>,

    pub ingest_connections: usize,
    pub target_eps: f64,
    pub entries_per_push: usize,
    pub streams_per_push: usize,

    pub corpus_rows: usize,
    pub tenants: usize,
    pub streams: usize,
    pub labels_per_stream: usize,
    pub metadata_pairs: usize,
    pub plain_weight: u32,
    pub json_weight: u32,
    pub logfmt_weight: u32,

    pub entry_spread_ms: u64,
    pub late_fraction: f64,
    pub late_max_ms: u64,

    pub query_connections: usize,
    pub query_eps: f64,
    pub query_window_seconds: i64,
    pub query_limit: usize,
    pub restore_lookback_seconds: i64,
    pub query_weights: [u32; 5],

    pub otlp_eps: f64,

    pub verify: Verify,
    pub targets: Targets,
}

/// The fixed dataset the query comparison runs on, and the query matrix over
/// it.
#[derive(Clone, Debug, Serialize)]
pub struct Verify {
    pub tenant_prefix: String,
    pub rows: usize,
    pub streams: usize,
    pub labels_per_stream: usize,
    /// Nanoseconds between consecutive rows in *log* time. The dataset spans
    /// `rows * step_ns`, which is what the query windows are cut out of.
    pub step_ns: i64,
    /// Unix nanoseconds the dataset starts at. Zero means "derive it from the
    /// clock", which is only correct for a single-system run: the two runs of
    /// a comparison must be given the same anchor explicitly, or they are not
    /// holding the same rows.
    pub anchor_ns: i64,
    pub entries_per_push: usize,
    pub push_connections: usize,
    /// Sub-windows the dataset is cut into. Queries are (app x sub-window), so
    /// this is what makes a cold query cold: a window nothing has asked for
    /// before cannot be answered from either system's result cache.
    pub windows: usize,
    /// Warm repeats of each query after its cold issue.
    pub repeats: usize,
    pub limit: usize,
    pub range: String,
    pub step_seconds: i64,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let duration_seconds = env_u64("LOGGYTRACY_LOAD_SECONDS", 60).max(1);
        Ok(Self {
            target: Target::parse(&env_string("LOGGYTRACY_LOAD_TARGET", "loggytracy"))?,
            phase: Phase::parse(&env_string("LOGGYTRACY_LOAD_PHASE", "load"))?,
            http_address: env_string("LOGGYTRACY_LOAD_ADDR", "127.0.0.1:3100"),
            otlp_address: env_string("LOGGYTRACY_LOAD_OTLP_ADDR", "127.0.0.1:4317"),
            tier: env_string("LOGGYTRACY_LOAD_TIER", "B"),
            seed: env_u64("LOGGYTRACY_LOAD_SEED", 0x5eed_2026),
            duration_seconds,
            warmup_seconds: env_u64("LOGGYTRACY_LOAD_WARMUP_SECONDS", 10)
                .min(duration_seconds.saturating_sub(1)),
            target_events: env_u64("LOGGYTRACY_LOAD_EVENTS", 0),
            request_timeout_seconds: env_u64("LOGGYTRACY_LOAD_REQUEST_TIMEOUT_SECONDS", 60).max(1),
            sample_interval_ms: env_u64("LOGGYTRACY_LOAD_SAMPLE_INTERVAL_MS", 1000).max(50),
            server_pid: std::env::var("LOGGYTRACY_LOAD_SERVER_PID")
                .ok()
                .and_then(|raw| raw.trim().parse::<u32>().ok()),
            cgroup_path: std::env::var("LOGGYTRACY_LOAD_CGROUP")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            result_path: std::env::var("LOGGYTRACY_LOAD_RESULT_PATH").ok(),

            ingest_connections: env_usize("LOGGYTRACY_LOAD_CONNECTIONS", 8).max(1),
            target_eps: env_f64("LOGGYTRACY_LOAD_TARGET_EPS", 3000.0).max(0.0),
            entries_per_push: env_usize("LOGGYTRACY_LOAD_ENTRIES_PER_PUSH", 100).max(1),
            streams_per_push: env_usize("LOGGYTRACY_LOAD_STREAMS_PER_PUSH", 4).max(1),

            corpus_rows: env_usize("LOGGYTRACY_LOAD_CORPUS_ROWS", 50_000).max(1),
            tenants: env_usize("LOGGYTRACY_LOAD_TENANTS", 4).max(1),
            streams: env_usize("LOGGYTRACY_LOAD_STREAMS", 256).max(1),
            labels_per_stream: env_usize("LOGGYTRACY_LOAD_LABELS_PER_STREAM", 6).clamp(1, 10),
            metadata_pairs: env_usize("LOGGYTRACY_LOAD_METADATA_PAIRS", 2),
            plain_weight: env_u64("LOGGYTRACY_LOAD_PLAIN_WEIGHT", 3) as u32,
            json_weight: env_u64("LOGGYTRACY_LOAD_JSON_WEIGHT", 5) as u32,
            logfmt_weight: env_u64("LOGGYTRACY_LOAD_LOGFMT_WEIGHT", 2) as u32,

            entry_spread_ms: env_u64("LOGGYTRACY_LOAD_ENTRY_SPREAD_MS", 250),
            late_fraction: env_f64("LOGGYTRACY_LOAD_LATE_FRACTION", 0.02).clamp(0.0, 1.0),
            late_max_ms: env_u64("LOGGYTRACY_LOAD_LATE_MAX_MS", 30_000),

            query_connections: env_usize("LOGGYTRACY_LOAD_QUERY_CONNECTIONS", 4).max(1),
            query_eps: env_f64("LOGGYTRACY_LOAD_QUERY_EPS", 5.0).max(0.0),
            query_window_seconds: env_u64("LOGGYTRACY_LOAD_QUERY_WINDOW_SECONDS", 60) as i64,
            query_limit: env_usize("LOGGYTRACY_LOAD_QUERY_LIMIT", 100).max(1),
            restore_lookback_seconds: env_u64("LOGGYTRACY_LOAD_RESTORE_LOOKBACK_SECONDS", 60)
                as i64,
            query_weights: [
                env_u64("LOGGYTRACY_LOAD_QUERY_WEIGHT_LABEL_ONLY", 3) as u32,
                env_u64("LOGGYTRACY_LOAD_QUERY_WEIGHT_LINE_FILTER", 3) as u32,
                env_u64("LOGGYTRACY_LOAD_QUERY_WEIGHT_JSON_FIELD", 3) as u32,
                env_u64("LOGGYTRACY_LOAD_QUERY_WEIGHT_RATE", 2) as u32,
                env_u64("LOGGYTRACY_LOAD_QUERY_WEIGHT_RESTORE_PROBE", 1) as u32,
            ],

            otlp_eps: env_f64("LOGGYTRACY_LOAD_OTLP_EPS", 5.0).max(0.0),

            verify: Verify {
                tenant_prefix: env_string("LOGGYTRACY_LOAD_VERIFY_TENANT_PREFIX", "verify-tenant"),
                rows: env_usize("LOGGYTRACY_LOAD_VERIFY_ROWS", 120_000).max(1),
                streams: env_usize("LOGGYTRACY_LOAD_VERIFY_STREAMS", 32).max(1),
                labels_per_stream: env_usize("LOGGYTRACY_LOAD_VERIFY_LABELS", 6).clamp(1, 10),
                step_ns: env_u64("LOGGYTRACY_LOAD_VERIFY_STEP_NS", 1_000_000).max(1) as i64,
                anchor_ns: env_u64("LOGGYTRACY_LOAD_VERIFY_ANCHOR_NS", 0) as i64,
                entries_per_push: env_usize("LOGGYTRACY_LOAD_VERIFY_ENTRIES_PER_PUSH", 100).max(1),
                push_connections: env_usize("LOGGYTRACY_LOAD_VERIFY_CONNECTIONS", 4).max(1),
                windows: env_usize("LOGGYTRACY_LOAD_MATRIX_WINDOWS", 3).max(1),
                repeats: env_usize("LOGGYTRACY_LOAD_MATRIX_REPEATS", 5).max(1),
                limit: env_usize("LOGGYTRACY_LOAD_MATRIX_LIMIT", 20_000).max(1),
                range: env_string("LOGGYTRACY_LOAD_MATRIX_RANGE", "1m"),
                step_seconds: env_u64("LOGGYTRACY_LOAD_MATRIX_STEP_SECONDS", 10).max(1) as i64,
            },

            targets: Targets {
                push_response_p95_ms: env_f64("LOGGYTRACY_TARGET_PUSH_RESPONSE_P95_MS", 250.0),
                push_response_p99_ms: env_f64("LOGGYTRACY_TARGET_PUSH_RESPONSE_P99_MS", 1000.0),
                query_response_p95_ms: env_f64("LOGGYTRACY_TARGET_QUERY_RESPONSE_P95_MS", 2000.0),
                rss_max_bytes: env_u64("LOGGYTRACY_TARGET_RSS_MAX_BYTES", 4 * 1024 * 1024 * 1024),
                max_error_rate: env_f64("LOGGYTRACY_TARGET_MAX_ERROR_RATE", 0.0),
                max_throttled_rate: env_f64("LOGGYTRACY_TARGET_MAX_THROTTLED_RATE", 0.05),
                // The engine's own `max_wal_backlog_bytes` is where
                // backpressure engages, and engaging is the design working, so
                // the ceiling is that rather than a number the harness invented.
                wal_backlog_max_bytes: env_u64(
                    "LOGGYTRACY_TARGET_WAL_BACKLOG_MAX_BYTES",
                    1024 * 1024 * 1024,
                ),
                min_backlog_samples: env_usize("LOGGYTRACY_TARGET_MIN_BACKLOG_SAMPLES", 8).max(2),
            },
        })
    }

    /// Where the server's resident memory is read from, or the reason no
    /// reading is possible. An unmeasurable peak is an error the run carries,
    /// never a zero.
    pub fn memory_source(&self) -> Result<crate::probe::MemorySource, String> {
        match (self.cgroup_path.as_ref(), self.server_pid) {
            (Some(dir), _) => Ok(crate::probe::MemorySource::Cgroup(dir.clone())),
            (None, Some(pid)) => Ok(crate::probe::MemorySource::Proc(pid)),
            (None, None) => Err(
                "neither LOGGYTRACY_LOAD_CGROUP nor LOGGYTRACY_LOAD_SERVER_PID was set, so no \
server memory could be watched"
                    .to_string(),
            ),
        }
    }

    /// Log-time span the verification dataset covers, in nanoseconds.
    pub fn verify_span_ns(&self) -> i64 {
        self.verify.rows as i64 * self.verify.step_ns
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }

    /// Seconds between pushes that hold the offered event rate, or `None` when
    /// the run is unpaced.
    pub fn push_interval(&self) -> Option<Duration> {
        (self.target_eps > 0.0)
            .then(|| Duration::from_secs_f64(self.entries_per_push as f64 / self.target_eps))
    }

    pub fn query_interval(&self) -> Option<Duration> {
        (self.query_eps > 0.0).then(|| Duration::from_secs_f64(1.0 / self.query_eps))
    }

    pub fn otlp_interval(&self) -> Option<Duration> {
        (self.otlp_eps > 0.0).then(|| Duration::from_secs_f64(1.0 / self.otlp_eps))
    }
}

/// The server's knobs as this process can see them.
///
/// The run script exports them into the environment the harness inherits, so
/// this is the configuration the server was started with — recorded because a
/// result that does not say what the flush interval was is not reproducible.
/// `LOGGYTRACY_LOAD_*` and `LOGGYTRACY_TARGET_*` are excluded: those are the
/// harness's own and are already in `Config`.
pub fn server_environment() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(name, _)| {
            name.starts_with("LOGGYTRACY_")
                && !name.starts_with("LOGGYTRACY_LOAD_")
                && !name.starts_with("LOGGYTRACY_TARGET_")
                && name != "LOGGYTRACY_BUILD_REVISION"
                && name != "LOGGYTRACY_MACHINE_PROFILE"
        })
        .collect()
}

pub fn build_revision() -> String {
    std::env::var("LOGGYTRACY_BUILD_REVISION")
        .or_else(|_| std::env::var("GIT_COMMIT"))
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn machine_profile() -> String {
    if let Ok(profile) = std::env::var("LOGGYTRACY_MACHINE_PROFILE") {
        return profile;
    }
    // Read rather than left as "unspecified": a latency number whose machine
    // is unknown cannot be compared with anything.
    let cpus = std::thread::available_parallelism()
        .map(|count| count.get().to_string())
        .unwrap_or_else(|_| "?".to_string());
    let memory = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("MemTotal:"))
                .and_then(|value| {
                    value
                        .trim()
                        .trim_end_matches(" kB")
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
        })
        .map(|kilobytes| format!("{:.1} GiB RAM", kilobytes as f64 / 1_048_576.0))
        .unwrap_or_else(|| "unknown RAM".to_string());
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|text| text.trim().to_string())
        .unwrap_or_else(|_| "unknown kernel".to_string());
    format!("{kernel}; {cpus} logical CPUs; {memory}")
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
