use std::path::PathBuf;
use std::time::Duration;

use crate::tenant::{MissingTenantPolicy, TenantId};

#[derive(Clone)]
pub struct Config {
    /// Loopback by default, and deliberately so.
    ///
    /// There is no TLS and no authentication in this process, and
    /// `X-Scope-OrgID` is trusted without proof — so the listener has to sit
    /// inside a trust boundary something else draws. A default of `0.0.0.0`
    /// makes the unsafe configuration the one you get by not deciding, and the
    /// mistake is invisible: it works. Binding loopback fails the other way,
    /// where the symptom is a connection refused and the startup log says
    /// exactly what was bound.
    pub listen_addr: String,
    pub otlp_grpc_addr: String,
    pub data_dir: PathBuf,
    /// Tenant a request is attributed to when it carries no `X-Scope-OrgID`
    /// and `missing_tenant_policy` accepts it.
    pub default_tenant: TenantId,
    pub missing_tenant_policy: MissingTenantPolicy,
    /// Tenants this instance accepts, or `None` for any well-formed id.
    ///
    /// The header is supplied by whatever sits in front of the engine and is
    /// trusted without proof, so without a list any caller that can reach the
    /// listener can mint tenants. Each new one costs a row group per part it
    /// appears in, a `meta.json` segment, and a policy the control plane never
    /// pushed — none of which anything else bounds.
    pub allowed_tenants: Option<std::collections::BTreeSet<TenantId>>,
    pub max_batch_bytes: usize,
    /// How long the journal writer waits for more records before writing the
    /// batch it already has. **Zero — the default — means it does not wait.**
    ///
    /// Group commit forms behind the write, not in front of it: while this task
    /// writes and fsyncs, later arrivals queue up and become the next batch.
    /// Waiting first charged every push the full linger even with an empty
    /// channel, which capped a single connection at 1000/max_batch_ms pushes
    /// per second. Raise it only on a disk where an fsync costs more than the
    /// added latency.
    pub max_batch_ms: u64,
    /// The largest single request the tenant ingest quota must always be able
    /// to admit: the token bucket's burst capacity is floored at this, or a
    /// legal body larger than the bucket would be refused forever. The OTLP
    /// body limit itself is `MAX_OTLP_REQUEST_BYTES`, a constant matched
    /// across both transports.
    pub max_push_bytes: usize,
    pub max_line_bytes: usize,
    pub max_label_names_per_stream: usize,
    pub max_label_name_bytes: usize,
    pub max_label_value_bytes: usize,
    /// How far behind and ahead of the server clock an entry timestamp may be.
    /// Timestamps outside the window create day partitions that retention can
    /// never expire, so they are rejected at ingest.
    pub max_timestamp_age: Option<Duration>,
    pub max_timestamp_skew: Option<Duration>,
    /// Ingest backpressure thresholds. Above either one the server answers
    /// `429` before appending to the journal, so a stalled flush costs the
    /// client a retry instead of costing this process its memory and disk.
    /// `off` disables a threshold, which restores the unbounded behaviour.
    pub max_memtable_bytes: Option<u64>,
    pub max_wal_backlog_bytes: Option<u64>,
    pub backpressure_retry_after: Duration,
    pub flush_max_bytes: u64,
    pub flush_max_interval: Duration,
    pub flush_check_interval: Duration,
    /// Most a flush materializes as `Vec<Row>` at once. The snapshot is
    /// written through the batch writer in chunks of this many bytes, so the
    /// flush transient is bounded by the chunk rather than by however large
    /// the memtable had grown while the previous flush ran —
    /// `docs/MEMORY_ATTRIBUTION.md` measured the unchunked copy at ~3.3x its
    /// memtable inside a 2 GiB container.
    pub flush_chunk_bytes: u64,
    pub row_group_size: usize,
    pub merge_min_part_count: usize,
    pub merge_target_part_rows: u64,
    pub merge_max_part_rows: u64,
    /// Largest group merge will assemble, and the hard ceiling on what one
    /// read may materialize. Both are **uncompressed** bytes, taken from each
    /// part's recorded `materialized_bytes`: comparing a compressed input size
    /// against a materialized budget let groups be selected that could then
    /// never be read. The first sits below the second so an ordinary merge
    /// leaves headroom rather than running at the hard limit.
    pub merge_max_input_bytes: u64,
    pub merge_max_memory_bytes: u64,
    pub merge_max_groups_per_tick: usize,
    pub merge_interval: Duration,
    /// Object-store URL, for example `s3://bucket/loggytracy` or
    /// `file:///var/lib/loggytracy-remote`. When unset, the engine keeps the
    /// M1 local-only behaviour.
    pub object_store_url: Option<String>,
    pub cache_max_bytes: u64,
    pub cache_eviction_interval: Duration,
    /// Global retention, or `None` for unbounded.
    ///
    /// Unbounded is the right default only because it is not the mechanism:
    /// per-tenant retention is pushed by the control plane, and configuring
    /// both is a validation error rather than a precedence rule. A default
    /// period here would silently delete data the control plane believes it
    /// still owns. Startup warns when neither is configured, since unbounded
    /// with nothing pushed means the object store grows forever.
    pub retention_period: Option<Duration>,
    pub retention_interval: Duration,
    pub retention_batch_size: usize,
    pub retention_grace_period: Duration,
    pub max_retention_runtime: Duration,
    /// Bearer token for the admin routes the control plane pushes retention
    /// through. When unset, retention is exactly the global `retention_period`
    /// behaviour and the routes are not mounted; when set, the pushed policies
    /// are the sole authority. Setting both is a validation error rather than a
    /// silently ignored setting.
    pub tenant_policy_token: Option<String>,
    /// Ingest rate for tenants the control plane has pushed no rate for.
    ///
    /// This is a default, not a plan: per-tenant rates are pushed, because
    /// plans differ between tenants and change after launch, and neither of
    /// those fits in this process's environment. What belongs here is the
    /// answer for a tenant nothing is known about — including every tenant
    /// when per-tenant policy is switched off entirely. `None` is unlimited,
    /// which is the pre-quota behaviour.
    pub default_tenant_ingest_bytes_per_second: Option<u64>,
    /// How long a tenant may bank an unused rate for and spend at once.
    ///
    /// Logs arrive in bursts, so a bucket sized at exactly one second of rate
    /// would refuse ordinary traffic. The capacity is also floored at
    /// `max_push_bytes` so that a single legal body always eventually fits:
    /// without that floor a low rate would reject the same request forever,
    /// which is the latching failure the backpressure gate is careful to avoid.
    pub tenant_ingest_burst: Duration,
    /// Query scan rate for tenants the control plane has pushed no rate for.
    /// The read counterpart to `default_tenant_ingest_bytes_per_second`, with
    /// the same division of responsibility: plans are pushed, this is the
    /// answer for a tenant nothing is known about.
    pub default_tenant_query_scan_bytes_per_second: Option<u64>,
    /// Queries one tenant may have running at once.
    ///
    /// The scan rate bounds a tenant's total work over time; this bounds how
    /// much of it happens simultaneously. Without it a single tenant issuing
    /// concurrent scans takes every permit of the shared query semaphore and
    /// the other tenants queue behind it however small their queries are.
    pub max_concurrent_queries_per_tenant: usize,
    /// Distinct streams a tenant may hold, for tenants the control plane has
    /// pushed no `max_streams` for. `None` is unbounded, which is the
    /// pre-limit behaviour.
    pub default_tenant_max_streams: Option<u64>,
    /// Expired share of a part's rows that justifies one rewrite through
    /// merge. Below it the rows stay on disk, already invisible to queries.
    pub retention_rewrite_threshold: f64,
    /// Live tail connections one instance will hold at once.
    ///
    /// A tail is a poll loop with an open socket, so this bounds scheduled
    /// work rather than a burst: the connections that do not fit are refused
    /// at the upgrade, where the client can see why.
    pub max_concurrent_tails: usize,
    /// How often a live tail asks for new lines. This is the tail's latency
    /// floor and its cost per connection at the same time.
    pub tail_poll_interval: Duration,
    pub max_query_range: Option<Duration>,
    pub max_query_scan_rows: usize,
    pub max_query_scan_bytes: u64,
    pub max_query_memory_bytes: u64,
    pub max_log_limit: usize,
    pub max_metric_evaluation_points: usize,
    pub max_metric_rows: usize,
    pub max_metric_series: usize,
    pub max_metric_samples: usize,
    /// How many `match[]` selectors one `series` request may carry. Each one
    /// is a separate full pass, so this bounds a multiplier the client picks.
    pub max_series_matchers: usize,
    pub max_concurrent_query_scans: usize,
    pub max_concurrent_metric_evaluations: usize,
    pub max_query_runtime: Duration,
    pub max_restore_runtime: Duration,
    pub max_trace_spans: usize,
    pub max_trace_search_limit: usize,
    pub max_concurrent_trace_scans: usize,
    pub max_trace_query_runtime: Duration,
    pub max_trace_restore_runtime: Duration,
    /// How long the shutdown force-flush retries silently before it starts
    /// warning on stdout and enabling operator-initiated abort.
    pub shutdown_flush_warn_after: Duration,
    /// How long startup keeps retrying an object-store step before giving up.
    ///
    /// Absorbs a transient outage instead of turning it into a crash loop.
    /// Bounded on purpose: waiting forever on a permanently misconfigured store
    /// would replace a visible crash with an invisible hang, and past this the
    /// orchestrator's own restart backoff is the better place to escalate.
    pub startup_retry_budget: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:3100".to_string(),
            otlp_grpc_addr: "127.0.0.1:4317".to_string(),
            data_dir: PathBuf::from("./data"),
            default_tenant: TenantId::parse("default")
                .expect("the built-in default tenant is valid"),
            missing_tenant_policy: MissingTenantPolicy::UseDefault,
            allowed_tenants: None,
            max_batch_bytes: 1024 * 1024,
            max_batch_ms: 0,
            max_push_bytes: 16 * 1024 * 1024,
            max_line_bytes: 256 * 1024,
            max_label_names_per_stream: 30,
            max_label_name_bytes: 1024,
            max_label_value_bytes: 2048,
            max_timestamp_age: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            max_timestamp_skew: Some(Duration::from_secs(60 * 60)),
            max_memtable_bytes: Some(256 * 1024 * 1024),
            max_wal_backlog_bytes: Some(1024 * 1024 * 1024),
            backpressure_retry_after: Duration::from_secs(1),
            flush_max_bytes: 1024 * 1024,
            flush_max_interval: Duration::from_secs(5),
            flush_check_interval: Duration::from_millis(500),
            flush_chunk_bytes: 32 * 1024 * 1024,
            row_group_size: 8192,
            merge_min_part_count: 4,
            merge_target_part_rows: 1_000_000,
            merge_max_part_rows: 4_000_000,
            merge_max_input_bytes: 512 * 1024 * 1024,
            merge_max_memory_bytes: 1024 * 1024 * 1024,
            merge_max_groups_per_tick: 16,
            merge_interval: Duration::from_secs(30),
            object_store_url: None,
            cache_max_bytes: 10 * 1024 * 1024 * 1024,
            cache_eviction_interval: Duration::from_secs(30),
            retention_period: None,
            retention_interval: Duration::from_secs(300),
            retention_batch_size: 100,
            retention_grace_period: Duration::from_secs(60 * 60),
            max_retention_runtime: Duration::from_secs(120),
            tenant_policy_token: None,
            default_tenant_ingest_bytes_per_second: None,
            tenant_ingest_burst: Duration::from_secs(10),
            default_tenant_query_scan_bytes_per_second: None,
            max_concurrent_queries_per_tenant: 4,
            default_tenant_max_streams: None,
            retention_rewrite_threshold: 0.5,
            max_concurrent_tails: 8,
            tail_poll_interval: Duration::from_secs(1),
            max_query_range: None,
            max_query_scan_rows: 5_000_000,
            max_query_scan_bytes: 2 * 1024 * 1024 * 1024,
            max_query_memory_bytes: 512 * 1024 * 1024,
            max_log_limit: 100_000,
            max_metric_evaluation_points: 10_000,
            max_metric_rows: 1_000_000,
            max_metric_series: 100_000,
            max_metric_samples: 5_000_000,
            max_series_matchers: 32,
            max_concurrent_query_scans: 8,
            max_concurrent_metric_evaluations: 4,
            max_query_runtime: Duration::from_secs(30),
            max_restore_runtime: Duration::from_secs(25),
            max_trace_spans: 100_000,
            max_trace_search_limit: 1_000,
            max_concurrent_trace_scans: 8,
            max_trace_query_runtime: Duration::from_secs(30),
            max_trace_restore_runtime: Duration::from_secs(25),
            shutdown_flush_warn_after: Duration::from_secs(30),
            startup_retry_budget: Duration::from_secs(300),
        }
    }
}

/// What this process is actually allowed to use, and where that came from.
///
/// **Observability only. Nothing derives a default from this**, and that is a
/// measured decision rather than an omission.
///
/// `merge_max_memory_bytes` defaults to 1 GiB, which in a 2 GiB container is
/// half the machine handed to one background task from no number the operator
/// gave, and `docs/MEMORY_ATTRIBUTION.md` had measured one merge group's rewrite
/// as the largest single live term at 771 MiB. Deriving the default from this
/// limit was the obvious fix and it was tried: at 25% of the container it made
/// the engine **worse**, moving the kill from the settle into ingest and raising
/// the ingest-phase peak by about 290 MiB across two runs each. Smaller groups
/// mean more merges, and more merges overlap ingest — the contention the
/// previous review recorded as N8. `docs/MEMORY_BUDGET_GATE.md` has the runs.
///
/// So the limit is read and logged, because an operator has nowhere else to
/// learn what the process is inside, and the default stays put until a
/// measurement says what to move it to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostMemory {
    pub limit_bytes: Option<u64>,
    pub source: &'static str,
}

impl HostMemory {
    pub fn detect() -> Self {
        if let Some(limit) = cgroup_v2_memory_max() {
            return Self {
                limit_bytes: Some(limit),
                source: "cgroup v2 memory.max",
            };
        }
        if let Some(total) = meminfo_total_bytes() {
            return Self {
                limit_bytes: Some(total),
                source: "/proc/meminfo MemTotal",
            };
        }
        Self {
            limit_bytes: None,
            source: "undetected",
        }
    }
}

/// `max` reads `"max"` when the cgroup is unlimited, which is not a limit and
/// must not be treated as one.
fn cgroup_v2_memory_max() -> Option<u64> {
    let own = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = own
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim_start_matches('/');
    let path = std::path::Path::new("/sys/fs/cgroup")
        .join(relative)
        .join("memory.max");
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn meminfo_total_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kib: u64 = text
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    kib.checked_mul(1024)
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        let config = Self {
            listen_addr: env_string("LOGGYTRACY_LISTEN_ADDR", defaults.listen_addr),
            otlp_grpc_addr: env_string("LOGGYTRACY_OTLP_GRPC_ADDR", defaults.otlp_grpc_addr),
            data_dir: std::env::var("LOGGYTRACY_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| defaults.data_dir.clone()),
            default_tenant: match std::env::var("LOGGYTRACY_DEFAULT_TENANT") {
                Ok(raw) => TenantId::parse(&raw)
                    .map_err(|error| format!("invalid LOGGYTRACY_DEFAULT_TENANT: {error}"))?,
                Err(_) => defaults.default_tenant.clone(),
            },
            allowed_tenants: env_tenant_set("LOGGYTRACY_ALLOWED_TENANTS")?,
            missing_tenant_policy: match std::env::var("LOGGYTRACY_MISSING_TENANT_POLICY") {
                Ok(raw) => raw.parse().map_err(|error| {
                    format!("invalid LOGGYTRACY_MISSING_TENANT_POLICY: {error}")
                })?,
                Err(_) => defaults.missing_tenant_policy,
            },
            max_batch_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_BATCH_BYTES",
                defaults.max_batch_bytes,
            )?,
            max_batch_ms: env_u64("LOGGYTRACY_MAX_BATCH_MS", defaults.max_batch_ms)?,
            max_push_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_PUSH_BYTES",
                defaults.max_push_bytes,
            )?,
            max_line_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_LINE_BYTES",
                defaults.max_line_bytes,
            )?,
            max_label_names_per_stream: env_positive_usize(
                "LOGGYTRACY_MAX_LABEL_NAMES_PER_STREAM",
                defaults.max_label_names_per_stream,
            )?,
            max_label_name_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_LABEL_NAME_BYTES",
                defaults.max_label_name_bytes,
            )?,
            max_label_value_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_LABEL_VALUE_BYTES",
                defaults.max_label_value_bytes,
            )?,
            max_timestamp_age: env_duration(
                "LOGGYTRACY_MAX_TIMESTAMP_AGE",
                defaults.max_timestamp_age,
            )?,
            max_memtable_bytes: env_optional_u64(
                "LOGGYTRACY_MAX_MEMTABLE_BYTES",
                defaults.max_memtable_bytes,
            )?,
            max_wal_backlog_bytes: env_optional_u64(
                "LOGGYTRACY_MAX_WAL_BACKLOG_BYTES",
                defaults.max_wal_backlog_bytes,
            )?,
            backpressure_retry_after: env_required_duration(
                "LOGGYTRACY_BACKPRESSURE_RETRY_AFTER",
                defaults.backpressure_retry_after,
            )?,
            max_timestamp_skew: env_duration(
                "LOGGYTRACY_MAX_TIMESTAMP_SKEW",
                defaults.max_timestamp_skew,
            )?,
            flush_max_bytes: env_positive_u64(
                "LOGGYTRACY_FLUSH_MAX_BYTES",
                defaults.flush_max_bytes,
            )?,
            flush_max_interval: env_required_duration(
                "LOGGYTRACY_FLUSH_MAX_INTERVAL",
                defaults.flush_max_interval,
            )?,
            flush_check_interval: env_required_duration(
                "LOGGYTRACY_FLUSH_CHECK_INTERVAL",
                defaults.flush_check_interval,
            )?,
            flush_chunk_bytes: env_positive_u64(
                "LOGGYTRACY_FLUSH_CHUNK_BYTES",
                defaults.flush_chunk_bytes,
            )?,
            row_group_size: env_positive_usize(
                "LOGGYTRACY_ROW_GROUP_SIZE",
                defaults.row_group_size,
            )?,
            merge_min_part_count: env_positive_usize(
                "LOGGYTRACY_MERGE_MIN_PART_COUNT",
                defaults.merge_min_part_count,
            )?,
            merge_target_part_rows: env_positive_u64(
                "LOGGYTRACY_MERGE_TARGET_PART_ROWS",
                defaults.merge_target_part_rows,
            )?,
            merge_max_part_rows: env_positive_u64(
                "LOGGYTRACY_MERGE_MAX_PART_ROWS",
                defaults.merge_max_part_rows,
            )?,
            merge_max_input_bytes: env_positive_u64(
                "LOGGYTRACY_MERGE_MAX_INPUT_BYTES",
                defaults.merge_max_input_bytes,
            )?,
            merge_max_memory_bytes: env_positive_u64(
                "LOGGYTRACY_MERGE_MAX_MEMORY_BYTES",
                defaults.merge_max_memory_bytes,
            )?,
            merge_max_groups_per_tick: env_positive_usize(
                "LOGGYTRACY_MERGE_MAX_GROUPS_PER_TICK",
                defaults.merge_max_groups_per_tick,
            )?,
            merge_interval: env_required_duration(
                "LOGGYTRACY_MERGE_INTERVAL",
                defaults.merge_interval,
            )?,
            object_store_url: std::env::var("LOGGYTRACY_OBJECT_STORE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            cache_max_bytes: env_positive_u64(
                "LOGGYTRACY_CACHE_MAX_BYTES",
                defaults.cache_max_bytes,
            )?,
            cache_eviction_interval: env_required_duration(
                "LOGGYTRACY_CACHE_EVICTION_INTERVAL",
                defaults.cache_eviction_interval,
            )?,
            retention_period: env_duration("LOGGYTRACY_RETENTION_PERIOD", None)?,
            retention_interval: env_required_duration(
                "LOGGYTRACY_RETENTION_INTERVAL",
                defaults.retention_interval,
            )?,
            retention_batch_size: env_positive_usize(
                "LOGGYTRACY_RETENTION_BATCH_SIZE",
                defaults.retention_batch_size,
            )?,
            retention_grace_period: env_required_duration(
                "LOGGYTRACY_RETENTION_GRACE_PERIOD",
                defaults.retention_grace_period,
            )?,
            max_retention_runtime: env_required_duration(
                "LOGGYTRACY_MAX_RETENTION_RUNTIME",
                defaults.max_retention_runtime,
            )?,
            tenant_policy_token: std::env::var("LOGGYTRACY_TENANT_POLICY_TOKEN")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            default_tenant_ingest_bytes_per_second: env_optional_u64(
                "LOGGYTRACY_DEFAULT_TENANT_INGEST_BYTES_PER_SECOND",
                defaults.default_tenant_ingest_bytes_per_second,
            )?,
            tenant_ingest_burst: env_required_duration(
                "LOGGYTRACY_TENANT_INGEST_BURST",
                defaults.tenant_ingest_burst,
            )?,
            default_tenant_query_scan_bytes_per_second: env_optional_u64(
                "LOGGYTRACY_DEFAULT_TENANT_QUERY_SCAN_BYTES_PER_SECOND",
                defaults.default_tenant_query_scan_bytes_per_second,
            )?,
            max_concurrent_queries_per_tenant: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_QUERIES_PER_TENANT",
                defaults.max_concurrent_queries_per_tenant,
            )?,
            default_tenant_max_streams: env_optional_u64(
                "LOGGYTRACY_DEFAULT_TENANT_MAX_STREAMS",
                defaults.default_tenant_max_streams,
            )?,
            retention_rewrite_threshold: env_value(
                "LOGGYTRACY_RETENTION_REWRITE_THRESHOLD",
                defaults.retention_rewrite_threshold,
            )?,
            max_concurrent_tails: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_TAILS",
                defaults.max_concurrent_tails,
            )?,
            tail_poll_interval: env_required_duration(
                "LOGGYTRACY_TAIL_POLL_INTERVAL",
                defaults.tail_poll_interval,
            )?,
            max_query_range: env_duration("LOGGYTRACY_MAX_QUERY_RANGE", None)?,
            max_query_scan_rows: env_positive_usize(
                "LOGGYTRACY_MAX_QUERY_SCAN_ROWS",
                defaults.max_query_scan_rows,
            )?,
            max_query_scan_bytes: env_positive_u64(
                "LOGGYTRACY_MAX_QUERY_SCAN_BYTES",
                defaults.max_query_scan_bytes,
            )?,
            max_query_memory_bytes: env_positive_u64(
                "LOGGYTRACY_MAX_QUERY_MEMORY_BYTES",
                defaults.max_query_memory_bytes,
            )?,
            max_log_limit: env_positive_usize("LOGGYTRACY_MAX_LOG_LIMIT", defaults.max_log_limit)?,
            max_metric_evaluation_points: env_positive_usize(
                "LOGGYTRACY_MAX_METRIC_EVALUATION_POINTS",
                defaults.max_metric_evaluation_points,
            )?,
            max_metric_rows: env_positive_usize(
                "LOGGYTRACY_MAX_METRIC_ROWS",
                defaults.max_metric_rows,
            )?,
            max_metric_series: env_positive_usize(
                "LOGGYTRACY_MAX_METRIC_SERIES",
                defaults.max_metric_series,
            )?,
            max_metric_samples: env_positive_usize(
                "LOGGYTRACY_MAX_METRIC_SAMPLES",
                defaults.max_metric_samples,
            )?,
            max_series_matchers: env_positive_usize(
                "LOGGYTRACY_MAX_SERIES_MATCHERS",
                defaults.max_series_matchers,
            )?,
            max_concurrent_query_scans: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS",
                defaults.max_concurrent_query_scans,
            )?,
            max_concurrent_metric_evaluations: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_METRIC_EVALUATIONS",
                defaults.max_concurrent_metric_evaluations,
            )?,
            max_query_runtime: env_required_duration(
                "LOGGYTRACY_MAX_QUERY_RUNTIME",
                defaults.max_query_runtime,
            )?,
            max_restore_runtime: env_required_duration(
                "LOGGYTRACY_MAX_RESTORE_RUNTIME",
                defaults.max_restore_runtime,
            )?,
            max_trace_spans: env_positive_usize(
                "LOGGYTRACY_MAX_TRACE_SPANS",
                defaults.max_trace_spans,
            )?,
            max_trace_search_limit: env_positive_usize(
                "LOGGYTRACY_MAX_TRACE_SEARCH_LIMIT",
                defaults.max_trace_search_limit,
            )?,
            max_concurrent_trace_scans: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_TRACE_SCANS",
                defaults.max_concurrent_trace_scans,
            )?,
            max_trace_query_runtime: env_required_duration(
                "LOGGYTRACY_MAX_TRACE_QUERY_RUNTIME",
                defaults.max_trace_query_runtime,
            )?,
            max_trace_restore_runtime: env_required_duration(
                "LOGGYTRACY_MAX_TRACE_RESTORE_RUNTIME",
                defaults.max_trace_restore_runtime,
            )?,
            startup_retry_budget: env_required_duration(
                "LOGGYTRACY_STARTUP_RETRY_BUDGET",
                defaults.startup_retry_budget,
            )?,
            shutdown_flush_warn_after: env_required_duration(
                "LOGGYTRACY_SHUTDOWN_FLUSH_WARN_AFTER",
                defaults.shutdown_flush_warn_after,
            )?,
        };
        config.validate()?;
        Ok(config)
    }

    /// The most this configuration can have materialized at once, in bytes.
    ///
    /// Every limit here is enforced on its own and none of them is enforced
    /// against the machine. Eight concurrent scans at 512 MiB is four gigabytes
    /// that no single knob mentions, and an operator sizing an instance from
    /// its idle footprint — measured at fifty times below its peak — has no way
    /// to arrive at that number by reading the configuration.
    ///
    /// Deliberately an upper bound rather than an estimate. Reaching it needs
    /// every scan slot full and each one at its cap, which a real workload will
    /// not do; the point is that nothing prevents it.
    pub fn peak_materialized_bytes(&self) -> u64 {
        let queries =
            (self.max_concurrent_query_scans as u64).saturating_mul(self.max_query_memory_bytes);
        // Trace scans have no byte budget of their own — `max_trace_spans` is a
        // count — so this is the honest floor rather than the true term, and
        // the log says so.
        let merge = self.merge_max_memory_bytes;
        queries.saturating_add(merge)
    }

    /// Logged once at startup. There is nowhere else an operator learns this.
    pub fn log_memory_budget(&self) {
        let host = HostMemory::detect();
        tracing::info!(
            peak_materialized_bytes = self.peak_materialized_bytes(),
            concurrent_query_scans = self.max_concurrent_query_scans,
            max_query_memory_bytes = self.max_query_memory_bytes,
            merge_max_memory_bytes = self.merge_max_memory_bytes,
            merge_max_input_bytes = self.merge_max_input_bytes,
            flush_chunk_bytes = self.flush_chunk_bytes,
            // The merge budget is derived unless it was set, so without this an
            // operator cannot learn what the process chose or what it read to
            // choose it.
            detected_memory_limit_bytes = host.limit_bytes,
            detected_memory_source = host.source,
            "configured peak materialized memory, excluding trace scans, the memtable and allocator retention"
        );
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.listen_addr.trim().is_empty() || self.otlp_grpc_addr.trim().is_empty() {
            return Err("listen and OTLP addresses must not be empty".to_string());
        }
        positive_usize("max_batch_bytes", self.max_batch_bytes)?;
        // Zero is the default and means "do not linger", so this one is not a
        // positive-value knob.
        positive_usize("max_push_bytes", self.max_push_bytes)?;
        positive_usize("max_line_bytes", self.max_line_bytes)?;
        positive_usize(
            "max_label_names_per_stream",
            self.max_label_names_per_stream,
        )?;
        positive_usize("max_label_name_bytes", self.max_label_name_bytes)?;
        positive_usize("max_label_value_bytes", self.max_label_value_bytes)?;
        if let Some(age) = self.max_timestamp_age {
            positive_duration("max_timestamp_age", age)?;
        }
        if let Some(skew) = self.max_timestamp_skew {
            positive_duration("max_timestamp_skew", skew)?;
        }
        positive_u64("flush_max_bytes", self.flush_max_bytes)?;
        positive_duration("flush_max_interval", self.flush_max_interval)?;
        positive_duration("backpressure_retry_after", self.backpressure_retry_after)?;
        // A memtable ceiling below the flush trigger would reject writes the
        // flush loop has not even been asked to move yet.
        if let Some(limit) = self.max_memtable_bytes
            && limit < self.flush_max_bytes
        {
            return Err(format!(
                "max_memtable_bytes ({limit}) must not be below flush_max_bytes ({})",
                self.flush_max_bytes
            ));
        }
        positive_duration("flush_check_interval", self.flush_check_interval)?;
        // A chunk is also a part: chunks much smaller than a row group's
        // worth of rows would turn every flush into a spray of tiny parts.
        if self.flush_chunk_bytes < 1024 * 1024 {
            return Err(format!(
                "flush_chunk_bytes ({}) must be at least 1 MiB",
                self.flush_chunk_bytes
            ));
        }
        positive_usize("row_group_size", self.row_group_size)?;
        if self.row_group_size > 65_536 {
            return Err("row_group_size must not exceed 65536".to_string());
        }
        if self.merge_min_part_count < 2 {
            return Err("merge_min_part_count must be at least 2".to_string());
        }
        positive_u64("merge_target_part_rows", self.merge_target_part_rows)?;
        positive_u64("merge_max_part_rows", self.merge_max_part_rows)?;
        if self.merge_target_part_rows > self.merge_max_part_rows {
            return Err("merge_target_part_rows must not exceed merge_max_part_rows".to_string());
        }
        positive_u64("merge_max_input_bytes", self.merge_max_input_bytes)?;
        positive_u64("merge_max_memory_bytes", self.merge_max_memory_bytes)?;
        if self.merge_max_input_bytes > self.merge_max_memory_bytes {
            return Err(format!(
                "merge_max_input_bytes ({}) must not exceed merge_max_memory_bytes ({}): a group \
selected above the read budget can never be merged",
                self.merge_max_input_bytes, self.merge_max_memory_bytes
            ));
        }
        positive_usize("merge_max_groups_per_tick", self.merge_max_groups_per_tick)?;
        positive_duration("merge_interval", self.merge_interval)?;
        positive_u64("cache_max_bytes", self.cache_max_bytes)?;
        positive_duration("cache_eviction_interval", self.cache_eviction_interval)?;
        if let Some(period) = self.retention_period {
            positive_duration("retention_period", period)?;
        }
        positive_duration("retention_interval", self.retention_interval)?;
        positive_usize("retention_batch_size", self.retention_batch_size)?;
        positive_duration("retention_grace_period", self.retention_grace_period)?;
        positive_duration("max_retention_runtime", self.max_retention_runtime)?;
        // A silently ignored retention setting is the worst possible outcome,
        // so the two modes fail at startup instead of quietly picking one.
        // A default tenant outside the list would be minted by every request
        // that omits the header, which is exactly what the list is for.
        if let Some(allowed) = &self.allowed_tenants
            && self.missing_tenant_policy == MissingTenantPolicy::UseDefault
            && !allowed.contains(&self.default_tenant)
        {
            return Err(format!(
                "default tenant {} is not in LOGGYTRACY_ALLOWED_TENANTS: add it, or set \
LOGGYTRACY_MISSING_TENANT_POLICY=reject so headerless requests are refused instead",
                self.default_tenant
            ));
        }
        if self.tenant_policy_token.is_some() && self.retention_period.is_some() {
            return Err(
                "LOGGYTRACY_RETENTION_PERIOD and LOGGYTRACY_TENANT_POLICY_TOKEN are mutually \
exclusive: per-tenant retention replaces the global period"
                    .to_string(),
            );
        }
        if !self.retention_rewrite_threshold.is_finite()
            || self.retention_rewrite_threshold <= 0.0
            || self.retention_rewrite_threshold > 1.0
        {
            return Err(
                "invalid retention_rewrite_threshold: expected a fraction in (0, 1]".to_string(),
            );
        }
        if let Some(range) = self.max_query_range {
            positive_duration("max_query_range", range)?;
        }
        positive_usize("max_query_scan_rows", self.max_query_scan_rows)?;
        positive_u64("max_query_scan_bytes", self.max_query_scan_bytes)?;
        positive_u64("max_query_memory_bytes", self.max_query_memory_bytes)?;
        positive_usize("max_log_limit", self.max_log_limit)?;
        positive_usize(
            "max_metric_evaluation_points",
            self.max_metric_evaluation_points,
        )?;
        positive_usize("max_metric_rows", self.max_metric_rows)?;
        positive_usize("max_metric_series", self.max_metric_series)?;
        positive_usize("max_metric_samples", self.max_metric_samples)?;
        positive_usize(
            "max_concurrent_query_scans",
            self.max_concurrent_query_scans,
        )?;
        positive_usize(
            "max_concurrent_metric_evaluations",
            self.max_concurrent_metric_evaluations,
        )?;
        positive_duration("max_query_runtime", self.max_query_runtime)?;
        positive_duration("max_restore_runtime", self.max_restore_runtime)?;
        positive_usize("max_trace_spans", self.max_trace_spans)?;
        positive_usize("max_trace_search_limit", self.max_trace_search_limit)?;
        positive_usize(
            "max_concurrent_trace_scans",
            self.max_concurrent_trace_scans,
        )?;
        positive_duration("max_trace_query_runtime", self.max_trace_query_runtime)?;
        positive_duration("max_trace_restore_runtime", self.max_trace_restore_runtime)?;
        positive_duration("shutdown_flush_warn_after", self.shutdown_flush_warn_after)
    }
}

fn env_string<T>(name: &str, default: T) -> String
where
    T: Into<String>,
{
    std::env::var(name).unwrap_or_else(|_| default.into())
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    env_value(name, default)
}

fn env_positive_usize(name: &str, default: usize) -> Result<usize, String> {
    let value = env_usize(name, default)?;
    if value == 0 {
        return Err(format!("invalid {name}: value must be greater than zero"));
    }
    Ok(value)
}

fn env_value<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| format!("invalid {name} {value:?}: {error}")),
        Err(_) => Ok(default),
    }
}

/// A comma-separated tenant allowlist. Empty or unset means no list at all,
/// which is deliberately different from an empty list: the latter would accept
/// nobody and is more likely a mistake than an intent.
fn env_tenant_set(name: &str) -> Result<Option<std::collections::BTreeSet<TenantId>>, String> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(None);
    };
    let mut tenants = std::collections::BTreeSet::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        tenants.insert(
            TenantId::parse(entry).map_err(|error| format!("invalid {name} entry: {error}"))?,
        );
    }
    if tenants.is_empty() {
        return Ok(None);
    }
    Ok(Some(tenants))
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    env_value(name, default)
}

/// A byte threshold that `off`/`none` turns into "no limit".
fn env_optional_u64(name: &str, default: Option<u64>) -> Result<Option<u64>, String> {
    let Ok(raw) = std::env::var(name) else {
        return Ok(default);
    };
    let value = raw.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("off") || value.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    let parsed: u64 = value
        .parse()
        .map_err(|error| format!("invalid {name} {raw:?}: {error}"))?;
    if parsed == 0 {
        return Err(format!(
            "invalid {name}: use 'off' to disable the limit rather than zero"
        ));
    }
    Ok(Some(parsed))
}

fn env_positive_u64(name: &str, default: u64) -> Result<u64, String> {
    let value = env_u64(name, default)?;
    if value == 0 {
        return Err(format!("invalid {name}: value must be greater than zero"));
    }
    Ok(value)
}

fn env_duration(name: &str, default: Option<Duration>) -> Result<Option<Duration>, String> {
    let Ok(value) = std::env::var(name) else {
        return Ok(default);
    };
    parse_duration_text(name, &value)
}

fn env_required_duration(name: &str, default: Duration) -> Result<Duration, String> {
    let value = env_duration(name, Some(default))?.ok_or_else(|| {
        format!("invalid {name}: value must be a positive duration, not disabled")
    })?;
    positive_duration(name, value)?;
    Ok(value)
}

fn parse_duration_text(name: &str, raw: &str) -> Result<Option<Duration>, String> {
    let value = raw.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("none") || value.eq_ignore_ascii_case("off") {
        return Ok(None);
    }
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 1_000_000u64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000_000_000u64)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60 * 1_000_000_000u64)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 60 * 60 * 1_000_000_000u64)
    } else if let Some(value) = value.strip_suffix('d') {
        (value, 24 * 60 * 60 * 1_000_000_000u64)
    } else {
        return Err(format!(
            "invalid {name} {value:?}: expected a duration such as 500ms, 5s, 10m, 2h, or 7d"
        ));
    };
    let number: u64 = number
        .parse()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))?;
    let nanos = number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("invalid {name} {value:?}: duration overflow"))?;
    Ok(Some(Duration::from_nanos(nanos)))
}

fn positive_usize(name: &str, value: usize) -> Result<(), String> {
    if value == 0 {
        Err(format!("invalid {name}: value must be greater than zero"))
    } else {
        Ok(())
    }
}

fn positive_u64(name: &str, value: u64) -> Result<(), String> {
    if value == 0 {
        Err(format!("invalid {name}: value must be greater than zero"))
    } else {
        Ok(())
    }
}

fn positive_duration(name: &str, value: Duration) -> Result<(), String> {
    if value.is_zero() {
        Err(format!(
            "invalid {name}: duration must be greater than zero"
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration_suffixes_and_disabled_values() {
        assert_eq!(
            parse_duration_text("TEST", "5ms").unwrap(),
            Some(Duration::from_millis(5))
        );
        assert_eq!(
            parse_duration_text("TEST", "2s").unwrap(),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            parse_duration_text("TEST", "3m").unwrap(),
            Some(Duration::from_secs(180))
        );
        assert_eq!(
            parse_duration_text("TEST", "1h").unwrap(),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            parse_duration_text("TEST", "1d").unwrap(),
            Some(Duration::from_secs(86_400))
        );
        assert_eq!(parse_duration_text("TEST", "off").unwrap(), None);
        assert!(
            parse_duration_text("TEST", "0s")
                .unwrap()
                .unwrap()
                .is_zero()
        );
    }

    #[test]
    fn the_two_retention_modes_are_mutually_exclusive() {
        let mut config = Config {
            tenant_policy_token: Some("secret".to_string()),
            ..Config::default()
        };
        assert!(config.validate().is_ok());

        // Silently ignoring one of the two would be the worst outcome, so it
        // fails at startup instead.
        config.retention_period = Some(Duration::from_secs(3600));
        assert!(config.validate().is_err());

        config.tenant_policy_token = None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_flush_chunk_must_be_at_least_a_mebibyte() {
        let mut config = Config::default();
        assert_eq!(config.flush_chunk_bytes, 32 * 1024 * 1024);
        assert!(config.validate().is_ok());

        // A sub-mebibyte chunk turns every flush into a spray of tiny parts.
        config.flush_chunk_bytes = 1024 * 1024 - 1;
        assert!(config.validate().is_err());
        config.flush_chunk_bytes = 1024 * 1024;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn the_rewrite_threshold_must_be_a_fraction() {
        let mut config = Config::default();
        for invalid in [0.0, -0.5, 1.5, f64::NAN] {
            config.retention_rewrite_threshold = invalid;
            assert!(config.validate().is_err(), "{invalid} must be rejected");
        }
        config.retention_rewrite_threshold = 1.0;
        assert!(config.validate().is_ok());
    }

    /// A settings reference that silently falls behind the code is worse than
    /// none: an operator trusts it and tunes a knob that no longer exists, or
    /// misses one that now decides whether their data survives. Adding a knob
    /// without documenting it breaks this test rather than shipping quietly.
    #[test]
    fn every_configuration_knob_is_documented() {
        let source = include_str!("config.rs");
        let reference = include_str!("../docs/CONFIGURATION.md");

        let mut undocumented: Vec<&str> = Vec::new();
        let mut rest = source;
        while let Some(start) = rest.find("\"LOGGYTRACY_") {
            rest = &rest[start + 1..];
            let Some(end) = rest.find('"') else { break };
            let name = &rest[..end];
            // Error messages quote knob names inside prose, so a match is only
            // a knob when the whole literal looks like one.
            let is_knob = name
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if is_knob && !reference.contains(name) && !undocumented.contains(&name) {
                undocumented.push(name);
            }
        }

        assert!(
            undocumented.is_empty(),
            "these knobs exist in config.rs but not in docs/CONFIGURATION.md: {undocumented:#?}"
        );
    }

    /// The largest term in this engine's memory footprint is a product of two
    /// knobs that never appear together, and an instance sized from its idle
    /// footprint is sized about fifty times too small (LOAD_RESULTS.md §7). So
    /// the product is computed and logged rather than left to be discovered.
    #[test]
    fn the_peak_memory_budget_is_the_product_nobody_reads() {
        let config = Config {
            max_concurrent_query_scans: 8,
            max_query_memory_bytes: 512 * 1024 * 1024,
            merge_max_memory_bytes: 1024 * 1024 * 1024,
            ..Config::default()
        };
        assert_eq!(
            config.peak_materialized_bytes(),
            5 * 1024 * 1024 * 1024,
            "eight scans at half a gigabyte plus one merge at a gigabyte"
        );

        // Halving the concurrency halves the term, which is the point: the
        // knob an operator reaches for is the one that moves the number.
        let halved = Config {
            max_concurrent_query_scans: 4,
            ..config
        };
        assert_eq!(halved.peak_materialized_bytes(), 3 * 1024 * 1024 * 1024);
    }

    #[test]
    fn the_host_memory_limit_is_detected_and_never_changes_a_default() {
        let host = HostMemory::detect();
        // Whatever it reads, it must not have moved the merge defaults: an
        // earlier attempt to derive them from it measured worse, and this test
        // is what keeps the revert honest rather than a comment.
        let config = Config::default();
        assert_eq!(config.merge_max_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.merge_max_input_bytes, 512 * 1024 * 1024);
        // Detection itself must be sane where it works at all.
        if let Some(limit) = host.limit_bytes {
            assert!(limit > 0, "a detected limit of zero is a misread");
            assert_ne!(host.source, "undetected");
        }
    }

    #[test]
    fn validates_that_the_default_tenant_is_itself_allowed() {
        let mut config = Config {
            allowed_tenants: Some(
                [TenantId::parse("acme").expect("valid")]
                    .into_iter()
                    .collect(),
            ),
            ..Config::default()
        };
        // The default tenant is what a headerless request becomes, so leaving
        // it out of the list would mint an unlisted tenant on every such push.
        let error = config
            .validate()
            .expect_err("a default tenant outside the list must not start");
        assert!(error.contains("default tenant"), "{error}");

        config.missing_tenant_policy = MissingTenantPolicy::Reject;
        assert!(config.validate().is_ok());

        config.missing_tenant_policy = MissingTenantPolicy::UseDefault;
        config.default_tenant = TenantId::parse("acme").expect("valid");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validates_merge_relationships_and_zero_limits() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());
        config.merge_target_part_rows = config.merge_max_part_rows + 1;
        assert!(config.validate().is_err());
        config = Config::default();
        config.max_query_runtime = Duration::ZERO;
        assert!(config.validate().is_err());
    }
}
