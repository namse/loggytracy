use std::path::PathBuf;
use std::time::Duration;

use crate::tenant::TenantId;

/// The shape of this process's own log lines.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

impl LogFormat {
    fn from_env(name: &str, default: Self) -> Result<Self, String> {
        let Ok(raw) = std::env::var(name) else {
            return Ok(default);
        };
        Self::parse(&raw, default).map_err(|error| format!("invalid {name}: {error}"))
    }

    /// Split from the environment read so it can be tested without setting a
    /// process-wide variable, which tests here do not do: they run in parallel
    /// threads and the next one along would read it.
    fn parse(raw: &str, default: Self) -> Result<Self, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(default),
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(format!("{other:?}: expected text or json")),
        }
    }
}

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
    /// Tenant a request without `X-Scope-OrgID` is attributed to, or `None` —
    /// the default — to reject such requests with 400. The opt-in exists for
    /// single-tenant deployments with no gateway minting the header; behind a
    /// gateway a missing header is the gateway failing, which should fail
    /// loudly rather than quietly pool everyone's data in one tenant.
    pub missing_tenant: Option<TenantId>,
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
    pub max_line_bytes: usize,
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
    /// Request bodies admitted at once, summed across in-flight pushes.
    ///
    /// The other two thresholds bound buffers the server owns; this one bounds
    /// what its callers hand it, which was `concurrency × MAX_OTLP_REQUEST_BYTES`
    /// with nothing limiting concurrency. `off` disables it. It cannot refuse a
    /// request on an idle server whatever it is set to — see
    /// `IngestGate::admit_body` — so it is safe at any value.
    pub max_inflight_push_bytes: Option<u64>,
    /// Floor for truncating the WAL's dead prefix in local-only mode. The
    /// bytes before the checkpoint are unreadable by every recovery path;
    /// without truncation `journal.wal` keeps everything ever ingested. The
    /// prefix is cut when it exceeds both this floor and the live suffix
    /// (which bounds the rewrite at O(1) amortized per logged byte). `off`
    /// restores the never-compact behaviour. Remote mode always compacts,
    /// regardless of this knob.
    pub wal_compact_min_bytes: Option<u64>,
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
    pub retention_interval: Duration,
    pub retention_batch_size: usize,
    pub retention_grace_period: Duration,
    pub max_retention_runtime: Duration,
    /// Queries one tenant may have running at once.
    ///
    /// Without it a single tenant issuing concurrent scans takes every permit
    /// of the shared query semaphore and the other tenants queue behind it
    /// however small their queries are.
    pub max_concurrent_queries_per_tenant: usize,
    /// Bytes a tenant may keep stored, for tenants the control plane has pushed
    /// no `max_stored_bytes` for. `None` is unbounded.
    ///
    /// A free tier is the reason this default exists at all: a tenant nothing
    /// has been pushed for is one nobody has sold anything to, and leaving that
    /// unbounded means the first such tenant decides how much disk everyone
    /// else gets.
    pub default_tenant_max_stored_bytes: Option<u64>,
    /// Free space on the data directory's filesystem below which ingest is
    /// refused, or `None` to accept until the writes themselves fail.
    ///
    /// The last guard rather than the first. What normally keeps this disk in
    /// bounds is cache eviction, and what keeps the WAL in bounds is the
    /// backlog limit; this exists for the case where neither applies — a
    /// sidecar set that eviction does not touch, a merge whose output doubles a
    /// part while its inputs still exist, a volume smaller than someone
    /// believed. Reaching it means refusing writes, which is recoverable.
    /// Passing it means a flush that cannot write, which is the state
    /// `RUNBOOK.md` calls the most dangerous one.
    ///
    /// The default leaves room for what has already been accepted to land:
    /// the memtable limit is 256 MiB and a merge output can double a part
    /// while its inputs are still there, so two gigabytes covers both with
    /// margin on any disk large enough to hold the default 10 GiB cache.
    pub min_free_disk_bytes: Option<u64>,
    /// How this process writes its own logs.
    ///
    /// A log engine whose own output has to be regex-parsed to be searched is
    /// an odd thing to ship, but the human-readable form is the one worth
    /// having in front of a terminal, so the default is what a developer sees
    /// and the container image sets the other.
    pub log_format: LogFormat,
    /// How often free space is re-read. It bounds how stale the number the
    /// ingest gate reads can be, and a `statvfs` costs nothing next to the
    /// flush tick beside it.
    pub disk_sample_interval: Duration,
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
    /// The shared byte budget every query materialization draws from,
    /// replacing the unenforced `max_concurrent_query_scans ×
    /// max_query_memory_bytes` product as the aggregate bound. A query still
    /// carries its own `max_query_memory_bytes` cap; this is what all of them
    /// together may hold.
    pub query_memory_budget_bytes: u64,
    /// Byte budget for decoded row groups kept in memory across scans
    /// (`off` disables). A part is immutable, so a group decoded once can
    /// serve every later scan without paying the reader build again; this is
    /// what the budget bounds. All three systems in the comparison bed run
    /// their own caches — Loki's result cache, VictoriaLogs' caches — so
    /// this is the same class of speedup, sized explicitly.
    pub row_group_cache_max_bytes: Option<u64>,
    /// Byte budget for the resident bloom half of part sidecars (`off` =
    /// unbounded, the pre-eviction behaviour). The blooms are durable in
    /// `index.bin`, so an evicted part's next pruning query pays one re-read;
    /// without a bound the resident total is ~2 MiB per live part and the
    /// live part count scales with ingest rate × retention window — the term
    /// that killed the first 24-hour soak at t≈1834 s (todo.md).
    pub sidecar_cache_max_bytes: Option<u64>,
    pub max_log_limit: usize,
    /// Most buckets one `/logs/histogram` answer may hold.
    pub max_histogram_buckets: usize,
    pub max_concurrent_query_scans: usize,
    pub max_query_runtime: Duration,
    pub max_restore_runtime: Duration,
    pub max_trace_spans: usize,
    pub max_trace_search_limit: usize,
    pub max_concurrent_trace_scans: usize,
    pub max_trace_query_runtime: Duration,
    pub max_trace_restore_runtime: Duration,
    /// Live metric series a tenant may hold before a datapoint for an
    /// *unknown* series is refused — the cardinality defence of the M14
    /// degradation ladder. Known series are accepted unconditionally.
    pub max_active_series: usize,
    /// How long a series may go without a new sample before its index state
    /// is evicted (once its samples are flushed). This is the horizon at
    /// which churned-away series return their capacity.
    pub metric_series_idle_timeout: Duration,
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
    /// How often the background loop calls glibc's `malloc_trim(0)`, which
    /// returns free pages from the *middle* of every arena to the kernel —
    /// the fixed trim threshold only releases heap tops, and the second
    /// 24-hour soak measured the difference as an ~130 MiB/hour anonymous
    /// creep with every gauged resident flat (todo.md). `off` disables the
    /// loop; non-glibc builds never start it.
    pub malloc_trim_interval: Option<Duration>,
    /// The declared memory budget the derived ceilings are computed from, or
    /// `None` when budgeting is off. Resolved once in [`Config::from_env`]:
    /// explicit `LOGGYTRACY_MEMORY_BUDGET` bytes, `off`, or — unset — 60% of
    /// the detected limit, which is VictoriaLogs' contract measured to hold
    /// this same workload in half a gigabyte where this engine and Loki were
    /// both OOM-killed.
    pub memory_budget_bytes: Option<u64>,
    /// Where the budget came from, for the startup log — an operator
    /// comparing runs needs to know whether a number was declared, derived,
    /// or absent.
    pub memory_budget_source: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:3100".to_string(),
            otlp_grpc_addr: "127.0.0.1:4317".to_string(),
            data_dir: PathBuf::from("./data"),
            missing_tenant: None,
            max_batch_bytes: 1024 * 1024,
            max_batch_ms: 0,
            max_line_bytes: 256 * 1024,
            max_timestamp_age: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            max_timestamp_skew: Some(Duration::from_secs(60 * 60)),
            max_memtable_bytes: Some(256 * 1024 * 1024),
            max_wal_backlog_bytes: Some(1024 * 1024 * 1024),
            // Eight bodies at the OTLP ceiling. Generous on purpose: the
            // measured in-flight total on the comparison bed was 0.3 MiB, so a
            // tight value here would only refuse traffic no measurement has
            // seen a need to refuse.
            max_inflight_push_bytes: Some(128 * 1024 * 1024),
            wal_compact_min_bytes: Some(64 * 1024 * 1024),
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
            retention_interval: Duration::from_secs(300),
            retention_batch_size: 100,
            retention_grace_period: Duration::from_secs(60 * 60),
            max_retention_runtime: Duration::from_secs(120),
            max_concurrent_queries_per_tenant: 4,
            default_tenant_max_stored_bytes: None,
            min_free_disk_bytes: Some(2 * 1024 * 1024 * 1024),
            log_format: LogFormat::Text,
            disk_sample_interval: Duration::from_secs(10),
            retention_rewrite_threshold: 0.5,
            max_concurrent_tails: 8,
            tail_poll_interval: Duration::from_secs(1),
            max_query_range: None,
            max_query_scan_rows: 5_000_000,
            max_query_scan_bytes: 2 * 1024 * 1024 * 1024,
            max_query_memory_bytes: 512 * 1024 * 1024,
            query_memory_budget_bytes: 512 * 1024 * 1024,
            row_group_cache_max_bytes: Some(256 * 1024 * 1024),
            sidecar_cache_max_bytes: None,
            max_log_limit: 100_000,
            max_histogram_buckets: 10_000,
            max_concurrent_query_scans: 8,
            max_query_runtime: Duration::from_secs(30),
            max_restore_runtime: Duration::from_secs(25),
            max_trace_spans: 100_000,
            max_trace_search_limit: 1_000,
            max_concurrent_trace_scans: 8,
            max_trace_query_runtime: Duration::from_secs(30),
            max_trace_restore_runtime: Duration::from_secs(25),
            // A guess until memprof measures the per-series cost; the memory
            // gate calibrates it before the M14 comparison publishes.
            max_active_series: 500_000,
            metric_series_idle_timeout: Duration::from_secs(600),
            shutdown_flush_warn_after: Duration::from_secs(30),
            startup_retry_budget: Duration::from_secs(300),
            malloc_trim_interval: Some(Duration::from_secs(60)),
            memory_budget_bytes: None,
            memory_budget_source: "off (Config::default)".to_string(),
        }
    }
}

/// What this process is actually allowed to use, and where that came from.
///
/// This fed observability alone for a month, and the history matters because it
/// was a measured rejection: deriving `merge_max_memory_bytes` — that knob only,
/// at 25% of the container, on the pre-streaming-merge build — moved the kill
/// *earlier* and raised the ingest-phase peak by ~290 MiB
/// (`docs/MEMORY_BUDGET_GATE.md`). Smaller groups then meant more merges
/// overlapping ingest, because a group's rewrite materialized it whole.
///
/// Two measurements reopened it (2026-08-08, both in `todo.md`'s soak section
/// and `docs/MEMORY_ATTRIBUTION.md`'s re-measurement). The soak rig showed
/// sustained load OOM-killing this engine *and Loki* at 2 GiB while
/// VictoriaLogs finishes the same workload in 554 MiB — because it detects the
/// cgroup limit and declares 60% of it as its own budget
/// (`vm_allowed_memory_bytes`), which every internal ceiling then fits. And
/// the streaming merge changed what the merge budget bounds: pages and writer
/// state rather than group size, so shrinking it no longer multiplies merges —
/// the failure mode the rejection was about.
///
/// So the detected limit now seeds `LOGGYTRACY_MEMORY_BUDGET`'s default (60%,
/// resolved in [`Config::from_env`]) and the derived ceilings are printed at
/// startup; every explicit knob still overrides its derived value.
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

/// The fraction of the detected limit the engine budgets for itself when no
/// explicit budget is given — VictoriaLogs' number, kept for the same reason
/// it works there: the other 40% absorbs what a live-byte budget cannot see
/// (page cache, allocator retention — anon/live 1.60 with the fixed mmap
/// threshold — thread stacks, and the kernel's own accounting).
const MEMORY_BUDGET_FRACTION_PERCENT: u64 = 60;

/// `LOGGYTRACY_MEMORY_BUDGET` resolved: explicit bytes, or `off`, or — unset —
/// [`MEMORY_BUDGET_FRACTION_PERCENT`] of the detected limit. Pure so the
/// precedence is testable without touching the process environment.
fn resolve_memory_budget(
    env_value: Option<&str>,
    detected: HostMemory,
) -> Result<(Option<u64>, String), String> {
    match env_value {
        Some(raw) => {
            let value = raw.trim();
            if value.is_empty() || value.eq_ignore_ascii_case("off") {
                return Ok((None, "off (LOGGYTRACY_MEMORY_BUDGET)".to_string()));
            }
            let bytes: u64 = value
                .parse()
                .map_err(|error| format!("invalid LOGGYTRACY_MEMORY_BUDGET {value:?}: {error}"))?;
            if bytes == 0 {
                return Err("LOGGYTRACY_MEMORY_BUDGET must be positive, or `off`".to_string());
            }
            Ok((Some(bytes), "LOGGYTRACY_MEMORY_BUDGET".to_string()))
        }
        None => match detected.limit_bytes {
            Some(limit) => Ok((
                Some(limit * MEMORY_BUDGET_FRACTION_PERCENT / 100),
                format!(
                    "{MEMORY_BUDGET_FRACTION_PERCENT}% of {} ({})",
                    limit, detected.source
                ),
            )),
            None => Ok((None, detected.source.to_string())),
        },
    }
}

/// The budgeted ceilings, as the *defaults* the env knobs fall back to — an
/// explicit knob overrides its derived value by construction, because the
/// derivation runs before the environment is read.
///
/// The shares are the re-measured attribution
/// (`docs/MEMORY_ATTRIBUTION.md`, build `b9165b0`): at the coincident live
/// peak merge held 607 MiB, the memtable's real cost 441 (accounted × ~1.73),
/// query + the row-group cache ~490, flush 111, sidecars 128. Nominal shares
/// below sum to 72.5% of the budget; the rest is flush (which rides ingest),
/// the sidecars (unbounded until their eviction lands), and slack for the
/// metering gap. Floors keep a tiny budget from deriving ceilings below what
/// [`Config::validate`] or one reservation chunk requires.
fn derive_defaults_from_budget(defaults: &mut Config, budget_bytes: u64) {
    const MIB: u64 = 1024 * 1024;
    let merge = (budget_bytes / 4).max(64 * MIB);
    defaults.merge_max_memory_bytes = merge;
    defaults.merge_max_input_bytes = (merge / 2).max(32 * MIB);
    let query_pool = (budget_bytes / 4).max(crate::query_memory::RESERVATION_CHUNK_BYTES);
    defaults.query_memory_budget_bytes = query_pool;
    defaults.max_query_memory_bytes = query_pool;
    defaults.row_group_cache_max_bytes = Some((budget_bytes / 8).max(16 * MIB));
    defaults.sidecar_cache_max_bytes = Some((budget_bytes / 10).max(32 * MIB));
    // Accounted bytes; the memtable's resident cost is ~1.73× this
    // (`docs/MEMORY_ATTRIBUTION.md`), so 10% accounted is ~17% real.
    defaults.max_memtable_bytes = Some((budget_bytes / 10).max(32 * MIB));
    // In-flight bodies are not in the 72.5% above: they were outside the
    // accounting entirely until this bound existed, and the attribution
    // measured them at 0.3 MiB. 5% is therefore a ceiling on an outlier rather
    // than a share of anything, floored at one legal OTLP request so a small
    // budget still admits a full-size push without waiting for the empty-server
    // rule to carry it.
    defaults.max_inflight_push_bytes =
        Some((budget_bytes / 20).max(crate::trace_ingest::MAX_OTLP_REQUEST_BYTES as u64));
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let mut defaults = Self::default();
        let host = HostMemory::detect();
        let budget_env = std::env::var("LOGGYTRACY_MEMORY_BUDGET").ok();
        let (memory_budget_bytes, memory_budget_source) =
            resolve_memory_budget(budget_env.as_deref(), host)?;
        if let Some(budget) = memory_budget_bytes {
            derive_defaults_from_budget(&mut defaults, budget);
        }
        let config = Self {
            malloc_trim_interval: env_duration(
                "LOGGYTRACY_MALLOC_TRIM_INTERVAL",
                defaults.malloc_trim_interval,
            )?,
            memory_budget_bytes,
            memory_budget_source,
            listen_addr: env_string("LOGGYTRACY_LISTEN_ADDR", defaults.listen_addr),
            otlp_grpc_addr: env_string("LOGGYTRACY_OTLP_GRPC_ADDR", defaults.otlp_grpc_addr),
            data_dir: std::env::var("LOGGYTRACY_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| defaults.data_dir.clone()),
            missing_tenant: match std::env::var("LOGGYTRACY_MISSING_TENANT") {
                Ok(raw) => Some(
                    TenantId::parse(&raw)
                        .map_err(|error| format!("invalid LOGGYTRACY_MISSING_TENANT: {error}"))?,
                ),
                Err(_) => defaults.missing_tenant.clone(),
            },
            max_batch_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_BATCH_BYTES",
                defaults.max_batch_bytes,
            )?,
            max_batch_ms: env_u64("LOGGYTRACY_MAX_BATCH_MS", defaults.max_batch_ms)?,
            max_line_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_LINE_BYTES",
                defaults.max_line_bytes,
            )?,
            max_timestamp_age: env_duration(
                "LOGGYTRACY_MAX_TIMESTAMP_AGE",
                defaults.max_timestamp_age,
            )?,
            max_memtable_bytes: env_optional_u64(
                "LOGGYTRACY_MAX_MEMTABLE_BYTES",
                defaults.max_memtable_bytes,
            )?,
            max_inflight_push_bytes: env_optional_u64(
                "LOGGYTRACY_MAX_INFLIGHT_PUSH_BYTES",
                defaults.max_inflight_push_bytes,
            )?,
            max_wal_backlog_bytes: env_optional_u64(
                "LOGGYTRACY_MAX_WAL_BACKLOG_BYTES",
                defaults.max_wal_backlog_bytes,
            )?,
            wal_compact_min_bytes: env_optional_u64(
                "LOGGYTRACY_WAL_COMPACT_MIN_BYTES",
                defaults.wal_compact_min_bytes,
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
            max_concurrent_queries_per_tenant: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_QUERIES_PER_TENANT",
                defaults.max_concurrent_queries_per_tenant,
            )?,
            default_tenant_max_stored_bytes: env_optional_u64(
                "LOGGYTRACY_DEFAULT_TENANT_MAX_STORED_BYTES",
                defaults.default_tenant_max_stored_bytes,
            )?,
            min_free_disk_bytes: env_optional_u64(
                "LOGGYTRACY_MIN_FREE_DISK_BYTES",
                defaults.min_free_disk_bytes,
            )?,
            log_format: LogFormat::from_env("LOGGYTRACY_LOG_FORMAT", defaults.log_format)?,
            disk_sample_interval: env_required_duration(
                "LOGGYTRACY_DISK_SAMPLE_INTERVAL",
                defaults.disk_sample_interval,
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
            query_memory_budget_bytes: env_positive_u64(
                "LOGGYTRACY_QUERY_MEMORY_BUDGET_BYTES",
                defaults.query_memory_budget_bytes,
            )?,
            row_group_cache_max_bytes: env_optional_u64(
                "LOGGYTRACY_ROW_GROUP_CACHE_MAX_BYTES",
                defaults.row_group_cache_max_bytes,
            )?,
            sidecar_cache_max_bytes: env_optional_u64(
                "LOGGYTRACY_SIDECAR_CACHE_MAX_BYTES",
                defaults.sidecar_cache_max_bytes,
            )?,
            max_query_memory_bytes: env_positive_u64(
                "LOGGYTRACY_MAX_QUERY_MEMORY_BYTES",
                defaults.max_query_memory_bytes,
            )?,
            max_log_limit: env_positive_usize("LOGGYTRACY_MAX_LOG_LIMIT", defaults.max_log_limit)?,
            max_histogram_buckets: env_positive_usize(
                "LOGGYTRACY_MAX_HISTOGRAM_BUCKETS",
                defaults.max_histogram_buckets,
            )?,
            max_concurrent_query_scans: env_positive_usize(
                "LOGGYTRACY_MAX_CONCURRENT_QUERY_SCANS",
                defaults.max_concurrent_query_scans,
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
            max_active_series: env_positive_usize(
                "LOGGYTRACY_MAX_ACTIVE_SERIES",
                defaults.max_active_series,
            )?,
            metric_series_idle_timeout: env_required_duration(
                "LOGGYTRACY_METRIC_SERIES_IDLE_TIMEOUT",
                defaults.metric_series_idle_timeout,
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
    /// The query term used to be `max_concurrent_query_scans ×
    /// max_query_memory_bytes` — 8 × 512 MiB, four gigabytes no single knob
    /// mentioned and nothing enforced. It is now the shared pool every scan
    /// and metric evaluation reserves from, so the term is a budget the
    /// process actually holds itself to rather than a product it hopes never
    /// multiplies out.
    ///
    /// Still an upper bound rather than an estimate for the whole, and the
    /// log says so.
    pub fn peak_materialized_bytes(&self) -> u64 {
        self.query_memory_budget_bytes
            .saturating_add(self.merge_max_memory_bytes)
    }

    /// Logged once at startup. There is nowhere else an operator learns this.
    pub fn log_memory_budget(&self) {
        let host = HostMemory::detect();
        tracing::info!(
            memory_budget_bytes = self.memory_budget_bytes,
            memory_budget_source = %self.memory_budget_source,
            peak_materialized_bytes = self.peak_materialized_bytes(),
            query_memory_budget_bytes = self.query_memory_budget_bytes,
            row_group_cache_max_bytes = self.row_group_cache_max_bytes,
            sidecar_cache_max_bytes = self.sidecar_cache_max_bytes,
            max_memtable_bytes = self.max_memtable_bytes,
            concurrent_query_scans = self.max_concurrent_query_scans,
            max_query_memory_bytes = self.max_query_memory_bytes,
            merge_max_memory_bytes = self.merge_max_memory_bytes,
            merge_max_input_bytes = self.merge_max_input_bytes,
            flush_chunk_bytes = self.flush_chunk_bytes,
            // Every ceiling above is derived from the budget unless its own
            // knob was set, so without this an operator cannot learn what the
            // process chose or what it read to choose it.
            detected_memory_limit_bytes = host.limit_bytes,
            detected_memory_source = host.source,
            "configured peak materialized memory, excluding allocator retention"
        );
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.listen_addr.trim().is_empty() || self.otlp_grpc_addr.trim().is_empty() {
            return Err("listen and OTLP addresses must not be empty".to_string());
        }
        positive_usize("max_batch_bytes", self.max_batch_bytes)?;
        // Zero is the default and means "do not linger", so this one is not a
        // positive-value knob.
        positive_usize("max_line_bytes", self.max_line_bytes)?;
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
        // A free-space floor below one flush leaves the guard nothing to
        // guard: it would keep accepting until the disk is too full for the
        // flush that makes the accepted data durable.
        if let Some(floor) = self.min_free_disk_bytes
            && floor < self.flush_max_bytes
        {
            return Err(format!(
                "min_free_disk_bytes ({floor}) must not be below flush_max_bytes ({})",
                self.flush_max_bytes
            ));
        }
        positive_duration("disk_sample_interval", self.disk_sample_interval)?;
        positive_duration("flush_check_interval", self.flush_check_interval)?;
        if let Some(bytes) = self.wal_compact_min_bytes {
            positive_u64("wal_compact_min_bytes", bytes)?;
        }
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
        positive_duration("retention_interval", self.retention_interval)?;
        positive_usize("retention_batch_size", self.retention_batch_size)?;
        positive_duration("retention_grace_period", self.retention_grace_period)?;
        positive_duration("max_retention_runtime", self.max_retention_runtime)?;
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
        positive_u64("query_memory_budget_bytes", self.query_memory_budget_bytes)?;
        if let Some(bytes) = self.row_group_cache_max_bytes {
            positive_u64("row_group_cache_max_bytes", bytes)?;
        }
        if let Some(bytes) = self.sidecar_cache_max_bytes {
            positive_u64("sidecar_cache_max_bytes", bytes)?;
        }
        // Smaller than one reservation chunk and the very first admission
        // fails: the pool would refuse every query at any load.
        if self.query_memory_budget_bytes < crate::query_memory::RESERVATION_CHUNK_BYTES {
            return Err(format!(
                "query_memory_budget_bytes ({}) must be at least one reservation chunk ({})",
                self.query_memory_budget_bytes,
                crate::query_memory::RESERVATION_CHUNK_BYTES
            ));
        }
        positive_usize("max_log_limit", self.max_log_limit)?;
        positive_usize("max_histogram_buckets", self.max_histogram_buckets)?;
        positive_usize(
            "max_concurrent_query_scans",
            self.max_concurrent_query_scans,
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
        positive_usize("max_active_series", self.max_active_series)?;
        positive_duration(
            "metric_series_idle_timeout",
            self.metric_series_idle_timeout,
        )?;
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
    #[test]
    fn the_log_format_accepts_both_shapes_and_refuses_a_third() {
        assert_eq!(
            LogFormat::parse("json", LogFormat::Text).unwrap(),
            LogFormat::Json
        );
        assert_eq!(
            LogFormat::parse(" TEXT ", LogFormat::Json).unwrap(),
            LogFormat::Text
        );
        assert_eq!(
            LogFormat::parse("", LogFormat::Json).unwrap(),
            LogFormat::Json,
            "an empty value is an unset one"
        );
        // Refused rather than fallen back on: a deployment that asked for a
        // format it did not get would find out by reading the logs, which is
        // the thing it cannot do.
        assert!(LogFormat::parse("logfmt", LogFormat::Text).is_err());
    }

    use super::*;

    #[test]
    fn the_budget_resolves_env_then_detection_then_off() {
        let detected = HostMemory {
            limit_bytes: Some(2 * 1024 * 1024 * 1024),
            source: "cgroup v2 memory.max",
        };
        // Unset: 60% of the detected limit, source naming what was read.
        let (bytes, source) = resolve_memory_budget(None, detected).unwrap();
        assert_eq!(bytes, Some(2 * 1024 * 1024 * 1024 * 60 / 100));
        assert!(
            source.contains("60%") && source.contains("cgroup"),
            "{source}"
        );
        // Explicit bytes win over detection.
        let (bytes, source) = resolve_memory_budget(Some("1073741824"), detected).unwrap();
        assert_eq!(bytes, Some(1024 * 1024 * 1024));
        assert_eq!(source, "LOGGYTRACY_MEMORY_BUDGET");
        // `off` is off even when a limit was detectable.
        let (bytes, _) = resolve_memory_budget(Some("off"), detected).unwrap();
        assert_eq!(bytes, None);
        // Nothing detected and nothing declared is off, not an error.
        let undetected = HostMemory {
            limit_bytes: None,
            source: "undetected",
        };
        let (bytes, source) = resolve_memory_budget(None, undetected).unwrap();
        assert_eq!(bytes, None);
        assert_eq!(source, "undetected");
        // Zero and garbage are startup errors, not silent defaults.
        assert!(resolve_memory_budget(Some("0"), detected).is_err());
        assert!(resolve_memory_budget(Some("2GiB"), detected).is_err());
    }

    #[test]
    fn the_derived_ceilings_follow_the_measured_shares_and_validate() {
        let mut config = Config::default();
        let budget = 2u64 * 1024 * 1024 * 1024 * 60 / 100;
        derive_defaults_from_budget(&mut config, budget);
        assert_eq!(config.merge_max_memory_bytes, budget / 4);
        assert_eq!(config.merge_max_input_bytes, budget / 8);
        assert_eq!(config.query_memory_budget_bytes, budget / 4);
        assert_eq!(config.max_query_memory_bytes, budget / 4);
        assert_eq!(config.row_group_cache_max_bytes, Some(budget / 8));
        assert_eq!(config.sidecar_cache_max_bytes, Some(budget / 10));
        assert_eq!(config.max_memtable_bytes, Some(budget / 10));
        config.memory_budget_bytes = Some(budget);
        config.validate().unwrap();
    }

    #[test]
    fn a_tiny_budget_derives_floors_a_running_engine_can_live_with() {
        let mut config = Config::default();
        derive_defaults_from_budget(&mut config, 1);
        assert_eq!(config.merge_max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(config.merge_max_input_bytes, 32 * 1024 * 1024);
        assert_eq!(
            config.query_memory_budget_bytes,
            crate::query_memory::RESERVATION_CHUNK_BYTES
        );
        assert_eq!(config.row_group_cache_max_bytes, Some(16 * 1024 * 1024));
        assert_eq!(config.sidecar_cache_max_bytes, Some(32 * 1024 * 1024));
        assert_eq!(config.max_memtable_bytes, Some(32 * 1024 * 1024));
        config.validate().unwrap();
    }

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

    /// The query term used to be `max_concurrent_query_scans ×
    /// max_query_memory_bytes` — a product no knob mentioned and nothing
    /// enforced. It is the shared pool now, so the reported peak is a budget
    /// the process holds itself to, and the scan concurrency no longer
    /// multiplies into it.
    #[test]
    fn the_peak_memory_budget_is_the_pool_plus_the_merge() {
        let config = Config {
            query_memory_budget_bytes: 512 * 1024 * 1024,
            row_group_cache_max_bytes: Some(256 * 1024 * 1024),
            merge_max_memory_bytes: 1024 * 1024 * 1024,
            ..Config::default()
        };
        assert_eq!(
            config.peak_materialized_bytes(),
            1536 * 1024 * 1024,
            "the shared query pool plus one merge"
        );

        // Concurrency does not move the number any more — that was the hole.
        let more_concurrent = Config {
            max_concurrent_query_scans: 16,
            ..config
        };
        assert_eq!(
            more_concurrent.peak_materialized_bytes(),
            1536 * 1024 * 1024
        );
    }

    #[test]
    fn the_type_default_is_budget_off_and_detection_is_sane() {
        // `Config::default()` is what every test and fixture builds on, so it
        // must not depend on the machine it runs on: the budget derivation
        // lives in `from_env` alone. (The old form of this test pinned "the
        // detected limit never changes a default" — that contract was
        // reopened by measurement and by decision, todo.md's soak section,
        // 2026-08-08.)
        let config = Config::default();
        assert_eq!(config.memory_budget_bytes, None);
        assert_eq!(config.merge_max_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.merge_max_input_bytes, 512 * 1024 * 1024);
        // Detection itself must be sane where it works at all.
        let host = HostMemory::detect();
        if let Some(limit) = host.limit_bytes {
            assert!(limit > 0, "a detected limit of zero is a misread");
            assert_ne!(host.source, "undetected");
        }
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
