use std::path::PathBuf;
use std::time::Duration;

use crate::tenant::{MissingTenantPolicy, TenantId};

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    pub otlp_grpc_addr: String,
    pub data_dir: PathBuf,
    /// Tenant a request is attributed to when it carries no `X-Scope-OrgID`
    /// and `missing_tenant_policy` accepts it.
    pub default_tenant: TenantId,
    pub missing_tenant_policy: MissingTenantPolicy,
    pub max_batch_bytes: usize,
    pub max_batch_ms: u64,
    /// Largest accepted compressed push body. Also bounds the length a snappy
    /// header may declare, so a tiny body cannot drive a huge allocation.
    pub max_push_bytes: usize,
    pub max_decompressed_push_bytes: usize,
    pub max_line_bytes: usize,
    pub max_label_names_per_stream: usize,
    pub max_label_name_bytes: usize,
    pub max_label_value_bytes: usize,
    /// How far behind and ahead of the server clock an entry timestamp may be.
    /// Timestamps outside the window create day partitions that retention can
    /// never expire, so they are rejected at ingest.
    pub max_timestamp_age: Option<Duration>,
    pub max_timestamp_skew: Option<Duration>,
    pub flush_max_bytes: u64,
    pub flush_max_interval: Duration,
    pub flush_check_interval: Duration,
    pub row_group_size: usize,
    pub merge_min_part_count: usize,
    pub merge_target_part_rows: u64,
    pub merge_max_part_rows: u64,
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
    pub retention_period: Option<Duration>,
    pub retention_interval: Duration,
    pub retention_batch_size: usize,
    pub retention_grace_period: Duration,
    pub max_retention_runtime: Duration,
    /// Control-plane endpoint serving the tenant→retention map. When unset,
    /// retention is exactly the global `retention_period` behaviour; when set,
    /// the fetched snapshot is the sole authority. Setting both is a
    /// validation error rather than a silently ignored setting.
    pub tenant_policy_url: Option<String>,
    pub tenant_policy_interval: Duration,
    pub tenant_policy_timeout: Duration,
    pub tenant_policy_auth_header: Option<String>,
    pub tenant_policy_max_bytes: usize,
    pub max_tenant_retention: Option<Duration>,
    /// Expired share of a part's rows that justifies one rewrite through
    /// merge. Below it the rows stay on disk, already invisible to queries.
    pub retention_rewrite_threshold: f64,
    pub max_query_range: Option<Duration>,
    pub max_query_scan_rows: usize,
    pub max_query_scan_bytes: u64,
    pub max_query_memory_bytes: u64,
    pub max_log_limit: usize,
    pub max_metric_evaluation_points: usize,
    pub max_metric_rows: usize,
    pub max_metric_series: usize,
    pub max_metric_samples: usize,
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:3100".to_string(),
            otlp_grpc_addr: "0.0.0.0:4317".to_string(),
            data_dir: PathBuf::from("./data"),
            default_tenant: TenantId::parse("default")
                .expect("the built-in default tenant is valid"),
            missing_tenant_policy: MissingTenantPolicy::UseDefault,
            max_batch_bytes: 1024 * 1024,
            max_batch_ms: 200,
            max_push_bytes: 16 * 1024 * 1024,
            max_decompressed_push_bytes: 64 * 1024 * 1024,
            max_line_bytes: 256 * 1024,
            max_label_names_per_stream: 30,
            max_label_name_bytes: 1024,
            max_label_value_bytes: 2048,
            max_timestamp_age: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            max_timestamp_skew: Some(Duration::from_secs(60 * 60)),
            flush_max_bytes: 1024 * 1024,
            flush_max_interval: Duration::from_secs(5),
            flush_check_interval: Duration::from_millis(500),
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
            tenant_policy_url: None,
            tenant_policy_interval: Duration::from_secs(300),
            tenant_policy_timeout: Duration::from_secs(10),
            tenant_policy_auth_header: None,
            tenant_policy_max_bytes: 8 * 1024 * 1024,
            max_tenant_retention: None,
            retention_rewrite_threshold: 0.5,
            max_query_range: None,
            max_query_scan_rows: 5_000_000,
            max_query_scan_bytes: 2 * 1024 * 1024 * 1024,
            max_query_memory_bytes: 512 * 1024 * 1024,
            max_log_limit: 100_000,
            max_metric_evaluation_points: 10_000,
            max_metric_rows: 1_000_000,
            max_metric_series: 100_000,
            max_metric_samples: 5_000_000,
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
        }
    }
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
            max_batch_ms: env_positive_u64("LOGGYTRACY_MAX_BATCH_MS", defaults.max_batch_ms)?,
            max_push_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_PUSH_BYTES",
                defaults.max_push_bytes,
            )?,
            max_decompressed_push_bytes: env_positive_usize(
                "LOGGYTRACY_MAX_DECOMPRESSED_PUSH_BYTES",
                defaults.max_decompressed_push_bytes,
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
            tenant_policy_url: std::env::var("LOGGYTRACY_TENANT_POLICY_URL")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            tenant_policy_interval: env_required_duration(
                "LOGGYTRACY_TENANT_POLICY_INTERVAL",
                defaults.tenant_policy_interval,
            )?,
            tenant_policy_timeout: env_required_duration(
                "LOGGYTRACY_TENANT_POLICY_TIMEOUT",
                defaults.tenant_policy_timeout,
            )?,
            tenant_policy_auth_header: std::env::var("LOGGYTRACY_TENANT_POLICY_AUTH_HEADER")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            tenant_policy_max_bytes: env_positive_usize(
                "LOGGYTRACY_TENANT_POLICY_MAX_BYTES",
                defaults.tenant_policy_max_bytes,
            )?,
            max_tenant_retention: env_duration("LOGGYTRACY_MAX_TENANT_RETENTION", None)?,
            retention_rewrite_threshold: env_value(
                "LOGGYTRACY_RETENTION_REWRITE_THRESHOLD",
                defaults.retention_rewrite_threshold,
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
            shutdown_flush_warn_after: env_required_duration(
                "LOGGYTRACY_SHUTDOWN_FLUSH_WARN_AFTER",
                defaults.shutdown_flush_warn_after,
            )?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.listen_addr.trim().is_empty() || self.otlp_grpc_addr.trim().is_empty() {
            return Err("listen and OTLP addresses must not be empty".to_string());
        }
        positive_usize("max_batch_bytes", self.max_batch_bytes)?;
        positive_u64("max_batch_ms", self.max_batch_ms)?;
        positive_usize("max_push_bytes", self.max_push_bytes)?;
        positive_usize(
            "max_decompressed_push_bytes",
            self.max_decompressed_push_bytes,
        )?;
        if self.max_decompressed_push_bytes < self.max_push_bytes {
            return Err("max_decompressed_push_bytes must not be below max_push_bytes".to_string());
        }
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
        positive_duration("flush_check_interval", self.flush_check_interval)?;
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
        if self.tenant_policy_url.is_some() && self.retention_period.is_some() {
            return Err(
                "LOGGYTRACY_RETENTION_PERIOD and LOGGYTRACY_TENANT_POLICY_URL are mutually \
exclusive: per-tenant retention replaces the global period"
                    .to_string(),
            );
        }
        positive_duration("tenant_policy_interval", self.tenant_policy_interval)?;
        positive_duration("tenant_policy_timeout", self.tenant_policy_timeout)?;
        positive_usize("tenant_policy_max_bytes", self.tenant_policy_max_bytes)?;
        if let Some(maximum) = self.max_tenant_retention {
            positive_duration("max_tenant_retention", maximum)?;
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

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    env_value(name, default)
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
            tenant_policy_url: Some("https://control-plane/policy".to_string()),
            ..Config::default()
        };
        assert!(config.validate().is_ok());

        // Silently ignoring one of the two would be the worst outcome, so it
        // fails at startup instead.
        config.retention_period = Some(Duration::from_secs(3600));
        assert!(config.validate().is_err());

        config.tenant_policy_url = None;
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
