use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::config::Config;
use crate::part::PartMeta;
use crate::tenant::TenantId;
use crate::trace_part::TracePartMeta;

/// How long one tenant's data is kept.
///
/// A tenant that is *absent* from the control plane's response has no
/// `TenantRetention` at all, which is deliberately different from
/// [`TenantRetention::Infinite`]: both keep the data, but only the second one
/// is an answer the control plane actually gave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantRetention {
    Finite(Duration),
    Infinite,
}

impl TenantRetention {
    fn clamped(self, maximum: Option<Duration>) -> Self {
        match (self, maximum) {
            (Self::Finite(period), Some(maximum)) => Self::Finite(period.min(maximum)),
            (Self::Infinite, Some(maximum)) => Self::Finite(maximum),
            (retention, None) => retention,
        }
    }
}

/// The last policy the control plane served, whole.
pub struct PolicySnapshot {
    retentions: BTreeMap<TenantId, TenantRetention>,
    fetched_at: SystemTime,
    infinite_tenants: usize,
}

impl PolicySnapshot {
    pub fn retention(&self, tenant: &TenantId) -> Option<TenantRetention> {
        self.retentions.get(tenant).copied()
    }

    pub fn tenant_count(&self) -> usize {
        self.retentions.len()
    }

    pub fn infinite_tenant_count(&self) -> usize {
        self.infinite_tenants
    }

    pub fn age(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fetched_at).unwrap_or_default()
    }
}

/// Per-tenant cutoffs resolved against one instant.
///
/// Every decision below reads `meta.json` only: the tenant index already
/// carries each tenant's row-group range, row count and timestamp bounds, so
/// no object body is downloaded to decide what has expired.
#[derive(Clone)]
pub struct Cutoffs {
    snapshot: Arc<PolicySnapshot>,
    now_ns: i64,
}

impl Cutoffs {
    /// Oldest timestamp still retained for `tenant`, or `None` when nothing
    /// expires — an unknown tenant or an explicitly infinite one.
    pub fn cutoff_ns(&self, tenant: &TenantId) -> Option<i64> {
        match self.snapshot.retention(tenant)? {
            TenantRetention::Infinite => None,
            TenantRetention::Finite(period) => Some(
                self.now_ns
                    .saturating_sub(period.as_nanos().min(i64::MAX as u128) as i64),
            ),
        }
    }

    pub fn is_expired(&self, tenant: &TenantId, timestamp_ns: i64) -> bool {
        self.cutoff_ns(tenant)
            .is_some_and(|cutoff_ns| timestamp_ns < cutoff_ns)
    }

    /// Whether every tenant in the part has expired, which is the free
    /// whole-part deletion case.
    pub fn log_part_fully_expired(&self, meta: &PartMeta) -> bool {
        !meta.tenants.is_empty()
            && meta
                .tenants
                .iter()
                .all(|segment| self.is_expired(&segment.tenant, segment.max_ts_ns))
    }

    /// Rows the part holds for tenants whose whole segment has expired.
    pub fn expired_log_rows(&self, meta: &PartMeta) -> u64 {
        meta.tenants
            .iter()
            .filter(|segment| self.is_expired(&segment.tenant, segment.max_ts_ns))
            .map(|segment| segment.row_count)
            .sum()
    }

    /// Expired share of a part's rows, used to decide whether reclaiming them
    /// is worth one rewrite.
    pub fn expired_log_row_fraction(&self, meta: &PartMeta) -> f64 {
        if meta.row_count == 0 {
            return 0.0;
        }
        self.expired_log_rows(meta) as f64 / meta.row_count as f64
    }

    /// Trace tenant segments carry no timestamps of their own, so a segment's
    /// bound comes from the row groups it owns.
    pub fn trace_part_fully_expired(&self, meta: &TracePartMeta) -> bool {
        if meta.tenants.is_empty() {
            return false;
        }
        meta.tenants.iter().all(|segment| {
            let groups = segment.row_group_start as usize..segment.row_group_end as usize;
            let segment_max = meta
                .row_group_max_ts
                .get(groups)
                .and_then(|bounds| bounds.iter().max().copied())
                .unwrap_or(meta.max_ts_ns);
            self.is_expired(&segment.tenant, segment_max)
        })
    }
}

#[derive(Default)]
pub struct TenantPolicyMetrics {
    pub fetch_success: AtomicU64,
    pub fetch_errors: AtomicU64,
    pub fetch_latency_ns: AtomicU64,
}

/// The tenant→retention map, polled from a configured control-plane endpoint.
///
/// The endpoint is never on the ingest or query hot path: writes ignore it
/// entirely, and reads fail open when there is no snapshot. A control plane
/// that is down therefore costs storage, never availability.
pub struct TenantPolicy {
    settings: Option<PolicySettings>,
    snapshot: RwLock<Option<Arc<PolicySnapshot>>>,
    client: Option<reqwest::Client>,
    pub metrics: TenantPolicyMetrics,
}

struct PolicySettings {
    url: String,
    max_bytes: usize,
    max_retention: Option<Duration>,
    auth_header: Option<(String, String)>,
}

#[derive(Deserialize)]
struct PolicyResponse {
    tenants: BTreeMap<String, String>,
}

impl TenantPolicy {
    /// A policy that is switched off: every tenant keeps everything, and no
    /// request is ever made.
    pub fn disabled() -> Self {
        Self {
            settings: None,
            snapshot: RwLock::new(None),
            client: None,
            metrics: TenantPolicyMetrics::default(),
        }
    }

    pub fn from_config(config: &Config) -> Result<Self, String> {
        let Some(url) = config.tenant_policy_url.clone() else {
            return Ok(Self::disabled());
        };
        let auth_header = config
            .tenant_policy_auth_header
            .as_deref()
            .map(parse_auth_header)
            .transpose()?;
        let client = reqwest::Client::builder()
            .timeout(config.tenant_policy_timeout)
            .build()
            .map_err(|error| format!("failed to build the tenant policy HTTP client: {error}"))?;
        Ok(Self {
            settings: Some(PolicySettings {
                url,
                max_bytes: config.tenant_policy_max_bytes,
                max_retention: config.max_tenant_retention,
                auth_header,
            }),
            snapshot: RwLock::new(None),
            client: Some(client),
            metrics: TenantPolicyMetrics::default(),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.settings.is_some()
    }

    pub fn snapshot(&self) -> Option<Arc<PolicySnapshot>> {
        self.snapshot.read().unwrap().clone()
    }

    /// Cutoffs for one instant, or `None` when per-tenant retention is off or
    /// no successful fetch has happened yet. `None` must always mean "delete
    /// nothing": a control plane that is down at boot cannot cause deletion.
    pub fn cutoffs_at(&self, now_ns: i64) -> Option<Cutoffs> {
        if !self.is_enabled() {
            return None;
        }
        Some(Cutoffs {
            snapshot: self.snapshot()?,
            now_ns,
        })
    }

    pub fn cutoffs_now(&self) -> Option<Cutoffs> {
        self.cutoffs_at(now_ns())
    }

    /// Oldest timestamp `tenant` may still read. `None` leaves the requested
    /// range untouched, which is the fail-open behaviour every read path wants
    /// when the control plane has said nothing about this tenant.
    pub fn query_floor_ns(&self, tenant: &TenantId) -> Option<i64> {
        self.cutoffs_now()?.cutoff_ns(tenant)
    }

    /// Apply the retention floor to a requested range start.
    pub fn clamp_start_ns(&self, tenant: &TenantId, start_ns: i64) -> i64 {
        match self.query_floor_ns(tenant) {
            Some(floor_ns) => start_ns.max(floor_ns),
            None => start_ns,
        }
    }

    pub async fn refresh(&self) -> Result<(), String> {
        let Some(settings) = &self.settings else {
            return Ok(());
        };
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| "tenant policy client is missing".to_string())?;
        let started = Instant::now();
        let result = fetch_snapshot(client, settings).await;
        crate::metrics::RuntimeMetrics::add_duration(
            &self.metrics.fetch_latency_ns,
            started.elapsed(),
        );
        match result {
            Ok(snapshot) => {
                *self.snapshot.write().unwrap() = Some(Arc::new(snapshot));
                self.metrics.fetch_success.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                // The previous snapshot stays intact. It is the last known
                // policy, and applying it is more correct than applying none.
                self.metrics.fetch_errors.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub fn install_for_test(&self, retentions: BTreeMap<TenantId, TenantRetention>) {
        let infinite_tenants = retentions
            .values()
            .filter(|retention| matches!(retention, TenantRetention::Infinite))
            .count();
        *self.snapshot.write().unwrap() = Some(Arc::new(PolicySnapshot {
            retentions,
            fetched_at: SystemTime::now(),
            infinite_tenants,
        }));
    }

    #[cfg(test)]
    pub fn enabled_for_test() -> Self {
        Self {
            settings: Some(PolicySettings {
                url: "http://127.0.0.1:1/policy".to_string(),
                max_bytes: 8 * 1024 * 1024,
                max_retention: None,
                auth_header: None,
            }),
            snapshot: RwLock::new(None),
            client: None,
            metrics: TenantPolicyMetrics::default(),
        }
    }
}

pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

async fn fetch_snapshot(
    client: &reqwest::Client,
    settings: &PolicySettings,
) -> Result<PolicySnapshot, String> {
    let mut request = client.get(&settings.url);
    if let Some((name, value)) = &settings.auth_header {
        request = request.header(name, value);
    }
    let mut response = request
        .send()
        .await
        .map_err(|error| format!("tenant policy request failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "tenant policy endpoint returned {}",
            response.status()
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > settings.max_bytes as u64)
    {
        return Err(format!(
            "tenant policy response declares more than {} bytes",
            settings.max_bytes
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("tenant policy response read failed: {error}"))?
    {
        if body.len().saturating_add(chunk.len()) > settings.max_bytes {
            return Err(format!(
                "tenant policy response exceeds {} bytes",
                settings.max_bytes
            ));
        }
        body.extend_from_slice(&chunk);
    }
    parse_snapshot(&body, settings.max_retention)
}

/// Parse a control-plane response. Untrusted input throughout.
///
/// A malformed *value* rejects the whole response, so a truncated or
/// half-written body can never be applied in part. A malformed *tenant key* is
/// dropped instead: it can never name a real tenant, so keeping the rest of an
/// otherwise well-formed policy loses nothing.
pub fn parse_snapshot(
    body: &[u8],
    max_retention: Option<Duration>,
) -> Result<PolicySnapshot, String> {
    let response: PolicyResponse = serde_json::from_slice(body)
        .map_err(|error| format!("tenant policy response is not valid JSON: {error}"))?;
    let mut retentions = BTreeMap::new();
    let mut infinite_tenants = 0usize;
    for (raw_tenant, raw_retention) in response.tenants {
        let tenant = match TenantId::parse(&raw_tenant) {
            Ok(tenant) => tenant,
            Err(error) => {
                tracing::warn!(
                    tenant = %raw_tenant,
                    %error,
                    "dropping a tenant policy entry with an invalid tenant id"
                );
                continue;
            }
        };
        let retention = parse_retention(&raw_retention).map_err(|error| {
            format!("invalid retention {raw_retention:?} for tenant {raw_tenant}: {error}")
        })?;
        if matches!(retention, TenantRetention::Infinite) {
            infinite_tenants += 1;
        }
        retentions.insert(tenant, retention.clamped(max_retention));
    }
    Ok(PolicySnapshot {
        retentions,
        fetched_at: SystemTime::now(),
        infinite_tenants,
    })
}

/// Prometheus-style duration, or the literal `infinite`.
fn parse_retention(raw: &str) -> Result<TenantRetention, String> {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("infinite") {
        return Ok(TenantRetention::Infinite);
    }
    if value == "0" {
        // Expires everything for this tenant. Honoured because it falls out of
        // the arithmetic, not because prompt deletion is a feature here.
        return Ok(TenantRetention::Finite(Duration::ZERO));
    }
    let (number, unit_nanos) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_000_000u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('d') {
        (number, 24 * 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('w') {
        (number, 7 * 24 * 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('y') {
        (number, 365 * 24 * 60 * 60 * 1_000_000_000u64)
    } else {
        return Err(
            "expected a duration such as 90m, 24h, 7d, or the literal \"infinite\"".to_string(),
        );
    };
    let number: u64 = number.trim().parse().map_err(|error| format!("{error}"))?;
    let nanos = number
        .checked_mul(unit_nanos)
        .ok_or_else(|| "duration overflow".to_string())?;
    Ok(TenantRetention::Finite(Duration::from_nanos(nanos)))
}

/// A single `Name: value` string, as configured. `Config::validate` calls this
/// too, so a malformed header is rejected with the rest of the configuration
/// instead of surfacing later as a failed `TenantPolicy::from_config`.
pub fn parse_auth_header(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw.split_once(':').ok_or_else(|| {
        "LOGGYTRACY_TENANT_POLICY_AUTH_HEADER must be a single \"Name: value\" string".to_string()
    })?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() || value.is_empty() {
        return Err(
            "LOGGYTRACY_TENANT_POLICY_AUTH_HEADER must have a non-empty name and value".to_string(),
        );
    }
    name.parse::<reqwest::header::HeaderName>()
        .map_err(|error| format!("invalid tenant policy auth header name: {error}"))?;
    value
        .parse::<reqwest::header::HeaderValue>()
        .map_err(|error| format!("invalid tenant policy auth header value: {error}"))?;
    Ok((name.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(raw: &str) -> TenantId {
        TenantId::parse(raw).expect("valid tenant id")
    }

    #[test]
    fn parses_prometheus_durations_and_the_infinite_literal() {
        assert_eq!(
            parse_retention("30d").unwrap(),
            TenantRetention::Finite(Duration::from_secs(30 * 24 * 60 * 60))
        );
        assert_eq!(
            parse_retention("90m").unwrap(),
            TenantRetention::Finite(Duration::from_secs(90 * 60))
        );
        assert_eq!(
            parse_retention("infinite").unwrap(),
            TenantRetention::Infinite
        );
        assert_eq!(
            parse_retention("0").unwrap(),
            TenantRetention::Finite(Duration::ZERO)
        );
        assert!(parse_retention("7").is_err());
        assert!(parse_retention("forever").is_err());
        assert!(parse_retention("-1d").is_err());
    }

    #[test]
    fn an_invalid_tenant_id_is_dropped_but_an_invalid_value_rejects_the_whole_response() {
        let body = br#"{"tenants":{"acme":"30d","../etc":"7d"}}"#;
        let snapshot = parse_snapshot(body, None).unwrap();
        assert_eq!(snapshot.tenant_count(), 1);
        assert_eq!(
            snapshot.retention(&tenant("acme")),
            Some(TenantRetention::Finite(Duration::from_secs(
                30 * 24 * 60 * 60
            )))
        );

        let body = br#"{"tenants":{"acme":"30d","hobby":"soon"}}"#;
        assert!(parse_snapshot(body, None).is_err());
        assert!(parse_snapshot(b"{\"tenants\":", None).is_err());
    }

    #[test]
    fn retention_values_are_clamped_including_infinite() {
        let body = br#"{"tenants":{"acme":"30d","intern":"infinite"}}"#;
        let maximum = Duration::from_secs(7 * 24 * 60 * 60);
        let snapshot = parse_snapshot(body, Some(maximum)).unwrap();
        assert_eq!(
            snapshot.retention(&tenant("acme")),
            Some(TenantRetention::Finite(maximum))
        );
        assert_eq!(
            snapshot.retention(&tenant("intern")),
            Some(TenantRetention::Finite(maximum))
        );
        // The distinction survives for metrics even after clamping.
        assert_eq!(snapshot.infinite_tenant_count(), 1);
    }

    #[test]
    fn an_unknown_tenant_never_expires_and_a_missing_snapshot_deletes_nothing() {
        let policy = TenantPolicy::enabled_for_test();
        assert!(policy.cutoffs_at(1_000).is_none());
        assert_eq!(policy.query_floor_ns(&tenant("acme")), None);

        policy.install_for_test(
            [(
                tenant("acme"),
                TenantRetention::Finite(Duration::from_nanos(100)),
            )]
            .into_iter()
            .collect(),
        );
        let cutoffs = policy.cutoffs_at(1_000).unwrap();
        assert_eq!(cutoffs.cutoff_ns(&tenant("acme")), Some(900));
        assert!(cutoffs.is_expired(&tenant("acme"), 899));
        assert!(!cutoffs.is_expired(&tenant("acme"), 900));
        // Unknown: no cutoff, never expired.
        assert_eq!(cutoffs.cutoff_ns(&tenant("hobby")), None);
        assert!(!cutoffs.is_expired(&tenant("hobby"), i64::MIN));
    }

    #[test]
    fn a_disabled_policy_never_expires_anything() {
        let policy = TenantPolicy::disabled();
        assert!(!policy.is_enabled());
        assert!(policy.cutoffs_now().is_none());
        assert_eq!(policy.clamp_start_ns(&tenant("acme"), 5), 5);
    }

    /// Serve one fixed response on a loopback port and return its URL.
    async fn serve(
        status: axum::http::StatusCode,
        body: &'static str,
        require_auth: bool,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/policy",
            axum::routing::get(move |headers: axum::http::HeaderMap| async move {
                if require_auth
                    && headers
                        .get("Authorization")
                        .and_then(|value| value.to_str().ok())
                        != Some("Bearer secret")
                {
                    return (axum::http::StatusCode::UNAUTHORIZED, "no".to_string());
                }
                (status, body.to_string())
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{address}/policy")
    }

    fn policy_for(url: String, configure: impl FnOnce(&mut Config)) -> TenantPolicy {
        let mut config = Config {
            tenant_policy_url: Some(url),
            retention_period: None,
            ..Config::default()
        };
        configure(&mut config);
        TenantPolicy::from_config(&config).expect("policy builds")
    }

    #[tokio::test]
    async fn a_successful_fetch_installs_the_snapshot() {
        let url = serve(
            axum::http::StatusCode::OK,
            r#"{"tenants":{"acme":"30d","intern":"infinite"}}"#,
            false,
        )
        .await;
        let policy = policy_for(url, |_| {});

        policy.refresh().await.unwrap();

        let snapshot = policy.snapshot().expect("snapshot installed");
        assert_eq!(snapshot.tenant_count(), 2);
        assert_eq!(
            snapshot.retention(&tenant("acme")),
            Some(TenantRetention::Finite(Duration::from_secs(
                30 * 24 * 60 * 60
            )))
        );
        assert_eq!(policy.metrics.fetch_success.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn malformed_oversized_and_unauthorised_responses_leave_the_snapshot_intact() {
        let good = serve(
            axum::http::StatusCode::OK,
            r#"{"tenants":{"acme":"30d"}}"#,
            false,
        )
        .await;
        let policy = policy_for(good, |_| {});
        policy.refresh().await.unwrap();
        let installed = policy.snapshot().unwrap();

        for (url, label) in [
            (
                serve(
                    axum::http::StatusCode::OK,
                    r#"{"tenants":{"acme":"soon"}}"#,
                    false,
                )
                .await,
                "malformed",
            ),
            (
                serve(axum::http::StatusCode::UNAUTHORIZED, "denied", false).await,
                "unauthorised",
            ),
            (
                serve(axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom", false).await,
                "server error",
            ),
        ] {
            let broken = policy_for(url, |_| {});
            broken.install_for_test(
                [(
                    tenant("acme"),
                    TenantRetention::Finite(Duration::from_secs(1)),
                )]
                .into_iter()
                .collect(),
            );
            let before = broken.snapshot().unwrap();
            assert!(broken.refresh().await.is_err(), "{label} must fail");
            assert!(
                Arc::ptr_eq(&before, &broken.snapshot().unwrap()),
                "{label} must leave the previous snapshot in place"
            );
            assert_eq!(broken.metrics.fetch_errors.load(Ordering::Relaxed), 1);
        }

        let oversized = policy_for(
            serve(
                axum::http::StatusCode::OK,
                r#"{"tenants":{"acme":"30d","beta":"7d"}}"#,
                false,
            )
            .await,
            |config| config.tenant_policy_max_bytes = 8,
        );
        assert!(oversized.refresh().await.is_err());

        // The first policy's own snapshot is untouched throughout.
        assert!(Arc::ptr_eq(&installed, &policy.snapshot().unwrap()));
    }

    #[tokio::test]
    async fn the_configured_auth_header_is_sent() {
        let url = serve(
            axum::http::StatusCode::OK,
            r#"{"tenants":{"acme":"30d"}}"#,
            true,
        )
        .await;

        let unauthenticated = policy_for(url.clone(), |_| {});
        assert!(unauthenticated.refresh().await.is_err());

        let authenticated = policy_for(url, |config| {
            config.tenant_policy_auth_header = Some("Authorization: Bearer secret".to_string());
        });
        authenticated.refresh().await.unwrap();
        assert_eq!(authenticated.snapshot().unwrap().tenant_count(), 1);
    }

    #[test]
    fn parses_the_auth_header_and_rejects_malformed_ones() {
        assert_eq!(
            parse_auth_header("Authorization: Bearer token").unwrap(),
            ("Authorization".to_string(), "Bearer token".to_string())
        );
        assert!(parse_auth_header("Authorization").is_err());
        assert!(parse_auth_header("Authorization: ").is_err());
        assert!(parse_auth_header("Bad Name: value").is_err());
    }
}
