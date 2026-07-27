use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::http::StatusCode;

use crate::backpressure::IngestError;
use crate::clock::Clock;
use crate::config::Config;
use crate::metrics::RuntimeMetrics;
use crate::tenant::TenantId;
use crate::tenant_policy::{TenantIngestRate, TenantPolicy};

/// Enforces the per-tenant ingest rate the control plane pushed.
///
/// This is the half of a quota an instance can answer for. The plan sells a
/// monthly volume, but a month is platform-wide state that outlives any
/// instance and can be spent across several of them, so the control plane owns
/// that number and this owns the share of *this* process one tenant may take.
/// The two do not overlap: exceeding a monthly budget is a billing decision,
/// and exceeding a rate is a neighbour taking flush capacity from the tenants
/// beside it.
///
/// The buckets live in memory and a restart refills them. That is the honest
/// state for a rate — a token bucket describes the last few seconds, and there
/// is nothing about the last few seconds worth surviving a process. A monthly
/// budget could not be held this way, which is precisely why it is not held
/// here.
pub struct TenantQuota {
    config: Arc<Config>,
    clock: Arc<Clock>,
    metrics: Arc<RuntimeMetrics>,
    policy: Arc<TenantPolicy>,
    buckets: Mutex<HashMap<TenantId, Bucket>>,
    checks: std::sync::atomic::AtomicU64,
}

struct Bucket {
    available: f64,
    /// Kept per bucket rather than recomputed during a sweep: rates are
    /// per-tenant, so one tenant's capacity says nothing about another's.
    capacity: f64,
    refill_per_ns: f64,
    updated_ns: i64,
}

/// Buckets are dropped once they have been idle long enough to have refilled,
/// because a full bucket permits everything its tenant could ask for and is
/// therefore indistinguishable from having no entry at all. Without this the
/// map would be a per-tenant allocation that only ever grows, on a path where
/// the tenant id comes from a request header.
const SWEEP_EVERY: u64 = 1024;

impl TenantQuota {
    pub fn new(
        config: Arc<Config>,
        clock: Arc<Clock>,
        metrics: Arc<RuntimeMetrics>,
        policy: Arc<TenantPolicy>,
    ) -> Self {
        Self {
            config,
            clock,
            metrics,
            policy,
            buckets: Mutex::new(HashMap::new()),
            checks: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// A quota with no policy behind it, for tests exercising something else.
    /// Unlimited unless the configuration sets a default rate, which is the
    /// same answer production gives when the control plane is silent.
    #[cfg(test)]
    pub fn for_test(config: &Config) -> Arc<Self> {
        Arc::new(Self::new(
            Arc::new(config.clone()),
            Clock::system(),
            Arc::new(RuntimeMetrics::new()),
            Arc::new(TenantPolicy::disabled()),
        ))
    }

    /// Charge `bytes` to `tenant`, or refuse.
    ///
    /// Called with the size of the request as it arrived on the wire and
    /// before it is decompressed or decoded, so a tenant over its rate cannot
    /// spend this instance's CPU on a body that will not be accepted.
    pub fn check(&self, tenant: &TenantId, bytes: u64) -> Result<(), IngestError> {
        let rate = match self.resolve(tenant) {
            TenantIngestRate::Unlimited => return Ok(()),
            TenantIngestRate::BytesPerSecond(rate) => rate,
        };
        if rate == 0 {
            return Err(self.refused(format!(
                "tenant {tenant} is not permitted to write: its ingest rate is 0"
            )));
        }
        let capacity = self.capacity(rate);
        let refill_per_ns = rate as f64 / 1e9;
        let now_ns = self.clock.now_ns();

        let mut buckets = self.buckets.lock().unwrap();
        let bucket = buckets.entry(tenant.clone()).or_insert(Bucket {
            available: capacity,
            capacity,
            refill_per_ns,
            updated_ns: now_ns,
        });
        // The rate can change under a running bucket, because a push applies
        // immediately. Take the new one and clamp what was banked to it.
        bucket.capacity = capacity;
        bucket.refill_per_ns = refill_per_ns;
        // A clock that moved backwards refills nothing rather than draining the
        // bucket by a negative amount.
        let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
        bucket.available = (bucket.available + elapsed_ns as f64 * refill_per_ns).min(capacity);
        bucket.updated_ns = now_ns;

        if bucket.available < bytes as f64 {
            let missing = bytes as f64 - bucket.available;
            let retry_after = Duration::from_secs_f64((missing / rate as f64).clamp(0.0, 3600.0));
            drop(buckets);
            return Err(self.refused_with_retry(
                format!(
                    "tenant {tenant} is over its ingest rate of {rate} bytes/s; \
this request needs {bytes} bytes of budget"
                ),
                retry_after,
            ));
        }
        bucket.available -= bytes as f64;
        drop(buckets);

        self.maybe_sweep();
        Ok(())
    }

    /// The same decision for OTLP, whose exporters read a status code rather
    /// than a header.
    pub fn check_grpc(&self, tenant: &TenantId, bytes: u64) -> Result<(), tonic::Status> {
        self.check(tenant, bytes)
            .map_err(|error| tonic::Status::resource_exhausted(error.message))
    }

    /// The rate in force for one tenant: what the control plane pushed, or the
    /// configured default when it has said nothing. A tenant the control plane
    /// does not know is treated the same whether per-tenant policy is off or
    /// simply silent about it.
    fn resolve(&self, tenant: &TenantId) -> TenantIngestRate {
        if let Some(rate) = self.policy.ingest_rate(tenant) {
            return rate;
        }
        match self.config.default_tenant_ingest_bytes_per_second {
            Some(rate) => TenantIngestRate::BytesPerSecond(rate),
            None => TenantIngestRate::Unlimited,
        }
    }

    /// Bucket size: a burst of banked rate, but never smaller than one legal
    /// request. A capacity below `max_push_bytes` would reject a request of
    /// that size no matter how long the client waited, turning a rate limit
    /// into a permanent refusal.
    fn capacity(&self, rate: u64) -> f64 {
        let burst = rate as f64 * self.config.tenant_ingest_burst.as_secs_f64();
        burst.max(self.config.max_push_bytes as f64)
    }

    fn maybe_sweep(&self) {
        let previous = self.checks.fetch_add(1, Ordering::Relaxed);
        if !(previous + 1).is_multiple_of(SWEEP_EVERY) {
            return;
        }
        let now_ns = self.clock.now_ns();
        let mut buckets = self.buckets.lock().unwrap();
        buckets.retain(|_, bucket| {
            let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
            // Refilled to capacity by now, so dropping it changes no decision.
            let refilled = bucket.available + elapsed_ns as f64 * bucket.refill_per_ns;
            refilled < bucket.capacity
        });
    }

    fn refused(&self, message: String) -> IngestError {
        self.refused_with_retry(message, self.config.backpressure_retry_after)
    }

    fn refused_with_retry(&self, message: String, retry_after: Duration) -> IngestError {
        // Counted apart from `ingest_throttled`, which says this instance is
        // behind. This one says the instance is fine and the tenant is over
        // what it was sold — a different question with a different answer.
        self.metrics
            .ingest_quota_rejected
            .fetch_add(1, Ordering::Relaxed);
        IngestError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message,
            retry_after: Some(retry_after),
        }
    }
}

#[cfg(test)]
mod tests {
    include!("tests/tenant_quota.rs");
}
