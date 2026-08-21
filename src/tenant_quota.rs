use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::http::StatusCode;

use crate::backpressure::IngestError;
use crate::clock::Clock;
use crate::config::Config;
use crate::metrics::RuntimeMetrics;
use crate::tenant::TenantId;
use crate::tenant_policy::{TenantIngestRate, TenantPolicy, TenantQueryRate, TenantStorageLimit};

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
    /// Held rather than passed in per call, so a storage limit cannot be
    /// enforced on one ingest path and forgotten on another — which is the
    /// failure the two transports were split apart to prevent.
    parts: Arc<crate::part_registry::PartRegistry>,
    trace_parts: Arc<crate::trace_registry::TraceRegistry>,
    buckets: Mutex<HashMap<TenantId, Bucket>>,
    /// The read side, kept in its own map so a tenant reading hard cannot
    /// spend the budget that decides whether its writes are accepted. They are
    /// separate resources and a shared bucket would couple them.
    scan_buckets: Mutex<HashMap<TenantId, Bucket>>,
    /// Queries a tenant currently has in flight.
    ///
    /// The rate bucket bounds total work over time; this bounds work happening
    /// at once. Without it one tenant issuing many concurrent scans takes every
    /// permit of the shared query semaphore, and the other tenants queue behind
    /// it however small their queries are.
    in_flight: Mutex<HashMap<TenantId, u32>>,
    checks: std::sync::atomic::AtomicU64,
}

/// Releases a tenant's in-flight slot however the query ends, including a
/// cancelled future. A leaked slot is permanent: the tenant loses that much
/// concurrency for the life of the process.
pub struct QuerySlot {
    quota: Arc<TenantQuota>,
    tenant: TenantId,
}

impl std::fmt::Debug for QuerySlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuerySlot")
            .field("tenant", &self.tenant)
            .finish()
    }
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        let mut in_flight = self.quota.in_flight.lock();
        if let Some(count) = in_flight.get_mut(&self.tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                in_flight.remove(&self.tenant);
            }
        }
    }
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

/// What a client is told to wait after being refused for storage.
///
/// Not a computed time-to-clear: what frees the space is retention retiring
/// parts, whose timing depends on the tenant's own retention period and on when
/// a merge tick reaches those parts. Nothing here can turn that into a number.
/// It is a floor that keeps a refused client from spinning at its own retry
/// rate for something that will not change within a second.
const STORAGE_LIMIT_RETRY_AFTER: Duration = Duration::from_secs(60);

impl TenantQuota {
    pub fn new(
        config: Arc<Config>,
        clock: Arc<Clock>,
        metrics: Arc<RuntimeMetrics>,
        policy: Arc<TenantPolicy>,
        parts: Arc<crate::part_registry::PartRegistry>,
        trace_parts: Arc<crate::trace_registry::TraceRegistry>,
    ) -> Self {
        Self {
            config,
            clock,
            metrics,
            policy,
            parts,
            trace_parts,
            buckets: Mutex::new(HashMap::new()),
            scan_buckets: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
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
            Arc::new(crate::part_registry::PartRegistry::new()),
            Arc::new(crate::trace_registry::TraceRegistry::standalone()),
        ))
    }

    /// Charge `bytes` to `tenant`, or refuse.
    ///
    /// Called with the size of the request as it arrived on the wire and
    /// before it is decompressed or decoded, so a tenant over its rate cannot
    /// spend this instance's CPU on a body that will not be accepted.
    pub fn check(&self, tenant: &TenantId, bytes: u64) -> Result<(), IngestError> {
        self.admit_storage(tenant)?;
        let rate = match self.resolve(tenant) {
            TenantIngestRate::Unlimited => return Ok(()),
            TenantIngestRate::BytesPerSecond(rate) => rate,
        };
        self.charge(&self.buckets, tenant, rate, bytes, "write", "ingest rate")
    }

    /// Charge a completed query's scanned bytes against the tenant's read rate.
    ///
    /// Charged *after* the scan, with what it actually read, because the cost
    /// of a query is not knowable before running it — an estimate would either
    /// refuse cheap queries or let expensive ones through. A tenant that
    /// overruns is therefore refused on its *next* query, which bounds the
    /// overrun at one query rather than preventing it.
    pub fn charge_scan(&self, tenant: &TenantId, bytes: u64) {
        let TenantQueryRate::BytesPerSecond(rate) = self.resolve_query(tenant) else {
            return;
        };
        if rate == 0 {
            return;
        }
        let capacity = self.capacity(rate);
        let refill_per_ns = rate as f64 / 1e9;
        let now_ns = self.clock.now_ns();

        let mut buckets = self.scan_buckets.lock();
        let bucket = buckets.entry(tenant.clone()).or_insert(Bucket {
            available: capacity,
            capacity,
            refill_per_ns,
            updated_ns: now_ns,
        });
        bucket.capacity = capacity;
        bucket.refill_per_ns = refill_per_ns;
        let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
        bucket.available = (bucket.available + elapsed_ns as f64 * refill_per_ns).min(capacity);
        bucket.updated_ns = now_ns;

        // The read bucket may go into debt, which the write bucket may not.
        // A write is refused before it happens, so its bucket never owes
        // anything; a scan has already run by the time its cost is known, so
        // the overrun has to be recorded somewhere. Debt is what makes the
        // charge proportional — reading twice the budget costs twice the wait,
        // where clamping at zero would make every overrun cost the same.
        //
        // Bounded at one bucket, so a single enormous query cannot lock a
        // tenant out for longer than its budget takes to refill once.
        bucket.available = (bucket.available - bytes as f64).max(-capacity);
        drop(buckets);
        self.maybe_sweep();
    }

    /// Whether the tenant may start another query, and the slot it holds while
    /// it runs.
    pub fn begin_query(self: &Arc<Self>, tenant: &TenantId) -> Result<QuerySlot, IngestError> {
        if let TenantQueryRate::BytesPerSecond(0) = self.resolve_query(tenant) {
            return Err(self.query_refused(format!(
                "tenant {tenant} is not permitted to query: its query rate is 0"
            )));
        }
        // Checked before the concurrency slot so an exhausted budget is
        // reported as a budget, not as contention. The test is for debt rather
        // than for emptiness: the bucket refills continuously, so "any budget
        // at all" is satisfied a nanosecond after it is spent.
        if let Some(available) = self.available_scan_budget(tenant)
            && available < 0.0
        {
            return Err(self.query_refused(format!(
                "tenant {tenant} is over its query scan rate; the budget refills over time"
            )));
        }

        let limit = self.config.max_concurrent_queries_per_tenant as u32;
        let mut in_flight = self.in_flight.lock();
        let count = in_flight.entry(tenant.clone()).or_insert(0);
        if *count >= limit {
            let running = *count;
            drop(in_flight);
            return Err(self.query_refused(format!(
                "tenant {tenant} already has {running} queries in flight, at its limit of {limit}"
            )));
        }
        *count += 1;
        drop(in_flight);
        Ok(QuerySlot {
            quota: self.clone(),
            tenant: tenant.clone(),
        })
    }

    /// Whether the tenant may hold this stream, given what it already holds.
    ///
    /// Stream cardinality is the one cost neither retention nor merge reclaims
    /// on its own: `stream.idx` is an eviction-exempt catalog, so a tenant that
    /// mints a label value per request turns disk into something nothing takes
    /// back. The limit is therefore on the *set*, not on a rate.
    ///
    /// Only a stream that is new to both the parts and the buffers can be
    /// refused. An existing stream is always accepted, so a tenant that is over
    /// its limit keeps working with what it has rather than going dark.
    pub fn admit_stream(
        &self,
        tenant: &TenantId,
        labels: &crate::memtable::Labels,
        parts: &crate::part_registry::PartRegistry,
        memtable: &crate::memtable::MemTable,
    ) -> Result<(), IngestError> {
        let Some(limit) = self.resolve_max_streams(tenant) else {
            return Ok(());
        };
        let key = crate::part_registry::stream_key(labels);
        if parts.contains_stream(tenant, key) || memtable.contains_stream(tenant, labels) {
            return Ok(());
        }
        // Reached only when a genuinely new stream appears, which is the rare
        // path. The buffered set is small — bounded by what one flush interval
        // accumulates — and it is walked to count only the streams the parts do
        // not already have, so the two sources are unioned rather than summed.
        let buffered_only = memtable
            .tenant_streams(tenant)
            .iter()
            .filter(|labels| {
                !parts.contains_stream(tenant, crate::part_registry::stream_key(labels))
            })
            .count();
        let live = parts
            .tenant_stream_count(tenant)
            .saturating_add(buffered_only) as u64;
        if live >= limit {
            self.metrics
                .stream_limit_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "tenant {tenant} already holds {live} streams, at its limit of {limit}; \
this write would create another"
                ),
            )
                .into());
        }
        Ok(())
    }

    /// What a tenant currently has left of each budget, for the control plane
    /// to read. `None` means that budget is unlimited for this tenant.
    pub fn budget_snapshot(&self, tenant: &TenantId) -> (Option<f64>, Option<f64>) {
        let ingest = match self.resolve(tenant) {
            TenantIngestRate::Unlimited => None,
            TenantIngestRate::BytesPerSecond(rate) => {
                Some(self.available(&self.buckets, tenant, rate))
            }
        };
        (ingest, self.available_scan_budget(tenant))
    }

    /// Remaining tokens in a bucket, refilled to now without spending any.
    fn available(
        &self,
        buckets: &Mutex<HashMap<TenantId, Bucket>>,
        tenant: &TenantId,
        rate: u64,
    ) -> f64 {
        if rate == 0 {
            return 0.0;
        }
        let now_ns = self.clock.now_ns();
        let buckets = buckets.lock();
        let Some(bucket) = buckets.get(tenant) else {
            return self.capacity(rate);
        };
        let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
        (bucket.available + elapsed_ns as f64 * bucket.refill_per_ns).min(bucket.capacity)
    }

    pub fn max_streams_for(&self, tenant: &TenantId) -> Option<u64> {
        self.resolve_max_streams(tenant)
    }

    /// The tenant's storage limit in bytes, or `None` when it has none.
    pub fn max_stored_bytes_for(&self, tenant: &TenantId) -> Option<u64> {
        match self.resolve_storage_limit(tenant) {
            TenantStorageLimit::Unlimited => None,
            TenantStorageLimit::Bytes(bytes) => Some(bytes),
        }
    }

    fn resolve_storage_limit(&self, tenant: &TenantId) -> TenantStorageLimit {
        self.policy
            .max_stored_bytes(tenant)
            .or_else(|| {
                self.config
                    .default_tenant_max_stored_bytes
                    .map(TenantStorageLimit::Bytes)
            })
            .unwrap_or(TenantStorageLimit::Unlimited)
    }

    /// Refuse the write when the tenant is already holding everything its plan
    /// sells it.
    ///
    /// Read from the registries' running census, so this costs the same on a
    /// tenant with one part as on one with ten thousand.
    ///
    /// The comparison is against what is *stored*, not against what this
    /// request would make stored. A request is at most `max_push_bytes` and the
    /// limit is measured in gigabytes, so the overrun a "check before, not
    /// after" rule allows is one request deep — and checking the other way
    /// would mean estimating a body's compressed size before compressing it.
    pub fn admit_storage(&self, tenant: &TenantId) -> Result<(), IngestError> {
        let TenantStorageLimit::Bytes(limit) = self.resolve_storage_limit(tenant) else {
            return Ok(());
        };
        let stored = self
            .parts
            .tenant_stored_bytes(tenant)
            .saturating_add(self.trace_parts.tenant_stored_bytes(tenant));
        if stored < limit {
            return Ok(());
        }
        self.metrics
            .storage_limit_rejected
            .fetch_add(1, Ordering::Relaxed);
        Err(IngestError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message: format!(
                "tenant {tenant} stores {stored} bytes, at its limit of {limit}; \
writes resume when retention retires enough of it"
            ),
            retry_after: Some(STORAGE_LIMIT_RETRY_AFTER),
        })
    }

    fn resolve_max_streams(&self, tenant: &TenantId) -> Option<u64> {
        self.policy
            .max_streams(tenant)
            .or(self.config.default_tenant_max_streams)
    }

    /// Remaining scan budget, or `None` when the tenant has no finite rate.
    fn available_scan_budget(&self, tenant: &TenantId) -> Option<f64> {
        let TenantQueryRate::BytesPerSecond(rate) = self.resolve_query(tenant) else {
            return None;
        };
        if rate == 0 {
            return Some(0.0);
        }
        let now_ns = self.clock.now_ns();
        let buckets = self.scan_buckets.lock();
        let Some(bucket) = buckets.get(tenant) else {
            // No bucket is a full bucket: nothing has been spent.
            return Some(self.capacity(rate));
        };
        let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
        Some((bucket.available + elapsed_ns as f64 * bucket.refill_per_ns).min(bucket.capacity))
    }

    /// One token bucket, shared by the read and write paths so the two cannot
    /// drift apart in how they refill, clamp or report.
    fn charge(
        &self,
        buckets: &Mutex<HashMap<TenantId, Bucket>>,
        tenant: &TenantId,
        rate: u64,
        bytes: u64,
        verb: &str,
        what: &str,
    ) -> Result<(), IngestError> {
        if rate == 0 {
            return Err(self.refused(format!(
                "tenant {tenant} is not permitted to {verb}: its {what} is 0"
            )));
        }
        let capacity = self.capacity(rate);
        let refill_per_ns = rate as f64 / 1e9;
        let now_ns = self.clock.now_ns();

        let mut buckets = buckets.lock();
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
            // Spend what is there anyway. A refused request still consumed the
            // budget it was measured against on the read path, and on the write
            // path the bucket is already empty, so this only matters for making
            // the two behave identically.
            bucket.available = 0.0;
            drop(buckets);
            return Err(self.refused_with_retry(
                format!(
                    "tenant {tenant} is over its {what} of {rate} bytes/s; \
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
    /// than a header. Rendered by the one mapping both gRPC services use: this
    /// refusal computes a delay from how far over the rate the tenant is, and
    /// dropping it on the floor here would have told the collector the batch was
    /// unrecoverable.
    pub fn check_grpc(&self, tenant: &TenantId, bytes: u64) -> Result<(), tonic::Status> {
        self.check(tenant, bytes)
            .map_err(crate::log_ingest::ingest_error_to_status)
    }

    /// The rate in force for one tenant: what the control plane pushed, or the
    /// configured default when it has said nothing. A tenant the control plane
    /// does not know is treated the same whether per-tenant policy is off or
    /// simply silent about it.
    fn resolve_query(&self, tenant: &TenantId) -> TenantQueryRate {
        if let Some(rate) = self.policy.query_rate(tenant) {
            return rate;
        }
        match self.config.default_tenant_query_scan_bytes_per_second {
            Some(rate) => TenantQueryRate::BytesPerSecond(rate),
            None => TenantQueryRate::Unlimited,
        }
    }

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
        let mut buckets = self.buckets.lock();
        buckets.retain(|_, bucket| {
            let elapsed_ns = now_ns.saturating_sub(bucket.updated_ns).max(0);
            // Refilled to capacity by now, so dropping it changes no decision.
            let refilled = bucket.available + elapsed_ns as f64 * bucket.refill_per_ns;
            refilled < bucket.capacity
        });
    }

    /// A read-side refusal, counted apart from the write-side one so an
    /// operator can tell which half of a tenant's quota is binding.
    fn query_refused(&self, message: String) -> IngestError {
        self.metrics
            .query_quota_rejected
            .fetch_add(1, Ordering::Relaxed);
        IngestError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message,
            retry_after: Some(self.config.backpressure_retry_after),
        }
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
