use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Low-cardinality process metrics shared by handlers and background workers.
/// Counters are deliberately monotonic; `/metrics` renders the current
/// snapshot in Prometheus text format.
pub struct RuntimeMetrics {
    pub ingest_requests: AtomicU64,
    pub ingest_errors: AtomicU64,
    /// Requests refused because the durable path was already behind. Distinct
    /// from `ingest_errors`: this one says the server is healthy and saying no,
    /// which is the signal an operator scales or tunes flush on.
    pub ingest_throttled: AtomicU64,
    /// Requests refused because the tenant was over the ingest rate its policy
    /// grants. Separate from `ingest_throttled`: that one says this instance is
    /// behind and an operator should scale or tune it, this one says the
    /// instance is healthy and the tenant asked for more than it was sold.
    pub ingest_quota_rejected: AtomicU64,
    /// Point-in-time backlog gauges, published by the workers that already walk
    /// the structures they describe. Computing them per scrape instead was
    /// O(parts × tenants) of work on an unauthenticated endpoint, and the
    /// numbers are only as fresh as a worker tick either way.
    pub merge_debt_parts: AtomicU64,
    pub unknown_tenants: AtomicU64,
    pub flush_success: AtomicU64,
    pub flush_errors: AtomicU64,
    pub merge_success: AtomicU64,
    pub merge_errors: AtomicU64,
    /// Merge groups abandoned because another writer replaced their inputs
    /// first. Benign on its own — retention retiring an expired part races with
    /// merge by design, nothing was written, and the next tick sees the new
    /// state. It is counted rather than only logged because a number that keeps
    /// rising while `merge_success` makes no progress is the one way to see the
    /// registry failing to converge on the manifest.
    pub merge_inputs_changed: AtomicU64,
    pub retention_success: AtomicU64,
    pub retention_errors: AtomicU64,
    pub retention_expired_rows_dropped: AtomicU64,
    pub retention_parts_rewritten: AtomicU64,
    /// Retention-only merge groups whose inputs could not be read, so the
    /// expired rows in them stay on disk. A number that keeps rising means a
    /// part is permanently too large for `merge_max_memory_bytes`.
    pub retention_rewrite_skipped: AtomicU64,
    pub query_success: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_scanned_rows: AtomicU64,
    pub query_scanned_bytes: AtomicU64,
    pub query_latency_ns: AtomicU64,
    /// Bucketed query latency, so an operator can compute a real quantile.
    ///
    /// The cumulative `*_latency_ns_total` counters only ever yielded a mean,
    /// and every target in the plan documents is written as p95/p99 — numbers
    /// that were literally not derivable from what this endpoint exposed.
    pub query_latency: LatencyHistogram,
    pub remote_restore_success: AtomicU64,
    pub remote_restore_errors: AtomicU64,
    pub remote_restore_latency_ns: AtomicU64,
    pub remote_restore_latency: LatencyHistogram,
    pub cache_evictions: AtomicU64,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        Self {
            ingest_requests: AtomicU64::new(0),
            ingest_errors: AtomicU64::new(0),
            ingest_throttled: AtomicU64::new(0),
            ingest_quota_rejected: AtomicU64::new(0),
            merge_debt_parts: AtomicU64::new(0),
            unknown_tenants: AtomicU64::new(0),
            flush_success: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            merge_success: AtomicU64::new(0),
            merge_errors: AtomicU64::new(0),
            merge_inputs_changed: AtomicU64::new(0),
            retention_success: AtomicU64::new(0),
            retention_errors: AtomicU64::new(0),
            retention_expired_rows_dropped: AtomicU64::new(0),
            retention_parts_rewritten: AtomicU64::new(0),
            retention_rewrite_skipped: AtomicU64::new(0),
            query_success: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            query_scanned_rows: AtomicU64::new(0),
            query_scanned_bytes: AtomicU64::new(0),
            query_latency_ns: AtomicU64::new(0),
            query_latency: LatencyHistogram::default(),
            remote_restore_success: AtomicU64::new(0),
            remote_restore_errors: AtomicU64::new(0),
            remote_restore_latency_ns: AtomicU64::new(0),
            remote_restore_latency: LatencyHistogram::default(),
            cache_evictions: AtomicU64::new(0),
        }
    }

    pub fn add_duration(target: &AtomicU64, duration: Duration) {
        target.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub fn observe(histogram: &LatencyHistogram, counter: &AtomicU64, duration: Duration) {
        Self::add_duration(counter, duration);
        histogram.observe(duration);
    }

    pub fn load(target: &AtomicU64) -> u64 {
        target.load(Ordering::Relaxed)
    }
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Prometheus-shaped latency buckets.
///
/// Cumulative (`le`) counts, which is the only shape `histogram_quantile` can
/// read. The bounds span 1 ms to 30 s: below that a query is instant and above
/// it the resource limits have already refused the work, so finer resolution at
/// either end would only cost cardinality.
pub const LATENCY_BUCKET_BOUNDS_MS: [f64; 12] = [
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 30_000.0,
];

#[derive(Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKET_BOUNDS_MS.len()],
    count: AtomicU64,
    sum_ms: AtomicU64,
}

impl LatencyHistogram {
    pub fn observe(&self, duration: Duration) {
        let millis = duration.as_secs_f64() * 1_000.0;
        for (index, bound) in LATENCY_BUCKET_BOUNDS_MS.iter().enumerate() {
            if millis <= *bound {
                // Cumulative: an observation belongs to its own bucket and to
                // every wider one, which is what makes the series monotonic in
                // `le` and therefore readable by `histogram_quantile`.
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ms
            .fetch_add(millis.round().max(0.0) as u64, Ordering::Relaxed);
    }

    /// `(le bound, cumulative count)` pairs plus the `+Inf` total and sum.
    pub fn render(&self, name: &str) -> String {
        let mut out = String::new();
        for (index, bound) in LATENCY_BUCKET_BOUNDS_MS.iter().enumerate() {
            out.push_str(&format!(
                "{name}_bucket{{le=\"{bound}\"}} {}\n",
                self.buckets[index].load(Ordering::Relaxed)
            ));
        }
        let count = self.count.load(Ordering::Relaxed);
        out.push_str(&format!("{name}_bucket{{le=\"+Inf\"}} {count}\n"));
        out.push_str(&format!(
            "{name}_sum {}\n",
            self.sum_ms.load(Ordering::Relaxed)
        ));
        out.push_str(&format!("{name}_count {count}\n"));
        out
    }
}
