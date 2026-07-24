use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Low-cardinality process metrics shared by handlers and background workers.
/// Counters are deliberately monotonic; `/metrics` renders the current
/// snapshot in Prometheus text format.
pub struct RuntimeMetrics {
    pub ingest_requests: AtomicU64,
    pub ingest_errors: AtomicU64,
    pub flush_success: AtomicU64,
    pub flush_errors: AtomicU64,
    pub merge_success: AtomicU64,
    pub merge_errors: AtomicU64,
    pub retention_success: AtomicU64,
    pub retention_errors: AtomicU64,
    pub query_success: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_scanned_rows: AtomicU64,
    pub query_scanned_bytes: AtomicU64,
    pub query_latency_ns: AtomicU64,
    pub remote_restore_success: AtomicU64,
    pub remote_restore_errors: AtomicU64,
    pub remote_restore_latency_ns: AtomicU64,
    pub cache_evictions: AtomicU64,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        Self {
            ingest_requests: AtomicU64::new(0),
            ingest_errors: AtomicU64::new(0),
            flush_success: AtomicU64::new(0),
            flush_errors: AtomicU64::new(0),
            merge_success: AtomicU64::new(0),
            merge_errors: AtomicU64::new(0),
            retention_success: AtomicU64::new(0),
            retention_errors: AtomicU64::new(0),
            query_success: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            query_scanned_rows: AtomicU64::new(0),
            query_scanned_bytes: AtomicU64::new(0),
            query_latency_ns: AtomicU64::new(0),
            remote_restore_success: AtomicU64::new(0),
            remote_restore_errors: AtomicU64::new(0),
            remote_restore_latency_ns: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
        }
    }

    pub fn add_duration(target: &AtomicU64, duration: Duration) {
        target.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
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
