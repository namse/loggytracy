//! The shared byte budget every query materialization draws from.
//!
//! Before this, query memory was a product of two knobs that never met:
//! `max_concurrent_query_scans × max_query_memory_bytes` — 8 × 512 MiB, four
//! gigabytes no single number mentioned, inside a process asked to live in
//! two (`config.rs::peak_materialized_bytes` documented the hole rather than
//! closing it). The pool is the closing: one byte budget, shared by every
//! log scan and metric evaluation, reserved incrementally as rows actually
//! materialize and released when the work drops it.
//!
//! Granularity: one semaphore permit is one KiB, acquired in
//! [`RESERVATION_CHUNK_BYTES`] steps so the semaphore is touched once per
//! eight mebibytes rather than once per row. Exhaustion is a refusal, not a
//! wait: the growing side runs on a blocking thread in the middle of a scan,
//! where parking on an async semaphore is not an option and queueing would
//! hold the scan's CPU slot while it waited for memory — the deadlock shape
//! semaphores ordered the other way are famous for. The per-query
//! `max_query_memory_bytes` cap is unchanged and still enforced by the scan.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One permit's worth of bytes.
const UNIT_BYTES: u64 = 1024;

/// How much a reservation grows per semaphore touch.
pub const RESERVATION_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// A synchronous byte pool for background writers.
///
/// Flush and compaction run on blocking threads, so they cannot await the
/// query semaphore without holding a runtime worker hostage. Their work is
/// therefore admitted with a non-blocking reservation: if another background
/// rewrite owns the declared budget, the caller leaves its input untouched and
/// retries on the next tick. The permit is RAII, so both success and every
/// error path return the bytes.
pub struct BackgroundMemoryPool {
    capacity_bytes: u64,
    reserved_bytes: Mutex<u64>,
}

/// One background writer's bounded slice of the shared pool.
pub struct BackgroundMemoryPermit {
    pool: Arc<BackgroundMemoryPool>,
    bytes: u64,
}

impl BackgroundMemoryPool {
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            reserved_bytes: Mutex::new(0),
        }
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn reserved_bytes(&self) -> u64 {
        *self
            .reserved_bytes
            .lock()
            .expect("background memory pool poisoned")
    }

    /// Try to reserve `bytes`, capped by the operation's configured hard
    /// limit. A caller that gets `None` must not start its write.
    pub fn try_reserve(
        self: &Arc<Self>,
        bytes: u64,
        hard_limit: u64,
    ) -> Option<BackgroundMemoryPermit> {
        if bytes == 0 || bytes > hard_limit {
            return None;
        }
        let limit = self.capacity_bytes.min(hard_limit);
        let mut reserved = self
            .reserved_bytes
            .lock()
            .expect("background memory pool poisoned");
        let next = reserved.checked_add(bytes)?;
        if next > limit {
            return None;
        }
        *reserved = next;
        Some(BackgroundMemoryPermit {
            pool: self.clone(),
            bytes,
        })
    }
}

impl Drop for BackgroundMemoryPermit {
    fn drop(&mut self) {
        let mut reserved = self
            .pool
            .reserved_bytes
            .lock()
            .expect("background memory pool poisoned");
        *reserved = reserved.saturating_sub(self.bytes);
    }
}

/// The refusal's opening words, shared by the code that writes it and the code
/// that classifies it.
///
/// The scan path reports `String`, so a status code is decided by reading the
/// message — the same reason [`crate::query::TENANT_QUOTA_PREFIX`] exists. A
/// literal typed twice is a refusal that silently becomes a `500` again the
/// day someone rewords it.
pub const EXHAUSTED_PREFIX: &str = "query memory pool of";

pub struct QueryMemoryPool {
    semaphore: Arc<Semaphore>,
    budget_bytes: u64,
    /// Queries this instance could not find memory for.
    ///
    /// Counted apart from every other refusal because it means something the
    /// others do not: not that a tenant asked for more than it was sold
    /// (`query_quota_rejected`), and not that a query was too broad
    /// (`query exceeds the maximum ... scanned rows`, a `400`), but that **this
    /// instance ran out of room for work it was willing to do**. It is the read
    /// side's `ingest_throttled` — the signal an operator scales or re-budgets
    /// on — and without it the event is invisible: as a `500` it hides among
    /// faults, and as a bare `429` it hides among healthy throttling.
    ///
    /// What it does not say, and no single counter could: whether the cause was
    /// too many concurrent queries, one query too greedy, or a budget too small
    /// for the deployment. It says to go and look.
    exhausted: Arc<AtomicU64>,
}

impl QueryMemoryPool {
    pub fn new(budget_bytes: u64) -> Self {
        let permits = usize::try_from(budget_bytes.div_ceil(UNIT_BYTES))
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            budget_bytes,
            exhausted: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Queries refused because this pool had no room. See [`Self::exhausted`].
    pub fn exhausted(&self) -> u64 {
        self.exhausted.load(Ordering::Relaxed)
    }

    /// The pool's admission step: one chunk, awaited, so a query arriving
    /// while the pool is briefly full queues here — before it holds a scan
    /// permit or a blocking thread — instead of failing on arrival.
    pub async fn reserve(&self) -> Result<QueryMemoryReservation, String> {
        let chunk_permits = chunk_permits();
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(chunk_permits)
            .await
            .map_err(|_| "query memory pool is closed".to_string())?;
        Ok(QueryMemoryReservation {
            semaphore: self.semaphore.clone(),
            budget_bytes: self.budget_bytes,
            granted_bytes: AtomicU64::new(u64::from(chunk_permits) * UNIT_BYTES),
            permits: Mutex::new(vec![permit]),
            exhausted: self.exhausted.clone(),
        })
    }
}

fn chunk_permits() -> u32 {
    u32::try_from(RESERVATION_CHUNK_BYTES / UNIT_BYTES).expect("chunk fits u32 permits")
}

/// One query's slice of the pool. Grows chunk-at-a-time from the scan's own
/// thread; everything returns to the pool when this drops with the blocking
/// task that owned it.
pub struct QueryMemoryReservation {
    semaphore: Arc<Semaphore>,
    budget_bytes: u64,
    granted_bytes: AtomicU64,
    permits: Mutex<Vec<OwnedSemaphorePermit>>,
    exhausted: Arc<AtomicU64>,
}

impl QueryMemoryReservation {
    /// Make the reservation cover `needed_bytes`, growing by chunks. `&self`
    /// on purpose: the sink that calls this holds the scan by shared
    /// reference, and the mutex inside is touched once per chunk, not per
    /// row.
    pub fn ensure(&self, needed_bytes: u64) -> Result<(), String> {
        while self.granted_bytes.load(Ordering::Acquire) < needed_bytes {
            match self
                .semaphore
                .clone()
                .try_acquire_many_owned(chunk_permits())
            {
                Ok(permit) => {
                    let mut permits = self.permits.lock().expect("query memory pool poisoned");
                    permits.push(permit);
                    self.granted_bytes
                        .fetch_add(RESERVATION_CHUNK_BYTES, Ordering::AcqRel);
                }
                Err(_) => {
                    self.exhausted.fetch_add(1, Ordering::Relaxed);
                    return Err(format!(
                        "{EXHAUSTED_PREFIX} {} bytes is exhausted",
                        self.budget_bytes
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_pool_refuses_while_held_and_releases_on_drop() {
        let pool = Arc::new(BackgroundMemoryPool::new(10));
        let permit = pool.try_reserve(10, 10).expect("first writer admitted");
        assert_eq!(pool.reserved_bytes(), 10);
        assert!(pool.try_reserve(1, 10).is_none());
        drop(permit);
        assert_eq!(pool.reserved_bytes(), 0);
        assert!(pool.try_reserve(10, 10).is_some());
    }

    #[test]
    fn background_pool_never_allows_an_operation_past_its_hard_limit() {
        let pool = Arc::new(BackgroundMemoryPool::new(100));
        assert!(pool.try_reserve(101, 100).is_none());
        let _permit = pool
            .try_reserve(50, 60)
            .expect("reservation under the hard limit");
        assert_eq!(pool.reserved_bytes(), 50);
    }

    #[tokio::test]
    async fn the_pool_refuses_growth_past_its_budget_and_recovers() {
        // Two chunks of budget: the first reservation takes one on arrival,
        // grows into the second, and the pool is dry.
        let pool = QueryMemoryPool::new(2 * RESERVATION_CHUNK_BYTES);
        let first = pool.reserve().await.expect("admission");
        first
            .ensure(RESERVATION_CHUNK_BYTES + 1)
            .expect("growth within budget");
        let error = first
            .ensure(2 * RESERVATION_CHUNK_BYTES + 1)
            .expect_err("growth past the budget must refuse");
        assert!(error.contains("exhausted"), "{error}");
        // The refusal is counted where it happens, and it is the one signal
        // that says this instance could not serve work it was willing to do.
        // The client is told `429`, which is correct and also indistinguishable
        // from healthy throttling — so if this counter stops moving, the event
        // becomes invisible rather than becoming rare.
        assert_eq!(pool.exhausted(), 1);
        assert!(
            error.starts_with(EXHAUSTED_PREFIX),
            "the classifier reads this prefix; {error}"
        );

        // Releasing the reservation returns every byte: the next query
        // admits and grows to the full budget again.
        drop(first);
        let second = pool.reserve().await.expect("admission after release");
        second
            .ensure(2 * RESERVATION_CHUNK_BYTES)
            .expect("the full budget is available again");
        assert_eq!(pool.exhausted(), 1, "a success must not count as a refusal");
    }

    #[tokio::test]
    async fn ensure_is_satisfied_by_what_is_already_granted() {
        let pool = QueryMemoryPool::new(RESERVATION_CHUNK_BYTES);
        let reservation = pool.reserve().await.expect("admission");
        // Within the initial chunk: no growth, no refusal, however often.
        for _ in 0..3 {
            reservation
                .ensure(RESERVATION_CHUNK_BYTES)
                .expect("granted");
        }
    }
}
