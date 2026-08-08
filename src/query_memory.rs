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

pub struct QueryMemoryPool {
    semaphore: Arc<Semaphore>,
    budget_bytes: u64,
}

impl QueryMemoryPool {
    pub fn new(budget_bytes: u64) -> Self {
        let permits = usize::try_from(budget_bytes.div_ceil(UNIT_BYTES))
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            budget_bytes,
        }
    }

    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
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
                    return Err(format!(
                        "query memory pool of {} bytes is exhausted",
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

        // Releasing the reservation returns every byte: the next query
        // admits and grows to the full budget again.
        drop(first);
        let second = pool.reserve().await.expect("admission after release");
        second
            .ensure(2 * RESERVATION_CHUNK_BYTES)
            .expect("the full budget is available again");
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
