/// The trace read path's scan orchestrator. Admission runs in the same order
/// as the log path's (`execution.rs`): pin (a restore is network wait and must
/// not hold a scan slot), then a scan permit, then admission to the shared
/// byte pool, then the blocking scan under an outer timeout — each stage
/// holding only what the previous stages granted while it waits. The permit
/// comes from the trace surface's own semaphore because a trace scan decodes
/// whole-span JSON payloads, a cost profile that would let either surface
/// starve the other if they shared slots.
const MAX_TRACE_SPANS: usize = 100_000;

// Search's `Window` variant joins with the `/traces` endpoint.
enum TraceScanTarget {
    ById(String),
}

struct TraceScanOutcome {
    /// Sorted by `(start_time_ns, span_id)`.
    spans: Vec<crate::trace::TraceSpan>,
    estimated_bytes: u64,
    /// Held with the spans it paid for; the handler drops both together after
    /// the NDJSON body is built (the `QueryExecution` precedent).
    _memory_reservation: crate::query_memory::QueryMemoryReservation,
}

async fn scan_trace_spans(
    state: Arc<AppState>,
    tenant: TenantId,
    target: TraceScanTarget,
) -> Result<TraceScanOutcome, ApiError> {
    let max_runtime = state.config.max_trace_query_runtime;
    let guard = match &target {
        TraceScanTarget::ById(trace_id) => pin_trace_parts(&state, &tenant, trace_id).await?,
    };
    let scan_permit = tokio::time::timeout(
        max_runtime,
        state.trace_scan_semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::from_engine("trace query timed out".to_string()))?
    .map_err(|error| ApiError::from_engine(format!("trace scan scheduler is closed: {error}")))?;
    let memory_reservation = tokio::time::timeout(max_runtime, state.query_memory_pool.reserve())
        .await
        .map_err(|_| ApiError::from_engine("trace query timed out".to_string()))?
        .map_err(ApiError::from_engine)?;

    let cancellation = Arc::new(AtomicBool::new(false));
    let task_cancellation = cancellation.clone();
    let max_spans = state.config.max_trace_spans.min(MAX_TRACE_SPANS);
    let max_query_memory_bytes = state.config.max_query_memory_bytes;
    let task_state = state.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        // Keep the permit until the blocking task actually exits; cancelling
        // the request must not admit a second scan while the first is still
        // consuming CPU and memory.
        let _scan_permit = scan_permit;
        let _guard = guard;
        let _arena = crate::memprof::enter(crate::memprof::Arena::Query);
        let memtable = task_state.journal.trace_memtable();
        let mut spans = match &target {
            TraceScanTarget::ById(trace_id) => {
                memtable.query_trace_id_limited(&tenant, trace_id, max_spans)?
            }
        };
        let mut estimated_bytes: u64 = spans.iter().map(crate::trace::span_query_bytes).sum();
        memory_reservation.ensure(estimated_bytes)?;
        let remaining = max_spans.saturating_sub(spans.len());
        let part_spans = match &target {
            TraceScanTarget::ById(trace_id) => task_state.trace_parts.query_trace_id(
                &tenant,
                trace_id,
                Some(remaining),
                Some(&task_cancellation),
                Some(&memory_reservation),
            )?,
        };
        estimated_bytes += part_spans
            .iter()
            .map(crate::trace::span_query_bytes)
            .sum::<u64>();
        spans.extend(part_spans);
        // The registry charged its own spans as they accumulated; this charge
        // covers the sum with the memtable's share included.
        memory_reservation.ensure(estimated_bytes)?;
        spans.sort_by(|left, right| {
            left.start_time_ns
                .cmp(&right.start_time_ns)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        Ok::<_, String>(TraceScanOutcome {
            spans,
            estimated_bytes,
            _memory_reservation: memory_reservation,
        })
    });

    let outcome = match tokio::time::timeout(max_runtime, &mut task).await {
        Ok(result) => result
            .map_err(|error| ApiError::from_engine(format!("trace query task failed: {error}")))?
            .map_err(ApiError::from_engine)?,
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            return Err(ApiError::from_engine("trace query timed out".to_string()));
        }
    };
    if outcome.estimated_bytes > max_query_memory_bytes {
        return Err(ApiError::from_engine(format!(
            "trace query exceeds the maximum of {max_query_memory_bytes} materialized bytes"
        )));
    }
    Ok(outcome)
}

fn pin_error(error: crate::runtime_error::RuntimeError) -> ApiError {
    let (status, message) = error.into_http();
    ApiError(status, message)
}

async fn pin_trace_parts(
    state: &AppState,
    tenant: &TenantId,
    trace_id: &str,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, ApiError> {
    crate::remote_lifecycle::pin_remote_parts(
        state.parts.operation_lock(),
        state.remote_cache.clone(),
        || state.trace_parts.candidate_part_ids(tenant, trace_id),
        |required| state.trace_parts.missing_data_ids(required),
        crate::remote_lifecycle::RemoteDomain::Traces,
        state.config.max_trace_restore_runtime,
        || Ok(()),
        Some(state.metrics.clone()),
    )
    .await
    .map_err(pin_error)
}

