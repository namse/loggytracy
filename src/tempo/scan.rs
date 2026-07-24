async fn scan_trace_spans(
    guard: tokio::sync::OwnedRwLockReadGuard<()>,
    journal: Arc<crate::journal::Journal>,
    trace_parts: Arc<TraceRegistry>,
    trace_id: Option<String>,
    config: Arc<crate::config::Config>,
    scan_semaphore: Arc<Semaphore>,
) -> Result<Vec<TraceSpan>, (StatusCode, String)> {
    let max_trace_query_runtime = config.max_trace_query_runtime;
    let max_trace_spans = config.max_trace_spans.min(MAX_TRACE_SPANS);
    let scan_permit = tokio::time::timeout(max_trace_query_runtime, scan_semaphore.acquire_owned())
        .await
        .map_err(|_| {
            (
                StatusCode::GATEWAY_TIMEOUT,
                "trace query timed out".to_string(),
            )
        })?
        .map_err(|error| internal_error(format!("trace scan scheduler is closed: {error}")))?;
    let cancellation = Arc::new(AtomicBool::new(false));
    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _scan_permit = scan_permit;
        let _guard = guard;
        let mut spans = match trace_id.as_deref() {
            Some(trace_id) => journal
                .trace_memtable()
                .query_trace_id_limited(trace_id, max_trace_spans)?,
            None => journal.trace_memtable().snapshot_limited(max_trace_spans)?,
        };
        let remaining = max_trace_spans.saturating_sub(spans.len());
        let part_spans = match trace_id.as_deref() {
            Some(trace_id) => {
                trace_parts.query_trace_id(trace_id, Some(remaining), Some(&task_cancellation))?
            }
            None => trace_parts.query_all(Some(remaining), Some(&task_cancellation))?,
        };
        spans.extend(part_spans);
        Ok::<_, String>(spans)
    });

    match tokio::time::timeout(max_trace_query_runtime, &mut task).await {
        Ok(Ok(Ok(spans))) => Ok(spans),
        Ok(Ok(Err(error))) => Err(trace_scan_error(error)),
        Ok(Err(error)) => Err(internal_error(format!("trace query task failed: {error}"))),
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            Err((
                StatusCode::GATEWAY_TIMEOUT,
                "trace query timed out".to_string(),
            ))
        }
    }
}

fn trace_scan_error(error: String) -> (StatusCode, String) {
    if error.starts_with("trace query exceeds") {
        (StatusCode::PAYLOAD_TOO_LARGE, error)
    } else {
        internal_error(error)
    }
}

async fn query_trace(
    state: &AppState,
    trace_id: &str,
) -> Result<Vec<TraceSpan>, (StatusCode, String)> {
    let guard = pin_trace_parts(state, trace_id).await?;
    let mut spans = scan_trace_spans(
        guard,
        state.journal.clone(),
        state.trace_parts.clone(),
        Some(trace_id.to_string()),
        state.config.clone(),
        state.trace_scan_semaphore.clone(),
    )
    .await?;
    spans.sort_by(|left, right| {
        left.start_time_ns
            .cmp(&right.start_time_ns)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
    if spans.len() > state.config.max_trace_spans.min(MAX_TRACE_SPANS) {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "trace contains more than {} spans",
                state.config.max_trace_spans.min(MAX_TRACE_SPANS)
            ),
        ));
    }
    Ok(spans)
}

async fn pin_trace_parts(
    state: &AppState,
    trace_id: &str,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, (StatusCode, String)> {
    crate::remote_lifecycle::pin_remote_parts(
        state.parts.operation_lock(),
        state.remote_cache.clone(),
        || state.trace_parts.candidate_part_ids(trace_id),
        |required| state.trace_parts.missing_data_ids(required),
        crate::remote_lifecycle::RemoteDomain::Traces,
        state.config.max_trace_restore_runtime,
        || Ok(()),
        Some(state.metrics.clone()),
    )
    .await
    .map_err(|error| error.into_http())
}

async fn pin_all_trace_parts(
    state: &AppState,
) -> Result<tokio::sync::OwnedRwLockReadGuard<()>, (StatusCode, String)> {
    crate::remote_lifecycle::pin_remote_parts(
        state.parts.operation_lock(),
        state.remote_cache.clone(),
        || state.trace_parts.part_ids(),
        |required| state.trace_parts.missing_data_ids(required),
        crate::remote_lifecycle::RemoteDomain::Traces,
        state.config.max_trace_restore_runtime,
        || Ok(()),
        Some(state.metrics.clone()),
    )
    .await
    .map_err(|error| error.into_http())
}
