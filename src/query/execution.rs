#[cfg(test)]
fn unified_query(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(
        unified_query_with_stats(state, tenant, parsed, range, limit, forward, None)?.results,
    )
}

struct QueryExecution {
    results: Vec<StreamResult>,
    scanned_rows: u64,
    scanned_bytes: u64,
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn unified_query_with_stats(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    unified_query_with_stats_cancellable(
        state,
        tenant,
        parsed,
        range,
        limit,
        forward,
        scan_budget,
        None,
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn unified_query_with_stats_cancellable(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Option<&AtomicBool>,
) -> Result<QueryExecution, String> {
    unified_query_with_stats_cancellable_with_memory(
        state,
        tenant,
        parsed,
        range,
        limit,
        forward,
        scan_budget,
        cancellation,
        None,
        None,
        part::ColumnSet::all(),
    )
}

#[allow(clippy::too_many_arguments)]
fn unified_query_with_stats_cancellable_with_memory(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Option<&AtomicBool>,
    max_memory_bytes: Option<u64>,
    max_scan_bytes: Option<u64>,
    columns: part::ColumnSet,
) -> Result<QueryExecution, String> {
    // The one place every read path meets its rows, which is why the deletion
    // mask is here and not at each handler. A second scan would be a second
    // place to forget it, and forgetting it means serving a line a tenant asked
    // to have deleted.
    let deleted = state.delete_requests.mask_for(tenant);
    let hidden_rows = &state.delete_requests.metrics.hidden_rows;
    let hidden = |labels: &Labels, entry: &LogEntry| {
        if deleted.hides(labels, entry) {
            hidden_rows.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    };
    let scan = crate::log_scan::LogScan::new(tenant, parsed, range, limit, forward)
        .columns(columns)
        .scan_budget(scan_budget)
        .max_scan_bytes(max_scan_bytes)
        .max_memory_bytes(max_memory_bytes)
        .cancellation(cancellation)
        .hidden(&hidden);
    let result = scan.run(&state.memtable, &state.parts)?;
    Ok(QueryExecution {
        results: result.results,
        scanned_rows: result.scanned_rows,
        scanned_bytes: result.scanned_bytes,
    })
}

#[cfg(test)]
async fn run_unified_query(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(
        run_unified_query_with_stats(
            state, tenant, parsed, range, limit, forward, None,
        )
        .await?
        .results,
    )
}

#[allow(clippy::too_many_arguments)]
async fn run_unified_query_with_stats(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    // Held for the whole scan. Every read path funnels through here — logs,
    // tail, volume, detected fields, restore probes — so the tenant's share of
    // this instance is bounded in one place rather than at each handler.
    let _slot = state
        .tenant_quota
        .begin_query(&tenant)
        .map_err(|error| format!("{TENANT_QUOTA_PREFIX}{}", error.message))?;
    let quota = state.tenant_quota.clone();
    let quota_tenant = tenant.clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    let metrics = state.metrics.clone();
    let started = std::time::Instant::now();
    let result = run_unified_query_with_stats_cancellable(
        state,
        tenant,
        parsed,
        range,
        limit,
        forward,
        scan_budget,
        cancellation,
    )
    .await;
    crate::metrics::RuntimeMetrics::observe(
        &metrics.query_latency,
        &metrics.query_latency_ns,
        started.elapsed(),
    );
    match &result {
        Ok(execution) => {
            metrics
                .query_success
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics
                .query_scanned_rows
                .fetch_add(execution.scanned_rows, std::sync::atomic::Ordering::Relaxed);
            metrics
                .query_scanned_bytes
                .fetch_add(execution.scanned_bytes, std::sync::atomic::Ordering::Relaxed);
            // Charged with what the scan actually read. A query's cost is not
            // knowable before running it, so the tenant pays afterwards and an
            // overrun is bounded at one query rather than prevented.
            quota.charge_scan(&quota_tenant, execution.scanned_bytes);
        }
        Err(_) => {
            metrics
                .query_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_unified_query_with_stats_cancellable(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
) -> Result<QueryExecution, String> {
    run_unified_query_with_stats_cancellable_for_runtime(
        state,
        tenant,
        parsed,
        range,
        limit,
        forward,
        scan_budget,
        cancellation,
        None,
        // The log path returns every pair a row stored, so it reads them all.
        part::ColumnSet::all(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_unified_query_with_stats_cancellable_for_runtime(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    range: part::QueryTimeRange,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
    runtime_override: Option<std::time::Duration>,
    columns: part::ColumnSet,
) -> Result<QueryExecution, String> {
    let max_runtime = runtime_override.unwrap_or(state.config.max_query_runtime);
    // Pin (and possibly restore) the parts *before* taking a scan slot. A
    // restore is object-store I/O; holding one of the few scan permits across
    // it stalled CPU-bound scans behind pure network wait. The pin is a
    // refcount, so nothing is scanned before the permit is held.
    let part_guard = tokio::time::timeout(
        max_runtime,
        pin_query_parts(&state, &tenant, &parsed, range),
    )
    .await
    .map_err(|_| "query timed out".to_string())??;
    let scan_permit = tokio::time::timeout(
        max_runtime,
        state.query_scan_semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| "query timed out".to_string())?
    .map_err(|_| "query scan scheduler is closed".to_string())?;
    let task_cancellation = cancellation.clone();
    let max_query_runtime = max_runtime;
    let max_query_memory_bytes = state.config.max_query_memory_bytes;
    let mut task = tokio::task::spawn_blocking(move || {
        // Keep the scheduler permit until the blocking task actually exits;
        // cancelling the request must not admit an unbounded second scan while
        // the first task is still consuming CPU and memory.
        let _scan_permit = scan_permit;
        let _part_guard = part_guard;
        let _arena = crate::memprof::enter(crate::memprof::Arena::Query);
        unified_query_with_stats_cancellable_with_memory(
            &state,
            &tenant,
            &parsed,
            range,
            limit,
            forward,
            scan_budget,
            Some(task_cancellation.as_ref()),
            Some(max_query_memory_bytes),
            Some(state.config.max_query_scan_bytes),
            columns,
        )
    });
    let execution = match tokio::time::timeout(max_query_runtime, &mut task).await {
        Ok(result) => result.map_err(|error| format!("query task failed: {error}"))?,
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            Err("query timed out".to_string())
        }
    }?;
    let estimated_memory = estimated_query_memory_bytes(&execution.results);
    if estimated_memory > max_query_memory_bytes {
        return Err(format!(
            "query exceeds the maximum of {max_query_memory_bytes} materialized bytes"
        ));
    }
    Ok(execution)
}

/// The metric fast path's scan: same admission (part pin, then the scan
/// permit), same budgets, but a [`crate::log_scan::CountingSink`] instead of a
/// bounded row collector — nothing is materialized and nothing is returned but
/// the per-point totals.
#[allow(clippy::too_many_arguments)]
async fn run_metric_count_scan(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    range: part::QueryTimeRange,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
    runtime_override: Option<std::time::Duration>,
    columns: part::ColumnSet,
    times: Vec<i64>,
    range_ns: i64,
    bytes: bool,
) -> Result<(Vec<f64>, u64, u64, u64), String> {
    let max_runtime = runtime_override.unwrap_or(state.config.max_query_runtime);
    let part_guard = tokio::time::timeout(
        max_runtime,
        pin_query_parts(&state, &tenant, &parsed, range),
    )
    .await
    .map_err(|_| "query timed out".to_string())??;
    let scan_permit = tokio::time::timeout(
        max_runtime,
        state.query_scan_semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| "query timed out".to_string())?
    .map_err(|_| "query scan scheduler is closed".to_string())?;
    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _scan_permit = scan_permit;
        let _part_guard = part_guard;
        let _arena = crate::memprof::enter(crate::memprof::Arena::Query);
        let deleted = state.delete_requests.mask_for(&tenant);
        let hidden_rows = &state.delete_requests.metrics.hidden_rows;
        let hidden = |labels: &Labels, entry: &LogEntry| {
            if deleted.hides(labels, entry) {
                hidden_rows.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            false
        };
        let mut sink = crate::log_scan::CountingSink {
            query: &parsed,
            hidden: Some(&hidden),
            cancellation: Some(task_cancellation.as_ref()),
            times: &times,
            range_ns,
            bytes,
            diff: vec![0.0; times.len() + 1],
            rows: 0,
        };
        let scan = crate::log_scan::LogScan::new(&tenant, &parsed, range, usize::MAX, true)
            .columns(columns)
            .scan_budget(scan_budget)
            .max_scan_bytes(Some(state.config.max_query_scan_bytes))
            .cancellation(Some(task_cancellation.as_ref()));
        let (scanned_rows, scanned_bytes) = scan.run_into(&state.memtable, &state.parts, &mut sink)?;
        Ok::<_, String>((sink.diff, sink.rows, scanned_rows, scanned_bytes))
    });
    match tokio::time::timeout(max_runtime, &mut task).await {
        Ok(result) => result.map_err(|error| format!("query task failed: {error}"))?,
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            Err("query timed out".to_string())
        }
    }
}
