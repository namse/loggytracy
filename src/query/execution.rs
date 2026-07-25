#[cfg(test)]
fn unified_query(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(
        unified_query_with_stats(state, tenant, parsed, start_ns, end_ns, limit, forward, None)?
            .results,
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
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    unified_query_with_stats_cancellable(
        state,
        tenant,
        parsed,
        start_ns,
        end_ns,
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
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Option<&AtomicBool>,
) -> Result<QueryExecution, String> {
    unified_query_with_stats_cancellable_with_memory(
        state,
        tenant,
        parsed,
        start_ns,
        end_ns,
        limit,
        forward,
        scan_budget,
        cancellation,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn unified_query_with_stats_cancellable_with_memory(
    state: &AppState,
    tenant: &TenantId,
    parsed: &logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Option<&AtomicBool>,
    max_memory_bytes: Option<u64>,
    max_scan_bytes: Option<u64>,
) -> Result<QueryExecution, String> {
    let mut all: Vec<(Labels, LogEntry)> = Vec::new();
    let mut scanned_rows = 0u64;
    let mut scanned_bytes = 0u64;
    let mut materialized_memory_bytes = 0u64;

    // Pipeline predicates run after storage scans. Do not let the API log
    // limit truncate raw rows before a json/logfmt/field stage has evaluated.
    let normal_scan_limit = if parsed.stages.len() == parsed.line_filters.len() {
        limit
    } else {
        usize::MAX
    };
    let scan_limit = scan_budget.map(|budget| budget.saturating_add(1));

    let memtable_result = state.memtable.query_with_scan_limit(
        tenant,
        &parsed.matchers,
        &parsed.line_filters,
        start_ns,
        end_ns,
        normal_scan_limit,
        forward,
        scan_limit,
        cancellation,
    );
    scanned_rows = scanned_rows.saturating_add(memtable_result.scanned_rows as u64);
    for sr in memtable_result.results {
        for mut e in sr.entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("query timed out".to_string());
            }
            if parsed.process_entry_with_labels_cancellable(&sr.labels, &mut e, cancellation)? {
                materialized_memory_bytes = materialized_memory_bytes
                    .checked_add(estimated_log_entry_memory_bytes(&sr.labels, &e))
                    .ok_or_else(|| "query materialized memory accounting overflowed".to_string())?;
                if max_memory_bytes.is_some_and(|max| materialized_memory_bytes > max) {
                    return Err(format!(
                        "query exceeds the maximum of {} materialized bytes",
                        max_memory_bytes.unwrap_or_default()
                    ));
                }
                all.push((sr.labels.clone(), e));
            }
        }
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if let Some(budget) = scan_budget
        && scanned_rows > budget as u64
    {
        return Err(format!(
            "query exceeds the maximum of {budget} scanned rows"
        ));
    }

    let exact_fields = parsed.exact_field_predicates();
    let part_scan_limit = scan_limit.map(|budget| budget.saturating_sub(scanned_rows as usize));
    let part_scan_bytes_limit = max_scan_bytes.map(|budget| budget.saturating_sub(scanned_bytes));
    let part_result = state.parts.query_with_exact_field_pruning_and_scan_limits(
        tenant,
        &parsed.matchers,
        crate::part::ExactFieldPruning::new(&parsed.line_filters, &exact_fields),
        start_ns,
        end_ns,
        normal_scan_limit,
        forward,
        part_scan_limit,
        part_scan_bytes_limit,
        cancellation,
    )?;
    scanned_rows = scanned_rows.saturating_add(part_result.scanned_rows as u64);
    scanned_bytes = scanned_bytes.saturating_add(part_result.scanned_bytes);
    for sr in part_result.results {
        for mut e in sr.entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("query timed out".to_string());
            }
            if parsed.process_entry_with_labels_cancellable(&sr.labels, &mut e, cancellation)? {
                materialized_memory_bytes = materialized_memory_bytes
                    .checked_add(estimated_log_entry_memory_bytes(&sr.labels, &e))
                    .ok_or_else(|| "query materialized memory accounting overflowed".to_string())?;
                if max_memory_bytes.is_some_and(|max| materialized_memory_bytes > max) {
                    return Err(format!(
                        "query exceeds the maximum of {} materialized bytes",
                        max_memory_bytes.unwrap_or_default()
                    ));
                }
                all.push((sr.labels.clone(), e));
            }
        }
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if let Some(budget) = scan_budget
        && scanned_rows > budget as u64
    {
        return Err(format!(
            "query exceeds the maximum of {budget} scanned rows"
        ));
    }
    if let Some(budget) = max_scan_bytes
        && scanned_bytes > budget
    {
        return Err(format!(
            "query exceeds the maximum of {budget} scanned bytes"
        ));
    }

    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err("query timed out".to_string());
    }
    if forward {
        all.sort_by_key(|e| e.1.timestamp_ns);
    } else {
        all.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
    }
    all.truncate(limit);

    Ok(QueryExecution {
        results: part::group_by_labels(all),
        scanned_rows,
        scanned_bytes,
    })
}

#[cfg(test)]
async fn run_unified_query(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
) -> Result<Vec<StreamResult>, String> {
    Ok(
        run_unified_query_with_stats(
            state, tenant, parsed, start_ns, end_ns, limit, forward, None,
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
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
) -> Result<QueryExecution, String> {
    let cancellation = Arc::new(AtomicBool::new(false));
    let metrics = state.metrics.clone();
    let started = std::time::Instant::now();
    let result = run_unified_query_with_stats_cancellable(
        state,
        tenant,
        parsed,
        start_ns,
        end_ns,
        limit,
        forward,
        scan_budget,
        cancellation,
    )
    .await;
    crate::metrics::RuntimeMetrics::add_duration(&metrics.query_latency_ns, started.elapsed());
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
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
) -> Result<QueryExecution, String> {
    run_unified_query_with_stats_cancellable_for_runtime(
        state,
        tenant,
        parsed,
        start_ns,
        end_ns,
        limit,
        forward,
        scan_budget,
        cancellation,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_unified_query_with_stats_cancellable_for_runtime(
    state: Arc<AppState>,
    tenant: TenantId,
    parsed: logql::LogQuery,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
    forward: bool,
    scan_budget: Option<usize>,
    cancellation: Arc<AtomicBool>,
    runtime_override: Option<std::time::Duration>,
) -> Result<QueryExecution, String> {
    let max_runtime = runtime_override.unwrap_or(state.config.max_query_runtime);
    let scan_permit = tokio::time::timeout(
        max_runtime,
        state.query_scan_semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| "query timed out".to_string())?
    .map_err(|_| "query scan scheduler is closed".to_string())?;
    let part_guard = tokio::time::timeout(
        max_runtime,
        pin_query_parts(&state, &tenant, &parsed, start_ns, end_ns),
    )
    .await
    .map_err(|_| "query timed out".to_string())??;
    let task_cancellation = cancellation.clone();
    let max_query_runtime = max_runtime;
    let max_query_memory_bytes = state.config.max_query_memory_bytes;
    let mut task = tokio::task::spawn_blocking(move || {
        // Keep the scheduler permit until the blocking task actually exits;
        // cancelling the request must not admit an unbounded second scan while
        // the first task is still consuming CPU and memory.
        let _scan_permit = scan_permit;
        let _part_guard = part_guard;
        unified_query_with_stats_cancellable_with_memory(
            &state,
            &tenant,
            &parsed,
            start_ns,
            end_ns,
            limit,
            forward,
            scan_budget,
            Some(task_cancellation.as_ref()),
            Some(max_query_memory_bytes),
            Some(state.config.max_query_scan_bytes),
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
