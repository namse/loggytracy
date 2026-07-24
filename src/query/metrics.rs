fn parse_step_ns(step: Option<&str>) -> Result<i64, String> {
    let Some(step) = step else {
        return Ok(1_000_000_000);
    };
    let step = step.trim();
    let value = match step.parse::<f64>() {
        Ok(seconds) if seconds.is_finite() => {
            let ns = seconds * 1_000_000_000.0;
            if !ns.is_finite() || ns > i64::MAX as f64 {
                return Err(format!("step '{step}' is out of range"));
            }
            ns.round() as i64
        }
        Ok(_) => return Err(format!("step '{step}' must be finite")),
        Err(_) => logql::parse_duration_ns(step)?,
    };
    if value <= 0 {
        return Err("step must be greater than zero".to_string());
    }
    Ok(value)
}

#[cfg(test)]
async fn run_metric_query(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
) -> Result<Vec<MetricSeries>, String> {
    Ok(
        run_metric_query_with_stats(state, expr, evaluation_times, None)
            .await?
            .series,
    )
}

struct MetricQueryResult {
    series: Vec<MetricSeries>,
    scanned_rows: u64,
}

async fn run_metric_query_with_stats(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
) -> Result<MetricQueryResult, String> {
    let cancellation = Arc::new(AtomicBool::new(false));
    run_metric_query_with_stats_cancellable(
        state,
        expr,
        evaluation_times,
        scan_start_override,
        cancellation,
    )
    .await
}

async fn run_metric_query_with_stats_cancellable(
    state: Arc<AppState>,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
    cancellation: Arc<AtomicBool>,
) -> Result<MetricQueryResult, String> {
    let deadline = tokio::time::Instant::now() + state.config.max_query_runtime;
    let evaluation_permit = tokio::time::timeout(
        state.config.max_query_runtime,
        state.metric_evaluation_semaphore.clone().acquire_owned(),
    )
    .await
    .map_err(|_| "metric query timed out".to_string())?
    .map_err(|_| "metric evaluation scheduler is closed".to_string())?;
    let first = *evaluation_times
        .first()
        .ok_or("metric query has no evaluation timestamps")?;
    let end = *evaluation_times.last().unwrap();
    let scan_start =
        scan_start_override.unwrap_or_else(|| first.saturating_sub(expr.lookback_ns()));
    let query = expr.log_query().clone();
    let max_metric_rows = state.config.max_metric_rows.min(MAX_METRIC_ROWS);
    let max_metric_series = state.config.max_metric_series.min(MAX_METRIC_SERIES);
    let max_metric_samples = state.config.max_metric_samples.min(MAX_METRIC_SAMPLES);
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let execution = run_unified_query_with_stats_cancellable_for_runtime(
        state.clone(),
        query,
        scan_start,
        end,
        max_metric_rows.saturating_add(1),
        true,
        Some(max_metric_rows),
        cancellation.clone(),
        Some(remaining),
    )
    .await?;
    let row_count: usize = execution
        .results
        .iter()
        .map(|stream| stream.entries.len())
        .sum();
    if row_count > max_metric_rows {
        return Err(format!(
            "metric query exceeds the maximum of {max_metric_rows} scanned rows"
        ));
    }
    let input_memory = estimated_query_memory_bytes(&execution.results);
    let streams = execution.results;
    let scanned_rows = execution.scanned_rows;
    let grouping_fields = expr.grouping_fields();
    let mut entries = Vec::new();
    for stream in streams {
        for entry in stream.entries {
            if cancellation.load(Ordering::Acquire) {
                return Err("metric query timed out".to_string());
            }
            let mut labels = stream.labels.clone();
            // Pipeline-extracted fields are query-local structured metadata
            // at this point. Promote them into the metric label set so range
            // functions and aggregations can distinguish entries such as
            // `level=info` and `level=error` from the same stream.
            for (name, value) in &entry.structured_metadata {
                // Pipeline fields must not replace original stream labels.
                // A colliding extraction has already been renamed by
                // process_entry_with_labels.
                if grouping_fields.contains(name) {
                    labels.entry(name.clone()).or_insert_with(|| value.clone());
                }
            }
            entries.push((labels, entry));
        }
    }

    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _evaluation_permit = evaluation_permit;
        evaluate_metric_stream_with_limits(
            &expr,
            &entries,
            &evaluation_times,
            Some(task_cancellation.as_ref()),
            max_metric_series,
            max_metric_samples,
        )
    });
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let output = match tokio::time::timeout(remaining, &mut task).await {
        Ok(result) => {
            result.map_err(|error| format!("metric evaluation task failed: {error}"))??
        }
        Err(_) => {
            cancellation.store(true, Ordering::Release);
            let _ = task.await;
            return Err("metric query timed out".to_string());
        }
    };
    if output.len() > max_metric_series {
        return Err(format!(
            "metric query exceeds the maximum of {max_metric_series} series"
        ));
    }
    let sample_count: usize = output.values().map(Vec::len).sum();
    if sample_count > max_metric_samples {
        return Err(format!(
            "metric query exceeds the maximum of {max_metric_samples} output samples"
        ));
    }
    let estimated_memory = output
        .iter()
        .map(|(labels, samples)| {
            labels
                .iter()
                .map(|(name, value)| name.len().saturating_add(value.len()))
                .sum::<usize>()
                .saturating_add(samples.len().saturating_mul(std::mem::size_of::<(i64, f64)>()))
        })
        .sum::<usize>() as u64;
    if input_memory.saturating_add(estimated_memory) > state.config.max_query_memory_bytes {
        return Err(format!(
            "metric query exceeds the maximum of {} materialized bytes",
            state.config.max_query_memory_bytes
        ));
    }
    Ok(MetricQueryResult {
        series: output
            .into_iter()
            .map(|(labels, samples)| MetricSeries { labels, samples })
            .collect(),
        scanned_rows,
    })
}

#[cfg(test)]
fn evaluate_metric_at(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    timestamp_ns: i64,
) -> Vec<(Labels, f64)> {
    evaluate_metric_all(expr, entries, &[timestamp_ns])
        .expect("test metric evaluation must remain within resource limits")
        .into_iter()
        .next()
        .unwrap_or_default()
}

#[cfg(test)]
fn ensure_metric_series_limit(values: &[Vec<(Labels, f64)>]) -> Result<(), String> {
    if values
        .iter()
        .any(|at_time| at_time.len() > MAX_METRIC_SERIES)
    {
        return Err(format!(
            "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
        ));
    }
    let samples: usize = values.iter().map(Vec::len).sum();
    if samples > MAX_METRIC_SAMPLES {
        return Err(format!(
            "metric query exceeds the maximum of {MAX_METRIC_SAMPLES} output samples"
        ));
    }
    Ok(())
}

/// Evaluate all timestamps in one pass over each label series. The previous
/// implementation rescanned every entry for every evaluation point, turning a
/// bounded metric query into O(points * rows). Sliding window cursors reduce
/// the range-function work to O(rows + points * series), while the explicit
/// output-sample cap still bounds the materialized result.
#[cfg(test)]
fn evaluate_metric_all(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    evaluation_times: &[i64],
) -> Result<Vec<Vec<(Labels, f64)>>, String> {
    let values = match expr {
        logql::MetricExpr::Range {
            function, range_ns, ..
        } => {
            let mut by_labels: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
            for (labels, entry) in entries {
                let increment = match function {
                    logql::RangeFunction::BytesOverTime => entry.line.len() as f64,
                    logql::RangeFunction::Rate | logql::RangeFunction::CountOverTime => 1.0,
                };
                by_labels
                    .entry(labels.clone())
                    .or_default()
                    .push((entry.timestamp_ns, increment));
            }
            for events in by_labels.values_mut() {
                events.sort_unstable_by_key(|(timestamp_ns, _)| *timestamp_ns);
            }

            let mut output: Vec<Vec<(Labels, f64)>> =
                (0..evaluation_times.len()).map(|_| Vec::new()).collect();
            let seconds = *range_ns as f64 / 1_000_000_000.0;
            for (labels, events) in by_labels {
                let mut left = 0usize;
                let mut right = 0usize;
                let mut active_count = 0usize;
                let mut active_value = 0.0f64;
                for (index, &evaluation_ns) in evaluation_times.iter().enumerate() {
                    while right < events.len() && events[right].0 <= evaluation_ns {
                        active_count += 1;
                        active_value += events[right].1;
                        right += 1;
                    }
                    if let Some(window_start) = evaluation_ns.checked_sub(*range_ns) {
                        while left < right && events[left].0 <= window_start {
                            active_count -= 1;
                            active_value -= events[left].1;
                            left += 1;
                        }
                    }
                    if active_count > 0 {
                        let value = if matches!(function, logql::RangeFunction::Rate) {
                            active_value / seconds
                        } else {
                            active_value
                        };
                        output[index].push((labels.clone(), value));
                        if output[index].len() > MAX_METRIC_SERIES {
                            return Err(format!(
                                "metric query exceeds the maximum of {MAX_METRIC_SERIES} series"
                            ));
                        }
                    }
                }
            }
            output
        }
        logql::MetricExpr::Aggregate { op, by, expr } => {
            let inner = evaluate_metric_all(expr, entries, evaluation_times)?;
            let mut output = Vec::with_capacity(inner.len());
            for values in inner {
                let mut grouped: BTreeMap<Labels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in values {
                    let group = match by {
                        Some(names) => names
                            .iter()
                            .filter_map(|name| {
                                labels.get(name).map(|value| (name.clone(), value.clone()))
                            })
                            .collect(),
                        None => Labels::new(),
                    };
                    let aggregate = grouped.entry(group).or_insert((value, 0));
                    if aggregate.1 > 0 {
                        aggregate.0 = match op {
                            logql::AggregateOp::Sum | logql::AggregateOp::Avg => {
                                aggregate.0 + value
                            }
                            logql::AggregateOp::Min => aggregate.0.min(value),
                            logql::AggregateOp::Max => aggregate.0.max(value),
                        };
                    }
                    aggregate.1 += 1;
                }
                output.push(
                    grouped
                        .into_iter()
                        .map(|(labels, (mut value, count))| {
                            if matches!(op, logql::AggregateOp::Avg) {
                                value /= count as f64;
                            }
                            (labels, value)
                        })
                        .collect(),
                );
            }
            output
        }
        logql::MetricExpr::TopK { k, expr } => {
            let mut output = evaluate_metric_all(expr, entries, evaluation_times)?;
            for values in &mut output {
                values.sort_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                values.truncate(*k);
            }
            output
        }
    };
    ensure_metric_series_limit(&values)?;
    Ok(values)
}

struct MetricRangeSeries {
    events: Vec<(i64, f64)>,
    prefix: Vec<f64>,
}

struct MetricEvaluator {
    function: logql::RangeFunction,
    range_ns: i64,
    max_series: usize,
    by_labels: BTreeMap<Labels, MetricRangeSeries>,
}

impl MetricEvaluator {
    fn new(
        expr: &logql::MetricExpr,
        entries: &[(Labels, LogEntry)],
        cancellation: Option<&AtomicBool>,
        max_series: usize,
    ) -> Result<Self, String> {
        let (function, range_ns) = metric_range_spec(expr);
        let mut events_by_labels: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
        for (labels, entry) in entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("metric query timed out".to_string());
            }
            let increment = match function {
                logql::RangeFunction::BytesOverTime => entry.line.len() as f64,
                logql::RangeFunction::Rate | logql::RangeFunction::CountOverTime => 1.0,
            };
            if !events_by_labels.contains_key(labels) && events_by_labels.len() == max_series {
                return Err(format!(
                    "metric query exceeds the maximum of {max_series} series"
                ));
            }
            events_by_labels
                .entry(labels.clone())
                .or_default()
                .push((entry.timestamp_ns, increment));
        }

        let mut by_labels = BTreeMap::new();
        for (labels, mut events) in events_by_labels {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("metric query timed out".to_string());
            }
            events.sort_unstable_by_key(|(timestamp_ns, _)| *timestamp_ns);
            let mut prefix = Vec::with_capacity(events.len() + 1);
            prefix.push(0.0);
            for &(_, value) in &events {
                if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    return Err("metric query timed out".to_string());
                }
                prefix.push(prefix.last().copied().unwrap_or_default() + value);
            }
            by_labels.insert(labels, MetricRangeSeries { events, prefix });
        }
        Ok(Self {
            function,
            range_ns,
            max_series,
            by_labels,
        })
    }

    fn evaluate_at(
        &self,
        expr: &logql::MetricExpr,
        evaluation_ns: i64,
        cancellation: Option<&AtomicBool>,
    ) -> Result<Vec<(Labels, f64)>, String> {
        let mut values = match expr {
            logql::MetricExpr::Range { .. } => {
                let seconds = self.range_ns as f64 / 1_000_000_000.0;
                let window_start = evaluation_ns.checked_sub(self.range_ns);
                let mut values = Vec::new();
                for (labels, series) in &self.by_labels {
                    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        return Err("metric query timed out".to_string());
                    }
                    let right = series
                        .events
                        .partition_point(|(timestamp_ns, _)| *timestamp_ns <= evaluation_ns);
                    let left = window_start
                        .map(|start| {
                            series
                                .events
                                .partition_point(|(timestamp_ns, _)| *timestamp_ns <= start)
                        })
                        .unwrap_or(0);
                    if right > left {
                        let mut value = series.prefix[right] - series.prefix[left];
                        if matches!(self.function, logql::RangeFunction::Rate) {
                            value /= seconds;
                        }
                        values.push((labels.clone(), value));
                        if values.len() > self.max_series {
                            return Err(format!(
                                "metric query exceeds the maximum of {} series",
                                self.max_series
                            ));
                        }
                    }
                }
                values
            }
            logql::MetricExpr::Aggregate { op, by, expr } => {
                let inner = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                let mut grouped: BTreeMap<Labels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in inner {
                    let group = match by {
                        Some(names) => names
                            .iter()
                            .filter_map(|name| {
                                labels.get(name).map(|value| (name.clone(), value.clone()))
                            })
                            .collect(),
                        None => Labels::new(),
                    };
                    let aggregate = grouped.entry(group).or_insert((value, 0));
                    if aggregate.1 > 0 {
                        aggregate.0 = match op {
                            logql::AggregateOp::Sum | logql::AggregateOp::Avg => {
                                aggregate.0 + value
                            }
                            logql::AggregateOp::Min => aggregate.0.min(value),
                            logql::AggregateOp::Max => aggregate.0.max(value),
                        };
                    }
                    aggregate.1 += 1;
                }
                grouped
                    .into_iter()
                    .map(|(labels, (mut value, count))| {
                        if matches!(op, logql::AggregateOp::Avg) {
                            value /= count as f64;
                        }
                        (labels, value)
                    })
                    .collect()
            }
            logql::MetricExpr::TopK { k, expr } => {
                let mut values = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                values.sort_by(|left, right| {
                    right
                        .1
                        .total_cmp(&left.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                values.truncate(*k);
                values
            }
        };
        if values.len() > self.max_series {
            return Err(format!(
                "metric query exceeds the maximum of {} series",
                self.max_series
            ));
        }
        Ok(std::mem::take(&mut values))
    }
}

fn metric_range_spec(expr: &logql::MetricExpr) -> (logql::RangeFunction, i64) {
    match expr {
        logql::MetricExpr::Range {
            function, range_ns, ..
        } => (*function, *range_ns),
        logql::MetricExpr::Aggregate { expr, .. } | logql::MetricExpr::TopK { expr, .. } => {
            metric_range_spec(expr)
        }
    }
}

#[cfg(test)]
fn evaluate_metric_stream(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
) -> Result<BTreeMap<Labels, Vec<(i64, f64)>>, String> {
    evaluate_metric_stream_with_limits(
        expr,
        entries,
        evaluation_times,
        cancellation,
        MAX_METRIC_SERIES,
        MAX_METRIC_SAMPLES,
    )
}

fn evaluate_metric_stream_with_limits(
    expr: &logql::MetricExpr,
    entries: &[(Labels, LogEntry)],
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
    max_series: usize,
    max_samples: usize,
) -> Result<BTreeMap<Labels, Vec<(i64, f64)>>, String> {
    let evaluator = MetricEvaluator::new(expr, entries, cancellation, max_series)?;
    let mut output: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
    let mut sample_count = 0usize;
    for &timestamp_ns in evaluation_times {
        if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err("metric query timed out".to_string());
        }
        let values = evaluator.evaluate_at(expr, timestamp_ns, cancellation)?;
        sample_count = sample_count.saturating_add(values.len());
        if sample_count > max_samples {
            return Err(format!(
                "metric query exceeds the maximum of {max_samples} output samples"
            ));
        }
        for (labels, value) in values {
            if !output.contains_key(&labels) && output.len() == max_series {
                return Err(format!(
                    "metric query exceeds the maximum of {max_series} series"
                ));
            }
            output
                .entry(labels)
                .or_default()
                .push((timestamp_ns, value));
        }
    }
    Ok(output)
}

fn evaluation_times(start_ns: i64, end_ns: i64, step_ns: i64) -> Result<Vec<i64>, String> {
    if start_ns > end_ns {
        return Err("query start must not be after end".to_string());
    }
    if step_ns <= 0 {
        return Err("step must be greater than zero".to_string());
    }
    let mut times = Vec::new();
    let mut current = start_ns;
    loop {
        if times.len() == MAX_METRIC_EVALUATION_POINTS {
            return Err(format!(
                "metric query exceeds the maximum of {MAX_METRIC_EVALUATION_POINTS} evaluation points"
            ));
        }
        times.push(current);
        if current == end_ns {
            break;
        }
        let Some(next) = current.checked_add(step_ns) else {
            break;
        };
        if next > end_ns {
            break;
        }
        current = next;
    }
    Ok(times)
}
