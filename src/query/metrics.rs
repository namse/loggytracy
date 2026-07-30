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
    tenant: TenantId,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
) -> Result<Vec<MetricSeries>, String> {
    Ok(
        run_metric_query_with_stats(state, tenant, expr, evaluation_times, None)
            .await?
            .series,
    )
}

struct MetricQueryResult {
    series: Vec<MetricSeries>,
    scanned_rows: u64,
    /// Carried so the tenant's scan budget is charged in bytes on both paths.
    /// Rows would be a different unit for the same bucket, and a metric query
    /// over wide lines would then look cheap.
    scanned_bytes: u64,
}

async fn run_metric_query_with_stats(
    state: Arc<AppState>,
    tenant: TenantId,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
) -> Result<MetricQueryResult, String> {
    // The same bound as the log path. A metric query is a scan with an
    // aggregation on top, so exempting it would leave the cheaper of the two
    // limited and the more expensive one free.
    let _slot = state
        .tenant_quota
        .begin_query(&tenant)
        .map_err(|error| format!("{TENANT_QUOTA_PREFIX}{}", error.message))?;
    let quota = state.tenant_quota.clone();
    let quota_tenant = tenant.clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    let result = run_metric_query_with_stats_cancellable(
        state,
        tenant,
        expr,
        evaluation_times,
        scan_start_override,
        cancellation,
    )
    .await;
    if let Ok(execution) = &result {
        quota.charge_scan(&quota_tenant, execution.scanned_bytes);
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_metric_query_with_stats_cancellable(
    state: Arc<AppState>,
    tenant: TenantId,
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
        tenant,
        query,
        // Closed, unlike a log query's window: `end` here is the last
        // evaluation point, and the range evaluator's window closes on it, so
        // dropping the row at exactly that instant would change every final
        // sample rather than trim one row off a log response.
        part::QueryTimeRange::closed(scan_start, end),
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
    let scanned_bytes = execution.scanned_bytes;
    let grouping_fields = expr.grouping_fields();
    let mut entries = Vec::new();
    for stream in streams {
        for entry in stream.entries {
            if cancellation.load(Ordering::Acquire) {
                return Err("metric query timed out".to_string());
            }
            // Shared with the stream unless a grouping field actually has to be
            // promoted into it. This clone was the whole `BTreeMap` per row, on
            // an async worker thread and outside every arena — 203 MiB at its
            // high-water (`docs/MEMORY_ATTRIBUTION.md`, "an eighth term nobody
            // proposed").
            let mut labels = stream.labels.clone();
            // Pipeline-extracted fields are query-local structured metadata
            // at this point. Promote them into the metric label set so range
            // functions and aggregations can distinguish entries such as
            // `level=info` and `level=error` from the same stream.
            for (name, value) in &entry.structured_metadata {
                // Pipeline fields must not replace original stream labels.
                // A colliding extraction has already been renamed by
                // process_entry_with_labels.
                if grouping_fields.contains(name) && !labels.contains_key(name) {
                    SharedLabels::make_mut(&mut labels).insert(name.clone(), value.clone());
                }
            }
            entries.push((labels, entry));
        }
    }

    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _evaluation_permit = evaluation_permit;
        let _arena = crate::memprof::enter(crate::memprof::Arena::Query);
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
        scanned_bytes,
    })
}

#[cfg(test)]
fn evaluate_metric_at(
    expr: &logql::MetricExpr,
    entries: &[(SharedLabels, LogEntry)],
    timestamp_ns: i64,
) -> Vec<(SharedLabels, f64)> {
    evaluate_metric_all(expr, entries, &[timestamp_ns])
        .expect("test metric evaluation must remain within resource limits")
        .into_iter()
        .next()
        .unwrap_or_default()
}

#[cfg(test)]
fn ensure_metric_series_limit(values: &[Vec<(SharedLabels, f64)>]) -> Result<(), String> {
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
    entries: &[(SharedLabels, LogEntry)],
    evaluation_times: &[i64],
) -> Result<Vec<Vec<(SharedLabels, f64)>>, String> {
    let values = match expr {
        logql::MetricExpr::Range { range_ns, .. } => {
            let spec = metric_range_spec(expr);
            let mut by_labels: BTreeMap<SharedLabels, Vec<(i64, f64)>> = BTreeMap::new();
            for (labels, entry) in entries {
                let Some(increment) = sample_value(&spec, labels, entry) else {
                    continue;
                };
                by_labels
                    .entry(labels.clone())
                    .or_default()
                    .push((entry.timestamp_ns, increment));
            }
            for events in by_labels.values_mut() {
                events.sort_unstable_by_key(|(timestamp_ns, _)| *timestamp_ns);
            }

            let mut output: Vec<Vec<(SharedLabels, f64)>> =
                (0..evaluation_times.len()).map(|_| Vec::new()).collect();
            for (labels, events) in by_labels {
                let mut prefix = Vec::with_capacity(events.len() + 1);
                prefix.push(0.0);
                for &(_, value) in &events {
                    prefix.push(prefix.last().copied().unwrap_or_default() + value);
                }
                let series = MetricRangeSeries { events, prefix };
                for (index, &evaluation_ns) in evaluation_times.iter().enumerate() {
                    let window_end = evaluation_ns.saturating_sub(spec.offset_ns);
                    let right = series
                        .events
                        .partition_point(|(timestamp_ns, _)| *timestamp_ns <= window_end);
                    let left = window_end
                        .checked_sub(*range_ns)
                        .map(|start| {
                            series
                                .events
                                .partition_point(|(timestamp_ns, _)| *timestamp_ns <= start)
                        })
                        .unwrap_or(0);
                    if right > left {
                        let value = window_value(&spec, &series, left, right);
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
        logql::MetricExpr::Aggregate { op, grouping, expr } => {
            let inner = evaluate_metric_all(expr, entries, evaluation_times)?;
            let mut output = Vec::with_capacity(inner.len());
            for values in inner {
                let mut grouped: BTreeMap<SharedLabels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in values {
                    let group = match grouping {
                        Some(grouping) => SharedLabels::new(grouping.key(&labels)),
                        None => empty_labels(),
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
        logql::MetricExpr::Binary {
            op,
            expr,
            scalar,
            scalar_on_left,
        } => evaluate_metric_all(expr, entries, evaluation_times)?
            .into_iter()
            .map(|values| {
                values
                    .into_iter()
                    .filter_map(|(labels, value)| {
                        let (left, right) = if *scalar_on_left {
                            (*scalar, value)
                        } else {
                            (value, *scalar)
                        };
                        op.apply(left, right).map(|value| (labels, value))
                    })
                    .collect()
            })
            .collect(),
        // Subqueries are evaluated only by the streaming path, which is what
        // production uses. This test-only evaluator exists to cross-check the
        // simpler expression forms against it and gains nothing from a second
        // implementation of the harder one.
        logql::MetricExpr::Subquery { .. } => {
            return Err("subqueries are evaluated by the streaming path".to_string());
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

/// The label set an ungrouped aggregation reports under, allocated once for the
/// process rather than once per series per evaluation point.
fn empty_labels() -> SharedLabels {
    static EMPTY: std::sync::OnceLock<SharedLabels> = std::sync::OnceLock::new();
    EMPTY.get_or_init(|| SharedLabels::new(Labels::new())).clone()
}

struct MetricEvaluator {
    range_ns: i64,
    max_series: usize,
    by_labels: BTreeMap<SharedLabels, MetricRangeSeries>,
}

impl MetricEvaluator {
    fn new(
        expr: &logql::MetricExpr,
        entries: &[(SharedLabels, LogEntry)],
        cancellation: Option<&AtomicBool>,
        max_series: usize,
    ) -> Result<Self, String> {
        let spec = metric_range_spec(expr);
        let range_ns = spec.range_ns;
        let mut events_by_labels: BTreeMap<SharedLabels, Vec<(i64, f64)>> = BTreeMap::new();
        for (labels, entry) in entries {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                return Err("metric query timed out".to_string());
            }
            let Some(increment) = sample_value(&spec, labels, entry) else {
                continue;
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
    ) -> Result<Vec<(SharedLabels, f64)>, String> {
        let mut values = match expr {
            logql::MetricExpr::Range { .. } => {
                let spec = metric_range_spec(expr);
                // The offset shifts the window back; the evaluation point it is
                // reported at does not move.
                let window_end = evaluation_ns.saturating_sub(spec.offset_ns);
                let window_start = window_end.checked_sub(self.range_ns);
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
                        let value = window_value(&spec, series, left, right);
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
            logql::MetricExpr::Subquery {
                function,
                quantile,
                inner,
                range_ns,
                step_ns,
                offset_ns,
            } => {
                // The inner expression is evaluated on the subquery's own step
                // grid inside the outer window, and those samples are what the
                // outer function aggregates. Evaluating it once at the outer
                // point instead would make `max_over_time(rate(…)[1h:1m])`
                // return the rate at one instant rather than the largest of
                // sixty.
                let window_end = evaluation_ns.saturating_sub(*offset_ns);
                let window_start = window_end.saturating_sub(*range_ns);
                let mut samples: BTreeMap<SharedLabels, Vec<f64>> = BTreeMap::new();
                let mut point = window_start.saturating_add(*step_ns);
                let mut steps = 0usize;
                while point <= window_end {
                    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        return Err("metric query timed out".to_string());
                    }
                    steps += 1;
                    if steps > MAX_METRIC_EVALUATION_POINTS {
                        return Err(format!(
                            "subquery exceeds the maximum of {MAX_METRIC_EVALUATION_POINTS} \
inner evaluation points"
                        ));
                    }
                    for (labels, value) in self.evaluate_at(inner, point, cancellation)? {
                        samples.entry(labels).or_default().push(value);
                    }
                    point = point.saturating_add(*step_ns);
                }
                samples
                    .into_iter()
                    .filter_map(|(labels, mut values)| {
                        if values.is_empty() {
                            return None;
                        }
                        let count = values.len() as f64;
                        let value = match function {
                            logql::RangeFunction::Rate => {
                                values.iter().sum::<f64>() / (*range_ns as f64 / 1e9)
                            }
                            logql::RangeFunction::CountOverTime => count,
                            logql::RangeFunction::BytesOverTime
                            | logql::RangeFunction::SumOverTime => values.iter().sum(),
                            logql::RangeFunction::AvgOverTime => {
                                values.iter().sum::<f64>() / count
                            }
                            logql::RangeFunction::MinOverTime => {
                                values.iter().copied().fold(f64::INFINITY, f64::min)
                            }
                            logql::RangeFunction::MaxOverTime => {
                                values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                            }
                            logql::RangeFunction::QuantileOverTime => {
                                values.sort_by(|left, right| {
                                    left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                quantile_of(&values, quantile.unwrap_or(0.0))
                            }
                        };
                        Some((labels, value))
                    })
                    .collect()
            }
            logql::MetricExpr::Binary {
                op,
                expr,
                scalar,
                scalar_on_left,
            } => {
                let inner = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                inner
                    .into_iter()
                    .filter_map(|(labels, value)| {
                        let (left, right) = if *scalar_on_left {
                            (*scalar, value)
                        } else {
                            (value, *scalar)
                        };
                        // A comparison that does not hold drops the series
                        // rather than yielding zero, which is what makes
                        // `> 100` a filter and not an indicator.
                        op.apply(left, right).map(|value| (labels, value))
                    })
                    .collect()
            }
            logql::MetricExpr::Aggregate { op, grouping, expr } => {
                let inner = self.evaluate_at(expr, evaluation_ns, cancellation)?;
                let mut grouped: BTreeMap<SharedLabels, (f64, usize)> = BTreeMap::new();
                for (labels, value) in inner {
                    let group = match grouping {
                        Some(grouping) => SharedLabels::new(grouping.key(&labels)),
                        None => empty_labels(),
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

/// The innermost range function and everything it needs to turn entries into
/// samples.
struct RangeSpec<'a> {
    function: logql::RangeFunction,
    range_ns: i64,
    unwrap: Option<&'a logql::Unwrap>,
    quantile: Option<f64>,
    offset_ns: i64,
}

fn metric_range_spec(expr: &logql::MetricExpr) -> RangeSpec<'_> {
    match expr {
        logql::MetricExpr::Range {
            function,
            range_ns,
            unwrap,
            quantile,
            offset_ns,
            ..
        } => RangeSpec {
            function: *function,
            range_ns: *range_ns,
            unwrap: unwrap.as_ref(),
            quantile: *quantile,
            offset_ns: *offset_ns,
        },
        logql::MetricExpr::Aggregate { expr, .. }
        | logql::MetricExpr::TopK { expr, .. }
        | logql::MetricExpr::Binary { expr, .. } => metric_range_spec(expr),
        logql::MetricExpr::Subquery { inner, .. } => metric_range_spec(inner),
    }
}

/// The value one entry contributes.
///
/// `None` drops the entry. For the counting functions that never happens; for
/// an unwrapped one it means the field was absent or did not convert, and a
/// failed conversion is not a measurement of zero.
fn sample_value(
    spec: &RangeSpec<'_>,
    labels: &Labels,
    entry: &crate::memtable::LogEntry,
) -> Option<f64> {
    match spec.unwrap {
        None => Some(match spec.function {
            logql::RangeFunction::BytesOverTime => entry.line.len() as f64,
            _ => 1.0,
        }),
        Some(unwrap) => {
            // The pipeline leaves its evaluated fields on the query-local
            // entry, and stream labels are visible to it, so the unwrap reads
            // the same set the field filters did.
            let mut fields: BTreeMap<String, String> = labels.clone();
            for (name, value) in &entry.structured_metadata {
                fields.insert(name.clone(), value.clone());
            }
            unwrap.value(&fields)
        }
    }
}

/// The value of a range function over the samples in one window.
fn window_value(spec: &RangeSpec<'_>, series: &MetricRangeSeries, left: usize, right: usize) -> f64 {
    let count = (right - left) as f64;
    match spec.function {
        logql::RangeFunction::Rate => {
            (series.prefix[right] - series.prefix[left]) / (spec.range_ns as f64 / 1e9)
        }
        logql::RangeFunction::CountOverTime => count,
        logql::RangeFunction::BytesOverTime | logql::RangeFunction::SumOverTime => {
            series.prefix[right] - series.prefix[left]
        }
        logql::RangeFunction::AvgOverTime => (series.prefix[right] - series.prefix[left]) / count,
        logql::RangeFunction::MinOverTime => series.events[left..right]
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::INFINITY, f64::min),
        logql::RangeFunction::MaxOverTime => series.events[left..right]
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::NEG_INFINITY, f64::max),
        logql::RangeFunction::QuantileOverTime => {
            let mut window: Vec<f64> = series.events[left..right]
                .iter()
                .map(|(_, value)| *value)
                .collect();
            // Sorted per evaluation point rather than kept sorted: the window
            // slides by time, so an order maintained incrementally would have
            // to support removal from the middle, and the windows here are
            // bounded by the sample cap anyway.
            window.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
            quantile_of(&window, spec.quantile.unwrap_or(0.0))
        }
    }
}

/// Linear interpolation between the two nearest ranks, which is what Prometheus
/// and Loki report. Picking the nearest sample instead would make a p99 over
/// three points meaningless in a different way for every window size.
fn quantile_of(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = quantile * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return sorted[lower];
    }
    sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
}

#[cfg(test)]
fn evaluate_metric_stream(
    expr: &logql::MetricExpr,
    entries: &[(SharedLabels, LogEntry)],
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
) -> Result<BTreeMap<SharedLabels, Vec<(i64, f64)>>, String> {
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
    entries: &[(SharedLabels, LogEntry)],
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
    max_series: usize,
    max_samples: usize,
) -> Result<BTreeMap<SharedLabels, Vec<(i64, f64)>>, String> {
    let evaluator = MetricEvaluator::new(expr, entries, cancellation, max_series)?;
    let mut output: BTreeMap<SharedLabels, Vec<(i64, f64)>> = BTreeMap::new();
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
