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
        run_metric_query_with_stats(
            state,
            tenant,
            expr,
            evaluation_times,
            None,
            crate::metrics::QueryEndpoint::Query,
        )
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
    tenant: TenantId,
    expr: logql::MetricExpr,
    evaluation_times: Vec<i64>,
    scan_start_override: Option<i64>,
    endpoint: crate::metrics::QueryEndpoint,
) -> Result<MetricQueryResult, String> {
    // The same bound as the log path. A metric query is a scan with an
    // aggregation on top, so exempting it would leave the cheaper of the two
    // limited and the more expensive one free.
    let _slot = state
        .tenant_quota
        .begin_query(&tenant)
        .map_err(|error| format!("{TENANT_QUOTA_PREFIX}{}", error.message))?;
    let cancellation = Arc::new(AtomicBool::new(false));
    // Timed here for the same reason the log path is timed at its own funnel,
    // and this is the half that was missing: `rate`, `count_over_time` and
    // everything `index/volume` reduces to ran without appearing in
    // `loggytracy_query_latency_ms` at all, so the read path's published
    // latency described log queries and was read as describing queries.
    let metrics = state.metrics.clone();
    let started = std::time::Instant::now();
    let result = run_metric_query_with_stats_cancellable(
        state,
        tenant,
        expr,
        evaluation_times,
        scan_start_override,
        cancellation,
    )
    .await;
    metrics.observe_query(endpoint, started.elapsed());
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
    let max_metric_series = state.config.max_metric_series.min(MAX_METRIC_SERIES);
    let max_metric_samples = state.config.max_metric_samples.min(MAX_METRIC_SAMPLES);

    // `sum()` of a count-shaped range function needs no rows at all: the
    // answer is how many rows (or line bytes) land in each evaluation window,
    // and the sink can accumulate that directly — two array updates per row
    // where the general path grouped, sorted and prefix-summed a `LogEntry`
    // per row. The general path stays for everything the accumulation cannot
    // express: unwraps, quantiles, per-series grouping, offsets, subqueries.
    if let logql::MetricExpr::Aggregate {
        op: logql::AggregateOp::Sum,
        grouping: None,
        expr: inner,
    } = &expr
        && let logql::MetricExpr::Range {
            function,
            unwrap: None,
            quantile: None,
            offset_ns: 0,
            range_ns,
            ..
        } = &**inner
        && matches!(
            function,
            logql::RangeFunction::Rate
                | logql::RangeFunction::CountOverTime
                | logql::RangeFunction::BytesOverTime
        )
    {
        let range_ns = *range_ns;
        let rate = matches!(function, logql::RangeFunction::Rate);
        let bytes = matches!(function, logql::RangeFunction::BytesOverTime);
        let columns = expr.required_columns();
        let (diff, accepted, scanned_rows, _scanned_bytes) = run_metric_count_scan(
            state,
            tenant,
            query,
            part::QueryTimeRange::closed(scan_start, end),
            None,
            cancellation,
            Some(deadline.saturating_duration_since(tokio::time::Instant::now())),
            columns,
            evaluation_times.clone(),
            range_ns,
            bytes,
        )
        .await?;
        drop(evaluation_permit);
        let mut series = Vec::new();
        if accepted > 0 {
            let mut samples = Vec::with_capacity(evaluation_times.len());
            let mut running = 0.0;
            for (index, &at) in evaluation_times.iter().enumerate() {
                running += diff[index];
                let value = if rate {
                    running / (range_ns as f64 / 1_000_000_000.0)
                } else {
                    running
                };
                samples.push((at, value));
            }
            series.push(MetricSeries {
                labels: SharedLabels::new(Labels::new()),
                samples,
            });
        }
        return Ok(MetricQueryResult {
            series,
            scanned_rows,
        });
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    // No row ceiling, and no log limit. Both used to be `max_metric_rows`,
    // because the scan handed back every matching line and the evaluator threw
    // them away one entry later — so a `rate()` over a busy stream was refused
    // for materializing an intermediate the client was never going to receive.
    // The fold below keeps sixteen bytes an event instead, and what a metric
    // query costs is still bounded: `max_query_scan_bytes` for the read,
    // `max_query_runtime` for the wall clock, `max_metric_series` and
    // `max_metric_samples` for the answer.
    let mask = state.delete_requests.mask_for(&tenant);
    let max_query_memory_bytes = state.config.max_query_memory_bytes;
    let sink = MetricEventSink::new(
        &expr,
        query.clone(),
        mask,
        Some(cancellation.clone()),
        max_metric_series,
        Some(max_query_memory_bytes),
    );
    let (sink, scanned_rows, _scanned_bytes) = run_metric_event_scan(
        state.clone(),
        tenant,
        query,
        // Closed, unlike a log query's window: `end` here is the last
        // evaluation point, and the range evaluator's window closes on it, so
        // dropping the row at exactly that instant would change every final
        // sample rather than trim one row off a log response.
        part::QueryTimeRange::closed(scan_start, end),
        cancellation.clone(),
        Some(remaining),
        // The metric path decodes only what the expression can read: the
        // timestamp, the labels, the grouping and unwrap fields — not the
        // line and not the rest of the metadata, unless a parser or template
        // stage forces everything. `sum(rate({app="x"}[5m]))` was decoding
        // every column of every row to add one to a counter per row.
        expr.required_columns(),
        sink,
    )
    .await?;
    let input_memory = sink.retained_bytes();

    let task_cancellation = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        let _evaluation_permit = evaluation_permit;
        let _arena = crate::memprof::enter(crate::memprof::Arena::Query);
        let evaluator = MetricEvaluator::from_sink(sink, Some(task_cancellation.as_ref()))?;
        evaluate_metric_stream_with_limits(
            &expr,
            &evaluator,
            &evaluation_times,
            Some(task_cancellation.as_ref()),
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

/// The owned half of `RangeSpec`, which borrows from the expression.
///
/// A sink crosses into the scan's blocking task, so it cannot hold a borrow of
/// the expression. It holds the same five fields and lends them back per row.
struct OwnedRangeSpec {
    function: logql::RangeFunction,
    range_ns: i64,
    unwrap: Option<logql::Unwrap>,
    quantile: Option<f64>,
    offset_ns: i64,
}

impl OwnedRangeSpec {
    fn of(expr: &logql::MetricExpr) -> Self {
        let spec = metric_range_spec(expr);
        Self {
            function: spec.function,
            range_ns: spec.range_ns,
            unwrap: spec.unwrap.cloned(),
            quantile: spec.quantile,
            offset_ns: spec.offset_ns,
        }
    }

    fn borrow(&self) -> RangeSpec<'_> {
        RangeSpec {
            function: self.function,
            range_ns: self.range_ns,
            unwrap: self.unwrap.as_ref(),
            quantile: self.quantile,
            offset_ns: self.offset_ns,
        }
    }
}

/// Folds scanned rows into per-series samples as they arrive.
///
/// `MetricEvaluator` never wanted the rows. Its first act was to reduce them to
/// a `(timestamp, value)` per label set and drop every line, so the
/// `Vec<(SharedLabels, LogEntry)>` in between existed only to be discarded — and
/// that intermediate is what `max_metric_rows` bounded. It is why a `rate()`
/// over a busy stream came back refused rather than answered: the ceiling was on
/// something the client was never going to receive.
///
/// What survives here is sixteen bytes an event plus one label set a series,
/// which is what `max_metric_series` and `max_metric_samples` already bound.
///
/// Owns its context rather than borrowing it, unlike `CountingSink`: this sink
/// is built before the blocking task and moves into it.
struct MetricEventSink {
    /// Stages still run per row — a field filter must reject before the fold.
    query: logql::LogQuery,
    mask: crate::delete_requests::DeleteMask,
    cancellation: Option<Arc<AtomicBool>>,
    spec: OwnedRangeSpec,
    /// Names the aggregation groups by. Pipeline-extracted fields are
    /// query-local structured metadata at this point, and the ones being
    /// grouped on have to be promoted into the label set so range functions can
    /// tell `level=info` from `level=error` within one stream.
    grouping_fields: BTreeSet<String>,
    max_series: usize,
    max_memory_bytes: Option<u64>,
    events: BTreeMap<SharedLabels, Vec<(i64, f64)>>,
    retained_bytes: u64,
    /// Counted here and published once, rather than through a borrowed counter.
    hidden_rows: u64,
}

impl MetricEventSink {
    fn new(
        expr: &logql::MetricExpr,
        query: logql::LogQuery,
        mask: crate::delete_requests::DeleteMask,
        cancellation: Option<Arc<AtomicBool>>,
        max_series: usize,
        max_memory_bytes: Option<u64>,
    ) -> Self {
        Self {
            query,
            mask,
            cancellation,
            spec: OwnedRangeSpec::of(expr),
            grouping_fields: expr.grouping_fields(),
            max_series,
            max_memory_bytes,
            events: BTreeMap::new(),
            retained_bytes: 0,
            hidden_rows: 0,
        }
    }

    /// The fold on its own, for tests that hand it rows a scan already produced:
    /// no mask, and no stages to re-run over entries the pipeline has passed.
    #[cfg(test)]
    fn for_tests(expr: &logql::MetricExpr, max_series: usize) -> Self {
        let mut query = expr.log_query().clone();
        query.stages.clear();
        Self::new(
            expr,
            query,
            crate::delete_requests::DeleteMask::default(),
            None,
            max_series,
            None,
        )
    }

    fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn accept_inner(
        &mut self,
        labels: &SharedLabels,
        mut entry: LogEntry,
        extracted_json: Option<std::collections::BTreeMap<String, String>>,
    ) -> Result<(), String> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Err("metric query timed out".to_string());
        }
        // Before the pipeline runs: a delete selector matches the line as it
        // was written, and `line_format` would have rewritten it.
        if !self.mask.is_empty() && self.mask.hides(&entry) {
            self.hidden_rows += 1;
            return Ok(());
        }
        if (!self.query.stages.is_empty() || !self.query.matchers.is_empty())
            && !self.query.process_entry_with_precomputed_json(
                labels,
                &mut entry,
                self.cancellation.as_deref(),
                extracted_json.as_ref(),
            )?
        {
            return Ok(());
        }
        // Shared with the stream unless a grouping field actually has to be
        // promoted into it. The `contains_key` guard is also the shadowing
        // rule: a metadata pair whose key is a stream label never wins, and a
        // colliding extraction has already been renamed by
        // `process_entry_with_labels`.
        let mut labels = labels.clone();
        for (name, value) in &entry.structured_metadata {
            if self.grouping_fields.contains(name) && !labels.contains_key(name) {
                SharedLabels::make_mut(&mut labels).insert(name.clone(), value.clone());
            }
        }
        let Some(increment) = sample_value(&self.spec.borrow(), &labels, &entry) else {
            return Ok(());
        };
        let sample = (entry.timestamp_ns, increment);
        match self.events.get_mut(&labels) {
            Some(events) => events.push(sample),
            None => {
                if self.events.len() == self.max_series {
                    return Err(format!(
                        "metric query exceeds the maximum of {} series",
                        self.max_series
                    ));
                }
                self.retained_bytes = self.retained_bytes.saturating_add(
                    labels
                        .iter()
                        .map(|(name, value)| name.len().saturating_add(value.len()))
                        .sum::<usize>() as u64,
                );
                self.events.insert(labels, vec![sample]);
            }
        }
        self.retained_bytes = self
            .retained_bytes
            .saturating_add(std::mem::size_of::<(i64, f64)>() as u64);
        if let Some(max) = self.max_memory_bytes
            && self.retained_bytes > max
        {
            return Err(format!(
                "query exceeds the maximum of {max} materialized bytes"
            ));
        }
        Ok(())
    }
}

impl crate::part::RowSink for MetricEventSink {
    fn accept_extracted(
        &mut self,
        labels: &SharedLabels,
        entry: LogEntry,
        extracted_json: Option<std::collections::BTreeMap<String, String>>,
    ) -> Result<(), String> {
        self.accept_inner(labels, entry, extracted_json)
    }

    fn accept(&mut self, labels: &SharedLabels, entry: LogEntry) -> Result<(), String> {
        self.accept_inner(labels, entry, None)
    }
}

struct MetricEvaluator {
    range_ns: i64,
    max_series: usize,
    by_labels: BTreeMap<SharedLabels, MetricRangeSeries>,
}

impl MetricEvaluator {
    /// Sorts each series and builds its prefix sums.
    ///
    /// Separate from the fold because the fold runs in the scan's blocking task
    /// and this runs in the evaluation task, which is what holds the metric
    /// permit.
    fn from_sink(sink: MetricEventSink, cancellation: Option<&AtomicBool>) -> Result<Self, String> {
        let range_ns = sink.spec.range_ns;
        let max_series = sink.max_series;
        let events_by_labels = sink.events;

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
            // the same set the field filters did — but it names one field, so
            // only that field is resolved. Building the whole set was a
            // `Labels` clone plus a `BTreeMap` per row.
            //
            // Resolution order is the one the map's inserts produced: the last
            // structured-metadata pair with the name wins over an earlier one,
            // and any of them wins over the stream label.
            let raw = entry
                .structured_metadata
                .iter()
                .rev()
                .find(|(name, _)| *name == unwrap.field)
                .map(|(_, value)| value.as_str())
                .or_else(|| labels.get(&unwrap.field).map(String::as_str))?;
            unwrap.convert(raw)
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
    // Feeds the sink the way a scan would, so the tests exercise the fold
    // rather than a second way of reaching the evaluator.
    let mut sink = MetricEventSink::for_tests(expr, MAX_METRIC_SERIES);
    for (labels, entry) in entries {
        crate::part::RowSink::accept(&mut sink, labels, entry.clone())?;
    }
    let evaluator = MetricEvaluator::from_sink(sink, cancellation)?;
    evaluate_metric_stream_with_limits(
        expr,
        &evaluator,
        evaluation_times,
        cancellation,
        MAX_METRIC_SAMPLES,
    )
}

fn evaluate_metric_stream_with_limits(
    expr: &logql::MetricExpr,
    evaluator: &MetricEvaluator,
    evaluation_times: &[i64],
    cancellation: Option<&AtomicBool>,
    max_samples: usize,
) -> Result<BTreeMap<SharedLabels, Vec<(i64, f64)>>, String> {
    let max_series = evaluator.max_series;
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
