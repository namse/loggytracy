//! OTLP metrics ingest: the decomposition of the five OTLP metric types into
//! float series, and the transport-independent admission contract (M14,
//! issue #8).
//!
//! [`normalize_request`] is deliberately a **pure function of the request** —
//! no clock, no config, no per-series state — because the journal stores the
//! raw `ExportMetricsServiceRequest` bytes (kind 3) and replay runs this same
//! function over them. Changing the decomposition changes what old journals
//! replay into; the journal is unversioned by policy, and the
//! replay-equals-live test pins the coupling. The one stateful step — folding
//! delta-temporality increments into running totals — happens at memtable
//! insert (`series::SeriesMemTable::insert`), which replay drives in the same
//! append order live ingest did, so the totals reproduce.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint,
    metric, number_data_point,
};
use prost014::Message;
use tonic::{Request, Response, Status};

use crate::backpressure::{IngestError, IngestGate};
use crate::config::Config;
use crate::journal::Journal;
use crate::otlp_log::normalize_attribute_key;
use crate::series::{METRIC_NAME_LABEL, MetricSample, SampleKind, SeriesLabels};
use crate::shutdown::ShutdownState;
use crate::tenant::TenantId;
use crate::trace_ingest::MAX_OTLP_REQUEST_BYTES;
use axum::http::StatusCode;

/// Decomposed-sample cap per request. Counted **after** decomposition, since
/// one histogram datapoint fans out to `bounds + 3` samples — the cap must
/// bound what the engine actually stores, not what the wire carried. The same
/// class of constant as `MAX_OTLP_SPANS`.
pub const MAX_OTLP_METRIC_SAMPLES: usize = 100_000;

/// Exponential histograms are downscaled until at most this many finite
/// bucket boundaries remain (plus `+Inf`), then stored as ordinary
/// `_bucket{le=}` series. The loss is boundary-limited quantile precision —
/// the decision record in `docs/M14_IMPLEMENTATION_PLAN.md` §3.
pub const MAX_EXP_HISTOGRAM_BUCKETS: usize = 64;

/// Resource attributes promoted to series labels, keys normalized by
/// [`normalize_attribute_key`]. One schema decision, small on purpose: series
/// identity must not inherit every resource attribute a collector attaches
/// (a per-process `process.pid` would mint a series per restart), so only the
/// names a dashboard actually groups by are promoted and the rest are
/// dropped. A datapoint attribute with the same normalized key wins.
pub const PROMOTED_RESOURCE_ATTRIBUTES: [&str; 8] = [
    "service.name",
    "service.namespace",
    "service.instance.id",
    "deployment.environment",
    "k8s.cluster.name",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.container.name",
];

#[derive(Debug, PartialEq, Eq)]
pub enum MetricIngestError {
    EmptyRequest,
    TimestampOutOfRange,
    TooManySamples,
}

impl std::fmt::Display for MetricIngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyRequest => "OTLP metrics request contains no datapoints",
            Self::TimestampOutOfRange => {
                "metric datapoint timestamp is outside the supported range"
            }
            Self::TooManySamples => "OTLP metrics request decomposes into too many samples",
        };
        f.write_str(message)
    }
}

impl std::error::Error for MetricIngestError {}

/// The `le`/`quantile` label rendering: Rust's shortest round-trip float
/// form, which prints `0.005` as `0.005` and infinity as `+Inf` the way the
/// Prometheus ecosystem spells it. Pinned by test because two renderings of
/// one boundary would be two series.
pub fn format_boundary(value: f64) -> String {
    if value == f64::INFINITY {
        return "+Inf".to_string();
    }
    format!("{value}")
}

struct Decomposition {
    samples: Vec<MetricSample>,
    /// The next datapoint's index in the request's deterministic traversal
    /// order — the same order [`filter_request`] walks, which is what lets an
    /// admission verdict made over samples drop the right datapoints from the
    /// WAL bytes.
    next_datapoint: u32,
    datapoint: u32,
}

impl Decomposition {
    /// Every datapoint consumes exactly one index, whether or not it produces
    /// samples, so the two traversals cannot drift on a skipped point.
    fn begin_datapoint(&mut self) {
        self.datapoint = self.next_datapoint;
        self.next_datapoint += 1;
    }

    fn push(
        &mut self,
        tenant: &TenantId,
        labels: SeriesLabels,
        ts_ns: i64,
        value: f64,
        kind: SampleKind,
    ) -> Result<(), MetricIngestError> {
        if self.samples.len() >= MAX_OTLP_METRIC_SAMPLES {
            return Err(MetricIngestError::TooManySamples);
        }
        self.samples.push(MetricSample {
            tenant: tenant.clone(),
            labels,
            ts_ns,
            value,
            kind,
            datapoint_index: self.datapoint,
        });
        Ok(())
    }
}

/// Base pairs for a datapoint: `__name__` first-pushed (wins any collision),
/// then the datapoint attributes, then the promoted resource attributes —
/// `SeriesLabels::from_pairs` keeps the first occurrence of a key, so this
/// push order is the precedence order.
fn base_pairs(
    name: &str,
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    promoted: &[(String, String)],
) -> Vec<(String, String)> {
    let mut pairs = Vec::with_capacity(2 + attributes.len() + promoted.len());
    pairs.push((METRIC_NAME_LABEL.to_string(), name.to_string()));
    for attribute in attributes {
        if let Some(value) = &attribute.value {
            pairs.push((
                normalize_attribute_key(&attribute.key),
                attribute_display(value),
            ));
        }
    }
    pairs.extend_from_slice(promoted);
    pairs
}

fn attribute_display(value: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &value.value {
        Some(Value::StringValue(text)) => text.clone(),
        Some(Value::BoolValue(value)) => value.to_string(),
        Some(Value::IntValue(value)) => value.to_string(),
        Some(Value::DoubleValue(value)) => value.to_string(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn with_extra(base: &[(String, String)], key: &str, value: String) -> SeriesLabels {
    let mut pairs = Vec::with_capacity(base.len() + 1);
    pairs.push((key.to_string(), value));
    pairs.extend_from_slice(base);
    SeriesLabels::from_pairs(pairs)
}

fn datapoint_ts(time_unix_nano: u64) -> Result<i64, MetricIngestError> {
    i64::try_from(time_unix_nano).map_err(|_| MetricIngestError::TimestampOutOfRange)
}

fn number_value(point: &NumberDataPoint) -> Option<f64> {
    match point.value {
        Some(number_data_point::Value::AsDouble(value)) => Some(value),
        Some(number_data_point::Value::AsInt(value)) => Some(value as f64),
        // A datapoint with no value carries nothing to store; OTLP permits it
        // and dropping it loses nothing a query could have read.
        None => None,
    }
}

/// Delta temporality accumulates at insert; everything else — cumulative and
/// the spec's "unspecified" — is stored as sent.
fn temporality_kind(temporality: i32) -> SampleKind {
    if temporality == AggregationTemporality::Delta as i32 {
        SampleKind::Delta
    } else {
        SampleKind::Cumulative
    }
}

/// Turn one OTLP export into float samples, per the decomposition in
/// `docs/M14_IMPLEMENTATION_PLAN.md` §3. Pure — see the module doc.
pub fn normalize_request(
    tenant: &TenantId,
    request: &ExportMetricsServiceRequest,
) -> Result<Vec<MetricSample>, MetricIngestError> {
    let mut out = Decomposition {
        samples: Vec::new(),
        next_datapoint: 0,
        datapoint: 0,
    };
    for resource_metrics in &request.resource_metrics {
        let promoted: Vec<(String, String)> = resource_metrics
            .resource
            .as_ref()
            .map(|resource| {
                resource
                    .attributes
                    .iter()
                    .filter(|attribute| {
                        PROMOTED_RESOURCE_ATTRIBUTES.contains(&attribute.key.as_str())
                    })
                    .filter_map(|attribute| {
                        attribute.value.as_ref().map(|value| {
                            (
                                normalize_attribute_key(&attribute.key),
                                attribute_display(value),
                            )
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                match &metric.data {
                    Some(metric::Data::Gauge(gauge)) => {
                        for point in &gauge.data_points {
                            out.begin_datapoint();
                            let Some(value) = number_value(point) else {
                                continue;
                            };
                            let pairs = base_pairs(&metric.name, &point.attributes, &promoted);
                            out.push(
                                tenant,
                                SeriesLabels::from_pairs(pairs),
                                datapoint_ts(point.time_unix_nano)?,
                                value,
                                SampleKind::Gauge,
                            )?;
                        }
                    }
                    Some(metric::Data::Sum(sum)) => {
                        let kind = temporality_kind(sum.aggregation_temporality);
                        for point in &sum.data_points {
                            out.begin_datapoint();
                            let Some(value) = number_value(point) else {
                                continue;
                            };
                            let pairs = base_pairs(&metric.name, &point.attributes, &promoted);
                            out.push(
                                tenant,
                                SeriesLabels::from_pairs(pairs),
                                datapoint_ts(point.time_unix_nano)?,
                                value,
                                kind,
                            )?;
                        }
                    }
                    Some(metric::Data::Histogram(histogram)) => {
                        let kind = temporality_kind(histogram.aggregation_temporality);
                        for point in &histogram.data_points {
                            out.begin_datapoint();
                            decompose_histogram(
                                tenant,
                                &metric.name,
                                point,
                                &promoted,
                                kind,
                                &mut out,
                            )?;
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(histogram)) => {
                        let kind = temporality_kind(histogram.aggregation_temporality);
                        for point in &histogram.data_points {
                            out.begin_datapoint();
                            decompose_exponential(
                                tenant,
                                &metric.name,
                                point,
                                &promoted,
                                kind,
                                &mut out,
                            )?;
                        }
                    }
                    Some(metric::Data::Summary(summary)) => {
                        for point in &summary.data_points {
                            out.begin_datapoint();
                            let ts = datapoint_ts(point.time_unix_nano)?;
                            let base = base_pairs(&metric.name, &point.attributes, &promoted);
                            for quantile in &point.quantile_values {
                                // A summary's quantiles cannot be
                                // re-aggregated; they are stored as the gauges
                                // they behave as.
                                out.push(
                                    tenant,
                                    with_extra(
                                        &base,
                                        "quantile",
                                        format_boundary(quantile.quantile),
                                    ),
                                    ts,
                                    quantile.value,
                                    SampleKind::Gauge,
                                )?;
                            }
                            let sum_base = base_pairs(
                                &format!("{}_sum", metric.name),
                                &point.attributes,
                                &promoted,
                            );
                            out.push(
                                tenant,
                                SeriesLabels::from_pairs(sum_base),
                                ts,
                                point.sum,
                                SampleKind::Cumulative,
                            )?;
                            let count_base = base_pairs(
                                &format!("{}_count", metric.name),
                                &point.attributes,
                                &promoted,
                            );
                            out.push(
                                tenant,
                                SeriesLabels::from_pairs(count_base),
                                ts,
                                point.count as f64,
                                SampleKind::Cumulative,
                            )?;
                        }
                    }
                    None => {}
                }
            }
        }
    }
    if out.samples.is_empty() {
        return Err(MetricIngestError::EmptyRequest);
    }
    Ok(out.samples)
}

/// The request minus the datapoints admission refused, walked in the same
/// traversal order [`normalize_request`] assigns indices — every datapoint
/// consumes one index whether kept or not, and the pin test asserts the two
/// walks agree. This is what the WAL stores on a partial acceptance, so
/// replay decomposes into exactly the admitted samples and a refused series
/// cannot be resurrected by a restart.
pub fn filter_request(
    request: &ExportMetricsServiceRequest,
    admitted: &std::collections::HashSet<u32>,
) -> ExportMetricsServiceRequest {
    let mut next: u32 = 0;
    let mut keep = move || {
        let index = next;
        next += 1;
        admitted.contains(&index)
    };
    let mut filtered = ExportMetricsServiceRequest::default();
    for resource_metrics in &request.resource_metrics {
        let mut kept_resource = resource_metrics.clone();
        kept_resource.scope_metrics.clear();
        for scope_metrics in &resource_metrics.scope_metrics {
            let mut kept_scope = scope_metrics.clone();
            kept_scope.metrics.clear();
            for metric in &scope_metrics.metrics {
                let mut kept_metric = metric.clone();
                let has_points = match &mut kept_metric.data {
                    Some(metric::Data::Gauge(gauge)) => {
                        gauge.data_points.retain(|_| keep());
                        !gauge.data_points.is_empty()
                    }
                    Some(metric::Data::Sum(sum)) => {
                        sum.data_points.retain(|_| keep());
                        !sum.data_points.is_empty()
                    }
                    Some(metric::Data::Histogram(histogram)) => {
                        histogram.data_points.retain(|_| keep());
                        !histogram.data_points.is_empty()
                    }
                    Some(metric::Data::ExponentialHistogram(histogram)) => {
                        histogram.data_points.retain(|_| keep());
                        !histogram.data_points.is_empty()
                    }
                    Some(metric::Data::Summary(summary)) => {
                        summary.data_points.retain(|_| keep());
                        !summary.data_points.is_empty()
                    }
                    None => false,
                };
                if has_points {
                    kept_scope.metrics.push(kept_metric);
                }
            }
            if !kept_scope.metrics.is_empty() {
                kept_resource.scope_metrics.push(kept_scope);
            }
        }
        if !kept_resource.scope_metrics.is_empty() {
            filtered.resource_metrics.push(kept_resource);
        }
    }
    filtered
}

/// Explicit-bounds histogram → Prometheus-style cumulative `_bucket{le=}`
/// series plus `+Inf`, `_sum` (when present) and `_count`. OTLP bucket counts
/// are per-bucket; `le` semantics are cumulative, so a running total converts.
fn decompose_histogram(
    tenant: &TenantId,
    name: &str,
    point: &HistogramDataPoint,
    promoted: &[(String, String)],
    kind: SampleKind,
    out: &mut Decomposition,
) -> Result<(), MetricIngestError> {
    let ts = datapoint_ts(point.time_unix_nano)?;
    let bucket_base = base_pairs(&format!("{name}_bucket"), &point.attributes, promoted);
    let mut running = 0u64;
    for (index, bound) in point.explicit_bounds.iter().enumerate() {
        running += point.bucket_counts.get(index).copied().unwrap_or(0);
        out.push(
            tenant,
            with_extra(&bucket_base, "le", format_boundary(*bound)),
            ts,
            running as f64,
            kind,
        )?;
    }
    let total: u64 = point.bucket_counts.iter().sum();
    out.push(
        tenant,
        with_extra(&bucket_base, "le", "+Inf".to_string()),
        ts,
        total as f64,
        kind,
    )?;
    if let Some(sum) = point.sum {
        let sum_base = base_pairs(&format!("{name}_sum"), &point.attributes, promoted);
        out.push(tenant, SeriesLabels::from_pairs(sum_base), ts, sum, kind)?;
    }
    let count_base = base_pairs(&format!("{name}_count"), &point.attributes, promoted);
    out.push(
        tenant,
        SeriesLabels::from_pairs(count_base),
        ts,
        point.count as f64,
        kind,
    )?;
    Ok(())
}

/// Exponential histogram → the same `_bucket{le=}` shape, downscaled until at
/// most [`MAX_EXP_HISTOGRAM_BUCKETS`] finite boundaries remain. The zero
/// bucket and any negative buckets fold into the smallest boundary — the
/// accepted loss the plan records; latency histograms have no negative half
/// in practice, and a signed boundary vocabulary would double the surface for
/// data fn0 never charts.
fn decompose_exponential(
    tenant: &TenantId,
    name: &str,
    point: &ExponentialHistogramDataPoint,
    promoted: &[(String, String)],
    kind: SampleKind,
    out: &mut Decomposition,
) -> Result<(), MetricIngestError> {
    let ts = datapoint_ts(point.time_unix_nano)?;
    let bucket_base = base_pairs(&format!("{name}_bucket"), &point.attributes, promoted);

    let positive: Vec<(i64, u64)> = point
        .positive
        .as_ref()
        .map(|buckets| {
            buckets
                .bucket_counts
                .iter()
                .enumerate()
                .filter(|(_, count)| **count > 0)
                .map(|(index, count)| (buckets.offset as i64 + index as i64, *count))
                .collect()
        })
        .unwrap_or_default();
    let negative_total: u64 = point
        .negative
        .as_ref()
        .map(|buckets| buckets.bucket_counts.iter().sum())
        .unwrap_or(0);
    let below_smallest = point.zero_count + negative_total;

    if positive.is_empty() {
        // All mass is at or below zero: one +Inf bucket carries the count.
        out.push(
            tenant,
            with_extra(&bucket_base, "le", "+Inf".to_string()),
            ts,
            point.count as f64,
            kind,
        )?;
    } else {
        // Downscale: shifting an index right by `d` halves the resolution
        // (scale − d) exactly — arithmetic shift keeps floor semantics for
        // negative indices.
        let min_index = positive.iter().map(|(index, _)| *index).min().unwrap();
        let max_index = positive.iter().map(|(index, _)| *index).max().unwrap();
        let mut downscale = 0u32;
        while ((max_index >> downscale) - (min_index >> downscale) + 1)
            > MAX_EXP_HISTOGRAM_BUCKETS as i64
        {
            downscale += 1;
        }
        let scale = point.scale - downscale as i32;
        let mut merged: std::collections::BTreeMap<i64, u64> = std::collections::BTreeMap::new();
        for (index, count) in positive {
            *merged.entry(index >> downscale).or_default() += count;
        }
        let mut running = below_smallest;
        for (index, count) in merged {
            running += count;
            // The bucket's upper boundary: 2^((index + 1) · 2^−scale).
            let boundary = 2f64.powf((index + 1) as f64 * 2f64.powi(-scale));
            out.push(
                tenant,
                with_extra(&bucket_base, "le", format_boundary(boundary)),
                ts,
                running as f64,
                kind,
            )?;
        }
        out.push(
            tenant,
            with_extra(&bucket_base, "le", "+Inf".to_string()),
            ts,
            point.count as f64,
            kind,
        )?;
    }
    if let Some(sum) = point.sum {
        let sum_base = base_pairs(&format!("{name}_sum"), &point.attributes, promoted);
        out.push(tenant, SeriesLabels::from_pairs(sum_base), ts, sum, kind)?;
    }
    let count_base = base_pairs(&format!("{name}_count"), &point.attributes, promoted);
    out.push(
        tenant,
        SeriesLabels::from_pairs(count_base),
        ts,
        point.count as f64,
        kind,
    )?;
    Ok(())
}

/// Accepting one OTLP metrics export, independent of how it arrived. The
/// metrics counterpart to [`crate::trace_ingest::OtlpTraceIngest`], split for
/// the same reason: a limit enforced on gRPC and forgotten on HTTP is not a
/// limit.
pub struct OtlpMetricIngest<'a> {
    pub journal: &'a Journal,
    pub shutdown: &'a ShutdownState,
    pub config: &'a Config,
    pub ingest_gate: &'a IngestGate,
    pub tenant_quota: &'a crate::tenant_quota::TenantQuota,
    pub clock: &'a crate::clock::Clock,
}

impl OtlpMetricIngest<'_> {
    pub fn admit_transport(&self) -> Result<(), IngestError> {
        if self.shutdown.is_fenced() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "this instance has been fenced by a newer writer and is shutting down".to_string(),
            )
                .into());
        }
        if self.shutdown.is_draining() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "server is draining for shutdown".to_string(),
            )
                .into());
        }
        self.ingest_gate.check()
    }

    pub fn admit_tenant(
        &self,
        tenant: &crate::tenant::TenantId,
        encoded_len: usize,
    ) -> Result<(), IngestError> {
        self.tenant_quota.admit_storage(tenant)?;
        if encoded_len > MAX_OTLP_REQUEST_BYTES {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("OTLP request exceeds the maximum of {MAX_OTLP_REQUEST_BYTES} bytes"),
            )
                .into());
        }
        Ok(())
    }

    pub async fn accept(
        &self,
        tenant: crate::tenant::TenantId,
        request: ExportMetricsServiceRequest,
    ) -> Result<MetricAcceptOutcome, IngestError> {
        // The cheap pre-check: every datapoint decomposes into at least one
        // sample, so a request past the cap on datapoints alone is refused
        // before any decomposition work.
        let datapoints = request
            .resource_metrics
            .iter()
            .flat_map(|resource| resource.scope_metrics.iter())
            .flat_map(|scope| scope.metrics.iter())
            .map(datapoint_count)
            .try_fold(0usize, |count, points| count.checked_add(points))
            .ok_or_else(|| {
                IngestError::from((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "OTLP metric datapoint count overflow".to_string(),
                ))
            })?;
        if datapoints > MAX_OTLP_METRIC_SAMPLES {
            return Err(too_many_samples());
        }
        let samples = normalize_request(&tenant, &request).map_err(|error| match error {
            MetricIngestError::TooManySamples => too_many_samples(),
            other => IngestError::from((StatusCode::BAD_REQUEST, other.to_string())),
        })?;
        let window = crate::ingest::TimestampWindow::from_config(self.config, self.clock);
        for sample in &samples {
            window
                .validate(sample.ts_ns)
                .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
        }
        // The max_active_series rung. Decided before anything is journaled,
        // per datapoint, with idle capacity reclaimed under pressure — see
        // `SeriesMemTable::admit_datapoints`.
        let series_memtable = self.journal.series_memtable();
        let idle_cutoff = self
            .clock
            .now_ns()
            .saturating_sub(self.config.metric_series_idle_timeout.as_nanos() as i64);
        let admission = series_memtable.admit_datapoints(
            &tenant,
            &samples,
            self.config.max_active_series,
            idle_cutoff,
        );
        if admission.rejected_any() && admission.admitted.is_empty() {
            // Everything was refused: a 429, because capacity does return —
            // at the idle horizon — and the collector should retry then.
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                self.refusal_message(&admission),
            )
                .into());
        }
        // On a partial acceptance the WAL gets the *filtered* request, so a
        // replay cannot resurrect the refused series and blow the budget the
        // refusal defended.
        let (wire_request, samples) = if admission.rejected_any() {
            let filtered = filter_request(&request, &admission.admitted);
            let samples: Vec<_> = samples
                .into_iter()
                .filter(|sample| admission.admitted.contains(&sample.datapoint_index))
                .collect();
            (filtered, samples)
        } else {
            (request, samples)
        };
        let mut encoded = Vec::new();
        wire_request.encode(&mut encoded).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to encode request: {error}"),
            )
        })?;
        self.journal
            .append_metrics(tenant, encoded, samples)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("journal write failed: {error}"),
                )
            })?;
        Ok(MetricAcceptOutcome {
            rejected_data_points: admission.rejected_datapoints,
            rejection: admission
                .rejected_any()
                .then(|| self.refusal_message(&admission)),
        })
    }

    /// The teaching refusal: the count, the limit, the knob, and the horizon
    /// at which capacity returns.
    fn refusal_message(&self, admission: &crate::series::AdmitOutcome) -> String {
        format!(
            "{} new series across {} datapoints refused: the tenant holds its \
max_active_series limit of {} live series (SIGNY_MAX_ACTIVE_SERIES). Known series are \
still accepted; capacity returns as series idle past {:?} \
(SIGNY_METRIC_SERIES_IDLE_TIMEOUT)",
            admission.rejected_new_series,
            admission.rejected_datapoints,
            self.config.max_active_series,
            self.config.metric_series_idle_timeout,
        )
    }
}

/// What `accept` answered on the success path: everything a transport needs
/// to build the OTLP `partial_success`.
pub struct MetricAcceptOutcome {
    pub rejected_data_points: u64,
    pub rejection: Option<String>,
}

impl MetricAcceptOutcome {
    pub fn partial_success(
        &self,
    ) -> Option<opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess>
    {
        (self.rejected_data_points > 0).then(|| {
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess {
                rejected_data_points: self.rejected_data_points.min(i64::MAX as u64) as i64,
                error_message: self.rejection.clone().unwrap_or_default(),
            }
        })
    }
}

fn too_many_samples() -> IngestError {
    // Non-retryable on purpose, like the span-count cap: the identical bytes
    // cannot become acceptable, so the collector must split the batch.
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "OTLP metrics request decomposes into more than {MAX_OTLP_METRIC_SAMPLES} samples; \
split the batch (histograms count bounds + 3 samples per datapoint)"
        ),
    )
        .into()
}

fn datapoint_count(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(metric::Data::Sum(sum)) => sum.data_points.len(),
        Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

#[derive(Clone)]
pub struct MetricsIngestService {
    journal: Arc<Journal>,
    shutdown: Arc<ShutdownState>,
    config: Arc<Config>,
    ingest_gate: Arc<IngestGate>,
    tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
    tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
    clock: Arc<crate::clock::Clock>,
}

impl MetricsIngestService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        journal: Arc<Journal>,
        shutdown: Arc<ShutdownState>,
        config: Arc<Config>,
        ingest_gate: Arc<IngestGate>,
        tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
        tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
        clock: Arc<crate::clock::Clock>,
    ) -> Self {
        Self {
            journal,
            shutdown,
            config,
            ingest_gate,
            tenant_quota,
            tenant_policy,
            clock,
        }
    }

    pub fn into_server(self) -> MetricsServiceServer<Self> {
        MetricsServiceServer::new(self)
            .max_decoding_message_size(MAX_OTLP_REQUEST_BYTES)
            .max_encoding_message_size(64 * 1024)
    }
}

#[tonic::async_trait]
impl MetricsService for MetricsIngestService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        use crate::log_ingest::ingest_error_to_status;

        let ingest = OtlpMetricIngest {
            journal: &self.journal,
            shutdown: &self.shutdown,
            config: &self.config,
            ingest_gate: &self.ingest_gate,
            tenant_quota: &self.tenant_quota,
            clock: &self.clock,
        };
        ingest.admit_transport().map_err(ingest_error_to_status)?;
        let tenant = crate::tenant::from_grpc_metadata(
            request.metadata(),
            &self.config,
            &self.tenant_policy,
        )
        .map_err(crate::tenant::TenantError::into_grpc)?;
        let request = request.into_inner();
        ingest
            .admit_tenant(&tenant, request.encoded_len())
            .map_err(ingest_error_to_status)?;
        let outcome = ingest
            .accept(tenant, request)
            .await
            .map_err(ingest_error_to_status)?;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: outcome.partial_success(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::SeriesMemTable;
    use crate::tenant::test_tenant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, Metric, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
        exponential_histogram_data_point, summary_data_point,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn gauge_point(ts: u64, value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            time_unix_nano: ts,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn request_with(
        metrics: Vec<Metric>,
        resource: Option<Resource>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource,
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn pairs_of(sample: &MetricSample) -> Vec<(String, String)> {
        sample.labels.pairs().unwrap()
    }

    fn label(sample: &MetricSample, key: &str) -> Option<String> {
        pairs_of(sample)
            .into_iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }

    #[test]
    fn a_gauge_becomes_one_sample_with_promoted_and_datapoint_labels() {
        let request = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![gauge_point(100, 7.5, vec![attr("shard", "3")])],
                })),
                ..Default::default()
            }],
            Some(Resource {
                attributes: vec![attr("service.name", "api"), attr("process.pid", "4242")],
                ..Default::default()
            }),
        );
        let samples = normalize_request(&test_tenant(), &request).unwrap();
        assert_eq!(samples.len(), 1);
        let sample = &samples[0];
        assert_eq!(sample.ts_ns, 100);
        assert_eq!(sample.value, 7.5);
        assert_eq!(sample.kind, SampleKind::Gauge);
        assert_eq!(
            label(sample, METRIC_NAME_LABEL).as_deref(),
            Some("queue_depth")
        );
        assert_eq!(label(sample, "service_name").as_deref(), Some("api"));
        assert_eq!(label(sample, "shard").as_deref(), Some("3"));
        assert_eq!(
            label(sample, "process_pid"),
            None,
            "unpromoted resource attributes are dropped, not series identity"
        );
    }

    #[test]
    fn a_datapoint_attribute_wins_over_a_promoted_resource_attribute() {
        let request = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![gauge_point(
                        100,
                        1.0,
                        vec![attr("service_name", "from-datapoint")],
                    )],
                })),
                ..Default::default()
            }],
            Some(Resource {
                attributes: vec![attr("service.name", "from-resource")],
                ..Default::default()
            }),
        );
        let samples = normalize_request(&test_tenant(), &request).unwrap();
        assert_eq!(
            label(&samples[0], "service_name").as_deref(),
            Some("from-datapoint")
        );
    }

    #[test]
    fn a_delta_sum_is_tagged_delta_and_a_cumulative_one_is_not() {
        let sum = |temporality: AggregationTemporality| {
            request_with(
                vec![Metric {
                    name: "http_requests_total".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![gauge_point(100, 5.0, vec![])],
                        aggregation_temporality: temporality as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                None,
            )
        };
        let delta = normalize_request(&test_tenant(), &sum(AggregationTemporality::Delta)).unwrap();
        assert_eq!(delta[0].kind, SampleKind::Delta);
        let cumulative =
            normalize_request(&test_tenant(), &sum(AggregationTemporality::Cumulative)).unwrap();
        assert_eq!(cumulative[0].kind, SampleKind::Cumulative);
    }

    #[test]
    fn a_histogram_decomposes_into_cumulative_le_buckets_sum_and_count() {
        let request = request_with(
            vec![Metric {
                name: "http_request_duration_seconds".to_string(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        attributes: vec![attr("instance", "a")],
                        time_unix_nano: 100,
                        count: 10,
                        sum: Some(1.25),
                        bucket_counts: vec![3, 4, 2, 1],
                        explicit_bounds: vec![0.005, 0.01, 0.025],
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                })),
                ..Default::default()
            }],
            None,
        );
        let samples = normalize_request(&test_tenant(), &request).unwrap();
        // 3 bounds + Inf + sum + count.
        assert_eq!(samples.len(), 6);
        let bucket = |le: &str| {
            samples
                .iter()
                .find(|sample| label(sample, "le").as_deref() == Some(le))
                .unwrap_or_else(|| panic!("bucket le={le} exists"))
        };
        assert_eq!(bucket("0.005").value, 3.0);
        assert_eq!(bucket("0.01").value, 7.0, "le counts are cumulative");
        assert_eq!(bucket("0.025").value, 9.0);
        assert_eq!(bucket("+Inf").value, 10.0);
        assert_eq!(
            bucket("0.005").labels.metric_name().as_deref(),
            Some("http_request_duration_seconds_bucket")
        );
        let sum = samples
            .iter()
            .find(|sample| {
                sample.labels.metric_name().as_deref() == Some("http_request_duration_seconds_sum")
            })
            .unwrap();
        assert_eq!(sum.value, 1.25);
        let count = samples
            .iter()
            .find(|sample| {
                sample.labels.metric_name().as_deref()
                    == Some("http_request_duration_seconds_count")
            })
            .unwrap();
        assert_eq!(count.value, 10.0);
        assert!(
            samples
                .iter()
                .all(|sample| sample.kind == SampleKind::Cumulative)
        );
    }

    #[test]
    fn an_exponential_histogram_downscales_to_the_bucket_cap() {
        // 200 populated buckets at scale 3 must downscale (200 > 64).
        let request = request_with(
            vec![Metric {
                name: "latency_seconds".to_string(),
                data: Some(metric::Data::ExponentialHistogram(
                    opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram {
                        data_points: vec![ExponentialHistogramDataPoint {
                            time_unix_nano: 100,
                            count: 205,
                            sum: Some(12.5),
                            scale: 3,
                            zero_count: 5,
                            positive: Some(exponential_histogram_data_point::Buckets {
                                offset: -20,
                                bucket_counts: vec![1; 200],
                            }),
                            negative: None,
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    },
                )),
                ..Default::default()
            }],
            None,
        );
        let samples = normalize_request(&test_tenant(), &request).unwrap();
        let buckets: Vec<&MetricSample> = samples
            .iter()
            .filter(|sample| label(sample, "le").is_some())
            .collect();
        let finite = buckets
            .iter()
            .filter(|sample| label(sample, "le").as_deref() != Some("+Inf"))
            .count();
        assert!(
            finite <= MAX_EXP_HISTOGRAM_BUCKETS,
            "{finite} finite boundaries survived the downscale"
        );
        assert!(
            finite >= MAX_EXP_HISTOGRAM_BUCKETS / 2,
            "one halving, not a collapse"
        );
        // The smallest bucket absorbed the zero bucket.
        let mut last = 0.0;
        for bucket in &buckets {
            assert!(bucket.value >= last, "le counts stay cumulative");
            last = bucket.value;
        }
        let first = buckets.first().unwrap();
        assert!(
            first.value >= 5.0,
            "the zero count folds into the smallest bound"
        );
        assert_eq!(
            buckets
                .last()
                .and_then(|sample| label(sample, "le"))
                .as_deref(),
            Some("+Inf")
        );
        assert_eq!(buckets.last().unwrap().value, 205.0);
    }

    #[test]
    fn a_summary_becomes_quantile_gauges_plus_sum_and_count() {
        let request = request_with(
            vec![Metric {
                name: "gc_pause_seconds".to_string(),
                data: Some(metric::Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: 100,
                        count: 42,
                        sum: 3.5,
                        quantile_values: vec![
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.5,
                                value: 0.01,
                            },
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.99,
                                value: 0.25,
                            },
                        ],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            }],
            None,
        );
        let samples = normalize_request(&test_tenant(), &request).unwrap();
        assert_eq!(samples.len(), 4);
        let p99 = samples
            .iter()
            .find(|sample| label(sample, "quantile").as_deref() == Some("0.99"))
            .unwrap();
        assert_eq!(p99.value, 0.25);
        assert_eq!(p99.kind, SampleKind::Gauge);
        assert!(samples.iter().any(|sample| {
            sample.labels.metric_name().as_deref() == Some("gc_pause_seconds_count")
                && sample.value == 42.0
                && sample.kind == SampleKind::Cumulative
        }));
    }

    #[test]
    fn an_empty_request_is_refused() {
        assert_eq!(
            normalize_request(&test_tenant(), &ExportMetricsServiceRequest::default()).unwrap_err(),
            MetricIngestError::EmptyRequest
        );
    }

    #[test]
    fn boundary_rendering_is_pinned() {
        assert_eq!(format_boundary(0.005), "0.005");
        assert_eq!(format_boundary(1.0), "1");
        assert_eq!(format_boundary(f64::INFINITY), "+Inf");
        assert_eq!(
            format_boundary(std::f64::consts::SQRT_2),
            "1.4142135623730951",
            "an exponential boundary keeps its shortest round-trip rendering"
        );
    }

    fn service_over(config: Config) -> (MetricsIngestService, Arc<SeriesMemTable>, Arc<Journal>) {
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let series_memtable = Arc::new(SeriesMemTable::new());
        let journal = Arc::new(
            crate::journal::Journal::spawn_with_signals(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                Arc::new(crate::trace::TraceMemTable::new()),
                series_memtable.clone(),
            )
            .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = MetricsIngestService::new(
            journal.clone(),
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
            crate::clock::Clock::system(),
        );
        (service, series_memtable, journal)
    }

    fn tenant_request(
        request: ExportMetricsServiceRequest,
    ) -> Request<ExportMetricsServiceRequest> {
        Request::from_parts(
            crate::tenant::test_tenant_metadata(),
            tonic::Extensions::default(),
            request,
        )
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn small_request() -> ExportMetricsServiceRequest {
        request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![gauge_point(now_ns(), 7.5, vec![attr("instance", "a")])],
                })),
                ..Default::default()
            }],
            None,
        )
    }

    #[tokio::test]
    async fn export_acknowledges_after_journal_append_and_replay_reproduces_it() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-ingest-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, journal) = service_over(config);
        service
            .export(tenant_request(small_request()))
            .await
            .unwrap();
        let live = series_memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(live.len(), 1);

        let replayed = SeriesMemTable::new();
        crate::journal::replay_with_signals(
            journal.wal_path(),
            journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &crate::trace::TraceMemTable::new(),
            &replayed,
        )
        .unwrap();
        assert_eq!(
            replayed.sorted_samples(&test_tenant()).unwrap(),
            live,
            "replay reproduces exactly what live ingest stored"
        );
    }

    #[tokio::test]
    async fn a_delta_stream_replays_into_the_same_running_totals() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-delta-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, journal) = service_over(config);
        let base = now_ns();
        for (offset, value) in [(0u64, 5.0), (1_000_000_000, 3.0), (2_000_000_000, 2.0)] {
            let request = request_with(
                vec![Metric {
                    name: "http_requests_total".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![gauge_point(base + offset, value, vec![])],
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                None,
            );
            service.export(tenant_request(request)).await.unwrap();
        }
        let live = series_memtable.sorted_samples(&test_tenant()).unwrap();
        let totals: Vec<f64> = live
            .values()
            .next()
            .unwrap()
            .iter()
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(totals, vec![5.0, 8.0, 10.0]);

        let replayed = SeriesMemTable::new();
        crate::journal::replay_with_signals(
            journal.wal_path(),
            journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &crate::trace::TraceMemTable::new(),
            &replayed,
        )
        .unwrap();
        assert_eq!(replayed.sorted_samples(&test_tenant()).unwrap(), live);
    }

    #[tokio::test]
    async fn export_rejects_while_draining_without_inserting() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-drain-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, _journal) = service_over(config);
        service.shutdown.begin_drain();
        let status = service
            .export(tenant_request(small_request()))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(series_memtable.is_empty());
    }

    #[tokio::test]
    async fn export_is_refused_once_the_buffers_are_over_their_limit() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-gate-{}", uuid::Uuid::new_v4())),
            flush_max_bytes: 1,
            max_memtable_bytes: Some(1),
            ..Config::default()
        };
        let (service, series_memtable, _journal) = service_over(config);
        service
            .export(tenant_request(small_request()))
            .await
            .expect("the first export is under the limit");
        let status = service
            .export(tenant_request(small_request()))
            .await
            .expect_err("a full buffer must be refused");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            series_memtable
                .sorted_samples(&test_tenant())
                .unwrap()
                .values()
                .next()
                .unwrap()
                .len(),
            1,
            "the refused export must not have been appended"
        );
    }

    #[tokio::test]
    async fn export_rejects_a_request_past_the_decomposed_sample_cap() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-cap-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, _journal) = service_over(config);
        // 25 000 histogram datapoints × (2 bounds + 3) = 125 000 decomposed
        // samples: over the cap while the datapoint count alone is not.
        let point = HistogramDataPoint {
            time_unix_nano: now_ns(),
            count: 1,
            sum: Some(0.1),
            bucket_counts: vec![1, 0, 0],
            explicit_bounds: vec![0.1, 0.2],
            ..Default::default()
        };
        let request = request_with(
            vec![Metric {
                name: "h".to_string(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![point; 25_000],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                })),
                ..Default::default()
            }],
            None,
        );
        let status = service.export(tenant_request(request)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(series_memtable.is_empty());
    }

    /// The traversal pin: filtering by an admitted set and re-decomposing
    /// yields exactly the admitted samples. If `filter_request` and
    /// `normalize_request` ever walked datapoints in different orders, this
    /// is the test that catches it before a replay resurrects a refusal.
    #[test]
    fn filtering_then_decomposing_equals_decomposing_then_filtering() {
        let request = request_with(
            vec![
                Metric {
                    name: "queue_depth".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![
                            gauge_point(100, 1.0, vec![attr("instance", "a")]),
                            gauge_point(100, 2.0, vec![attr("instance", "b")]),
                        ],
                    })),
                    ..Default::default()
                },
                Metric {
                    name: "http_request_duration_seconds".to_string(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![HistogramDataPoint {
                            time_unix_nano: 100,
                            count: 3,
                            sum: Some(0.3),
                            bucket_counts: vec![1, 2],
                            explicit_bounds: vec![0.1],
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    })),
                    ..Default::default()
                },
                Metric {
                    name: "http_requests_total".to_string(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![gauge_point(100, 5.0, vec![])],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                },
            ],
            None,
        );
        let all = normalize_request(&test_tenant(), &request).unwrap();
        // Admit datapoints 1 (second gauge) and 2 (the histogram); refuse 0 and 3.
        let admitted: std::collections::HashSet<u32> = [1, 2].into_iter().collect();
        let filtered = filter_request(&request, &admitted);
        let refiltered = normalize_request(&test_tenant(), &filtered).unwrap();
        let expected: Vec<_> = all
            .iter()
            .filter(|sample| admitted.contains(&sample.datapoint_index))
            .collect();
        assert_eq!(refiltered.len(), expected.len());
        for (kept, original) in refiltered.iter().zip(expected) {
            assert_eq!(kept.labels, original.labels);
            assert_eq!(kept.ts_ns, original.ts_ns);
            assert_eq!(kept.value, original.value);
        }
        assert!(
            refiltered
                .iter()
                .any(|sample| sample.labels.metric_name().as_deref()
                    == Some("http_request_duration_seconds_bucket")),
            "the whole admitted histogram family survives"
        );
    }

    #[tokio::test]
    async fn past_the_cap_known_series_keep_landing_and_new_ones_get_partial_success() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-ladder-{}", uuid::Uuid::new_v4())),
            max_active_series: 2,
            ..Config::default()
        };
        let (service, series_memtable, journal) = service_over(config);
        let base = now_ns();
        let known = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![
                        gauge_point(base, 1.0, vec![attr("instance", "a")]),
                        gauge_point(base, 2.0, vec![attr("instance", "b")]),
                    ],
                })),
                ..Default::default()
            }],
            None,
        );
        let response = service.export(tenant_request(known)).await.unwrap();
        assert!(response.into_inner().partial_success.is_none());

        let mixed = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![
                        gauge_point(base + 1_000_000_000, 3.0, vec![attr("instance", "a")]),
                        gauge_point(base + 1_000_000_000, 9.0, vec![attr("instance", "new-1")]),
                        gauge_point(base + 1_000_000_000, 9.0, vec![attr("instance", "new-2")]),
                    ],
                })),
                ..Default::default()
            }],
            None,
        );
        let response = service.export(tenant_request(mixed)).await.unwrap();
        let partial = response
            .into_inner()
            .partial_success
            .expect("a partial acceptance names what it refused");
        assert_eq!(partial.rejected_data_points, 2);
        assert!(
            partial.error_message.contains("SIGNY_MAX_ACTIVE_SERIES"),
            "{}",
            partial.error_message
        );
        assert!(
            partial
                .error_message
                .contains("SIGNY_METRIC_SERIES_IDLE_TIMEOUT"),
            "{}",
            partial.error_message
        );

        let live = series_memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(live.len(), 2, "no refused series exists");
        assert!(
            live.values().any(|samples| samples.len() == 2),
            "the known series took its second sample"
        );

        // The WAL was filtered: replay reproduces the admitted state, not the
        // refusal-inflated one — the budget survives a restart mid-explosion.
        let replayed = SeriesMemTable::new();
        crate::journal::replay_with_signals(
            journal.wal_path(),
            journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &crate::trace::TraceMemTable::new(),
            &replayed,
        )
        .unwrap();
        assert_eq!(replayed.sorted_samples(&test_tenant()).unwrap(), live);
        assert_eq!(replayed.active_series(&test_tenant()), 2);
    }

    #[tokio::test]
    async fn an_export_of_only_new_series_past_the_cap_is_refused_whole() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-429-{}", uuid::Uuid::new_v4())),
            max_active_series: 1,
            ..Config::default()
        };
        let (service, series_memtable, _journal) = service_over(config);
        let base = now_ns();
        service
            .export(tenant_request(request_with(
                vec![Metric {
                    name: "queue_depth".to_string(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![gauge_point(base, 1.0, vec![attr("instance", "a")])],
                    })),
                    ..Default::default()
                }],
                None,
            )))
            .await
            .unwrap();
        let all_new = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![gauge_point(base, 9.0, vec![attr("instance", "b")])],
                })),
                ..Default::default()
            }],
            None,
        );
        let status = service.export(tenant_request(all_new)).await.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::ResourceExhausted,
            "capacity returns at the idle horizon, so the refusal is retryable"
        );
        assert_eq!(series_memtable.active_series(&test_tenant()), 1);
    }

    #[tokio::test]
    async fn export_flushes_to_a_metric_part_and_reloads_after_restart() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-flush-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, journal) = service_over(config.clone());
        service
            .export(tenant_request(small_request()))
            .await
            .unwrap();
        let live = series_memtable.sorted_samples(&test_tenant()).unwrap();

        let registry = crate::part_registry::PartRegistry::new();
        let trace_registry = crate::trace_registry::TraceRegistry::new(registry.operation_lock());
        let series_registry =
            crate::series_registry::SeriesRegistry::new(registry.operation_lock());
        let mut pending_checkpoint = None;
        crate::flush::force_flush_pass(crate::flush::ForceFlush {
            memtable: &crate::memtable::MemTable::new(),
            trace_memtable: &journal.trace_memtable(),
            journal: &journal,
            registry: &registry,
            trace_registry: &trace_registry,
            series_registry: &series_registry,
            remote_cache: None,
            config: &config,
            pending_checkpoint: &mut pending_checkpoint,
        })
        .await
        .unwrap();
        assert!(series_memtable.is_empty(), "the samples left for the part");
        assert_eq!(
            series_memtable.active_series(&test_tenant()),
            1,
            "the series state survives its samples' flush"
        );
        assert_eq!(series_registry.part_count(), 1);
        assert!(
            series_registry.tenant_stored_bytes(&test_tenant()) > 0,
            "the quota census sees the metric bytes"
        );

        // A restart discovers the part from disk and can read the samples back.
        let restored = crate::series_registry::SeriesRegistry::load_from_disk(
            &config.data_dir.join("metrics"),
            registry.operation_lock(),
        )
        .unwrap();
        assert_eq!(restored.part_count(), 1);
        let reader = restored.snapshot().into_iter().next().unwrap();
        let catalog = reader.tenant_catalog(&test_tenant());
        assert_eq!(catalog.len(), 1);
        let stored = reader.read_series(&catalog[0]).unwrap();
        assert_eq!(&stored, live.values().next().unwrap());
        // And the WAL was retired: a replay after the flush puts nothing back.
        let replayed = SeriesMemTable::new();
        crate::journal::replay_with_signals(
            journal.wal_path(),
            journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &crate::trace::TraceMemTable::new(),
            &replayed,
        )
        .unwrap();
        assert!(replayed.is_empty());
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[tokio::test]
    async fn export_rejects_a_timestamp_outside_the_window() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("signy-metric-window-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        let (service, series_memtable, _journal) = service_over(config);
        let request = request_with(
            vec![Metric {
                name: "queue_depth".to_string(),
                data: Some(metric::Data::Gauge(Gauge {
                    // Far past: outside any default max_timestamp_age.
                    data_points: vec![gauge_point(1, 7.5, vec![])],
                })),
                ..Default::default()
            }],
            None,
        );
        let status = service.export(tenant_request(request)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(series_memtable.is_empty());
    }
}
