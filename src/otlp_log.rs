use std::collections::BTreeMap;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;

use crate::memtable::{Labels, LogEntry};

/// Resource attributes promoted to stream labels.
///
/// Everything else becomes structured metadata. The split matters because
/// labels are what stream cardinality is counted in — the plan sells a bounded
/// number of active streams — and an OTLP resource routinely carries dozens of
/// attributes, several of them unique per process. Promoting all of them would
/// make every pod its own stream.
///
/// This is Loki's own default promotion list rather than a set invented here,
/// so a collector configured for Loki keeps producing the same streams.
const PROMOTED_RESOURCE_ATTRIBUTES: &[&str] = &[
    "cloud.availability_zone",
    "cloud.region",
    "container.name",
    "deployment.environment",
    "deployment.environment.name",
    "k8s.cluster.name",
    "k8s.container.name",
    "k8s.cronjob.name",
    "k8s.daemonset.name",
    "k8s.deployment.name",
    "k8s.job.name",
    "k8s.namespace.name",
    "k8s.pod.name",
    "k8s.replicaset.name",
    "k8s.statefulset.name",
    "service.instance.id",
    "service.name",
    "service.namespace",
];

#[derive(Debug, PartialEq, Eq)]
pub enum OtlpLogError {
    EmptyRequest,
    TimestampOutOfRange,
}

impl std::fmt::Display for OtlpLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyRequest => "OTLP log request contains no records",
            Self::TimestampOutOfRange => "log record timestamp is outside the supported range",
        };
        f.write_str(message)
    }
}

/// Turn an OTLP export into the same `(labels, entries)` shape a Loki push
/// produces, so both protocols land in one memtable and one part format.
///
/// Records that share a label set are grouped, because a stream is what the
/// storage layer keys on and one export usually carries many records from the
/// same resource.
pub fn normalize_request(
    request: &ExportLogsServiceRequest,
) -> Result<Vec<(Labels, Vec<LogEntry>)>, OtlpLogError> {
    let mut streams: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
    for resource_logs in &request.resource_logs {
        let resource_attributes = resource_logs
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice())
            .unwrap_or_default();
        let (labels, resource_metadata) = split_resource_attributes(resource_attributes);
        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|scope| scope.name.as_str())
                .unwrap_or("");
            for record in &scope_logs.log_records {
                let entry = normalize_record(record, &resource_metadata, scope_name)?;
                streams.entry(labels.clone()).or_default().push(entry);
            }
        }
    }
    if streams.is_empty() {
        return Err(OtlpLogError::EmptyRequest);
    }
    Ok(streams.into_iter().collect())
}

fn split_resource_attributes(attributes: &[KeyValue]) -> (Labels, Vec<(String, String)>) {
    let mut labels: Labels = BTreeMap::new();
    let mut metadata = Vec::new();
    for attribute in attributes {
        let Some(value) = attribute.value.as_ref().and_then(scalar_string) else {
            // A non-scalar resource attribute is never a label: a label value
            // is a string, and flattening a map into one would invent a format
            // that queries would then have to guess at.
            if let Some(value) = attribute.value.as_ref() {
                metadata.push((attribute.key.clone(), value_json(value)));
            }
            continue;
        };
        if PROMOTED_RESOURCE_ATTRIBUTES.contains(&attribute.key.as_str()) {
            labels.insert(sanitize_label_name(&attribute.key), value);
        } else {
            metadata.push((attribute.key.clone(), value));
        }
    }
    (labels, metadata)
}

fn normalize_record(
    record: &LogRecord,
    resource_metadata: &[(String, String)],
    scope_name: &str,
) -> Result<LogEntry, OtlpLogError> {
    // `time_unix_nano` is optional in OTLP and collectors leave it at zero when
    // the source had no timestamp; `observed_time_unix_nano` is when the
    // collector saw it, which is the best answer available then.
    let raw_timestamp = if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    let timestamp_ns =
        i64::try_from(raw_timestamp).map_err(|_| OtlpLogError::TimestampOutOfRange)?;

    let line = record
        .body
        .as_ref()
        .and_then(scalar_string)
        .or_else(|| record.body.as_ref().map(value_json))
        .unwrap_or_default();

    let mut structured_metadata: Vec<(String, String)> = resource_metadata.to_vec();
    if !scope_name.is_empty() {
        structured_metadata.push(("scope_name".to_string(), scope_name.to_string()));
    }
    if !record.severity_text.is_empty() {
        structured_metadata.push(("severity_text".to_string(), record.severity_text.clone()));
    }
    if record.severity_number != 0 {
        structured_metadata.push((
            "severity_number".to_string(),
            record.severity_number.to_string(),
        ));
    }
    // Kept as hex, the form every other surface here uses for these: the Tempo
    // handlers, the trace part format and the trace-to-logs link in Grafana all
    // speak hex, so a raw byte string would be the one place that does not.
    if !record.trace_id.is_empty() {
        structured_metadata.push(("trace_id".to_string(), hex(&record.trace_id)));
    }
    if !record.span_id.is_empty() {
        structured_metadata.push(("span_id".to_string(), hex(&record.span_id)));
    }
    for attribute in &record.attributes {
        if let Some(value) = attribute.value.as_ref() {
            structured_metadata.push((
                attribute.key.clone(),
                scalar_string(value).unwrap_or_else(|| value_json(value)),
            ));
        }
    }

    Ok(LogEntry {
        timestamp_ns,
        line,
        structured_metadata,
    })
}

/// Label names are matched by LogQL, whose grammar is Prometheus-shaped, so
/// the dots OTLP uses have to become underscores. This is what the Loki
/// exporter does too, which is why `service.name` is queried as `service_name`.
fn sanitize_label_name(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn scalar_string(value: &AnyValue) -> Option<String> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value.clone()),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) => Some(value.to_string()),
        _ => None,
    }
}

/// A composite attribute or body, rendered as JSON so the parser filters that
/// already exist for structured lines can reach into it.
fn value_json(value: &AnyValue) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    include!("tests/otlp_log.rs");
}
