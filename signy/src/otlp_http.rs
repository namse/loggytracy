use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use prost014::Message;

use crate::AppState;
use crate::backpressure::IngestError;
use crate::log_ingest::OtlpLogIngest;
use crate::series_ingest::OtlpMetricIngest;
use crate::trace_ingest::{MAX_OTLP_REQUEST_BYTES, OtlpTraceIngest};

/// OTLP over HTTP.
///
/// The same exports the gRPC services take, framed differently. A collector
/// configured with `otlphttp` is at least as common as one using `otlp`, and
/// it is the only option where a proxy in front will not carry gRPC. Both
/// transports share the admission and normalization code, so a limit cannot be
/// enforced on one and forgotten on the other.
///
/// Protobuf and JSON are both accepted, chosen by `Content-Type`, and the
/// response is encoded the same way the request was — the specification
/// requires a body of the matching `ExportServiceResponse` type, and a
/// collector that sent JSON cannot read protobuf back.
#[derive(Clone, Copy, PartialEq, Eq)]
enum OtlpEncoding {
    Protobuf,
    Json,
}

impl OtlpEncoding {
    fn from_headers(headers: &HeaderMap) -> Result<Self, IngestError> {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/x-protobuf");
        let content_type = content_type.split(';').next().unwrap_or("").trim();
        match content_type {
            "application/x-protobuf" | "application/protobuf" | "" => Ok(Self::Protobuf),
            "application/json" => Ok(Self::Json),
            other => Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format!(
                    "unsupported OTLP content type {other:?}; \
use application/x-protobuf or application/json"
                ),
            )
                .into()),
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Protobuf => "application/x-protobuf",
            Self::Json => "application/json",
        }
    }

    fn decode<T>(self, body: &[u8]) -> Result<T, IngestError>
    where
        T: Message + Default + serde::de::DeserializeOwned,
    {
        match self {
            Self::Protobuf => T::decode(body).map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("OTLP protobuf decode failed: {error}"),
                )
                    .into()
            }),
            Self::Json => serde_json::from_slice(body).map_err(|error| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("OTLP JSON decode failed: {error}"),
                )
                    .into()
            }),
        }
    }

    fn encode<T>(self, message: &T) -> Response
    where
        T: Message + serde::Serialize,
    {
        let body = match self {
            Self::Protobuf => message.encode_to_vec(),
            // An encoding failure here would be a bug in a response type with
            // no user-supplied content, so an empty body is closer to the
            // truth than a 500 that blames the client's request.
            Self::Json => serde_json::to_vec(message).unwrap_or_default(),
        };
        ([(header::CONTENT_TYPE, self.content_type())], body).into_response()
    }
}

pub async fn logs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, IngestError> {
    let ingest = OtlpLogIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        config: &state.config,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
        clock: &state.clock,
    };
    // Ahead of the request counter as well as the body work, the accounting
    // the push handler this replaces kept: a refusal at the gate is not an
    // ingest the server attempted, so it is neither a request nor an error.
    ingest.admit_transport()?;
    state
        .metrics
        .ingest_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let result = logs_inner(&state, &ingest, headers, body).await;
    if result.is_err() {
        state
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    result
}

async fn logs_inner(
    state: &Arc<AppState>,
    ingest: &OtlpLogIngest<'_>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, IngestError> {
    let encoding = OtlpEncoding::from_headers(&headers)?;
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    // Charged on the wire size and before decoding, unlike the gRPC path which
    // is handed an already-decoded message. This is the earlier of the two
    // points and the better one: a tenant over its rate does not get to spend
    // this instance's CPU on a body that will not be accepted.
    ingest.admit_tenant(&tenant, body.len())?;
    let request: ExportLogsServiceRequest = encoding.decode(&body)?;
    // A protobuf body is already the WAL's encoding of choice, so it rides
    // through untouched; a JSON body has no protobuf bytes to keep.
    let wire = match encoding {
        OtlpEncoding::Protobuf => Some(body.to_vec()),
        OtlpEncoding::Json => None,
    };
    ingest.accept(tenant, request, wire).await?;
    Ok(encoding.encode(&ExportLogsServiceResponse::default()))
}

pub async fn traces(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, IngestError> {
    let ingest = OtlpTraceIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
    };
    ingest.admit_transport()?;
    let encoding = OtlpEncoding::from_headers(&headers)?;
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    ingest.admit_tenant(&tenant, body.len())?;
    let request: ExportTraceServiceRequest = encoding.decode(&body)?;
    ingest.accept(tenant, request).await?;
    Ok(encoding.encode(&ExportTraceServiceResponse::default()))
}

pub async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, IngestError> {
    let ingest = OtlpMetricIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        config: &state.config,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
        clock: &state.clock,
    };
    ingest.admit_transport()?;
    let encoding = OtlpEncoding::from_headers(&headers)?;
    let tenant = crate::tenant::from_headers(&headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    ingest.admit_tenant(&tenant, body.len())?;
    let request: ExportMetricsServiceRequest = encoding.decode(&body)?;
    let outcome = ingest.accept(tenant, request).await?;
    // A partial acceptance answers 200 with the OTLP `partial_success` naming
    // what was refused and why; only an all-refused export is an error.
    Ok(encoding.encode(&ExportMetricsServiceResponse {
        partial_success: outcome.partial_success(),
    }))
}

/// Body limit for the OTLP HTTP routes, matching what the gRPC services accept
/// so a collector sees one size whichever transport it picks.
pub const MAX_OTLP_HTTP_BODY_BYTES: usize = MAX_OTLP_REQUEST_BYTES;

pub const COLLECT_UNCOMPRESSED_BYTES_HEADER: &str = "x-collecty-uncompressed-bytes";

pub const MAX_COLLECT_COMPRESSED_BYTES: usize = MAX_OTLP_REQUEST_BYTES;

/// The header collecty writes in front of each record's payload: one signal
/// tag and the payload's length, little-endian.
const COLLECT_RECORD_HEADER_BYTES: usize = 5;

/// Ceiling on a collected batch's *decompressed* size.
///
/// A batch of n records decompresses to `5n + Σ payload`, so allowing the
/// framing of a single record on top of the request maximum is what keeps a
/// maximal export shippable — and it is also all that can be allowed: any
/// batch under this ceiling has every signal's merged payload at or under
/// `MAX_OTLP_REQUEST_BYTES`, so no group can be built that the ingest path
/// would then refuse as too large.
pub const MAX_COLLECT_PLAIN_BYTES: usize = MAX_OTLP_REQUEST_BYTES + COLLECT_RECORD_HEADER_BYTES;

/// The signals a collected batch may carry, in the order they are ingested.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CollectSignal {
    Logs,
    Traces,
    Metrics,
}

impl CollectSignal {
    const ALL: [CollectSignal; 3] = [
        CollectSignal::Logs,
        CollectSignal::Traces,
        CollectSignal::Metrics,
    ];

    fn from_tag(tag: u8) -> Option<CollectSignal> {
        match tag {
            1 => Some(CollectSignal::Logs),
            2 => Some(CollectSignal::Traces),
            3 => Some(CollectSignal::Metrics),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            CollectSignal::Logs => "logs",
            CollectSignal::Traces => "traces",
            CollectSignal::Metrics => "metrics",
        }
    }

    fn index(self) -> usize {
        match self {
            CollectSignal::Logs => 0,
            CollectSignal::Traces => 1,
            CollectSignal::Metrics => 2,
        }
    }
}

/// One collected batch, every signal.
///
/// collecty keeps a single queue, so what arrives here is a mix: records in
/// arrival order, each naming its signal. Records of one signal concatenate
/// into that signal's merged export, exactly as they did when each signal had
/// a route of its own, so this walks the batch once and hands each signal's
/// bytes to the ingest path it already had.
pub async fn collect(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, IngestError> {
    // Ahead of the request counter, and once for the whole batch: the checks
    // behind it are the instance's, not a signal's, so a fenced or overloaded
    // server refuses the batch rather than dropping part of it.
    log_ingest(&state).admit_transport()?;
    state
        .metrics
        .ingest_requests
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let result = collect_inner(&state, &headers, &body).await;
    if result.is_err() {
        state
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    result
}

async fn collect_inner(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Response, IngestError> {
    let mut payloads: [Vec<u8>; 3] = Default::default();
    let mut records = [0u64; 3];
    for (signal, payload) in split_collected_records(body)? {
        payloads[signal.index()].extend_from_slice(payload);
        records[signal.index()] += 1;
    }

    for signal in CollectSignal::ALL {
        let payload = std::mem::take(&mut payloads[signal.index()]);
        if payload.is_empty() {
            continue;
        }
        let bytes = payload.len();
        let outcome = match signal {
            CollectSignal::Logs => collect_logs(state, headers, payload).await,
            CollectSignal::Traces => collect_traces(state, headers, payload).await,
            CollectSignal::Metrics => collect_metrics(state, headers, payload).await,
        };
        match outcome {
            Ok(()) => {}
            // Permanent: a decode failure, a body past a limit, a tenant over
            // what it stores. Sending these back would only have collecty halve
            // the batch to find them and drop them anyway, one wasted round
            // trip at a time, so they are dropped here and counted.
            Err(error) if never_acceptable(error.status) => {
                tracing::warn!(
                    signal = signal.as_str(),
                    status = error.status.as_u16(),
                    records = records[signal.index()],
                    bytes,
                    reason = error.message,
                    "dropping collected records signy will not take"
                );
                state.metrics.collect_dropped_records.fetch_add(
                    records[signal.index()],
                    std::sync::atomic::Ordering::Relaxed,
                );
                state
                    .metrics
                    .collect_dropped_bytes
                    .fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
            }
            // Everything else is this instance's problem, not the batch's — it
            // is behind, draining, or configured against the tenant that sent
            // this. Answering with it stops the batch here and collecty holds
            // the whole thing until the answer changes.
            Err(error) => return Err(error),
        }
    }

    Ok(StatusCode::OK.into_response())
}

/// Whether an answer means "this body, forever", so that dropping it loses
/// nothing a retry would have saved.
///
/// The four client errors are the same four collecty treats as permanent, and
/// `403` is deliberately not among them: an unknown tenant is a policy mistake
/// to fix, not data to destroy. `429` is here for one reason only — the tenant
/// is storing everything its plan sells, which clears when retention retires
/// parts and not before. The other `429`, the one an overloaded instance
/// answers, cannot reach this: it comes from the gate, and the gate is checked
/// once for the whole batch before any of this runs.
fn never_acceptable(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::UNSUPPORTED_MEDIA_TYPE
            | StatusCode::UNPROCESSABLE_ENTITY
            | StatusCode::TOO_MANY_REQUESTS
    )
}

fn split_collected_records(body: &[u8]) -> Result<Vec<(CollectSignal, &[u8])>, IngestError> {
    let mut records = Vec::new();
    let mut at = 0;
    while at < body.len() {
        let Some(header) = body.get(at..at + COLLECT_RECORD_HEADER_BYTES) else {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "a record header needs {COLLECT_RECORD_HEADER_BYTES} bytes and {} are left",
                    body.len() - at
                ),
            )
                .into());
        };
        let Some(signal) = CollectSignal::from_tag(header[0]) else {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{} is not a signal tag", header[0]),
            )
                .into());
        };
        let len = u32::from_le_bytes(header[1..5].try_into().expect("four bytes")) as usize;
        at += COLLECT_RECORD_HEADER_BYTES;
        let Some(payload) = body.get(at..at + len) else {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "a record claims {len} bytes and {} are left",
                    body.len() - at
                ),
            )
                .into());
        };
        at += len;
        records.push((signal, payload));
    }
    Ok(records)
}

fn log_ingest(state: &Arc<AppState>) -> OtlpLogIngest<'_> {
    OtlpLogIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        config: &state.config,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
        clock: &state.clock,
    }
}

async fn collect_logs(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    payload: Vec<u8>,
) -> Result<(), IngestError> {
    let ingest = log_ingest(state);
    let tenant = crate::tenant::from_headers(headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    ingest.admit_tenant(&tenant, payload.len())?;
    let request = ExportLogsServiceRequest::decode(payload.as_slice()).map_err(|error| {
        IngestError::from((
            StatusCode::BAD_REQUEST,
            format!("OTLP protobuf decode failed: {error}"),
        ))
    })?;
    ingest.accept(tenant, request, Some(payload)).await
}

async fn collect_traces(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    payload: Vec<u8>,
) -> Result<(), IngestError> {
    let ingest = OtlpTraceIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
    };
    let tenant = crate::tenant::from_headers(headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    ingest.admit_tenant(&tenant, payload.len())?;
    let request = ExportTraceServiceRequest::decode(payload.as_slice()).map_err(|error| {
        IngestError::from((
            StatusCode::BAD_REQUEST,
            format!("OTLP protobuf decode failed: {error}"),
        ))
    })?;
    ingest.accept(tenant, request).await
}

async fn collect_metrics(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    payload: Vec<u8>,
) -> Result<(), IngestError> {
    let ingest = OtlpMetricIngest {
        journal: &state.journal,
        shutdown: &state.shutdown,
        config: &state.config,
        ingest_gate: &state.ingest_gate,
        tenant_quota: &state.tenant_quota,
        clock: &state.clock,
    };
    let tenant = crate::tenant::from_headers(headers, &state.config, &state.tenant_policy)
        .map_err(crate::tenant::TenantError::into_http)?;
    ingest.admit_tenant(&tenant, payload.len())?;
    let request = ExportMetricsServiceRequest::decode(payload.as_slice()).map_err(|error| {
        IngestError::from((
            StatusCode::BAD_REQUEST,
            format!("OTLP protobuf decode failed: {error}"),
        ))
    })?;
    let outcome = ingest.accept(tenant, request).await?;
    // The collect route answers a bare 200 — collecty reads the status and
    // nothing else — so a partial refusal has only the log to land in.
    if let Some(partial) = outcome.partial_success() {
        tracing::warn!(
            rejected_points = partial.rejected_data_points,
            reason = partial.error_message,
            "collected metrics were partially refused"
        );
    }
    Ok(())
}

pub async fn decompress_collected_body(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (mut parts, body) = request.into_parts();

    let encoding = parts
        .headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !encoding.eq_ignore_ascii_case("zstd") {
        return IngestError::from((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("the collect route takes Content-Encoding: zstd, not {encoding:?}"),
        ))
        .into_response();
    }

    let declared = parts
        .headers
        .get(COLLECT_UNCOMPRESSED_BYTES_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MAX_COLLECT_PLAIN_BYTES);
    if declared > MAX_COLLECT_PLAIN_BYTES {
        return IngestError::from((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "the batch declares {declared} uncompressed bytes, \
over the maximum of {MAX_COLLECT_PLAIN_BYTES}"
            ),
        ))
        .into_response();
    }

    let _permit = match state.ingest_gate.admit_body(declared as u64) {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };

    let compressed = match axum::body::to_bytes(body, MAX_COLLECT_COMPRESSED_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return IngestError::from((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("the compressed batch could not be read: {error}"),
            ))
            .into_response();
        }
    };

    let plain = match decompress_batch(&compressed, declared) {
        Ok(plain) => plain,
        Err(error) => return error.into_response(),
    };

    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.remove(COLLECT_UNCOMPRESSED_BYTES_HEADER);
    parts.headers.insert(
        header::CONTENT_LENGTH,
        axum::http::HeaderValue::from(plain.len()),
    );
    next.run(axum::extract::Request::from_parts(
        parts,
        axum::body::Body::from(plain),
    ))
    .await
}

fn decompress_batch(compressed: &[u8], declared: usize) -> Result<Vec<u8>, IngestError> {
    use std::io::Read;

    let mut plain = Vec::with_capacity(declared.min(MAX_COLLECT_PLAIN_BYTES));
    let decoder = zstd::stream::read::Decoder::new(compressed).map_err(|error| {
        IngestError::from((
            StatusCode::BAD_REQUEST,
            format!("the batch is not a zstd stream: {error}"),
        ))
    })?;
    decoder
        .take(MAX_COLLECT_PLAIN_BYTES as u64 + 1)
        .read_to_end(&mut plain)
        .map_err(|error| {
            IngestError::from((
                StatusCode::BAD_REQUEST,
                format!("the batch could not be decompressed: {error}"),
            ))
        })?;

    if plain.len() > MAX_COLLECT_PLAIN_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("the batch decompresses past the maximum of {MAX_COLLECT_PLAIN_BYTES} bytes"),
        )
            .into());
    }
    if plain.len() > declared {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "the batch declared {declared} uncompressed bytes and produced {}",
                plain.len()
            ),
        )
            .into());
    }
    Ok(plain)
}

#[cfg(test)]
mod tests {
    include!("tests/otlp_http.rs");
}
