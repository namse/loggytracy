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
    ingest.accept(tenant, request).await?;
    Ok(encoding.encode(&ExportMetricsServiceResponse::default()))
}

/// Body limit for the OTLP HTTP routes, matching what the gRPC services accept
/// so a collector sees one size whichever transport it picks.
pub const MAX_OTLP_HTTP_BODY_BYTES: usize = MAX_OTLP_REQUEST_BYTES;

#[cfg(test)]
mod tests {
    include!("tests/otlp_http.rs");
}
