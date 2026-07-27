use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use prost014::Message;
use tonic::{Request, Response, Status};

use crate::backpressure::IngestError;
use crate::backpressure::IngestGate;
use crate::config::Config;
use crate::journal::Journal;
use crate::otlp_log::normalize_request;
use crate::shutdown::ShutdownState;
use crate::trace_ingest::MAX_OTLP_REQUEST_BYTES;
use axum::http::StatusCode;

/// Records one export may carry. Sized like `MAX_OTLP_SPANS` and for the same
/// reason: the request is normalized in memory before anything is appended, so
/// the count has to be bounded before that work starts.
pub const MAX_OTLP_LOG_RECORDS: usize = 100_000;

/// OTLP log ingest.
///
/// The gRPC counterpart to the Loki push handler, landing in the same journal,
/// the same memtable and the same part format. `ARCHITECTURE.md` has claimed
/// OTLP ingest since the beginning while only the trace service was registered,
/// so a collector exporting logs got `UNIMPLEMENTED`.
#[derive(Clone)]
pub struct LogIngestService {
    journal: Arc<Journal>,
    shutdown: Arc<ShutdownState>,
    config: Arc<Config>,
    ingest_gate: Arc<IngestGate>,
    tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
    clock: Arc<crate::clock::Clock>,
}

impl LogIngestService {
    pub fn new(
        journal: Arc<Journal>,
        shutdown: Arc<ShutdownState>,
        config: Arc<Config>,
        ingest_gate: Arc<IngestGate>,
        tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
        clock: Arc<crate::clock::Clock>,
    ) -> Self {
        Self {
            journal,
            shutdown,
            config,
            ingest_gate,
            tenant_quota,
            clock,
        }
    }

    pub fn into_server(self) -> LogsServiceServer<Self> {
        LogsServiceServer::new(self)
            .max_decoding_message_size(MAX_OTLP_REQUEST_BYTES)
            .max_encoding_message_size(64 * 1024)
    }
}

/// Accepting one OTLP log export, independent of how it arrived.
///
/// gRPC and HTTP differ only in how the request is framed and how a refusal is
/// spelled, so both go through this. Splitting the decision here rather than
/// duplicating it is what keeps a limit from being enforced on one transport
/// and forgotten on the other.
pub struct OtlpLogIngest<'a> {
    pub journal: &'a Journal,
    pub shutdown: &'a ShutdownState,
    pub config: &'a Config,
    pub ingest_gate: &'a IngestGate,
    pub tenant_quota: &'a crate::tenant_quota::TenantQuota,
    pub clock: &'a crate::clock::Clock,
}

impl OtlpLogIngest<'_> {
    /// Refusals that do not depend on knowing the tenant, checked before the
    /// transport goes looking for the header. A draining instance has to say
    /// so even when the request is also malformed, or an operator reads a
    /// planned shutdown as a client bug.
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

    /// What the tenant and the size decide, once both are known.
    pub fn admit_tenant(
        &self,
        tenant: &crate::tenant::TenantId,
        encoded_len: usize,
    ) -> Result<(), IngestError> {
        self.tenant_quota.check(tenant, encoded_len as u64)?;
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
        request: ExportLogsServiceRequest,
    ) -> Result<(), IngestError> {
        let record_count = request
            .resource_logs
            .iter()
            .flat_map(|resource| resource.scope_logs.iter())
            .map(|scope| scope.log_records.len())
            .try_fold(0usize, |count, records| count.checked_add(records))
            .ok_or_else(|| {
                IngestError::from((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "OTLP log record count overflow".to_string(),
                ))
            })?;
        if record_count > MAX_OTLP_LOG_RECORDS {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("OTLP request contains more than {MAX_OTLP_LOG_RECORDS} log records"),
            )
                .into());
        }

        let streams = normalize_request(&request)
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        // The same input limits the Loki path applies. A record that arrives
        // over OTLP is not exempt from the bounds that keep a label set from
        // becoming a cardinality problem or a timestamp from landing in a
        // partition retention has already swept.
        let window = crate::ingest::TimestampWindow::from_config(self.config, self.clock);
        for (labels, entries) in &streams {
            crate::ingest::validate_labels(labels, self.config)
                .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
            for entry in entries {
                if entry.line.len() > self.config.max_line_bytes {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "log line is {} bytes, exceeding the maximum of {}",
                            entry.line.len(),
                            self.config.max_line_bytes
                        ),
                    )
                        .into());
                }
                window
                    .validate(entry.timestamp_ns)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
            }
        }

        // The journal keeps one encoding for a log record whatever protocol it
        // arrived on, so replay has one decoder rather than a kind byte and two
        // paths that can drift apart. The normalization above is therefore
        // done once, here, and not again after a crash.
        let encoded = crate::proto::encode_push_request(&streams);
        self.journal
            .append(tenant, encoded, streams)
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("journal write failed: {error}"),
                )
            })?;
        Ok(())
    }
}

/// An HTTP-shaped refusal, spelled for a gRPC client.
///
/// OTLP exporters read the code, not the message, so this mapping is what
/// decides whether a collector retries or drops the batch.
pub fn ingest_error_to_status(error: IngestError) -> Status {
    match error.status {
        StatusCode::SERVICE_UNAVAILABLE => Status::unavailable(error.message),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::PAYLOAD_TOO_LARGE => {
            Status::resource_exhausted(error.message)
        }
        StatusCode::BAD_REQUEST => Status::invalid_argument(error.message),
        StatusCode::FORBIDDEN => Status::permission_denied(error.message),
        _ => Status::internal(error.message),
    }
}

#[tonic::async_trait]
impl LogsService for LogIngestService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let ingest = OtlpLogIngest {
            journal: &self.journal,
            shutdown: &self.shutdown,
            config: &self.config,
            ingest_gate: &self.ingest_gate,
            tenant_quota: &self.tenant_quota,
            clock: &self.clock,
        };
        ingest.admit_transport().map_err(ingest_error_to_status)?;
        let tenant = crate::tenant::from_grpc_metadata(request.metadata(), &self.config)
            .map_err(crate::tenant::TenantError::into_grpc)?;
        let request = request.into_inner();
        ingest
            .admit_tenant(&tenant, request.encoded_len())
            .map_err(ingest_error_to_status)?;
        ingest
            .accept(tenant, request)
            .await
            .map_err(ingest_error_to_status)?;
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    include!("tests/log_ingest.rs");
}
