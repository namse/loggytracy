use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use prost014::Message;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

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
    tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
    clock: Arc<crate::clock::Clock>,
}

impl LogIngestService {
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

    /// `wire` is the encoded request when the transport still has it — the
    /// HTTP protobuf body arrives as exactly the bytes the WAL wants, so they
    /// are passed through instead of being re-encoded. gRPC and JSON hand
    /// over a decoded message only, and those re-encode it here.
    pub async fn accept(
        &self,
        tenant: crate::tenant::TenantId,
        request: ExportLogsServiceRequest,
        wire: Option<Vec<u8>>,
    ) -> Result<(), IngestError> {
        self.enqueue(tenant, request, wire)
            .await?
            .settle()
            .await
            .map_err(journal_write_failed)
    }

    /// The same admission and normalization, stopping at the point the writer
    /// has the record and has not yet fsynced it. A caller with a run of
    /// records to write hands them over back to back and awaits them
    /// afterwards, so one fsync covers the run instead of one each.
    pub async fn enqueue(
        &self,
        tenant: crate::tenant::TenantId,
        request: ExportLogsServiceRequest,
        wire: Option<Vec<u8>>,
    ) -> Result<crate::journal::PendingAppend, IngestError> {
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

        // The WAL bytes are settled before normalization consumes the message:
        // the HTTP protobuf body arrives as exactly the bytes the WAL wants,
        // and the other transports re-encode the message they decoded.
        let encoded = match wire {
            Some(bytes) => bytes,
            None => request.encode_to_vec(),
        };
        let entries = normalize_request(request)
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        // A record that arrives over OTLP is still bounded: a line has a size
        // limit, and a timestamp outside the window would land in a partition
        // retention has already swept.
        let window = crate::ingest::TimestampWindow::from_config(self.config, self.clock);
        for entry in &entries {
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

        // The WAL stores the export itself (`encoded` above). This used to
        // materialize a Loki `PushRequest` — a second message with a clone
        // per line and per label, serialized just so replay had one decoder —
        // measured as the largest remaining term of `docs/VISION.md`
        // invariant II's copy count. Replay decodes by the record's kind
        // instead, the way traces always have.
        self.journal
            .enqueue_otlp_logs(tenant, encoded, entries)
            .await
            .map_err(journal_write_failed)
    }
}

pub fn journal_write_failed(error: std::io::Error) -> IngestError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("journal write failed: {error}"),
    )
        .into()
}

/// An HTTP-shaped refusal, spelled for a gRPC client.
///
/// OTLP exporters read the code, not the message, so this mapping is what
/// decides whether a collector retries or drops the batch.
///
/// The two refusals that share a status code over HTTP's cousin do **not** share
/// one here, because the OTLP specification's retry table is what a collector
/// acts on and they want opposite actions:
///
/// * A **limit violation** — a body over `MAX_OTLP_REQUEST_BYTES`, more spans
///   than `MAX_OTLP_SPANS` — is permanent for that batch. Retrying it produces
///   the identical refusal forever. The specification names `INVALID_ARGUMENT`
///   for exactly this ("to indicate non-retryable errors, the server is
///   recommended to use code InvalidArgument"), and a client MUST NOT retry it,
///   so the collector splits or drops the batch instead of looping on it.
/// * **Backpressure** — the memtable or WAL backlog over its threshold, a
///   tenant over its rate, too many bodies in flight — is temporary by
///   construction, and `RESOURCE_EXHAUSTED` is where it belongs.
///
/// And backpressure carries `RetryInfo`, because on this transport that is what
/// makes it backpressure at all. The specification treats `RESOURCE_EXHAUSTED`
/// as retryable *only* when the server signals recovery is possible — "a client
/// SHOULD interpret it as retryable only if the server signals that recovery is
/// possible", by attaching `RetryInfo` — and says a client SHOULD **drop** the
/// telemetry otherwise. A bare `RESOURCE_EXHAUSTED` is therefore not a softer
/// refusal than `INVALID_ARGUMENT`; it is the same instruction to discard,
/// which would make the architecture's own premise false on one of its two
/// transports: a client can only hold data back if the server declines it.
///
/// The delay is the refusal's own `retry_after` — the identical field the HTTP
/// transport renders as `Retry-After` — in the identical whole-second
/// granularity ([`crate::backpressure::retry_after_seconds`]), so the two
/// transports carry one instruction rather than two compatible ones. A 429 that
/// somehow carried no delay still gets one: every producer sets it today
/// (`backpressure::overloaded`, `tenant_quota`'s two refusals), and the fallback
/// is here so that a fourth one added later cannot silently return the transport
/// to telling collectors to drop.
pub fn ingest_error_to_status(error: IngestError) -> Status {
    match error.status {
        StatusCode::SERVICE_UNAVAILABLE => Status::unavailable(error.message),
        StatusCode::TOO_MANY_REQUESTS => {
            let seconds = crate::backpressure::retry_after_seconds(
                error.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
            );
            Status::with_error_details(
                Code::ResourceExhausted,
                error.message,
                ErrorDetails::with_retry_info(Some(Duration::from_secs(seconds))),
            )
        }
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::BAD_REQUEST => {
            Status::invalid_argument(error.message)
        }
        StatusCode::FORBIDDEN => Status::permission_denied(error.message),
        _ => Status::internal(error.message),
    }
}

/// The delay a throttled push is told to wait when its refusal named none.
///
/// Matches `Config::backpressure_retry_after`'s default rather than being a
/// second number: this is a floor for a case that does not arise, not a policy.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

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
        ingest
            .accept(tenant, request, None)
            .await
            .map_err(ingest_error_to_status)?;
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    include!("tests/log_ingest.rs");
}
