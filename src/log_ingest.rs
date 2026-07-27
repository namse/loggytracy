use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::{LogsService, LogsServiceServer},
};
use prost014::Message;
use tonic::{Request, Response, Status};

use crate::backpressure::IngestGate;
use crate::config::Config;
use crate::journal::Journal;
use crate::otlp_log::normalize_request;
use crate::shutdown::ShutdownState;
use crate::trace_ingest::MAX_OTLP_REQUEST_BYTES;

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

#[tonic::async_trait]
impl LogsService for LogIngestService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        if self.shutdown.is_fenced() {
            return Err(Status::unavailable(
                "this instance has been fenced by a newer writer and is shutting down",
            ));
        }
        if self.shutdown.is_draining() {
            return Err(Status::unavailable("server is draining for shutdown"));
        }
        self.ingest_gate.check_grpc()?;
        let tenant = crate::tenant::from_grpc_metadata(request.metadata(), &self.config)
            .map_err(crate::tenant::TenantError::into_grpc)?;
        let request = request.into_inner();
        self.tenant_quota
            .check_grpc(&tenant, request.encoded_len() as u64)?;
        if request.encoded_len() > MAX_OTLP_REQUEST_BYTES {
            return Err(Status::resource_exhausted(format!(
                "OTLP request exceeds the maximum of {MAX_OTLP_REQUEST_BYTES} bytes"
            )));
        }
        let record_count = request
            .resource_logs
            .iter()
            .flat_map(|resource| resource.scope_logs.iter())
            .map(|scope| scope.log_records.len())
            .try_fold(0usize, |count, records| count.checked_add(records))
            .ok_or_else(|| Status::resource_exhausted("OTLP log record count overflow"))?;
        if record_count > MAX_OTLP_LOG_RECORDS {
            return Err(Status::resource_exhausted(format!(
                "OTLP request contains more than {MAX_OTLP_LOG_RECORDS} log records"
            )));
        }

        let streams = normalize_request(&request)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        // The same input limits the Loki path applies. A record that arrives
        // over gRPC is not exempt from the bounds that keep a label set from
        // becoming a cardinality problem or a timestamp from landing in a
        // partition retention has already swept.
        let window = crate::ingest::TimestampWindow::from_config(&self.config, &self.clock);
        for (labels, entries) in &streams {
            crate::ingest::validate_labels(labels, &self.config)
                .map_err(Status::invalid_argument)?;
            for entry in entries {
                if entry.line.len() > self.config.max_line_bytes {
                    return Err(Status::invalid_argument(format!(
                        "log line is {} bytes, exceeding the maximum of {}",
                        entry.line.len(),
                        self.config.max_line_bytes
                    )));
                }
                window
                    .validate(entry.timestamp_ns)
                    .map_err(Status::invalid_argument)?;
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
            .map_err(|error| Status::internal(format!("journal write failed: {error}")))?;
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    include!("tests/log_ingest.rs");
}
