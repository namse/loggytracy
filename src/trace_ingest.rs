use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use prost014::Message;
use tonic::{Request, Response, Status};

use crate::backpressure::{IngestError, IngestGate};
use crate::config::Config;
use crate::journal::Journal;
use crate::shutdown::ShutdownState;
use crate::trace::normalize_request;
use axum::http::StatusCode;

pub const MAX_OTLP_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OTLP_SPANS: usize = 100_000;

#[derive(Clone)]
pub struct TraceIngestService {
    journal: Arc<Journal>,
    shutdown: Arc<ShutdownState>,
    config: Arc<Config>,
    ingest_gate: Arc<IngestGate>,
    tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
    tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
}

impl TraceIngestService {
    pub fn new(
        journal: Arc<Journal>,
        shutdown: Arc<ShutdownState>,
        config: Arc<Config>,
        ingest_gate: Arc<IngestGate>,
        tenant_quota: Arc<crate::tenant_quota::TenantQuota>,
        tenant_policy: Arc<crate::tenant_policy::TenantPolicy>,
    ) -> Self {
        Self {
            journal,
            shutdown,
            config,
            ingest_gate,
            tenant_quota,
            tenant_policy,
        }
    }

    pub fn into_server(self) -> TraceServiceServer<Self> {
        TraceServiceServer::new(self)
            .max_decoding_message_size(MAX_OTLP_REQUEST_BYTES)
            .max_encoding_message_size(64 * 1024)
    }
}

/// Accepting one OTLP trace export, independent of how it arrived. The trace
/// counterpart to [`crate::log_ingest::OtlpLogIngest`], and split for the same
/// reason: a limit enforced on gRPC and forgotten on HTTP is not a limit.
pub struct OtlpTraceIngest<'a> {
    pub journal: &'a Journal,
    pub shutdown: &'a ShutdownState,
    pub ingest_gate: &'a IngestGate,
    pub tenant_quota: &'a crate::tenant_quota::TenantQuota,
}

impl OtlpTraceIngest<'_> {
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
        request: ExportTraceServiceRequest,
    ) -> Result<(), IngestError> {
        let span_count = request
            .resource_spans
            .iter()
            .flat_map(|resource| resource.scope_spans.iter())
            .map(|scope| scope.spans.len())
            .try_fold(0usize, |count, spans| count.checked_add(spans))
            .ok_or_else(|| {
                IngestError::from((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "OTLP span count overflow".to_string(),
                ))
            })?;
        if span_count > MAX_OTLP_SPANS {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("OTLP request contains more than {MAX_OTLP_SPANS} spans"),
            )
                .into());
        }
        let spans = normalize_request(&tenant, request.clone())
            .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
        let mut encoded = Vec::new();
        request.encode(&mut encoded).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                format!("failed to encode request: {error}"),
            )
        })?;
        self.journal
            .append_trace(tenant, encoded, spans)
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

#[tonic::async_trait]
impl TraceService for TraceIngestService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        use crate::log_ingest::ingest_error_to_status;

        let ingest = OtlpTraceIngest {
            journal: &self.journal,
            shutdown: &self.shutdown,
            ingest_gate: &self.ingest_gate,
            tenant_quota: &self.tenant_quota,
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
            .accept(tenant, request)
            .await
            .map_err(ingest_error_to_status)?;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::journal;
    use crate::part_registry::PartRegistry;
    use crate::tenant::test_tenant;
    use crate::trace::TraceMemTable;
    use crate::trace_registry::TraceRegistry;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use std::time::Duration;

    fn tenant_request(request: ExportTraceServiceRequest) -> Request<ExportTraceServiceRequest> {
        Request::from_parts(
            crate::tenant::test_tenant_metadata(),
            tonic::Extensions::default(),
            request,
        )
    }

    fn request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn export_rejects_while_draining_for_shutdown() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-trace-drain-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let shutdown = Arc::new(crate::shutdown::ShutdownState::new());
        shutdown.begin_drain();
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            shutdown,
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );

        let status = service.export(tenant_request(request())).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            trace_memtable.is_empty(),
            "a drained OTLP request must not be appended"
        );
    }

    /// One process, one memory budget: OTLP has to answer to the same
    /// thresholds as Loki push, or refusing one protocol just moves the
    /// overrun into the other.
    #[tokio::test]
    async fn export_is_refused_once_the_buffers_are_over_their_limit() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-trace-ingest-{}", uuid::Uuid::new_v4())),
            flush_max_bytes: 1,
            max_memtable_bytes: Some(1),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );

        service
            .export(tenant_request(request()))
            .await
            .expect("the first export is under the limit");
        let status = service
            .export(tenant_request(request()))
            .await
            .expect_err("a full buffer must be refused");

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            trace_memtable
                .query_trace_id(&test_tenant(), &"01".repeat(16))
                .len(),
            1,
            "the refused export must not have been appended"
        );
    }

    #[tokio::test]
    async fn export_acknowledges_after_journal_append() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-trace-ingest-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let log_memtable = Arc::new(crate::memtable::MemTable::new());
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(&config, log_memtable, trace_memtable.clone())
                .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );
        let response = service.export(tenant_request(request())).await.unwrap();
        assert!(response.into_inner().partial_success.is_none());
        assert_eq!(
            trace_memtable
                .query_trace_id(&test_tenant(), &"01".repeat(16))
                .len(),
            1
        );

        let replayed = TraceMemTable::new();
        journal::replay_with_traces(
            service.journal.wal_path(),
            service.journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &replayed,
        )
        .unwrap();
        assert_eq!(
            replayed
                .query_trace_id(&test_tenant(), &"01".repeat(16))
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn export_rejects_invalid_request_without_inserting() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-trace-invalid-{}", uuid::Uuid::new_v4())),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );
        let mut invalid = request();
        invalid.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![0; 16];
        let status = service.export(tenant_request(invalid)).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(trace_memtable.is_empty());
    }

    #[tokio::test]
    async fn export_rejects_oversized_request_without_inserting() {
        let config = Config {
            data_dir: std::env::temp_dir().join(format!(
                "loggytracy-trace-oversized-{}",
                uuid::Uuid::new_v4()
            )),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );
        let mut oversized = request();
        oversized.resource_spans[0].scope_spans[0].spans[0].name =
            "x".repeat(MAX_OTLP_REQUEST_BYTES);

        let status = service.export(tenant_request(oversized)).await.unwrap_err();

        // Non-retryable, and that is the point: this batch is over the ceiling
        // permanently, so the OTLP retry table has to tell the collector to split
        // or drop it rather than send the identical bytes again.
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(trace_memtable.is_empty());
    }

    #[tokio::test]
    async fn export_rejects_too_many_spans_before_normalization() {
        let config = Config {
            data_dir: std::env::temp_dir().join(format!(
                "loggytracy-trace-span-limit-{}",
                uuid::Uuid::new_v4()
            )),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                Arc::new(crate::memtable::MemTable::new()),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );
        let mut too_many = request();
        too_many.resource_spans[0].scope_spans[0].spans = vec![Span::default(); MAX_OTLP_SPANS + 1];

        let status = service.export(tenant_request(too_many)).await.unwrap_err();

        // Same class as the oversized body: a span count over the cap cannot
        // become acceptable by being retried.
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(trace_memtable.is_empty());
    }

    #[tokio::test]
    async fn export_flushes_to_trace_part_and_reloads_after_restart() {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-trace-flush-{}", uuid::Uuid::new_v4())),
            flush_max_interval: Duration::from_millis(20),
            flush_check_interval: Duration::from_millis(10),
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let log_memtable = Arc::new(crate::memtable::MemTable::new());
        let trace_memtable = Arc::new(TraceMemTable::new());
        let journal = Arc::new(
            journal::Journal::spawn_with_traces(
                &config,
                log_memtable.clone(),
                trace_memtable.clone(),
            )
            .unwrap(),
        );
        let parts = Arc::new(PartRegistry::new());
        let trace_parts = Arc::new(TraceRegistry::new(parts.operation_lock()));
        let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flush_task = tokio::spawn(crate::flush::flush_loop(
            log_memtable,
            trace_memtable.clone(),
            journal.clone(),
            parts.clone(),
            trace_parts.clone(),
            Arc::new(crate::series_registry::SeriesRegistry::standalone()),
            None,
            Arc::new(config.clone()),
            healthy,
            Arc::new(crate::metrics::RuntimeMetrics::new()),
            tokio::sync::watch::channel(false).1,
        ));

        let ingest_gate = IngestGate::for_test(&journal, &config);
        let service = TraceIngestService::new(
            journal,
            Arc::new(crate::shutdown::ShutdownState::new()),
            Arc::new(config.clone()),
            ingest_gate,
            crate::tenant_quota::TenantQuota::for_test(&config),
            Arc::new(crate::tenant_policy::TenantPolicy::disabled()),
        );
        service.export(tenant_request(request())).await.unwrap();
        for _ in 0..100 {
            if trace_parts.part_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(trace_parts.part_count(), 1);
        assert_eq!(
            trace_parts
                .query_trace_id(&test_tenant(), &"01".repeat(16), None, None, None)
                .unwrap()
                .len(),
            1
        );
        flush_task.abort();

        let restored =
            TraceRegistry::load_from_disk(&config.data_dir.join("traces"), parts.operation_lock())
                .unwrap();
        assert_eq!(
            restored
                .query_trace_id(&test_tenant(), &"01".repeat(16), None, None, None)
                .unwrap()
                .len(),
            1
        );
    }

    // Keep the generated AnyValue import exercised when the dependency's
    // generated API changes; the test data intentionally uses the minimal
    // valid request above.
    #[allow(dead_code)]
    fn _any_value_type_is_available(_: AnyValue) {}
}
