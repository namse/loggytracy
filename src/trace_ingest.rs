use std::sync::Arc;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use prost014::Message;
use tonic::{Request, Response, Status};

use crate::journal::Journal;
use crate::trace::normalize_request;

pub const MAX_OTLP_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OTLP_SPANS: usize = 100_000;

#[derive(Clone)]
pub struct TraceIngestService {
    journal: Arc<Journal>,
}

impl TraceIngestService {
    pub fn new(journal: Arc<Journal>) -> Self {
        Self { journal }
    }

    pub fn into_server(self) -> TraceServiceServer<Self> {
        TraceServiceServer::new(self)
            .max_decoding_message_size(MAX_OTLP_REQUEST_BYTES)
            .max_encoding_message_size(64 * 1024)
    }
}

#[tonic::async_trait]
impl TraceService for TraceIngestService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();
        if request.encoded_len() > MAX_OTLP_REQUEST_BYTES {
            return Err(Status::resource_exhausted(format!(
                "OTLP request exceeds the maximum of {MAX_OTLP_REQUEST_BYTES} bytes"
            )));
        }
        let span_count = request
            .resource_spans
            .iter()
            .flat_map(|resource| resource.scope_spans.iter())
            .map(|scope| scope.spans.len())
            .try_fold(0usize, |count, spans| count.checked_add(spans))
            .ok_or_else(|| Status::resource_exhausted("OTLP span count overflow"))?;
        if span_count > MAX_OTLP_SPANS {
            return Err(Status::resource_exhausted(format!(
                "OTLP request contains more than {MAX_OTLP_SPANS} spans"
            )));
        }
        let spans = normalize_request(request.clone())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let mut encoded = Vec::new();
        request.encode(&mut encoded).map_err(|error| {
            Status::invalid_argument(format!("failed to encode request: {error}"))
        })?;
        self.journal
            .append_trace(encoded, spans)
            .await
            .map_err(|error| Status::internal(format!("journal write failed: {error}")))?;
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::journal;
    use crate::part_registry::PartRegistry;
    use crate::trace::TraceMemTable;
    use crate::trace_registry::TraceRegistry;
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use std::time::Duration;

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
        let service = TraceIngestService::new(journal);
        let response = service.export(Request::new(request())).await.unwrap();
        assert!(response.into_inner().partial_success.is_none());
        assert_eq!(trace_memtable.query_trace_id(&"01".repeat(16)).len(), 1);

        let replayed = TraceMemTable::new();
        journal::replay_with_traces(
            service.journal.wal_path(),
            service.journal.ckpt_path(),
            &crate::memtable::MemTable::new(),
            &replayed,
        )
        .unwrap();
        assert_eq!(replayed.query_trace_id(&"01".repeat(16)).len(), 1);
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
        let service = TraceIngestService::new(journal);
        let mut invalid = request();
        invalid.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![0; 16];
        let status = service.export(Request::new(invalid)).await.unwrap_err();
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
        let service = TraceIngestService::new(journal);
        let mut oversized = request();
        oversized.resource_spans[0].scope_spans[0].spans[0].name =
            "x".repeat(MAX_OTLP_REQUEST_BYTES);

        let status = service.export(Request::new(oversized)).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
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
        let service = TraceIngestService::new(journal);
        let mut too_many = request();
        too_many.resource_spans[0].scope_spans[0].spans = vec![Span::default(); MAX_OTLP_SPANS + 1];

        let status = service.export(Request::new(too_many)).await.unwrap_err();

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
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
            None,
            Arc::new(config.clone()),
            healthy,
        ));

        let service = TraceIngestService::new(journal);
        service.export(Request::new(request())).await.unwrap();
        for _ in 0..100 {
            if trace_parts.part_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(trace_parts.part_count(), 1);
        assert_eq!(
            trace_parts
                .query_trace_id(&"01".repeat(16), None, None)
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
                .query_trace_id(&"01".repeat(16), None, None)
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
