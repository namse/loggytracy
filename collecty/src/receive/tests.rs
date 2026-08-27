use std::collections::HashMap;
use std::path::PathBuf;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use prost::Message;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use super::*;
use crate::queue::QueueLimits;
use crate::test_support::Scratch;

struct Harness {
    _scratch: Scratch,
    queues: HashMap<Signal, Arc<Queue>>,
    socket: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Harness {
    async fn start(label: &str, max_request_bytes: usize) -> Harness {
        let scratch = Scratch::new(label);
        let mut queues = HashMap::new();
        for signal in Signal::ALL {
            let dir = scratch.path().join(signal.as_str());
            queues.insert(
                signal,
                Arc::new(Queue::open(&dir, QueueLimits::default()).expect("a queue")),
            );
        }
        let socket = scratch.path().join("otlp.sock");
        let listener = bind(&socket, DEFAULT_SOCKET_MODE).expect("a bound socket");
        let intake = Intake::new(
            queues.clone(),
            max_request_bytes,
            DEFAULT_MAX_INFLIGHT_BYTES,
            crate::wire::ZSTD_LEVEL,
        );
        let (tx, rx) = oneshot::channel();
        let server = tokio::spawn(serve(intake, listener, async move {
            let _ = rx.await;
        }));

        Harness {
            _scratch: scratch,
            queues,
            socket,
            shutdown: Some(tx),
            server,
        }
    }

    async fn channel(&self) -> Channel {
        let path = self.socket.clone();
        Endpoint::try_from("http://collecty.invalid")
            .expect("a placeholder endpoint")
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        tokio::net::UnixStream::connect(path).await?,
                    ))
                }
            }))
            .await
            .expect("a connected channel")
    }

    fn queue(&self, signal: Signal) -> &Arc<Queue> {
        self.queues.get(&signal).expect("a queue")
    }

    fn frames(&self, signal: Signal) -> Vec<Vec<u8>> {
        let queue = self.queue(signal);
        let Some(batch) = queue.read_batch(usize::MAX, usize::MAX).expect("a batch") else {
            return Vec::new();
        };
        batch
            .records
            .iter()
            .map(|record| batch.frames[record.span.clone()].to_vec())
            .collect()
    }

    async fn stop(mut self) {
        let _ = self.shutdown.take().expect("one shutdown").send(());
        let _ = self.server.await;
    }
}

fn logs(body: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 7,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn an_export_is_stored_as_a_frame_that_decompresses_to_the_request() {
    let harness = Harness::start("receive-store", DEFAULT_MAX_REQUEST_BYTES).await;
    let request = logs("a line worth compressing, repeated repeated repeated");
    let expected = request.encode_to_vec();

    LogsServiceClient::new(harness.channel().await)
        .export(request)
        .await
        .expect("an accepted export");

    let frames = harness.frames(Signal::Logs);
    assert_eq!(frames.len(), 1);
    let plain = crate::wire::decompress_concatenated(&frames[0], expected.len()).expect("plain");
    assert_eq!(plain, expected);
    harness.stop().await;
}

#[tokio::test]
async fn each_signal_lands_in_its_own_queue() {
    let harness = Harness::start("receive-signals", DEFAULT_MAX_REQUEST_BYTES).await;
    let channel = harness.channel().await;

    LogsServiceClient::new(channel.clone())
        .export(logs("one log"))
        .await
        .expect("an accepted log export");
    TraceServiceClient::new(channel.clone())
        .export(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans::default()],
        })
        .await
        .expect("an accepted trace export");
    MetricsServiceClient::new(channel)
        .export(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics::default()],
        })
        .await
        .expect("an accepted metric export");

    for signal in Signal::ALL {
        assert_eq!(
            harness.frames(signal).len(),
            1,
            "{signal} stored the wrong number of records"
        );
    }
    harness.stop().await;
}

#[tokio::test]
async fn an_export_over_the_ceiling_is_refused_and_stores_nothing() {
    let harness = Harness::start("receive-ceiling", 64).await;

    let status = LogsServiceClient::new(harness.channel().await)
        .export(logs(&"x".repeat(4096)))
        .await
        .expect_err("a refusal");

    assert_eq!(status.code(), tonic::Code::OutOfRange);
    assert!(harness.frames(Signal::Logs).is_empty());
    assert_eq!(harness.queue(Signal::Logs).stats().appended_records, 0);
    harness.stop().await;
}

#[tokio::test]
async fn a_payload_over_the_ceiling_is_refused_off_the_grpc_path_too() {
    let scratch = Scratch::new("intake-ceiling");
    let mut queues = HashMap::new();
    for signal in Signal::ALL {
        queues.insert(
            signal,
            Arc::new(
                Queue::open(
                    &scratch.path().join(signal.as_str()),
                    QueueLimits::default(),
                )
                .expect("a queue"),
            ),
        );
    }
    let intake = Intake::new(
        queues.clone(),
        64,
        DEFAULT_MAX_INFLIGHT_BYTES,
        crate::wire::ZSTD_LEVEL,
    );

    let status = intake
        .accept(Signal::Logs, bytes::Bytes::from(vec![0u8; 128]))
        .await
        .expect_err("a refusal");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        queues
            .get(&Signal::Logs)
            .expect("a queue")
            .stats()
            .appended_records,
        0
    );
}

#[tokio::test]
async fn an_empty_export_succeeds_without_storing_a_record() {
    let harness = Harness::start("receive-empty", DEFAULT_MAX_REQUEST_BYTES).await;

    LogsServiceClient::new(harness.channel().await)
        .export(ExportLogsServiceRequest {
            resource_logs: Vec::new(),
        })
        .await
        .expect("an accepted empty export");

    assert_eq!(harness.queue(Signal::Logs).stats().appended_records, 0);
    harness.stop().await;
}

#[tokio::test]
async fn separate_exports_reassemble_into_one_merged_request() {
    let harness = Harness::start("receive-merge", DEFAULT_MAX_REQUEST_BYTES).await;
    let channel = harness.channel().await;
    let requests: Vec<_> = (0..6).map(|index| logs(&format!("line {index}"))).collect();

    for request in &requests {
        LogsServiceClient::new(channel.clone())
            .export(request.clone())
            .await
            .expect("an accepted export");
    }

    let batch = harness
        .queue(Signal::Logs)
        .read_batch(usize::MAX, usize::MAX)
        .expect("a batch")
        .expect("records");
    let plain =
        crate::wire::decompress_concatenated(&batch.frames, batch.plain_bytes).expect("plain");
    let merged = ExportLogsServiceRequest::decode(plain.as_slice()).expect("a merged request");

    assert_eq!(merged.resource_logs.len(), requests.len());
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(merged.resource_logs[index], request.resource_logs[0]);
    }
    harness.stop().await;
}

#[tokio::test]
async fn binding_over_a_live_socket_is_refused() {
    let harness = Harness::start("receive-bind", DEFAULT_MAX_REQUEST_BYTES).await;
    let _ = harness.channel().await;

    let error = bind(&harness.socket, DEFAULT_SOCKET_MODE).expect_err("a refusal");
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
    harness.stop().await;
}

#[tokio::test]
async fn a_stale_socket_file_is_replaced() {
    let scratch = Scratch::new("receive-stale");
    let socket = scratch.path().join("otlp.sock");
    std::fs::write(&socket, b"left behind by a killed process").expect("a stale file");

    let listener = bind(&socket, DEFAULT_SOCKET_MODE).expect("a bound socket");
    drop(listener);
}

#[tokio::test]
async fn a_socket_path_that_cannot_be_bound_names_itself() {
    let scratch = Scratch::new("receive-long");
    let socket = scratch.path().join("x".repeat(200));

    let error = bind(&socket, DEFAULT_SOCKET_MODE).expect_err("a refusal");
    assert!(
        error.to_string().contains(&socket.display().to_string()),
        "the error does not name the path: {error}"
    );
}
