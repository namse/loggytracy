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

/// The tests take what the sender would take, so the open segment has to close
/// the moment they ask.
fn eager_limits() -> QueueLimits {
    QueueLimits {
        max_segment_age: std::time::Duration::from_nanos(1),
        ..QueueLimits::default()
    }
}
use crate::test_support::Scratch;

struct Harness {
    _scratch: Scratch,
    queue: Arc<Queue>,
    socket: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Harness {
    async fn start(label: &str, max_request_bytes: usize) -> Harness {
        let scratch = Scratch::new(label);
        let queue = Arc::new(
            Queue::open(&scratch.path().join("queue"), eager_limits()).expect("a queue"),
        );
        let socket = scratch.path().join("otlp.sock");
        let listener = bind(&socket, DEFAULT_SOCKET_MODE).expect("a bound socket");
        let intake = Intake::new(
            queue.clone(),
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
            queue,
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

    fn records(&self) -> Vec<(Signal, Vec<u8>)> {
        let frames = self.sealed_frames();
        if frames.is_empty() {
            return Vec::new();
        }
        let plain = crate::wire::decompress_concatenated(&frames, frames.len() * 8)
            .expect("decompressed frames");
        crate::wire::split_records(&plain)
            .expect("framed records")
            .into_iter()
            .map(|(signal, payload)| (signal, payload.to_vec()))
            .collect()
    }

    /// What the sender would ship: the open segment closed, then read whole.
    fn sealed_frames(&self) -> Vec<u8> {
        self.queue.seal_if_due().expect("a seal");
        let mut frames = Vec::new();
        while let Some(seq) = self.queue.oldest_sealed() {
            let sealed = self.queue.read_segment(seq).expect("a segment");
            frames.extend_from_slice(&sealed.frames);
            self.queue.commit(seq, sealed.records).expect("a commit");
            self.queue.seal_if_due().expect("a seal");
        }
        frames
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

    assert_eq!(harness.records(), vec![(Signal::Logs, expected)]);
    harness.stop().await;
}

#[tokio::test]
async fn every_signal_lands_in_the_one_queue_tagged_with_itself() {
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

    let tags: Vec<Signal> = harness
        .records()
        .into_iter()
        .map(|(signal, _)| signal)
        .collect();
    assert_eq!(tags, vec![Signal::Logs, Signal::Traces, Signal::Metrics]);
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
    assert!(harness.records().is_empty());
    assert_eq!(harness.queue.stats().appended_records, 0);
    harness.stop().await;
}

#[tokio::test]
async fn a_payload_over_the_ceiling_is_refused_off_the_grpc_path_too() {
    let scratch = Scratch::new("intake-ceiling");
    let queue = Arc::new(
        Queue::open(&scratch.path().join("queue"), eager_limits()).expect("a queue"),
    );
    let intake = Intake::new(
        queue.clone(),
        64,
        DEFAULT_MAX_INFLIGHT_BYTES,
        crate::wire::ZSTD_LEVEL,
    );

    let status = intake
        .accept(Signal::Logs, bytes::Bytes::from(vec![0u8; 128]))
        .await
        .expect_err("a refusal");

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(queue.stats().appended_records, 0);
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

    assert_eq!(harness.queue.stats().appended_records, 0);
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

    let frames = harness.sealed_frames();
    let plain =
        crate::wire::decompress_concatenated(&frames, frames.len() * 8).expect("plain");
    let mut payloads = Vec::new();
    for (signal, payload) in crate::wire::split_records(&plain).expect("framed records") {
        assert_eq!(signal, Signal::Logs);
        payloads.extend_from_slice(payload);
    }
    let merged = ExportLogsServiceRequest::decode(payloads.as_slice()).expect("a merged request");

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
