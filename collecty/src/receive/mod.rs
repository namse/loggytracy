mod codec;
#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::net::UnixListener;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Status;
use tonic::transport::Server;

use crate::queue::{Queue, Record};
use crate::signal::Signal;
use crate::wire;
use codec::PassthroughCodec;

pub const DEFAULT_MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_SOCKET_MODE: u32 = 0o666;

pub struct Intake {
    queue: Arc<Queue>,
    inflight: Semaphore,
    max_request_bytes: usize,
}

impl Intake {
    pub fn new(
        queue: Arc<Queue>,
        max_request_bytes: usize,
        max_inflight_bytes: usize,
    ) -> Arc<Intake> {
        assert!(
            max_request_bytes <= u32::MAX as usize - wire::RECORD_HEADER_BYTES,
            "a request ceiling above u32::MAX cannot be charged to the in-flight gate"
        );
        assert!(
            max_inflight_bytes >= max_request_bytes,
            "an in-flight ceiling below the request ceiling would refuse every large request forever"
        );
        Arc::new(Intake {
            queue,
            inflight: Semaphore::new(max_inflight_bytes),
            max_request_bytes,
        })
    }

    pub fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    pub async fn accept(&self, signal: Signal, payload: Bytes) -> Result<(), Status> {
        let plain_len = payload.len();
        if plain_len > self.max_request_bytes {
            return Err(Status::invalid_argument(format!(
                "an OTLP export of {plain_len} bytes exceeds the {} byte maximum",
                self.max_request_bytes
            )));
        }
        if plain_len == 0 {
            return Ok(());
        }

        let _permit = self
            .inflight
            .acquire_many(plain_len as u32)
            .await
            .map_err(|_| Status::unavailable("collecty is shutting down"))?;

        // Framing is nothing, but the queue compresses inside its lock, so the
        // append is still the blocking pool's work.
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || {
            queue.append(
                signal,
                &Record {
                    plain: wire::frame_record(&payload),
                },
            )
        })
        .await
        .map_err(|error| Status::internal(format!("the spool task did not finish: {error}")))?
        .map_err(spool_failure)
    }
}

fn spool_failure(error: io::Error) -> Status {
    match error.kind() {
        io::ErrorKind::InvalidInput => Status::invalid_argument(error.to_string()),
        _ => Status::internal(format!("the queue refused the record: {error}")),
    }
}

struct Export {
    intake: Arc<Intake>,
    signal: Signal,
}

impl tonic::server::UnaryService<Bytes> for Export {
    type Response = ();
    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<()>, Status>> + Send>>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let intake = self.intake.clone();
        let signal = self.signal;
        Box::pin(async move {
            intake.accept(signal, request.into_inner()).await?;
            Ok(tonic::Response::new(()))
        })
    }
}

macro_rules! export_service {
    ($name:ident, $signal:expr, $service:literal) => {
        #[derive(Clone)]
        pub struct $name(Arc<Intake>);

        impl tonic::server::NamedService for $name {
            const NAME: &'static str = $service;
        }

        impl tower_service::Service<http::Request<tonic::body::Body>> for $name {
            type Response = http::Response<tonic::body::Body>;
            type Error = Infallible;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
                let intake = self.0.clone();
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(PassthroughCodec)
                        .max_decoding_message_size(intake.max_request_bytes());
                    let export = Export {
                        intake,
                        signal: $signal,
                    };
                    Ok(grpc.unary(export, request).await)
                })
            }
        }
    };
}

export_service!(
    LogsService,
    Signal::Logs,
    "opentelemetry.proto.collector.logs.v1.LogsService"
);
export_service!(
    TraceService,
    Signal::Traces,
    "opentelemetry.proto.collector.trace.v1.TraceService"
);
export_service!(
    MetricsService,
    Signal::Metrics,
    "opentelemetry.proto.collector.metrics.v1.MetricsService"
);

pub fn bind(path: &Path, mode: u32) -> io::Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;

    if std::fs::symlink_metadata(path).is_ok() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    format!("{} is already served by a running collecty", path.display()),
                ));
            }
            Err(_) => std::fs::remove_file(path)?,
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot listen on {}: {error}", path.display()),
        )
    })?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(listener)
}

pub async fn serve<F>(
    intake: Arc<Intake>,
    listener: UnixListener,
    shutdown: F,
) -> Result<(), tonic::transport::Error>
where
    F: Future<Output = ()> + Send + 'static,
{
    Server::builder()
        .add_service(LogsService(intake.clone()))
        .add_service(TraceService(intake.clone()))
        .add_service(MetricsService(intake))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
}
