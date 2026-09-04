#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{ALLOW, CONTENT_ENCODING, CONTENT_TYPE};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::memprof::{self, Arena};
use crate::queue::Queue;
use crate::signal::Signal;
use crate::wire;

pub const DEFAULT_MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;
/// Jobs that may be waiting for the spool thread. Deep enough that a burst
/// does not serialize on the channel and shallow enough to be nothing: the
/// bytes are already bounded by the in-flight gate.
const SPOOL_DEPTH: usize = 256;
/// Loopback, because a bind address is now the whole of the access control.
/// A deployment that needs to take exports from other containers says so.
pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:4318";

/// The only encoding served. OTLP/HTTP also defines a JSON one, which this
/// collector cannot carry: the queue's whole design rests on serialized
/// export requests concatenating into one merged request, which is a protobuf
/// property and not a JSON one.
const PROTOBUF: &str = "application/x-protobuf";

pub struct Intake {
    spool: Spool,
    inflight: Semaphore,
    max_inflight_bytes: usize,
    max_request_bytes: usize,
}

/// One export on its way to the queue, and where the answer goes.
struct Job {
    signal: Signal,
    payload: Bytes,
    reply: tokio::sync::oneshot::Sender<io::Result<()>>,
}

/// The one thread that appends.
///
/// `Queue::append` takes the queue's lock as its first act, so every append is
/// serialized whichever thread runs it. Handing each one to the blocking pool
/// therefore bought no parallelism at all, and cost a thread per concurrent
/// export -- tokio will spawn up to 512 of them. Under glibc a thread that
/// allocates is given an arena of its own, and an arena keeps whatever it grew
/// to: `docs/MEMORY.md` measured that term at three quarters of the footprint
/// by taking it away with `MALLOC_ARENA_MAX=1`.
///
/// So the appends go to one thread that owns the queue, which is what the lock
/// already made them. The channel is bounded, but the bound that matters is
/// still the in-flight gate: this one only stops an unbounded queue of jobs
/// forming behind a stalled disk.
struct Spool {
    jobs: tokio::sync::mpsc::Sender<Job>,
}

impl Spool {
    fn new(queue: Arc<Queue>) -> Spool {
        let (jobs, mut inbox) = tokio::sync::mpsc::channel::<Job>(SPOOL_DEPTH);
        std::thread::Builder::new()
            .name("spool".to_string())
            .spawn(move || {
                while let Some(job) = inbox.blocking_recv() {
                    let _tag = memprof::enter(Arena::Intake);
                    let outcome = queue.append(job.signal, &job.payload);
                    // A caller that has gone away is a client that hung up
                    // between the append and the answer. The record is in the
                    // segment either way.
                    let _ = job.reply.send(outcome);
                }
            })
            .expect("the spool thread starts");
        Spool { jobs }
    }
}

/// Why an export was not taken, in the vocabulary of the status code it
/// becomes. `Intake` is reachable off the HTTP path -- collecty queues its own
/// metrics through it -- so this is a type of its own rather than a response.
#[derive(Debug)]
pub enum Refusal {
    TooLarge { bytes: usize, limit: usize },
    ShuttingDown,
    Rejected(String),
    Failed(String),
}

impl Refusal {
    pub fn status(&self) -> StatusCode {
        match self {
            Refusal::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Refusal::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Refusal::Rejected(_) => StatusCode::BAD_REQUEST,
            Refusal::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Refusal::TooLarge { bytes, limit } => {
                write!(
                    f,
                    "an OTLP export of {bytes} bytes exceeds the {limit} byte maximum"
                )
            }
            Refusal::ShuttingDown => write!(f, "collecty is shutting down"),
            Refusal::Rejected(reason) => write!(f, "{reason}"),
            Refusal::Failed(reason) => write!(f, "{reason}"),
        }
    }
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
            spool: Spool::new(queue),
            inflight: Semaphore::new(max_inflight_bytes),
            max_inflight_bytes,
            max_request_bytes,
        })
    }

    pub fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    /// Bytes admitted through the gate and not yet appended.
    ///
    /// The arena instrument cannot see a request body -- it is allocated on
    /// hyper's own future, where no synchronous region can be guarded -- but
    /// the gate has the exact number, so it is reported from here instead of
    /// estimated from there.
    pub fn inflight_occupied(&self) -> u64 {
        (self.max_inflight_bytes - self.inflight.available_permits()) as u64
    }

    pub async fn accept(&self, signal: Signal, payload: Bytes) -> Result<(), Refusal> {
        let plain_len = payload.len();
        if plain_len > self.max_request_bytes {
            return Err(Refusal::TooLarge {
                bytes: plain_len,
                limit: self.max_request_bytes,
            });
        }
        if plain_len == 0 {
            return Ok(());
        }

        let _permit = self
            .inflight
            .acquire_many(plain_len as u32)
            .await
            .map_err(|_| Refusal::ShuttingDown)?;

        let (reply, answer) = tokio::sync::oneshot::channel();
        self.spool
            .jobs
            .send(Job {
                signal,
                payload,
                reply,
            })
            .await
            .map_err(|_| Refusal::ShuttingDown)?;
        answer
            .await
            .map_err(|_| Refusal::Failed("the spool thread stopped".to_string()))?
            .map_err(spool_failure)
    }
}

fn spool_failure(error: io::Error) -> Refusal {
    match error.kind() {
        io::ErrorKind::InvalidInput => Refusal::Rejected(error.to_string()),
        _ => Refusal::Failed(format!("the queue refused the record: {error}")),
    }
}

/// A media type without its parameters, lowercased, so `application/x-protobuf`
/// and `application/x-protobuf; charset=utf-8` are the same answer.
fn media_type(raw: &str) -> String {
    raw.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn text(status: StatusCode, reason: impl fmt::Display) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(reason.to_string())))
        .expect("a well-formed refusal")
}

/// An empty body is a valid `ExportLogsServiceResponse` with no
/// `partial_success`, which is what a wholly successful export answers. So a
/// success costs zero bytes and no encoding, and prost stays off this path.
fn accepted() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, PROTOBUF)
        .body(Full::new(Bytes::new()))
        .expect("a well-formed acknowledgement")
}

async fn route(intake: Arc<Intake>, request: Request<Incoming>) -> Response<Full<Bytes>> {
    // The three paths the spec names and nothing else: answering an unknown
    // one invites a client to believe an export landed somewhere.
    let Some(signal) = Signal::from_otlp_path(request.uri().path()) else {
        return text(
            StatusCode::NOT_FOUND,
            "collecty serves /v1/logs, /v1/traces and /v1/metrics",
        );
    };
    if request.method() != Method::POST {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header(ALLOW, "POST")
            .header(CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from_static(b"an OTLP export is a POST")))
            .expect("a well-formed refusal");
    }

    let headers = request.headers();
    match headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if media_type(value) == PROTOBUF => {}
        _ => {
            return text(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format_args!("an OTLP export must be {PROTOBUF}"),
            );
        }
    }
    // Decompressing would make the bytes that arrive different from the bytes
    // that are stored, which is the one property the queue is built on.
    match headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    {
        None => {}
        Some(value) if value.trim().eq_ignore_ascii_case("identity") => {}
        Some(value) => {
            return text(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                format_args!("collecty takes uncompressed exports; this one is {value}"),
            );
        }
    }

    let limit = intake.max_request_bytes();
    // A declared length is refused before a byte of it is read. `Limited`
    // covers what a chunked request that declares nothing can spend.
    if let Some(declared) = request.body().size_hint().exact()
        && declared > limit as u64
    {
        return text(
            StatusCode::PAYLOAD_TOO_LARGE,
            Refusal::TooLarge {
                bytes: declared as usize,
                limit,
            },
        );
    }

    let payload = match Limited::new(request.into_body(), limit).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => {
            return text(
                StatusCode::PAYLOAD_TOO_LARGE,
                format_args!("an OTLP export may not exceed {limit} bytes"),
            );
        }
    };

    match intake.accept(signal, payload).await {
        Ok(()) => accepted(),
        Err(refusal) => {
            let status = refusal.status();
            if status.is_server_error() {
                tracing::warn!(%refusal, "an export was not queued");
            }
            text(status, refusal)
        }
    }
}

pub fn bind(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(addr).map_err(|error| {
        io::Error::new(error.kind(), format!("cannot listen on {addr}: {error}"))
    })?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

pub async fn serve<F>(
    intake: Arc<Intake>,
    listener: std::net::TcpListener,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::from_std(listener)?;
    let mut shutdown = std::pin::pin!(shutdown);
    // Every live connection watches this. A keep-alive connection sitting
    // idle would otherwise hold the process open until its client hung up.
    let (closing, watcher) = watch::channel(false);
    let mut connections = JoinSet::new();
    let outcome = loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                // One client losing its connection between the queue and the
                // accept is not the listener failing.
                Err(error) if is_transient(&error) => continue,
                Err(error) => break Err(error),
            },
            () = &mut shutdown => break Ok(()),
        };
        // Reaped here rather than in a task of their own, so a long-lived
        // process does not accumulate one handle per connection it ever took.
        while connections.try_join_next().is_some() {}
        let _ = stream.set_nodelay(true);
        let intake = intake.clone();
        let mut watcher = watcher.clone();
        connections.spawn(async move {
            let service = service_fn(move |request| {
                let intake = intake.clone();
                async move { Ok::<_, std::convert::Infallible>(route(intake, request).await) }
            });
            let connection = http1::Builder::new().serve_connection(TokioIo::new(stream), service);
            let mut connection = std::pin::pin!(connection);
            let result = tokio::select! {
                result = connection.as_mut() => result,
                _ = watcher.changed() => {
                    // Finish the request in flight, then stop reading. An
                    // export answered for is one the client will not send
                    // again, so cutting it here would lose it.
                    connection.as_mut().graceful_shutdown();
                    connection.await
                }
            };
            if let Err(error) = result {
                tracing::debug!(%error, "an OTLP connection ended badly");
            }
        });
    };

    // The queue's last `fsync` closes the open segment after this returns, so
    // an append still on its way in has to land before then.
    let _ = closing.send(true);
    drop(watcher);
    while connections.join_next().await.is_some() {}
    outcome
}

fn is_transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}
