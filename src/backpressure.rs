use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::config::Config;
use crate::journal::Journal;
use crate::metrics::RuntimeMetrics;

/// An ingest rejection, carrying the one header a throttled client needs.
///
/// Handlers keep returning `(StatusCode, String)` where nothing more is
/// needed; the `From` below lets `?` widen those into this without touching
/// the call sites.
#[derive(Debug)]
pub struct IngestError {
    pub status: StatusCode,
    pub message: String,
    pub retry_after: Option<Duration>,
}

impl From<(StatusCode, String)> for IngestError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self {
            status,
            message,
            retry_after: None,
        }
    }
}

impl IntoResponse for IngestError {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.message).into_response();
        if let Some(retry_after) = self.retry_after {
            let seconds = retry_after_seconds(retry_after);
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

/// The delay a refusal promises, in the granularity both transports use.
///
/// `Retry-After` has whole seconds and nothing finer, and the two transports
/// have to carry the *same* instruction rather than merely compatible ones — a
/// gRPC `RetryInfo` of 1.7 s beside a header of 1 s is two answers to one
/// question. So the header's granularity is the shared one and
/// [`crate::log_ingest::ingest_error_to_status`] sends whole seconds too.
///
/// Rounded **up**, and never zero. `Retry-After: 0` reads as "retry
/// immediately", which is the opposite of what an overloaded server is asking
/// for; and truncating 1.7 s to 1 s sends the client back before the server's
/// own arithmetic says it may, which just spends another refusal. The ceiling
/// matches `tenant_quota`'s own clamp so a computed delay cannot become a
/// client that never returns.
pub fn retry_after_seconds(retry_after: Duration) -> u64 {
    let seconds = retry_after.as_secs_f64().ceil();
    if seconds.is_finite() {
        (seconds as u64).clamp(1, MAX_RETRY_AFTER_SECONDS)
    } else {
        MAX_RETRY_AFTER_SECONDS
    }
}

const MAX_RETRY_AFTER_SECONDS: u64 = 3600;

/// Decides whether the durable path is far enough behind to stop accepting.
///
/// Both ingest protocols consult the same instance, because the thresholds
/// bound one process's memory and one WAL: letting OTLP keep writing while
/// Loki push is refused would just move the overrun.
pub struct IngestGate {
    journal: Arc<Journal>,
    config: Arc<Config>,
    metrics: Arc<RuntimeMetrics>,
    /// Request bodies admitted and not yet answered.
    ///
    /// Every other term in this gate is a *buffer* the server owns and can
    /// measure whenever it likes. This one is the bodies in flight, which no
    /// gauge could see after the fact: by the time a handler runs, its body is
    /// already a `Bytes` in the heap. So it is counted at admission and
    /// released by the guard's `Drop`, and the sum is what a scrape reads.
    inflight_body_bytes: Arc<std::sync::atomic::AtomicU64>,
    disk: Arc<crate::disk::DiskSpace>,
}

/// One admitted body's charge against the in-flight ceiling.
///
/// Held for the whole request — through decode, quota, journal append and the
/// response — because that is how long the body is resident. Dropping it is
/// the release, so an early `?` return cannot leak the charge.
#[derive(Debug)]
pub struct InflightBody {
    counter: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}

impl Drop for InflightBody {
    fn drop(&mut self) {
        // Saturating: a decrement that would go below zero means the counter
        // was reset under a live request, and a wrapped counter would then
        // refuse every future body forever. Losing the charge is the harmless
        // direction.
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(self.bytes))
            });
    }
}

impl IngestGate {
    pub fn new(
        journal: Arc<Journal>,
        config: Arc<Config>,
        metrics: Arc<RuntimeMetrics>,
        disk: Arc<crate::disk::DiskSpace>,
    ) -> Self {
        Self {
            journal,
            config,
            metrics,
            inflight_body_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            disk,
        }
    }

    /// Bytes of request body admitted and not yet answered.
    pub fn inflight_body_bytes(&self) -> u64 {
        self.inflight_body_bytes.load(Ordering::Relaxed)
    }

    /// Admit `bytes` of request body against the in-flight ceiling, or refuse.
    ///
    /// The hole this closes: every other ingest bound is checked once per
    /// request and bounds a buffer, while the bodies themselves were
    /// `concurrency × MAX_OTLP_REQUEST_BYTES` and nothing limited concurrency —
    /// outside the accounting entirely (`todo.md`, M10). Measured at 0.3 MiB on
    /// the comparison bed, so this closes a hole rather than recovering memory,
    /// and the ceiling is deliberately generous.
    ///
    /// **An empty server always admits one body**, whatever the ceiling says.
    /// Without that a ceiling set below one legal request would refuse it
    /// forever with nothing in flight to wait for. Progress is the invariant;
    /// the ceiling only decides how many bodies share the server.
    pub fn admit_body(&self, bytes: u64) -> Result<InflightBody, IngestError> {
        let Some(limit) = self.config.max_inflight_push_bytes else {
            return Ok(self.charge(0));
        };
        let admitted =
            self.inflight_body_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    if current == 0 || current.saturating_add(bytes) <= limit {
                        Some(current.saturating_add(bytes))
                    } else {
                        None
                    }
                });
        match admitted {
            Ok(_) => Ok(InflightBody {
                counter: self.inflight_body_bytes.clone(),
                bytes,
            }),
            Err(current) => Err(self.overloaded(format!(
                "in-flight request bodies hold {current} bytes and this one is {bytes}, \
over the limit of {limit}; too many pushes are in flight at once"
            ))),
        }
    }

    /// The same admission for OTLP over gRPC, in its own vocabulary.
    ///
    /// The gRPC transport is charged for what it can be charged for and no
    /// more. There is no `Content-Length` on a gRPC request — the framing is
    /// streamed — and tonic hands the service an already-decoded message, so
    /// the wire size is gone by the time any code here could read it. What
    /// bounds that path is tonic's own `max_decoding_message_size`
    /// (`MAX_OTLP_REQUEST_BYTES`) times its concurrency, which is recorded as a
    /// known limit rather than papered over: charging a flat ceiling per gRPC
    /// push instead would refuse four concurrent 100 KB batches on a 2 GiB
    /// container, which is a throughput regression wearing a memory bound's
    /// clothes.
    pub fn admit_body_grpc(&self, bytes: u64) -> Result<InflightBody, tonic::Status> {
        self.admit_body(bytes)
            .map_err(crate::log_ingest::ingest_error_to_status)
    }

    fn charge(&self, bytes: u64) -> InflightBody {
        InflightBody {
            counter: self.inflight_body_bytes.clone(),
            bytes,
        }
    }

    /// Bytes accepted but not yet in a part, across both memtables.
    pub fn buffered_bytes(&self) -> u64 {
        (self.journal.log_memtable().approximate_size() as u64)
            .saturating_add(self.journal.trace_memtable().approximate_size() as u64)
    }

    /// Refuse the write when the durable path is already behind.
    ///
    /// Called before the body is decompressed and before the journal append,
    /// so a rejected request costs neither CPU nor WAL bytes. Returning 429
    /// rather than accepting is what makes the architecture's "the client's own
    /// WAL is the safety net" premise true: a client can only hold data back if
    /// the server declines it. Acknowledging while flush is stalled instead
    /// turns a recoverable backlog into an OOM.
    pub fn check(&self) -> Result<(), IngestError> {
        let buffered_bytes = self.buffered_bytes();
        if let Some(limit) = self.config.max_memtable_bytes
            && buffered_bytes > limit
        {
            return Err(self.overloaded(format!(
                "memtable holds {buffered_bytes} bytes, over the limit of {limit}; \
flush is not keeping up"
            )));
        }
        let backlog_bytes = self.journal.wal_backlog_bytes();
        if let Some(limit) = self.config.max_wal_backlog_bytes
            && backlog_bytes > limit
        {
            return Err(self.overloaded(format!(
                "WAL backlog is {backlog_bytes} bytes, over the limit of {limit}; \
flush is not keeping up"
            )));
        }
        // Last, because it is the condition the other two exist to prevent
        // reaching. Both limits above bound something this process controls and
        // clear on their own; this one bounds the machine, and past it the
        // failure is a flush that cannot write — data acknowledged and stuck in
        // a WAL that cannot be drained.
        let free_bytes = self.disk.free_bytes();
        if let Some(floor) = self.config.min_free_disk_bytes
            && free_bytes < floor
        {
            return Err(self.overloaded(format!(
                "the data directory's filesystem has {free_bytes} bytes free, below the floor \
of {floor}; refusing writes so flush keeps room to run"
            )));
        }
        Ok(())
    }

    /// The same decision for OTLP, whose exporters read a status code rather
    /// than a header. Rendered by the one mapping both gRPC services use, so
    /// the refusal carries the `RetryInfo` that makes `RESOURCE_EXHAUSTED`
    /// retryable instead of a bare code a collector is told to drop on.
    pub fn check_grpc(&self) -> Result<(), tonic::Status> {
        self.check()
            .map_err(crate::log_ingest::ingest_error_to_status)
    }

    /// A gate over the given journal with its own metrics, for tests that are
    /// exercising something else and just need one to exist.
    #[cfg(test)]
    pub fn for_test(journal: &Arc<Journal>, config: &Config) -> Arc<Self> {
        Arc::new(Self::new(
            journal.clone(),
            Arc::new(config.clone()),
            Arc::new(RuntimeMetrics::new()),
            Arc::new(crate::disk::DiskSpace::unknown()),
        ))
    }

    fn overloaded(&self, message: String) -> IngestError {
        self.metrics
            .ingest_throttled
            .fetch_add(1, Ordering::Relaxed);
        IngestError {
            status: StatusCode::TOO_MANY_REQUESTS,
            message,
            retry_after: Some(self.config.backpressure_retry_after),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(max_inflight_push_bytes: Option<u64>) -> Arc<IngestGate> {
        let config = Config {
            data_dir: std::env::temp_dir()
                .join(format!("loggytracy-inflight-{}", uuid::Uuid::new_v4())),
            max_inflight_push_bytes,
            ..Config::default()
        };
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let journal = Arc::new(
            crate::journal::Journal::spawn(&config, Arc::new(crate::memtable::MemTable::new()))
                .unwrap(),
        );
        IngestGate::for_test(&journal, &config)
    }

    #[tokio::test]
    async fn bodies_over_the_ceiling_are_refused_with_a_retry_after() {
        let gate = gate(Some(1000));
        let first = gate.admit_body(600).expect("the first body fits");
        assert_eq!(gate.inflight_body_bytes(), 600);
        let error = gate
            .admit_body(600)
            .expect_err("the second body would exceed the ceiling");
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(
            error.retry_after.is_some(),
            "a throttled client needs the header"
        );
        // The refusal charged nothing: a body that was never admitted must not
        // hold the ceiling down for the ones behind it.
        assert_eq!(gate.inflight_body_bytes(), 600);
        drop(first);
        assert_eq!(gate.inflight_body_bytes(), 0);
        gate.admit_body(600).expect("the ceiling released");
    }

    /// The invariant that makes the knob safe at any value.
    #[tokio::test]
    async fn an_idle_server_admits_one_body_however_small_the_ceiling() {
        let gate = gate(Some(1));
        let permit = gate
            .admit_body(16 * 1024 * 1024)
            .expect("an empty server admits one body whatever the ceiling says");
        // And exactly one: the next has something to wait for now.
        gate.admit_body(16 * 1024 * 1024)
            .expect_err("the second body is refused while the first is in flight");
        drop(permit);
        assert_eq!(gate.inflight_body_bytes(), 0);
    }

    #[tokio::test]
    async fn off_admits_everything_and_counts_nothing() {
        let gate = gate(None);
        let permits: Vec<_> = (0..64)
            .map(|_| {
                gate.admit_body(16 * 1024 * 1024)
                    .expect("off never refuses")
            })
            .collect();
        assert_eq!(
            gate.inflight_body_bytes(),
            0,
            "with no ceiling there is nothing to account against"
        );
        drop(permits);
    }

    /// gRPC gets the OTLP specification's status rather than the HTTP one —
    /// and the `RetryInfo` that makes that status mean "come back". Without it
    /// the specification tells a collector to drop the batch, which would make
    /// the in-flight ceiling a data-loss mechanism instead of a memory bound.
    #[tokio::test]
    async fn the_grpc_refusal_is_resource_exhausted_and_says_when_to_return() {
        use tonic_types::StatusExt;

        let gate = gate(Some(1000));
        let _first = gate.admit_body(1000).expect("the first body fits");
        let status = gate
            .admit_body_grpc(1000)
            .expect_err("the second body is refused");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status
                .get_details_retry_info()
                .and_then(|info| info.retry_delay),
            Some(Config::default().backpressure_retry_after),
        );
    }
    use crate::memtable::MemTable;

    fn disk_config(label: &str) -> Config {
        Config {
            data_dir: std::env::temp_dir().join(format!(
                "loggytracy-disk-gate-{label}-{}",
                uuid::Uuid::new_v4()
            )),
            ..Config::default()
        }
    }

    fn disk_gate(config: Config, disk: crate::disk::DiskSpace) -> IngestGate {
        std::fs::create_dir_all(&config.data_dir).unwrap();
        let memtable = Arc::new(MemTable::new());
        let journal = Arc::new(crate::journal::Journal::spawn(&config, memtable).unwrap());
        IngestGate::new(
            journal,
            Arc::new(config),
            Arc::new(RuntimeMetrics::new()),
            Arc::new(disk),
        )
    }

    /// The floor refuses writes rather than letting them run the disk out from
    /// under the flush that has to make them durable.
    #[tokio::test]
    async fn a_disk_below_the_floor_refuses_the_write() {
        let mut config = disk_config("below");
        config.min_free_disk_bytes = Some(64 * 1024 * 1024);
        let gate = disk_gate(config, crate::disk::DiskSpace::with_free_bytes(1024));

        let error = gate.check().expect_err("below the floor is a refusal");
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(error.message.contains("free"), "{}", error.message);
        assert_eq!(gate.metrics.ingest_throttled.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_disk_above_the_floor_admits_the_write() {
        let mut config = disk_config("above");
        config.min_free_disk_bytes = Some(1024);
        let gate = disk_gate(
            config,
            crate::disk::DiskSpace::with_free_bytes(64 * 1024 * 1024),
        );
        gate.check().expect("above the floor is not a refusal");
    }

    /// An unmeasured disk is not an empty one. Between process start and the
    /// first reading the gate has nothing to go on, and refusing then would
    /// make an unreadable filesystem look like a full one.
    #[tokio::test]
    async fn an_unmeasured_disk_does_not_refuse() {
        let mut config = disk_config("unknown");
        config.min_free_disk_bytes = Some(u64::MAX - 1);
        let gate = disk_gate(config, crate::disk::DiskSpace::unknown());
        gate.check().expect("an unread disk refuses nothing");
    }

    #[tokio::test]
    async fn the_floor_can_be_turned_off() {
        let mut config = disk_config("off");
        config.min_free_disk_bytes = None;
        let gate = disk_gate(config, crate::disk::DiskSpace::with_free_bytes(0));
        gate.check().expect("no floor is no refusal");
    }
}
