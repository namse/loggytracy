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
            // Whole seconds, and never zero: `Retry-After: 0` reads as "retry
            // immediately", which is the opposite of what an overloaded server
            // is asking for.
            let seconds = retry_after.as_secs().max(1);
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
        }
        response
    }
}

/// Decides whether the durable path is far enough behind to stop accepting.
///
/// Both ingest protocols consult the same instance, because the thresholds
/// bound one process's memory and one WAL: letting OTLP keep writing while
/// Loki push is refused would just move the overrun.
pub struct IngestGate {
    journal: Arc<Journal>,
    config: Arc<Config>,
    metrics: Arc<RuntimeMetrics>,
}

impl IngestGate {
    pub fn new(journal: Arc<Journal>, config: Arc<Config>, metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            journal,
            config,
            metrics,
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
        Ok(())
    }

    /// The same decision for OTLP, whose exporters read a status code rather
    /// than a header. `RESOURCE_EXHAUSTED` is what the OTLP specification tells
    /// them to back off and retry on.
    pub fn check_grpc(&self) -> Result<(), tonic::Status> {
        self.check()
            .map_err(|error| tonic::Status::resource_exhausted(error.message))
    }

    /// A gate over the given journal with its own metrics, for tests that are
    /// exercising something else and just need one to exist.
    #[cfg(test)]
    pub fn for_test(journal: &Arc<Journal>, config: &Config) -> Arc<Self> {
        Arc::new(Self::new(
            journal.clone(),
            Arc::new(config.clone()),
            Arc::new(RuntimeMetrics::new()),
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
