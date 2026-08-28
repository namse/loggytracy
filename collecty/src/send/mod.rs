#[cfg(test)]
mod tests;
mod transport;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::watch;

use crate::queue::{Queue, SealedSegment, SenderId};

pub use transport::HttpTransport;

pub type DeliverFuture<'a> = Pin<Box<dyn Future<Output = Outcome> + Send + 'a>>;

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Taken, and the highest segment signy says it now holds whole.
    ///
    /// Normally the segment that was sent. It can be higher — signy answered
    /// an earlier attempt collecty never heard — and then everything up to it
    /// can go at once.
    Accepted(u64),
    Retry(String),
    Refused(String),
}

/// One segment on its way to signy.
///
/// The body is the segment file, byte for byte: one zstd stream over every
/// record the segment took. The sender and the segment number are what let
/// signy skip what it already stored: a segment is sent from its first record
/// every time, so signy counts as it reads and knows exactly which records it
/// has seen before.
pub struct Shipment {
    pub body: Bytes,
    pub sender: SenderId,
    pub segment: u64,
}

pub trait Transport: Send + Sync + 'static {
    fn deliver<'a>(&'a self, shipment: Shipment) -> DeliverFuture<'a>;
}

#[derive(Clone, Copy, Debug)]
pub struct SenderConfig {
    pub retry_initial: Duration,
    pub retry_max: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(30),
        }
    }
}

/// Segments and bytes, never records.
///
/// What a segment holds is inside its compression, and reading it back would
/// mean decompressing every segment on the way out to learn a number nobody
/// acts on. `collecty_records_appended_total` is where the record count lives,
/// counted where it is free.
#[derive(Default, Debug)]
pub struct SenderStats {
    pub sent_segments: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub refused_segments: AtomicU64,
    pub refused_bytes: AtomicU64,
    pub retries: AtomicU64,
}

pub struct Sender<T> {
    queue: Arc<Queue>,
    transport: Arc<T>,
    config: SenderConfig,
    stats: Arc<SenderStats>,
}

impl<T: Transport> Sender<T> {
    pub fn new(queue: Arc<Queue>, transport: Arc<T>, config: SenderConfig) -> Sender<T> {
        Sender {
            queue,
            transport,
            config,
            stats: Arc::new(SenderStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<SenderStats> {
        self.stats.clone()
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        while !*shutdown.borrow() {
            // Before looking for work: a quiet host would otherwise hold its
            // records until the open segment filled.
            if let Err(error) = self.queue.seal_if_due() {
                tracing::error!(%error, "the open segment could not be closed");
            }

            let Some(seq) = self.queue.oldest_sealed() else {
                tokio::select! {
                    _ = self.queue.wait_for_sealed() => {}
                    _ = tokio::time::sleep(self.config.retry_initial) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            };

            let queue = self.queue.clone();
            let segment = tokio::task::spawn_blocking(move || queue.read_segment(seq)).await;
            let segment = match segment {
                Ok(Ok(segment)) => segment,
                Ok(Err(error)) => {
                    tracing::error!(%error, segment = seq, "the segment could not be read");
                    tokio::time::sleep(self.config.retry_initial).await;
                    continue;
                }
                Err(error) => {
                    tracing::error!(%error, "the segment reader did not finish");
                    continue;
                }
            };

            self.deliver(segment, &mut shutdown).await;
        }
    }

    pub(crate) async fn deliver(
        &self,
        segment: SealedSegment,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        let SealedSegment { seq, body } = segment;
        let bytes = body.len() as u64;
        let body = Bytes::from(body);

        let mut backoff = self.config.retry_initial;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let shipment = Shipment {
                body: body.clone(),
                sender: self.queue.sender_id(),
                segment: seq,
            };
            match self.transport.deliver(shipment).await {
                Outcome::Accepted(stored) => {
                    self.stats.sent_segments.fetch_add(1, Ordering::Relaxed);
                    self.stats.sent_bytes.fetch_add(bytes, Ordering::Relaxed);
                    self.commit(stored.max(seq));
                    return;
                }
                // Permanent for this segment's shape rather than its content:
                // signy drops a record it cannot decode on its own side and
                // answers 200, so what reaches here is a stream or a framing
                // that will fail the same way however many times it is sent.
                Outcome::Refused(reason) => {
                    tracing::error!(
                        reason,
                        segment = seq,
                        bytes,
                        "dropping a segment signy refuses to accept"
                    );
                    self.stats.refused_segments.fetch_add(1, Ordering::Relaxed);
                    self.stats.refused_bytes.fetch_add(bytes, Ordering::Relaxed);
                    self.commit(seq);
                    return;
                }
                Outcome::Retry(reason) => {
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        reason,
                        segment = seq,
                        backoff_ms = backoff.as_millis() as u64,
                        "signy did not take the segment"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(jittered(backoff)) => {}
                        _ = shutdown.changed() => {}
                    }
                    backoff = (backoff * 2).min(self.config.retry_max);
                }
            }
        }
    }

    fn commit(&self, acked: u64) {
        if let Err(error) = self.queue.commit(acked) {
            tracing::error!(%error, "the cursor could not be advanced");
        }
    }
}

fn jittered(backoff: Duration) -> Duration {
    let spread = backoff.as_millis() as u64 / 4;
    if spread == 0 {
        return backoff;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.subsec_nanos() as u64)
        .unwrap_or(0);
    backoff + Duration::from_millis(nanos % spread)
}
