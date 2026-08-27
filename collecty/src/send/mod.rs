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

use crate::queue::{Batch, BatchRecord, Queue, SenderId};

pub use transport::HttpTransport;

pub type DeliverFuture<'a> = Pin<Box<dyn Future<Output = Outcome> + Send + 'a>>;

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Taken, and the number signy says it now holds for this sender.
    ///
    /// Normally the last record's number, and then the whole attempt is
    /// committed. It can be higher — signy remembers more than this batch
    /// carried, because an earlier attempt got further than collecty heard —
    /// and the next batch simply starts under it and is skipped. Zero means
    /// the answer said nothing, and the attempt is committed whole.
    Accepted(u64),
    Retry(String),
    Refused(String),
}

/// One attempt's worth of a batch, and who it belongs to.
///
/// The frames are unchanged from what the queue holds. The sender and the
/// number of the first record are what let signy place every record without
/// the wire carrying a number per record: it counts as it reads, so record
/// `i` of the body is `start_sequence + i`.
pub struct Shipment {
    pub frames: Bytes,
    pub sender: SenderId,
    pub start_sequence: u64,
}

pub trait Transport: Send + Sync + 'static {
    fn deliver<'a>(&'a self, shipment: Shipment) -> DeliverFuture<'a>;
}

#[derive(Clone, Copy, Debug)]
pub struct SenderConfig {
    pub max_batch_plain_bytes: usize,
    pub max_batch_records: usize,
    pub retry_initial: Duration,
    pub retry_max: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            // What one batch costs *this* process, not what signy will hold.
            // signy reads a batch as it arrives and never has more than a
            // record of it in hand, so the number that used to sit under its
            // admission ceiling now sits under nothing but the frames kept
            // here while an attempt is out. Raised because the only time a
            // large batch happens is when the backlog is large, which is
            // exactly when the round trips are worth saving.
            max_batch_plain_bytes: 64 * 1024 * 1024,
            max_batch_records: 8192,
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(30),
        }
    }
}

#[derive(Default, Debug)]
pub struct SenderStats {
    pub sent_batches: AtomicU64,
    pub sent_records: AtomicU64,
    pub sent_bytes: AtomicU64,
    pub refused_records: AtomicU64,
    pub retries: AtomicU64,
}

pub struct Sender<T> {
    queue: Arc<Queue>,
    transport: Arc<T>,
    config: SenderConfig,
    stats: Arc<SenderStats>,
}

enum Attempt {
    Accepted(u64),
    Refused(String),
    Aborted,
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
            if !self.queue.has_records() {
                tokio::select! {
                    _ = self.queue.wait_for_records() => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }

            let queue = self.queue.clone();
            let plain_ceiling = self.config.max_batch_plain_bytes;
            let record_ceiling = self.config.max_batch_records;
            let batch = tokio::task::spawn_blocking(move || {
                queue.read_batch(plain_ceiling, record_ceiling)
            })
            .await;

            let batch = match batch {
                Ok(Ok(Some(batch))) => batch,
                Ok(Ok(None)) => continue,
                Ok(Err(error)) => {
                    tracing::error!(%error, "the queue could not be read");
                    tokio::time::sleep(self.config.retry_initial).await;
                    continue;
                }
                Err(error) => {
                    tracing::error!(%error, "the queue reader did not finish");
                    continue;
                }
            };

            self.deliver(batch, &mut shutdown).await;
        }
    }

    pub(crate) async fn deliver(&self, batch: Batch, shutdown: &mut watch::Receiver<bool>) {
        let Batch {
            frames, records, ..
        } = batch;
        let frames = Bytes::from(frames);
        let mut from = 0;

        while from < records.len() {
            let mut to = records.len();
            loop {
                let span = records[from].span.start..records[to - 1].span.end;
                let plain_bytes: usize = records[from..to]
                    .iter()
                    .map(|record| record.plain_len as usize)
                    .sum();

                let shipment = Shipment {
                    frames: frames.slice(span.clone()),
                    sender: self.queue.sender_id(),
                    start_sequence: records[from].end.sequence,
                };

                match self.attempt(shipment, shutdown).await {
                    Attempt::Accepted(stored) => {
                        let Some(taken) = taken_upto(&records[from..to], stored) else {
                            tracing::warn!(
                                stored,
                                first = records[from].end.sequence,
                                "signy took none of the attempt it answered; holding it"
                            );
                            return;
                        };
                        let to = from + taken;
                        self.stats.sent_batches.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .sent_records
                            .fetch_add(taken as u64, Ordering::Relaxed);
                        self.stats
                            .sent_bytes
                            .fetch_add(span.len() as u64, Ordering::Relaxed);
                        self.commit(&records, to, taken as u64);
                        from = to;
                        break;
                    }
                    Attempt::Refused(reason) if to - from == 1 => {
                        tracing::error!(
                            reason,
                            plain_bytes,
                            "dropping one record signy refuses to accept"
                        );
                        self.stats.refused_records.fetch_add(1, Ordering::Relaxed);
                        self.commit(&records, to, 0);
                        from = to;
                        break;
                    }
                    Attempt::Refused(reason) => {
                        tracing::warn!(
                            reason,
                            records = to - from,
                            "halving a refused batch to find the record signy will not take"
                        );
                        to = from + (to - from) / 2;
                    }
                    Attempt::Aborted => return,
                }
            }
        }
    }

    async fn attempt(&self, shipment: Shipment, shutdown: &mut watch::Receiver<bool>) -> Attempt {
        let mut backoff = self.config.retry_initial;
        loop {
            if *shutdown.borrow() {
                return Attempt::Aborted;
            }
            let attempt = Shipment {
                frames: shipment.frames.clone(),
                ..shipment
            };
            match self.transport.deliver(attempt).await {
                Outcome::Accepted(stored) => return Attempt::Accepted(stored),
                Outcome::Refused(reason) => return Attempt::Refused(reason),
                Outcome::Retry(reason) => {
                    self.stats.retries.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        reason,
                        backoff_ms = backoff.as_millis() as u64,
                        "signy did not take the batch"
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

    fn commit(&self, records: &[BatchRecord], to: usize, sent: u64) {
        if let Err(error) = self.queue.commit(records[to - 1].end, sent) {
            tracing::error!(%error, "the cursor could not be advanced");
        }
    }
}

/// How many of an attempt's records the answer covers.
///
/// Normally all of them: signy answers with the last record's number, or with
/// a higher one of its own. A lower answer means it stopped short of what was
/// sent, and the rest stay in the queue rather than being taken on trust —
/// they are offered again on the next batch and skipped there if it turns out
/// signy had them after all.
fn taken_upto(records: &[BatchRecord], stored: u64) -> Option<usize> {
    if stored == 0 {
        return Some(records.len());
    }
    let taken = records
        .iter()
        .take_while(|record| record.end.sequence <= stored)
        .count();
    (taken > 0).then_some(taken)
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
