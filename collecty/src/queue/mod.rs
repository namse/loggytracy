mod identity;
mod segment;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use identity::SenderId;
use segment::{SegmentMeta, SegmentWriter};

/// Segments are numbered from one so that zero can mean "signy has none of
/// them" in the cursor and in signy's own memory.
const FIRST_SEGMENT: u64 = 1;

#[derive(Clone, Copy, Debug)]
pub struct QueueLimits {
    pub max_bytes: u64,
    pub max_segment_bytes: u64,
    /// How long an open segment may keep collecting before it is closed and
    /// becomes sendable.
    ///
    /// A segment is what a request carries and what an `fsync` covers, so
    /// nothing leaves this machine and nothing is on the device until one
    /// closes. On a busy host the size closes it first; on a quiet one this
    /// does, and it is the floor on how long a record waits.
    pub max_segment_age: Duration,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024 * 1024,
            max_segment_bytes: 8 * 1024 * 1024,
            max_segment_age: Duration::from_secs(1),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueStats {
    /// Everything on disk, which is also everything still owed: a segment
    /// signy has answered for is unlinked, so nothing here has reached it.
    pub queued_bytes: u64,
    pub segments: usize,
    pub appended_records: u64,
    /// What applications handed over, before compression. Against the bytes
    /// the sender ships this is the ratio a segment achieved.
    pub appended_bytes: u64,
    pub dropped_bytes: u64,
    pub dropped_segments: u64,
}

pub struct Queue {
    dir: PathBuf,
    sender: SenderId,
    limits: QueueLimits,
    level: i32,
    inner: Mutex<Inner>,
    appended: Notify,
}

struct Inner {
    segments: VecDeque<SegmentMeta>,
    active: SegmentWriter,
    /// When the open segment took its first record. `None` while it is empty,
    /// so an idle collector does not roll empty segments forever, and the only
    /// honest answer to "is it empty" — the file can still be nothing while
    /// the encoder holds a block's worth.
    active_since: Option<Instant>,
    stats: QueueStats,
}

/// One record on its way into a segment: a signal tag, a length, and an OTLP
/// export. Uncompressed — the segment compresses.
pub struct Record {
    pub plain: Vec<u8>,
}

/// A closed segment, ready to be shipped whole.
///
/// `body` is the file, byte for byte. One zstd stream over every record the
/// segment took, which is exactly what the wire wants, so nothing is parsed,
/// unwrapped or copied on the way out.
pub struct SealedSegment {
    pub seq: u64,
    pub body: Vec<u8>,
}

impl Queue {
    pub fn open(dir: &Path, limits: QueueLimits, level: i32) -> io::Result<Queue> {
        std::fs::create_dir_all(dir)?;
        segment::sweep_temporaries(dir)?;
        let mut metas = segment::list(dir)?;

        // From the highest number the directory ever held, not from the
        // highest still there: signy holds a high-water mark under this
        // sender's name, and numbering that went backwards would have it skip
        // every segment under that mark as one it already stored.
        let next = metas
            .last()
            .map(|meta| meta.seq + 1)
            .unwrap_or(FIRST_SEGMENT);

        // A stream cannot be resumed: the encoder's state went with the
        // process that held it. So the segment a previous run left open is
        // closed where it stopped, and this run starts a fresh one. Only the
        // last segment can be unfinished — a roll closes and syncs the old
        // segment before it creates the next one.
        if let Some(last) = metas.pop() {
            match segment::reseal(dir, last.seq, level)? {
                Some(bytes) => metas.push(SegmentMeta {
                    seq: last.seq,
                    bytes,
                }),
                None => tracing::warn!(
                    segment = last.seq,
                    "a segment a crash left with nothing readable in it is dropped"
                ),
            }
        }

        let active = SegmentWriter::create(dir, next, level)?;
        let queued_bytes = metas.iter().map(|meta| meta.bytes).sum();
        metas.push(SegmentMeta {
            seq: next,
            bytes: 0,
        });

        // An identity file that cannot be read is replaced rather than
        // repaired. Keeping the name while the segment numbering restarted
        // would have signy skip every segment under the high-water mark it
        // still held for that name.
        let sender = match identity::load(dir)? {
            Some(sender) => sender,
            None => {
                let sender = SenderId::generate()?;
                identity::store(dir, sender)?;
                sender
            }
        };

        // How far signy has got is not read back from anywhere, because the
        // files say it: one it has answered for was unlinked on the spot. A
        // crash between the answer and the unlink leaves a segment behind, and
        // offering it again costs one request that signy answers unread.
        Ok(Queue {
            dir: dir.to_path_buf(),
            sender,
            limits,
            level,
            inner: Mutex::new(Inner {
                segments: metas.into(),
                active,
                active_since: None,
                stats: QueueStats {
                    queued_bytes,
                    ..QueueStats::default()
                },
            }),
            appended: Notify::new(),
        })
    }

    pub fn sender_id(&self) -> SenderId {
        self.sender
    }

    pub fn append(&self, record: &Record) -> io::Result<()> {
        let plain_len = record.plain.len() as u64;
        if plain_len > self.limits.max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a {plain_len} byte record cannot fit a {} byte queue",
                    self.limits.max_bytes
                ),
            ));
        }

        let mut inner = self.inner.lock();

        // Against the plain length, which is all that is known before the
        // record is compressed. It only ever overstates what the record will
        // occupy, so the queue errs towards keeping room rather than filling.
        while inner.stats.queued_bytes + plain_len > self.limits.max_bytes {
            self.drop_oldest(&mut inner)?;
        }

        // After crossing rather than before: the compressed size of a record
        // is not known until it has been written, and the encoder emits in
        // blocks, so a segment overshoots by at most the last block.
        if inner.active.written() >= self.limits.max_segment_bytes {
            self.roll(&mut inner)?;
        }

        inner.active.write_all(&record.plain)?;
        if inner.active_since.is_none() {
            inner.active_since = Some(Instant::now());
        }

        let written = inner.active.written();
        let back = inner
            .segments
            .back_mut()
            .expect("a queue always holds its active segment");
        let grew = written - back.bytes;
        back.bytes = written;
        inner.stats.queued_bytes += grew;
        inner.stats.appended_records += 1;
        inner.stats.appended_bytes += plain_len;
        drop(inner);

        self.appended.notify_waiters();
        Ok(())
    }

    /// Close the open segment if it has been collecting for long enough.
    ///
    /// Called by the sender before it looks for work: without it a quiet host
    /// would hold its records until the segment filled, which at eight
    /// mebibytes could be hours.
    pub fn seal_if_due(&self) -> io::Result<()> {
        let mut inner = self.inner.lock();
        let Some(since) = inner.active_since else {
            return Ok(());
        };
        if since.elapsed() < self.limits.max_segment_age {
            return Ok(());
        }
        self.roll(&mut inner)
    }

    /// Close the open segment whatever its age. What a clean shutdown does, so
    /// that the records it holds are on the device rather than left for the
    /// next process to recover.
    pub fn seal(&self) -> io::Result<()> {
        let mut inner = self.inner.lock();
        self.roll(&mut inner)
    }

    /// The lowest-numbered closed segment. Everything on disk is still owed,
    /// so this is simply the oldest one that is not still being written.
    pub fn oldest_sealed(&self) -> Option<u64> {
        let inner = self.inner.lock();
        let active = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .seq;
        inner
            .segments
            .iter()
            .map(|meta| meta.seq)
            .find(|seq| *seq != active)
    }

    pub fn has_sealed(&self) -> bool {
        self.oldest_sealed().is_some()
    }

    pub async fn wait_for_sealed(&self) {
        loop {
            let notified = self.appended.notified();
            if self.has_sealed() {
                return;
            }
            notified.await;
        }
    }

    /// Read a closed segment into the bytes that go on the wire.
    ///
    /// The file is the body. Whether it decompresses is signy's to find out —
    /// a batch it cannot read is a `400`, which is a refusal, which drops the
    /// segment. Checking here would mean decompressing every segment twice to
    /// reach the same place.
    pub fn read_segment(&self, seq: u64) -> io::Result<SealedSegment> {
        let mut body = Vec::new();
        segment::open_for_read(&self.dir, seq)?.read_to_end(&mut body)?;
        Ok(SealedSegment { seq, body })
    }

    /// signy has every segment up to and including this one, so they are
    /// unlinked. That is the whole of the bookkeeping: nothing is written down
    /// about how far signy has got, because what is left on disk says it.
    pub fn commit(&self, acked: u64) -> io::Result<()> {
        let mut inner = self.inner.lock();
        let active = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .seq;
        while let Some(front) = inner.segments.front() {
            if front.seq > acked || front.seq == active {
                break;
            }
            let meta = inner.segments.pop_front().expect("just inspected");
            segment::remove(&self.dir, meta.seq)?;
            inner.stats.queued_bytes -= meta.bytes;
        }
        Ok(())
    }

    pub fn stats(&self) -> QueueStats {
        let inner = self.inner.lock();
        QueueStats {
            segments: inner.segments.len(),
            ..inner.stats
        }
    }

    fn roll(&self, inner: &mut Inner) -> io::Result<()> {
        // The file can still be empty while the encoder holds a block's worth,
        // so what says the segment is empty is that nothing was appended.
        if inner.active_since.is_none() {
            return Ok(());
        }
        let next = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .seq
            + 1;

        // Closing writes out what the encoder held and forces the lot to the
        // device, so the segment's true size is only known here.
        let bytes = inner.active.finish()?;
        let back = inner
            .segments
            .back_mut()
            .expect("a queue always holds its active segment");
        inner.stats.queued_bytes += bytes - back.bytes;
        back.bytes = bytes;

        inner.active = SegmentWriter::create(&self.dir, next, self.level)?;
        inner.active_since = None;
        inner.segments.push_back(SegmentMeta {
            seq: next,
            bytes: 0,
        });
        self.appended.notify_waiters();
        Ok(())
    }

    fn drop_oldest(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.segments.len() == 1 {
            self.roll(inner)?;
        }
        if inner.segments.len() == 1 {
            // An empty active segment cannot be rolled and cannot be dropped.
            return Ok(());
        }
        let meta = inner.segments.pop_front().expect("length exceeds one");
        segment::remove(&self.dir, meta.seq)?;
        inner.stats.queued_bytes -= meta.bytes;
        inner.stats.dropped_bytes += meta.bytes;
        inner.stats.dropped_segments += 1;
        Ok(())
    }
}
