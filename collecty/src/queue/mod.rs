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
use segment::{SegmentFile, SegmentMeta};

/// A record's on-disk header: the frame's length and its crc32.
///
/// Nothing else. It used to carry the decompressed length as well, so a batch
/// could be sized against what signy would admit without decompressing it;
/// both the batch and that ceiling are gone.
pub const RECORD_HEADER_BYTES: usize = 8;

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
    /// A segment is what a request carries, so nothing leaves this machine
    /// until one closes. On a busy host the size closes it first; on a quiet
    /// one this does, and it is the floor on how long a record waits.
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
    pub appended_bytes: u64,
    pub dropped_bytes: u64,
    pub dropped_segments: u64,
    pub sent_records: u64,
}

pub struct Queue {
    dir: PathBuf,
    sender: SenderId,
    limits: QueueLimits,
    inner: Mutex<Inner>,
    appended: Notify,
}

struct Inner {
    segments: VecDeque<SegmentMeta>,
    active: SegmentFile,
    /// When the open segment took its first record. `None` while it is empty,
    /// so an idle collector does not roll empty segments forever.
    active_since: Option<Instant>,
    unsynced: bool,
    stats: QueueStats,
}

pub struct Record {
    pub frame: Vec<u8>,
}

/// A closed segment, ready to be shipped whole.
///
/// `frames` is what goes on the wire: the records' zstd frames, concatenated,
/// with the twelve-byte on-disk header of each stripped. That header is this
/// machine's own — it exists to find a torn tail and to size a segment without
/// decompressing — and signy has no use for it.
pub struct SealedSegment {
    pub seq: u64,
    pub frames: Vec<u8>,
    pub records: u64,
}

impl Queue {
    pub fn open(dir: &Path, limits: QueueLimits) -> io::Result<Queue> {
        std::fs::create_dir_all(dir)?;
        let mut metas = segment::list(dir)?;

        if metas.is_empty() {
            metas.push(SegmentMeta {
                seq: FIRST_SEGMENT,
                bytes: 0,
            });
            SegmentFile::create(dir, FIRST_SEGMENT)?;
        }

        let last = metas.last_mut().expect("just ensured non-empty");
        last.bytes = segment::truncate_torn_tail(dir, last.seq)?;

        let active = SegmentFile::open_for_append(dir, last.seq)?;
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
        let queued_bytes = metas.iter().map(|meta| meta.bytes).sum();
        let segments: VecDeque<SegmentMeta> = metas.into();
        let active_since = (segments
            .back()
            .expect("a queue always holds its active segment")
            .bytes
            > 0)
            .then(Instant::now);
        Ok(Queue {
            dir: dir.to_path_buf(),
            sender,
            limits,
            inner: Mutex::new(Inner {
                segments,
                active,
                active_since,
                unsynced: false,
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
        let framed_len = (RECORD_HEADER_BYTES + record.frame.len()) as u64;
        if framed_len > self.limits.max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a {framed_len} byte record cannot fit a {} byte queue",
                    self.limits.max_bytes
                ),
            ));
        }

        let mut header = [0u8; RECORD_HEADER_BYTES];
        header[0..4].copy_from_slice(&(record.frame.len() as u32).to_le_bytes());
        header[4..8].copy_from_slice(&crc32fast::hash(&record.frame).to_le_bytes());

        let mut inner = self.inner.lock();

        while inner.stats.queued_bytes + framed_len > self.limits.max_bytes {
            self.drop_oldest(&mut inner)?;
        }

        let active_bytes = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .bytes;
        if active_bytes > 0 && active_bytes + framed_len > self.limits.max_segment_bytes {
            self.roll(&mut inner)?;
        }

        inner.active.write_all(&header)?;
        inner.active.write_all(&record.frame)?;
        inner.unsynced = true;
        if inner.active_since.is_none() {
            inner.active_since = Some(Instant::now());
        }

        let back = inner
            .segments
            .back_mut()
            .expect("a queue always holds its active segment");
        back.bytes += framed_len;
        inner.stats.queued_bytes += framed_len;
        inner.stats.appended_records += 1;
        inner.stats.appended_bytes += framed_len;
        drop(inner);

        self.appended.notify_waiters();
        Ok(())
    }

    pub fn sync(&self) -> io::Result<()> {
        let mut inner = self.inner.lock();
        if !inner.unsynced {
            return Ok(());
        }
        inner.active.sync()?;
        inner.unsynced = false;
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
    /// A record whose header does not fit, whose length runs past the file, or
    /// whose crc does not match ends the segment: the rest is not reachable
    /// without trusting a length field that has already been shown to be
    /// wrong.
    pub fn read_segment(&self, seq: u64) -> io::Result<SealedSegment> {
        let bytes = {
            let inner = self.inner.lock();
            inner
                .segments
                .iter()
                .find(|meta| meta.seq == seq)
                .map(|meta| meta.bytes)
                .unwrap_or(0)
        };
        let mut file = segment::open_for_read(&self.dir, seq)?;
        let mut sealed = SealedSegment {
            seq,
            frames: Vec::with_capacity(bytes as usize),
            records: 0,
        };
        let mut at = 0u64;
        while at < bytes {
            match read_record(&mut file, bytes - at)? {
                Some(frame) => {
                    at += (RECORD_HEADER_BYTES + frame.len()) as u64;
                    sealed.frames.extend_from_slice(&frame);
                    sealed.records += 1;
                }
                None => {
                    tracing::warn!(
                        segment = seq,
                        at,
                        records = sealed.records,
                        "a segment ends early at a record that does not check out"
                    );
                    break;
                }
            }
        }
        Ok(sealed)
    }

    /// signy has every segment up to and including this one, so they are
    /// unlinked. That is the whole of the bookkeeping: nothing is written down
    /// about how far signy has got, because what is left on disk says it.
    pub fn commit(&self, acked: u64, records: u64) -> io::Result<()> {
        let mut inner = self.inner.lock();
        inner.stats.sent_records += records;
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
        if inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .bytes
            == 0
        {
            return Ok(());
        }
        let next = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .seq
            + 1;
        inner.active.sync()?;
        inner.active = SegmentFile::create(&self.dir, next)?;
        inner.unsynced = false;
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
        let meta = inner
            .segments
            .pop_front()
            .expect("length exceeds one");
        segment::remove(&self.dir, meta.seq)?;
        inner.stats.queued_bytes -= meta.bytes;
        inner.stats.dropped_bytes += meta.bytes;
        inner.stats.dropped_segments += 1;
        Ok(())
    }
}

fn read_record(file: &mut std::fs::File, remaining: u64) -> io::Result<Option<Vec<u8>>> {
    if remaining < RECORD_HEADER_BYTES as u64 {
        return Ok(None);
    }
    let mut header = [0u8; RECORD_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let frame_len = u32::from_le_bytes(header[0..4].try_into().expect("four bytes")) as usize;
    let crc = u32::from_le_bytes(header[4..8].try_into().expect("four bytes"));
    if (RECORD_HEADER_BYTES + frame_len) as u64 > remaining {
        return Ok(None);
    }
    let mut frame = vec![0u8; frame_len];
    file.read_exact(&mut frame)?;
    if crc32fast::hash(&frame) != crc {
        return Ok(None);
    }
    Ok(Some(frame))
}
