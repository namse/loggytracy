mod cursor;
mod segment;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub use cursor::Cursor;
use segment::{SegmentFile, SegmentMeta};

pub const RECORD_HEADER_BYTES: usize = 12;

#[derive(Clone, Copy, Debug)]
pub struct QueueLimits {
    pub max_bytes: u64,
    pub max_segment_bytes: u64,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024 * 1024,
            max_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub queued_bytes: u64,
    pub backlog_bytes: u64,
    pub segments: usize,
    pub appended_records: u64,
    pub appended_bytes: u64,
    pub dropped_bytes: u64,
    pub dropped_segments: u64,
    pub sent_records: u64,
}

pub struct Queue {
    dir: PathBuf,
    limits: QueueLimits,
    inner: Mutex<Inner>,
    appended: Notify,
}

struct Inner {
    segments: VecDeque<SegmentMeta>,
    active: SegmentFile,
    cursor: Cursor,
    unsynced: bool,
    stats: QueueStats,
}

pub struct Record {
    pub frame: Vec<u8>,
    pub plain_len: u32,
}

#[derive(Clone, Debug)]
pub struct BatchRecord {
    pub span: std::ops::Range<usize>,
    pub plain_len: u32,
    pub end: Cursor,
}

pub struct Batch {
    pub frames: Vec<u8>,
    pub records: Vec<BatchRecord>,
    pub plain_bytes: usize,
    pub end: Cursor,
}

impl Batch {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn slice(&self, from: usize, to: usize) -> &[u8] {
        let start = self.records[from].span.start;
        let end = self.records[to - 1].span.end;
        &self.frames[start..end]
    }

    pub fn plain_bytes_of(&self, from: usize, to: usize) -> usize {
        self.records[from..to]
            .iter()
            .map(|record| record.plain_len as usize)
            .sum()
    }
}

impl Queue {
    pub fn open(dir: &Path, limits: QueueLimits) -> io::Result<Queue> {
        std::fs::create_dir_all(dir)?;
        let mut metas = segment::list(dir)?;

        if metas.is_empty() {
            metas.push(SegmentMeta { seq: 0, bytes: 0 });
            SegmentFile::create(dir, 0)?;
        }

        let last = metas.last_mut().expect("just ensured non-empty");
        last.bytes = segment::truncate_torn_tail(dir, last.seq)?;

        let active = SegmentFile::open_for_append(dir, last.seq)?;
        let oldest = metas.first().expect("just ensured non-empty").seq;
        let newest = metas.last().expect("just ensured non-empty");
        let tail = Cursor {
            segment: newest.seq,
            offset: newest.bytes,
        };
        let cursor = cursor::load(dir)?
            .filter(|loaded| {
                loaded.segment >= oldest
                    && *loaded <= tail
                    && metas.iter().any(|meta| meta.seq == loaded.segment)
            })
            .unwrap_or(Cursor {
                segment: oldest,
                offset: 0,
            });

        let queued_bytes = metas.iter().map(|meta| meta.bytes).sum();
        Ok(Queue {
            dir: dir.to_path_buf(),
            limits,
            inner: Mutex::new(Inner {
                segments: metas.into(),
                active,
                cursor,
                unsynced: false,
                stats: QueueStats {
                    queued_bytes,
                    ..QueueStats::default()
                },
            }),
            appended: Notify::new(),
        })
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
        header[4..8].copy_from_slice(&record.plain_len.to_le_bytes());
        header[8..12].copy_from_slice(&crc32fast::hash(&record.frame).to_le_bytes());

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

    pub fn read_batch(
        &self,
        max_plain_bytes: usize,
        max_records: usize,
    ) -> io::Result<Option<Batch>> {
        let (mut position, segments) = {
            let inner = self.inner.lock();
            (inner.cursor, inner.segments.clone())
        };

        let mut batch = Batch {
            frames: Vec::new(),
            records: Vec::new(),
            plain_bytes: 0,
            end: position,
        };
        let mut reader: Option<(u64, std::fs::File)> = None;

        loop {
            let Some(meta) = segments.iter().find(|meta| meta.seq == position.segment) else {
                break;
            };
            if position.offset >= meta.bytes {
                let Some(next) = segments.iter().find(|meta| meta.seq > position.segment) else {
                    break;
                };
                position = Cursor {
                    segment: next.seq,
                    offset: 0,
                };
                batch.end = position;
                reader = None;
                continue;
            }
            if batch.records.len() >= max_records {
                break;
            }

            if reader.as_ref().map(|(seq, _)| *seq) != Some(position.segment) {
                let mut file = segment::open_for_read(&self.dir, position.segment)?;
                file.seek(SeekFrom::Start(position.offset))?;
                reader = Some((position.segment, file));
            }
            let file = &mut reader.as_mut().expect("just ensured a reader").1;

            let remaining = meta.bytes - position.offset;
            match read_record(file, remaining)? {
                Some((frame, plain_len)) => {
                    if !batch.records.is_empty()
                        && batch.plain_bytes + plain_len as usize > max_plain_bytes
                    {
                        break;
                    }
                    let start = batch.frames.len();
                    batch.frames.extend_from_slice(&frame);
                    batch.plain_bytes += plain_len as usize;
                    position.offset += (RECORD_HEADER_BYTES + frame.len()) as u64;
                    batch.end = position;
                    batch.records.push(BatchRecord {
                        span: start..batch.frames.len(),
                        plain_len,
                        end: position,
                    });
                    if batch.plain_bytes >= max_plain_bytes {
                        break;
                    }
                }
                None => {
                    let Some(next) = segments.iter().find(|meta| meta.seq > position.segment)
                    else {
                        break;
                    };
                    position = Cursor {
                        segment: next.seq,
                        offset: 0,
                    };
                    batch.end = position;
                    reader = None;
                }
            }
        }

        if batch.records.is_empty() {
            if batch.end != self.inner.lock().cursor {
                self.commit(batch.end, 0)?;
            }
            return Ok(None);
        }
        Ok(Some(batch))
    }

    pub fn commit(&self, upto: Cursor, records: u64) -> io::Result<()> {
        let mut inner = self.inner.lock();
        if upto < inner.cursor {
            return Ok(());
        }
        inner.cursor = upto;
        inner.stats.sent_records += records;
        while inner.segments.len() > 1 {
            let front = inner.segments.front().expect("length exceeds one").seq;
            if front >= upto.segment {
                break;
            }
            let meta = inner.segments.pop_front().expect("length exceeds one");
            segment::remove(&self.dir, meta.seq)?;
            inner.stats.queued_bytes -= meta.bytes;
        }
        cursor::store(&self.dir, upto)?;
        Ok(())
    }

    pub fn stats(&self) -> QueueStats {
        let inner = self.inner.lock();
        QueueStats {
            segments: inner.segments.len(),
            backlog_bytes: backlog_bytes(&inner),
            ..inner.stats
        }
    }

    pub fn cursor(&self) -> Cursor {
        self.inner.lock().cursor
    }

    pub fn has_records(&self) -> bool {
        let inner = self.inner.lock();
        let cursor = inner.cursor;
        inner.segments.iter().any(|meta| {
            meta.seq > cursor.segment || (meta.seq == cursor.segment && meta.bytes > cursor.offset)
        })
    }

    pub async fn wait_for_records(&self) {
        loop {
            let notified = self.appended.notified();
            if self.has_records() {
                return;
            }
            notified.await;
        }
    }

    fn roll(&self, inner: &mut Inner) -> io::Result<()> {
        let next = inner
            .segments
            .back()
            .expect("a queue always holds its active segment")
            .seq
            + 1;
        inner.active.sync()?;
        inner.active = SegmentFile::create(&self.dir, next)?;
        inner.unsynced = false;
        inner.segments.push_back(SegmentMeta {
            seq: next,
            bytes: 0,
        });
        Ok(())
    }

    fn drop_oldest(&self, inner: &mut Inner) -> io::Result<()> {
        if inner.segments.len() == 1 {
            self.roll(inner)?;
        }
        let meta = inner
            .segments
            .pop_front()
            .expect("roll guarantees a second segment");
        segment::remove(&self.dir, meta.seq)?;
        inner.stats.queued_bytes -= meta.bytes;
        inner.stats.dropped_bytes += meta.bytes;
        inner.stats.dropped_segments += 1;
        if inner.cursor.segment <= meta.seq {
            inner.cursor = Cursor {
                segment: inner
                    .segments
                    .front()
                    .expect("roll guarantees a second segment")
                    .seq,
                offset: 0,
            };
            cursor::store(&self.dir, inner.cursor)?;
        }
        Ok(())
    }
}

fn backlog_bytes(inner: &Inner) -> u64 {
    inner
        .segments
        .iter()
        .map(|meta| match meta.seq.cmp(&inner.cursor.segment) {
            std::cmp::Ordering::Less => 0,
            std::cmp::Ordering::Equal => meta.bytes.saturating_sub(inner.cursor.offset),
            std::cmp::Ordering::Greater => meta.bytes,
        })
        .sum()
}

fn read_record(file: &mut std::fs::File, remaining: u64) -> io::Result<Option<(Vec<u8>, u32)>> {
    if remaining < RECORD_HEADER_BYTES as u64 {
        return Ok(None);
    }
    let mut header = [0u8; RECORD_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let frame_len = u32::from_le_bytes(header[0..4].try_into().expect("four bytes")) as usize;
    let plain_len = u32::from_le_bytes(header[4..8].try_into().expect("four bytes"));
    let crc = u32::from_le_bytes(header[8..12].try_into().expect("four bytes"));
    if (RECORD_HEADER_BYTES + frame_len) as u64 > remaining {
        return Ok(None);
    }
    let mut frame = vec![0u8; frame_len];
    file.read_exact(&mut frame)?;
    if crc32fast::hash(&frame) != crc {
        return Ok(None);
    }
    Ok(Some((frame, plain_len)))
}
