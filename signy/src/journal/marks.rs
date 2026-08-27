use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MARKS_FILE: &str = "journal.marks";
const SENDER_ID_BYTES: usize = 16;
const MARK_ENTRY_BYTES: usize = SENDER_ID_BYTES + 8 + 8;

/// How long a collecty may stay silent before its number is forgotten.
///
/// Forgetting one that is still alive costs duplicates, not loss: it comes
/// back as a sender this instance has never heard of, and everything it sends
/// is stored. Keeping every id that ever connected costs a file that only
/// grows, and a collecty whose disk is replaced leaves its old id behind every
/// time.
const FORGET_AFTER: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Which collecty a batch came from.
///
/// Random and made with the collector's queue directory, so it names the queue
/// rather than the host: a replaced volume arrives under a new id and its
/// numbering starts again without colliding with what is remembered here.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SenderId([u8; SENDER_ID_BYTES]);

impl SenderId {
    pub fn parse(hex: &str) -> Option<SenderId> {
        if hex.len() != SENDER_ID_BYTES * 2 {
            return None;
        }
        let mut bytes = [0u8; SENDER_ID_BYTES];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(hex.get(index * 2..index * 2 + 2)?, 16).ok()?;
        }
        Some(SenderId(bytes))
    }
}

impl std::fmt::Display for SenderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for SenderId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self}")
    }
}

/// One batch's claim: this sender's records are durable up to this number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CollectMark {
    pub sender: SenderId,
    pub sequence: u64,
}

/// How far each collecty has got, as far as this instance is concerned.
///
/// A collecty numbers the records it sends and starts again from its own
/// cursor after a crash, so a resend overlaps whatever was already stored.
/// The number here is the highest that is durable; a record at or below it has
/// been stored once and is skipped rather than stored twice.
///
/// The mark is written by the journal writer **in the same batch as the
/// records it covers**, so one `sync_all` makes both durable together. There
/// is no window in which a record is on disk and the mark that would skip its
/// twin is not.
#[derive(Default)]
pub struct CollectMarks {
    inner: std::sync::RwLock<HashMap<SenderId, Seen>>,
}

#[derive(Clone, Copy)]
struct Seen {
    sequence: u64,
    at: SystemTime,
}

impl CollectMarks {
    /// What is durable for this sender. Zero for one never heard from, which
    /// is below every record's number, so nothing is skipped.
    pub fn sequence(&self, sender: &SenderId) -> u64 {
        self.inner
            .read()
            .expect("collect marks lock")
            .get(sender)
            .map(|seen| seen.sequence)
            .unwrap_or(0)
    }

    /// Only ever forward. A batch that arrives out of order, or a resend of
    /// one already covered, must not walk the mark backwards over records that
    /// would then be sent again.
    pub fn advance(&self, mark: CollectMark) {
        let mut inner = self.inner.write().expect("collect marks lock");
        let seen = inner.entry(mark.sender).or_insert(Seen {
            sequence: 0,
            at: UNIX_EPOCH,
        });
        seen.sequence = seen.sequence.max(mark.sequence);
        seen.at = SystemTime::now();
    }

    pub fn senders(&self) -> usize {
        self.inner.read().expect("collect marks lock").len()
    }

    /// Read what the last checkpoint knew.
    ///
    /// Anything unreadable is nothing: the file only ever makes the marks more
    /// complete than the WAL suffix alone, so losing it costs duplicates after
    /// a restart rather than correctness.
    pub fn load(dir: &std::path::Path) -> CollectMarks {
        let marks = CollectMarks::default();
        let Ok(bytes) = std::fs::read(dir.join(MARKS_FILE)) else {
            return marks;
        };
        if bytes.len() < 8 {
            return marks;
        }
        let body = bytes.len() - 4;
        let stored = u32::from_le_bytes(bytes[body..].try_into().expect("four bytes"));
        if crc32fast::hash(&bytes[..body]) != stored {
            tracing::warn!("journal marks failed their checksum; starting from the WAL alone");
            return marks;
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().expect("four bytes")) as usize;
        if body != 4 + count * MARK_ENTRY_BYTES {
            return marks;
        }
        let mut inner = marks.inner.write().expect("collect marks lock");
        for index in 0..count {
            let at = 4 + index * MARK_ENTRY_BYTES;
            let mut id = [0u8; SENDER_ID_BYTES];
            id.copy_from_slice(&bytes[at..at + SENDER_ID_BYTES]);
            let sequence = u64::from_le_bytes(
                bytes[at + SENDER_ID_BYTES..at + SENDER_ID_BYTES + 8]
                    .try_into()
                    .expect("eight bytes"),
            );
            let seconds = u64::from_le_bytes(
                bytes[at + SENDER_ID_BYTES + 8..at + MARK_ENTRY_BYTES]
                    .try_into()
                    .expect("eight bytes"),
            );
            inner.insert(
                SenderId(id),
                Seen {
                    sequence,
                    at: UNIX_EPOCH + Duration::from_secs(seconds),
                },
            );
        }
        drop(inner);
        marks
    }

    /// Write the marks out, forgetting whoever has been silent too long.
    ///
    /// Written **before** the checkpoint offset that retires the WAL prefix
    /// these marks came from. The two are separate files and a crash can land
    /// between them, and this is the harmless order: marks ahead of the
    /// checkpoint name records the WAL still holds and replay will put back,
    /// while marks behind it would have those records sent again.
    pub fn store(&self, dir: &std::path::Path) -> Result<(), std::io::Error> {
        let now = SystemTime::now();
        let live: Vec<(SenderId, Seen)> = {
            let mut inner = self.inner.write().expect("collect marks lock");
            inner.retain(|_, seen| {
                now.duration_since(seen.at)
                    .map(|silent| silent < FORGET_AFTER)
                    .unwrap_or(true)
            });
            inner.iter().map(|(id, seen)| (*id, *seen)).collect()
        };

        let mut bytes = Vec::with_capacity(8 + live.len() * MARK_ENTRY_BYTES);
        bytes.extend_from_slice(&(live.len() as u32).to_le_bytes());
        for (id, seen) in &live {
            bytes.extend_from_slice(&id.0);
            bytes.extend_from_slice(&seen.sequence.to_le_bytes());
            let seconds = seen
                .at
                .duration_since(UNIX_EPOCH)
                .map(|since| since.as_secs())
                .unwrap_or(0);
            bytes.extend_from_slice(&seconds.to_le_bytes());
        }
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());

        let path = dir.join(MARKS_FILE);
        let tmp = path.with_extension("marks.tmp");
        std::fs::write(&tmp, &bytes)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }
}

/// `LGYM | sender | sequence`, a WAL record of its own rather than a field on
/// the records it covers.
///
/// One per sender per batch, not one per record: the writer already groups
/// what is in its channel into a single write and a single `sync_all`, and
/// the mark rides in that group. Putting the number on every record instead
/// would have paid for it once per export to say something the batch says
/// once.
const MARK_RECORD_MAGIC: &[u8; 4] = b"LGYM";
pub const MARK_RECORD_BYTES: usize = 4 + SENDER_ID_BYTES + 8;

pub fn frame_mark(mark: &CollectMark, into: &mut Vec<u8>) {
    into.extend_from_slice(MARK_RECORD_MAGIC);
    into.extend_from_slice(&mark.sender.0);
    into.extend_from_slice(&mark.sequence.to_le_bytes());
}

pub fn decode_mark(data: &[u8]) -> Option<CollectMark> {
    if data.len() != MARK_RECORD_BYTES || !data.starts_with(MARK_RECORD_MAGIC) {
        return None;
    }
    let mut id = [0u8; SENDER_ID_BYTES];
    id.copy_from_slice(&data[4..4 + SENDER_ID_BYTES]);
    Some(CollectMark {
        sender: SenderId(id),
        sequence: u64::from_le_bytes(
            data[4 + SENDER_ID_BYTES..]
                .try_into()
                .expect("eight bytes"),
        ),
    })
}
