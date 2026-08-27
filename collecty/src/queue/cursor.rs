use std::io::{self, Read, Write};
use std::path::Path;

const CURSOR_FILE: &str = "cursor";
const SENDER_ID_BYTES: usize = 16;
const CURSOR_BYTES: usize = 8 + 8 + 8 + SENDER_ID_BYTES + 4;

/// How far the sender has got, and how many records it has handed out numbers
/// to.
///
/// The segment and offset locate the next unsent byte. The sequence counts
/// records rather than bytes: signy remembers a number per sender and skips
/// anything at or below it, so the number has to survive a restart and never
/// go backwards. It is assigned when a record is read for sending, not when it
/// is appended, which is why a segment dropped under a full queue leaves no
/// gap — those records never got a number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub segment: u64,
    pub offset: u64,
    pub sequence: u64,
}

/// Which collecty this queue belongs to.
///
/// Random, and made when the queue directory is, so a lost volume produces a
/// new one. An operator-set name would survive the volume it names: the queue
/// would restart its numbering while signy still remembered the old high-water
/// mark, and everything under it would be dropped as already-seen. Sharing a
/// file with the cursor is what keeps the two from being recovered apart.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SenderId([u8; SENDER_ID_BYTES]);

impl SenderId {
    pub fn generate() -> io::Result<SenderId> {
        let mut bytes = [0u8; SENDER_ID_BYTES];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        Ok(SenderId(bytes))
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

/// What the cursor file holds: the two things that have to be recovered
/// together or not at all.
#[derive(Clone, Copy, Debug)]
pub struct Committed {
    pub sender: SenderId,
    pub cursor: Cursor,
}

pub fn load(dir: &Path) -> io::Result<Option<Committed>> {
    let mut file = match std::fs::File::open(dir.join(CURSOR_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::with_capacity(CURSOR_BYTES);
    file.read_to_end(&mut bytes)?;
    if bytes.len() != CURSOR_BYTES {
        return Ok(None);
    }
    let body = CURSOR_BYTES - 4;
    let stored = u32::from_le_bytes(bytes[body..].try_into().expect("four bytes"));
    if crc32fast::hash(&bytes[0..body]) != stored {
        return Ok(None);
    }
    let mut id = [0u8; SENDER_ID_BYTES];
    id.copy_from_slice(&bytes[24..body]);
    Ok(Some(Committed {
        sender: SenderId(id),
        cursor: Cursor {
            segment: u64::from_le_bytes(bytes[0..8].try_into().expect("eight bytes")),
            offset: u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes")),
            sequence: u64::from_le_bytes(bytes[16..24].try_into().expect("eight bytes")),
        },
    }))
}

pub fn store(dir: &Path, sender: SenderId, cursor: Cursor) -> io::Result<()> {
    let mut bytes = [0u8; CURSOR_BYTES];
    bytes[0..8].copy_from_slice(&cursor.segment.to_le_bytes());
    bytes[8..16].copy_from_slice(&cursor.offset.to_le_bytes());
    bytes[16..24].copy_from_slice(&cursor.sequence.to_le_bytes());
    let body = CURSOR_BYTES - 4;
    bytes[24..body].copy_from_slice(&sender.0);
    let crc = crc32fast::hash(&bytes[0..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(dir.join(CURSOR_FILE))?;
    file.write_all(&bytes)
}
