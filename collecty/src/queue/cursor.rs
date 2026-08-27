use std::io::{self, Read, Write};
use std::path::Path;

const CURSOR_FILE: &str = "cursor";
const SENDER_ID_BYTES: usize = 16;
const CURSOR_BYTES: usize = 8 + SENDER_ID_BYTES + 4;

/// Which collecty this queue belongs to.
///
/// Random, and made when the queue directory is, so a lost volume produces a
/// new one. An operator-set name would survive the volume it names: the queue
/// would restart its segment numbering while signy still remembered the old
/// high-water mark, and everything under it would be dropped as already-seen.
/// Sharing a file with the acknowledged segment is what keeps the two from
/// being recovered apart.
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

/// What the cursor file holds: the name signy knows this queue by, and the
/// last segment signy said it had whole.
///
/// One number, because one segment is what a request carries. There is no
/// byte offset and no record count: a segment is sent from its first record
/// every time, so nothing has to be remembered about how far into one an
/// earlier attempt reached — signy remembers that, and skips what it already
/// stored.
#[derive(Clone, Copy, Debug)]
pub struct Committed {
    pub sender: SenderId,
    /// Segments are numbered from one, so zero means signy has none of them.
    pub acked: u64,
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
    id.copy_from_slice(&bytes[8..body]);
    Ok(Some(Committed {
        sender: SenderId(id),
        acked: u64::from_le_bytes(bytes[0..8].try_into().expect("eight bytes")),
    }))
}

pub fn store(dir: &Path, sender: SenderId, acked: u64) -> io::Result<()> {
    let mut bytes = [0u8; CURSOR_BYTES];
    bytes[0..8].copy_from_slice(&acked.to_le_bytes());
    let body = CURSOR_BYTES - 4;
    bytes[8..body].copy_from_slice(&sender.0);
    let crc = crc32fast::hash(&bytes[0..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(dir.join(CURSOR_FILE))?;
    file.write_all(&bytes)
}
