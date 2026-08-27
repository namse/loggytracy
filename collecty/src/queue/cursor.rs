use std::io::{self, Read, Write};
use std::path::Path;

const CURSOR_FILE: &str = "cursor";
const CURSOR_BYTES: usize = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cursor {
    pub segment: u64,
    pub offset: u64,
}

pub fn load(dir: &Path) -> io::Result<Option<Cursor>> {
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
    let stored = u32::from_le_bytes(bytes[16..20].try_into().expect("four bytes"));
    if crc32fast::hash(&bytes[0..16]) != stored {
        return Ok(None);
    }
    Ok(Some(Cursor {
        segment: u64::from_le_bytes(bytes[0..8].try_into().expect("eight bytes")),
        offset: u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes")),
    }))
}

pub fn store(dir: &Path, cursor: Cursor) -> io::Result<()> {
    let mut bytes = [0u8; CURSOR_BYTES];
    bytes[0..8].copy_from_slice(&cursor.segment.to_le_bytes());
    bytes[8..16].copy_from_slice(&cursor.offset.to_le_bytes());
    let crc = crc32fast::hash(&bytes[0..16]);
    bytes[16..20].copy_from_slice(&crc.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(dir.join(CURSOR_FILE))?;
    file.write_all(&bytes)
}
