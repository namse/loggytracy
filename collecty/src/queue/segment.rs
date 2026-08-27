use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::RECORD_HEADER_BYTES;

const SEGMENT_SUFFIX: &str = ".seg";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    pub seq: u64,
    pub bytes: u64,
}

pub struct SegmentFile {
    file: File,
}

impl SegmentFile {
    pub fn create(dir: &Path, seq: u64) -> io::Result<SegmentFile> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path(dir, seq))?;
        file.sync_all()?;
        File::open(dir)?.sync_all()?;
        Ok(SegmentFile { file })
    }

    pub fn open_for_append(dir: &Path, seq: u64) -> io::Result<SegmentFile> {
        Ok(SegmentFile {
            file: OpenOptions::new().append(true).open(path(dir, seq))?,
        })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

pub fn open_for_read(dir: &Path, seq: u64) -> io::Result<File> {
    File::open(path(dir, seq))
}

pub fn remove(dir: &Path, seq: u64) -> io::Result<()> {
    std::fs::remove_file(path(dir, seq))
}

pub fn list(dir: &Path) -> io::Result<Vec<SegmentMeta>> {
    let mut metas = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(digits) = name.strip_suffix(SEGMENT_SUFFIX) else {
            continue;
        };
        let Ok(seq) = digits.parse::<u64>() else {
            continue;
        };
        metas.push(SegmentMeta {
            seq,
            bytes: entry.metadata()?.len(),
        });
    }
    metas.sort_by_key(|meta| meta.seq);
    Ok(metas)
}

pub fn truncate_torn_tail(dir: &Path, seq: u64) -> io::Result<u64> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path(dir, seq))?;
    let len = file.metadata()?.len();
    let mut valid = 0u64;

    loop {
        let remaining = len - valid;
        if remaining < RECORD_HEADER_BYTES as u64 {
            break;
        }
        let mut header = [0u8; RECORD_HEADER_BYTES];
        file.seek(SeekFrom::Start(valid))?;
        file.read_exact(&mut header)?;
        let frame_len = u32::from_le_bytes(header[0..4].try_into().expect("four bytes")) as usize;
        let crc = u32::from_le_bytes(header[4..8].try_into().expect("four bytes"));
        if (RECORD_HEADER_BYTES + frame_len) as u64 > remaining {
            break;
        }
        let mut frame = vec![0u8; frame_len];
        file.read_exact(&mut frame)?;
        if crc32fast::hash(&frame) != crc {
            break;
        }
        valid += (RECORD_HEADER_BYTES + frame_len) as u64;
    }

    if valid < len {
        file.set_len(valid)?;
        file.sync_all()?;
    }
    Ok(valid)
}

fn path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}{SEGMENT_SUFFIX}"))
}
