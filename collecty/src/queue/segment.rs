use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_SUFFIX: &str = ".seg";
const TEMPORARY_SUFFIX: &str = ".tmp";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    pub seq: u64,
    pub bytes: u64,
}

/// The open segment: one zstd stream, written until the segment closes.
///
/// Records go in as plain bytes and the encoder decides when to emit a block,
/// so what the file holds trails what has been accepted. Nothing forces that
/// gap shut before the segment closes — `finish` is the only durability point
/// the queue has, and it is the same moment the segment becomes sendable.
pub struct SegmentWriter {
    encoder: Option<zstd::stream::write::Encoder<'static, Counted>>,
}

/// The segment file, counting what the encoder has actually handed it.
///
/// This is the segment's size for every purpose the queue has: what the disk
/// holds and what the wire will carry are the same bytes.
struct Counted {
    file: File,
    written: u64,
}

impl Write for Counted {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let wrote = self.file.write(buf)?;
        self.written += wrote as u64;
        Ok(wrote)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl SegmentWriter {
    pub fn create(dir: &Path, seq: u64, level: i32) -> io::Result<SegmentWriter> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path(dir, seq))?;
        file.sync_all()?;
        sync_dir(dir)?;
        Ok(SegmentWriter {
            encoder: Some(encoder(file, level)?),
        })
    }

    pub fn write_all(&mut self, plain: &[u8]) -> io::Result<()> {
        self.encoder.as_mut().ok_or_else(closed)?.write_all(plain)
    }

    /// What the file holds. Behind what has been accepted by whatever the
    /// encoder is still holding, and caught up by `finish`.
    pub fn written(&self) -> u64 {
        self.encoder
            .as_ref()
            .map(|encoder| encoder.get_ref().written)
            .unwrap_or(0)
    }

    /// Close the stream and force it to the device.
    pub fn finish(&mut self) -> io::Result<u64> {
        let counted = self.encoder.take().ok_or_else(closed)?.finish()?;
        counted.file.sync_all()?;
        Ok(counted.written)
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

/// A rewrite that a crash interrupted. The segment it was repairing is still
/// there under its own name, so this is only a file to unlink.
pub fn sweep_temporaries(dir: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|name| name.ends_with(TEMPORARY_SUFFIX))
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Close a segment that a previous process left open.
///
/// The encoder's state died with that process, so there is no appending to an
/// unfinished stream — the segment is closed where it stopped instead. What
/// decompresses is kept, cut back to the last record that arrived whole, and
/// written out again as a stream that ends properly. A segment ending mid
/// record is one signy refuses whole, which is why the cut is not left to it.
///
/// Returns the segment's size, or `None` if nothing in it survived and the
/// file is gone.
pub fn reseal(dir: &Path, seq: u64, level: i32) -> io::Result<Option<u64>> {
    let bytes = std::fs::metadata(path(dir, seq))?.len();
    if bytes == 0 {
        remove(dir, seq)?;
        sync_dir(dir)?;
        return Ok(None);
    }

    // An error here is the ordinary case, not a surprise: an unfinished stream
    // reads as far as its last complete block and then says so. What it read
    // before that is what a crash left behind.
    let mut plain = Vec::new();
    let whole = zstd::stream::read::Decoder::new(open_for_read(dir, seq)?)
        .and_then(|mut decoder| decoder.read_to_end(&mut plain))
        .is_ok();
    let kept = crate::wire::whole_records_len(&plain);

    if whole && kept == plain.len() {
        return Ok(Some(bytes));
    }
    tracing::warn!(
        segment = seq,
        bytes,
        recovered = kept,
        dropped = plain.len() - kept,
        "closing a segment a crash left open"
    );
    plain.truncate(kept);
    if plain.is_empty() {
        remove(dir, seq)?;
        sync_dir(dir)?;
        return Ok(None);
    }
    rewrite(dir, seq, &plain, level).map(Some)
}

fn rewrite(dir: &Path, seq: u64, plain: &[u8], level: i32) -> io::Result<u64> {
    let temporary = path(dir, seq).with_extension("tmp");
    let mut encoder = encoder(File::create(&temporary)?, level)?;
    encoder.write_all(plain)?;
    let counted = encoder.finish()?;
    counted.file.sync_all()?;
    std::fs::rename(&temporary, path(dir, seq))?;
    sync_dir(dir)?;
    Ok(counted.written)
}

fn encoder(file: File, level: i32) -> io::Result<zstd::stream::write::Encoder<'static, Counted>> {
    zstd::stream::write::Encoder::new(Counted { file, written: 0 }, level)
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "the segment is already closed")
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}{SEGMENT_SUFFIX}"))
}
