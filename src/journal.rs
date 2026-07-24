use std::collections::HashMap;
use std::io::Error as IoError;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use prost::Message;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::memtable::{Labels, LogEntry, MemTable};
use crate::proto::{self, PushRequest};

const RECORD_HEADER_SIZE: usize = 8;
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const WAL_FILE: &str = "journal.wal";
const CKPT_FILE: &str = "journal.ckpt";
const COMPACTION_STATE_FILE: &str = "journal.wal.compact.state";
const COMPACTION_STATE_VERSION: u8 = 1;

#[cfg(test)]
static FAIL_AFTER_COMPACTION_RENAME: AtomicBool = AtomicBool::new(false);

pub struct CheckpointSnapshot {
    pub offset: u64,
    pub snapshot: HashMap<Labels, Vec<LogEntry>>,
}

enum JournalCmd {
    Append {
        data: Vec<u8>,
        streams: Vec<(Labels, Vec<LogEntry>)>,
        done: oneshot::Sender<Result<(), IoError>>,
    },
    Checkpoint {
        done: oneshot::Sender<Result<CheckpointSnapshot, IoError>>,
    },
    Compact {
        offset: u64,
        done: oneshot::Sender<Result<(), IoError>>,
    },
}

type AppendBatchItem = (
    Vec<u8>,
    Vec<(Labels, Vec<LogEntry>)>,
    oneshot::Sender<Result<(), IoError>>,
);

pub struct Journal {
    tx: mpsc::Sender<JournalCmd>,
    wal_path: PathBuf,
    ckpt_path: PathBuf,
    healthy: Arc<AtomicBool>,
}

impl Journal {
    pub fn spawn(config: &Config, memtable: Arc<MemTable>) -> Result<Self, IoError> {
        let dir = &config.data_dir;
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);

        // Initialize the WAL synchronously so startup/readiness cannot race a
        // failed open in the background writer. Sync both the empty file and
        // its parent directory: fsyncing a newly-created file alone does not
        // make its directory entry crash-durable on POSIX filesystems.
        let wal_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        wal_file.sync_all()?;
        std::fs::File::open(dir)?.sync_all()?;
        drop(wal_file);

        let (tx, rx) = mpsc::channel::<JournalCmd>(4096);

        let max_batch_bytes = config.max_batch_bytes;
        let max_batch_ms = config.max_batch_ms;
        let healthy = Arc::new(AtomicBool::new(true));

        let wal_path_clone = wal_path.clone();
        let ckpt_path_clone = ckpt_path.clone();
        let writer_health = healthy.clone();
        tokio::spawn(async move {
            let result = writer_loop(
                rx,
                &wal_path_clone,
                &ckpt_path_clone,
                memtable,
                max_batch_bytes,
                max_batch_ms,
            )
            .await;
            writer_health.store(false, Ordering::Release);
            if let Err(e) = result {
                tracing::error!(error = %e, "journal writer terminated");
            }
        });

        Ok(Self {
            tx,
            wal_path,
            ckpt_path,
            healthy,
        })
    }

    pub async fn append(
        &self,
        data: Vec<u8>,
        streams: Vec<(Labels, Vec<LogEntry>)>,
    ) -> Result<(), IoError> {
        if data.len() > MAX_RECORD_BYTES {
            return Err(IoError::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "journal record is too large: {} bytes (maximum {})",
                    data.len(),
                    MAX_RECORD_BYTES
                ),
            ));
        }
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Append {
                data,
                streams,
                done: done_tx,
            })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped",
            )),
        }
    }

    pub async fn checkpoint(&self) -> Result<CheckpointSnapshot, IoError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Checkpoint { done: done_tx })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped",
            )),
        }
    }

    pub fn set_checkpoint(&self, offset: u64) -> Result<(), IoError> {
        write_checkpoint(&self.ckpt_path, offset)
    }

    /// Drops the durable WAL prefix through `offset`. This command runs in
    /// the writer task, so appends that arrived after the flush snapshot are
    /// copied into the replacement WAL before new appends can proceed.
    pub async fn compact_checkpoint(&self, offset: u64) -> Result<(), IoError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Compact {
                offset,
                done: done_tx,
            })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        done_rx.await.map_err(|_| {
            IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped during compaction",
            )
        })?
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn ckpt_path(&self) -> &Path {
        &self.ckpt_path
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
}

pub fn read_checkpoint(ckpt_path: &Path) -> Result<u64, IoError> {
    match std::fs::read(ckpt_path) {
        Ok(bytes) => {
            if bytes.len() != 8 {
                return Err(IoError::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "journal checkpoint must be exactly 8 bytes, got {}",
                        bytes.len()
                    ),
                ));
            }
            Ok(u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e),
    }
}

pub fn write_checkpoint(ckpt_path: &Path, offset: u64) -> Result<(), IoError> {
    let tmp = ckpt_path.with_extension("ckpt.tmp");
    std::fs::write(&tmp, offset.to_le_bytes())?;
    let tmp_file = std::fs::File::open(&tmp)?;
    tmp_file.sync_all()?;
    std::fs::rename(&tmp, ckpt_path)?;
    if let Some(parent) = ckpt_path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

pub fn replay(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
) -> Result<(u64, u64), String> {
    recover_unfinished_compaction(wal_path, ckpt_path).map_err(|e| e.to_string())?;
    let checkpoint = read_checkpoint(ckpt_path).map_err(|e| e.to_string())?;
    replay_from(wal_path, checkpoint, memtable).map(|end| (checkpoint, end))
}

fn recover_unfinished_compaction(wal_path: &Path, ckpt_path: &Path) -> Result<(), IoError> {
    let state_path = wal_path.with_file_name(COMPACTION_STATE_FILE);
    let Some(state) = read_compaction_state(&state_path)? else {
        return Ok(());
    };
    if state.phase != 1 {
        return Ok(());
    }
    let tmp_path = wal_path.with_extension("wal.compact.tmp");
    if !tmp_path.exists() {
        // The replacement WAL is already in place; replay its suffix from
        // checkpoint zero.
        return Ok(());
    }

    // Rename never committed. Restore the old checkpoint before replay so a
    // crash between checkpoint=0 and rename cannot replay flushed records.
    write_checkpoint(ckpt_path, state.offset)?;
    std::fs::remove_file(&tmp_path)?;
    std::fs::remove_file(&state_path)?;
    if let Some(parent) = wal_path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn replay_from(wal_path: &Path, checkpoint: u64, memtable: &MemTable) -> Result<u64, String> {
    if !wal_path.exists() {
        if checkpoint == 0 {
            return Ok(0);
        }
        return Err(format!(
            "journal checkpoint {checkpoint} exists but WAL {} is missing",
            wal_path.display()
        ));
    }
    let mut file = std::fs::File::open(wal_path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    if checkpoint > file_len {
        return Err(format!(
            "journal checkpoint {checkpoint} is beyond WAL length {file_len}"
        ));
    }
    if checkpoint == file_len {
        return Ok(checkpoint);
    }
    file.seek(SeekFrom::Start(checkpoint))
        .map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(file);
    let mut offset = checkpoint;
    let mut replayed = 0u64;
    loop {
        let mut header = [0u8; RECORD_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.to_string()),
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let record_end = offset
            .checked_add(RECORD_HEADER_SIZE as u64)
            .and_then(|end| end.checked_add(len as u64))
            .ok_or_else(|| format!("journal record length overflows at offset {offset}"))?;
        if record_end > file_len {
            tracing::warn!(
                offset,
                len,
                "journal partial record at tail, stopping replay"
            );
            break;
        }
        if len > MAX_RECORD_BYTES {
            return Err(format!(
                "journal record at offset {offset} is too large: {len} bytes (maximum {MAX_RECORD_BYTES})"
            ));
        }
        let mut data = vec![0u8; len];
        match reader.read_exact(&mut data) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::warn!(offset, "journal partial record at tail, stopping replay");
                break;
            }
            Err(e) => return Err(e.to_string()),
        }
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != expected_crc {
            if record_end == file_len {
                tracing::warn!(offset, "journal crc mismatch at tail, stopping replay");
                break;
            }
            return Err(format!("journal record crc mismatch at offset {offset}"));
        }
        match PushRequest::decode(data.as_slice()) {
            Ok(req) => {
                for stream in &req.streams {
                    let labels = match proto::parse_labels(&stream.labels) {
                        Ok(l) => l,
                        Err(e) => {
                            return Err(format!(
                                "journal record has invalid labels at offset {offset}: {e}"
                            ));
                        }
                    };
                    let entries: Vec<LogEntry> = stream
                        .entries
                        .iter()
                        .map(|e| {
                            let timestamp_ns = e.timestamp_ns().map_err(|error| {
                                format!(
                                    "journal record has invalid timestamp at offset {offset}: {error}"
                                )
                            })?;
                            Ok(LogEntry {
                                timestamp_ns,
                                line: e.line.clone(),
                                structured_metadata: e
                                    .structured_metadata
                                    .iter()
                                    .map(|lp| (lp.name.clone(), lp.value.clone()))
                                    .collect(),
                            })
                        })
                        .collect::<Result<_, String>>()?;
                    memtable.insert(labels, entries);
                }
            }
            Err(e) => {
                return Err(format!(
                    "journal protobuf decode failed at offset {offset}: {e}"
                ));
            }
        }
        offset += (RECORD_HEADER_SIZE + len) as u64;
        replayed += 1;
    }
    if replayed > 0 {
        tracing::info!(replayed, offset, "journal replay complete");
    }
    Ok(offset)
}

async fn writer_loop(
    mut rx: mpsc::Receiver<JournalCmd>,
    path: &Path,
    ckpt_path: &Path,
    memtable: Arc<MemTable>,
    max_batch_bytes: usize,
    max_batch_ms: u64,
) -> Result<(), IoError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let mut good_len = file.metadata().await?.len();

    loop {
        let first = match rx.recv().await {
            Some(c) => c,
            None => break,
        };

        let mut pending_checkpoint: Option<oneshot::Sender<Result<CheckpointSnapshot, IoError>>> =
            None;
        let mut pending_compact: Option<(u64, oneshot::Sender<Result<(), IoError>>)> = None;

        let mut batch: Vec<AppendBatchItem> = Vec::new();
        let mut batch_bytes = 0usize;
        let mut closed = false;

        match first {
            JournalCmd::Append {
                data,
                streams,
                done,
            } => {
                batch_bytes += data.len();
                batch.push((data, streams, done));
                let deadline = tokio::time::Instant::now() + Duration::from_millis(max_batch_ms);
                while batch_bytes < max_batch_bytes {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(JournalCmd::Append {
                            data,
                            streams,
                            done,
                        })) => {
                            batch_bytes += data.len();
                            batch.push((data, streams, done));
                        }
                        Ok(Some(JournalCmd::Checkpoint { done })) => {
                            pending_checkpoint = Some(done);
                            break;
                        }
                        Ok(Some(JournalCmd::Compact { offset, done })) => {
                            pending_compact = Some((offset, done));
                            break;
                        }
                        Ok(None) => {
                            closed = true;
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            JournalCmd::Checkpoint { done } => {
                pending_checkpoint = Some(done);
            }
            JournalCmd::Compact { offset, done } => {
                pending_compact = Some((offset, done));
            }
        }

        if !batch.is_empty() {
            let mut buf = Vec::with_capacity(batch_bytes + batch.len() * RECORD_HEADER_SIZE);
            for (data, _, _) in &batch {
                let len = u32::try_from(data.len()).map_err(|_| {
                    IoError::new(
                        std::io::ErrorKind::InvalidInput,
                        "journal record exceeds u32",
                    )
                })?;
                let crc = crc32fast::hash(data);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&crc.to_le_bytes());
                buf.extend_from_slice(data);
            }

            let write_result = async {
                file.write_all(&buf).await?;
                file.flush().await?;
                file.sync_all().await
            }
            .await;

            match write_result {
                Ok(()) => {
                    good_len += buf.len() as u64;
                    for (_, streams, done) in batch.drain(..) {
                        for (labels, entries) in streams {
                            memtable.insert(labels, entries);
                        }
                        let _ = done.send(Ok(()));
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "journal write failed, truncating partial record");
                    for (_, _, done) in batch.drain(..) {
                        let _ = done.send(Err(IoError::new(e.kind(), e.to_string())));
                    }
                    let recovered = async {
                        file.set_len(good_len).await?;
                        file.sync_all().await
                    }
                    .await;
                    if let Err(te) = recovered {
                        tracing::error!(error = %te, "journal truncate failed, fencing writer");
                        return Err(te);
                    }
                }
            }
        }

        if let Some(done) = pending_checkpoint {
            if let Err(e) = file.sync_all().await {
                let _ = done.send(Err(IoError::new(e.kind(), e.to_string())));
                return Err(e);
            }
            let offset = good_len;
            let snapshot = memtable.begin_flush();
            let _ = done.send(Ok(CheckpointSnapshot { offset, snapshot }));
        }

        if let Some((offset, done)) = pending_compact {
            let result = compact_wal(&mut file, path, ckpt_path, offset, &mut good_len).await;
            match result {
                Ok(()) => {
                    let _ = done.send(Ok(()));
                }
                Err(error) => {
                    let error_for_caller = IoError::new(error.kind(), error.to_string());
                    let _ = done.send(Err(error_for_caller));
                    // Compaction can fail before or after replacing the WAL
                    // (for example, a directory fsync can fail after rename).
                    // Reopen the path and continue serving appends so the
                    // caller can retry the same checkpoint instead of
                    // permanently fencing the journal writer.
                    let reopened = async {
                        let reopened = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(path)
                            .await?;
                        let length = reopened.metadata().await?.len();
                        Ok::<_, IoError>((reopened, length))
                    }
                    .await;
                    match reopened {
                        Ok((reopened, length)) => {
                            file = reopened;
                            good_len = length;
                        }
                        Err(reopen_error) => {
                            tracing::error!(
                                error = %reopen_error,
                                "journal reopen after compaction failure failed; fencing writer"
                            );
                            return Err(reopen_error);
                        }
                    }
                }
            }
        }

        if closed {
            break;
        }
    }

    Ok(())
}

async fn compact_wal(
    file: &mut tokio::fs::File,
    wal_path: &Path,
    ckpt_path: &Path,
    offset: u64,
    good_len: &mut u64,
) -> Result<(), IoError> {
    let state_path = wal_path.with_file_name(COMPACTION_STATE_FILE);
    let compaction_state = read_compaction_state(&state_path)?;
    if let Some(state) = compaction_state {
        if state.phase == 2 {
            if offset == state.offset {
                *file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(wal_path)
                    .await?;
                *good_len = file.metadata().await?.len();
                return Ok(());
            }
            if offset < state.offset {
                return Err(IoError::new(
                    std::io::ErrorKind::InvalidInput,
                    "WAL compaction checkpoint moved backwards",
                ));
            }
        } else if state.phase == 1 {
            let wal_len = std::fs::metadata(wal_path)?.len();
            let tmp_path = wal_path.with_extension("wal.compact.tmp");
            if tmp_path.exists() {
                // Rename did not happen. The checkpoint may already be zero,
                // so restore the old WAL/checkpoint pair before retrying. New
                // appends are included in the next source length.
                if wal_len < state.source_len {
                    return Err(IoError::new(
                        std::io::ErrorKind::InvalidData,
                        "source WAL is shorter than its recorded compaction length",
                    ));
                }
                write_checkpoint(ckpt_path, state.offset)?;
                std::fs::remove_file(&tmp_path)?;
                std::fs::remove_file(&state_path)?;
                if let Some(parent) = wal_path.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
                *good_len = wal_len;
            } else {
                // Rename happened; only the directory durability step (or a
                // later state update) failed. Never apply the old offset to
                // this replacement WAL a second time.
                if wal_len < state.retained_len {
                    return Err(IoError::new(
                        std::io::ErrorKind::InvalidData,
                        "replacement WAL is shorter than its recorded suffix",
                    ));
                }
                sync_wal_parent(wal_path)?;
                write_compaction_state(&state_path, &state, 2)?;
                *file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(wal_path)
                    .await?;
                *good_len = file.metadata().await?.len();
                return Ok(());
            }
        }
    }
    if offset > *good_len {
        return Err(IoError::new(
            std::io::ErrorKind::InvalidInput,
            format!("WAL compaction offset {offset} exceeds durable length {good_len}"),
        ));
    }
    file.sync_all().await?;
    let retained_len = *good_len - offset;
    let tmp_path = wal_path.with_extension("wal.compact.tmp");
    let mut source = OpenOptions::new().read(true).open(wal_path).await?;
    source.seek(SeekFrom::Start(offset)).await?;
    let mut tmp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .await?;
    let copied = tokio::io::copy(&mut source.take(retained_len), &mut tmp).await?;
    if copied != retained_len {
        return Err(IoError::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("copied {copied} of {retained_len} WAL bytes during compaction"),
        ));
    }
    tmp.flush().await?;
    tmp.sync_all().await?;
    drop(tmp);

    // This durable intent record makes compaction idempotent across the
    // rename/fsync boundary. A retry observes it and keeps the current WAL
    // intact instead of interpreting the suffix length as the old offset.
    let state = CompactionState {
        phase: 1,
        offset,
        source_len: *good_len,
        retained_len,
    };
    write_compaction_state(&state_path, &state, 1)?;

    // Resetting the checkpoint before replacing the WAL makes every crash
    // point at-least-once safe: a crash before rename replays the old WAL,
    // while a crash after rename replays only the retained suffix.
    write_checkpoint(ckpt_path, 0)?;
    tokio::fs::rename(&tmp_path, wal_path).await?;
    sync_wal_parent(wal_path)?;
    *file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(wal_path)
        .await?;
    *good_len = retained_len;
    write_compaction_state(&state_path, &state, 2)?;
    Ok(())
}

#[derive(Clone, Copy)]
struct CompactionState {
    phase: u8,
    offset: u64,
    source_len: u64,
    retained_len: u64,
}

fn read_compaction_state(path: &Path) -> Result<Option<CompactionState>, IoError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match bytes.as_slice() {
        [version, phase, offset @ ..]
            if *version == COMPACTION_STATE_VERSION
                && matches!(*phase, 1 | 2)
                && offset.len() == 24 =>
        {
            Ok(Some(CompactionState {
                phase: *phase,
                offset: u64::from_le_bytes(offset[0..8].try_into().unwrap()),
                source_len: u64::from_le_bytes(offset[8..16].try_into().unwrap()),
                retained_len: u64::from_le_bytes(offset[16..24].try_into().unwrap()),
            }))
        }
        _ => Err(IoError::new(
            std::io::ErrorKind::InvalidData,
            "invalid WAL compaction state",
        )),
    }
}

fn write_compaction_state(path: &Path, state: &CompactionState, phase: u8) -> Result<(), IoError> {
    let mut bytes = Vec::with_capacity(26);
    bytes.push(COMPACTION_STATE_VERSION);
    bytes.push(phase);
    bytes.extend_from_slice(&state.offset.to_le_bytes());
    bytes.extend_from_slice(&state.source_len.to_le_bytes());
    bytes.extend_from_slice(&state.retained_len.to_le_bytes());
    let tmp = path.with_extension("compact.state.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn sync_wal_parent(wal_path: &Path) -> Result<(), IoError> {
    #[cfg(test)]
    if FAIL_AFTER_COMPACTION_RENAME.swap(false, Ordering::AcqRel) {
        return Err(IoError::other("injected WAL directory fsync failure"));
    }
    if let Some(parent) = wal_path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::MemTable;
    use crate::proto::{EntryAdapter, StreamAdapter};
    use std::sync::Arc;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-journal-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn make_push_req(streams: &[(&str, Vec<(&str, i64)>)]) -> Vec<u8> {
        let streams: Vec<StreamAdapter> = streams
            .iter()
            .map(|(labels, entries)| StreamAdapter {
                labels: labels.to_string(),
                entries: entries
                    .iter()
                    .map(|(line, ts)| EntryAdapter {
                        timestamp: Some(::prost_types::Timestamp {
                            seconds: *ts,
                            nanos: 0,
                        }),
                        line: line.to_string(),
                        structured_metadata: vec![],
                    })
                    .collect(),
                hash: 0,
            })
            .collect();
        let req = PushRequest { streams };
        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        buf
    }

    struct Harness {
        journal: Journal,
        memtable: Arc<MemTable>,
    }

    async fn harness(name: &str) -> Harness {
        let dir = tmp_dir(name);
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };
        let memtable = Arc::new(MemTable::new());
        let journal = Journal::spawn(&config, memtable.clone()).unwrap();
        Harness { journal, memtable }
    }

    async fn push(h: &Harness, raw: Vec<u8>) {
        let req = PushRequest::decode(raw.as_slice()).unwrap();
        let mut streams = Vec::with_capacity(req.streams.len());
        for stream in &req.streams {
            let labels = proto::parse_labels(&stream.labels).unwrap();
            let entries: Vec<LogEntry> = stream
                .entries
                .iter()
                .map(|e| LogEntry {
                    timestamp_ns: e.timestamp_ns().unwrap(),
                    line: e.line.clone(),
                    structured_metadata: e
                        .structured_metadata
                        .iter()
                        .map(|lp| (lp.name.clone(), lp.value.clone()))
                        .collect(),
                })
                .collect();
            streams.push((labels, entries));
        }
        h.journal.append(raw, streams).await.unwrap();
    }

    #[tokio::test]
    async fn append_and_checkpoint() {
        let h = harness("append_checkpoint").await;
        push(&h, make_push_req(&[("{app=\"a\"}", vec![("hi", 100)])])).await;
        push(&h, make_push_req(&[("{app=\"b\"}", vec![("yo", 200)])])).await;

        let ckpt = h.journal.checkpoint().await.unwrap();
        assert!(ckpt.offset > 0);
        assert_eq!(ckpt.snapshot.len(), 2);
        h.journal.set_checkpoint(ckpt.offset).unwrap();

        let (start, end) = replay(
            h.journal.wal_path(),
            h.journal.ckpt_path(),
            &MemTable::new(),
        )
        .unwrap();
        assert_eq!(start, ckpt.offset);
        assert_eq!(end, ckpt.offset);
    }

    #[tokio::test]
    async fn compact_checkpoint_retains_appends_after_snapshot() {
        let h = harness("compact_retains_suffix").await;
        push(
            &h,
            make_push_req(&[("{app=\"flushed\"}", vec![("old", 100)])]),
        )
        .await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        push(
            &h,
            make_push_req(&[("{app=\"inflight\"}", vec![("new", 200)])]),
        )
        .await;
        let before = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();

        assert_eq!(read_checkpoint(h.journal.ckpt_path()).unwrap(), 0);
        let after = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        assert!(after < before);
        let restored = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &restored).unwrap();
        let results = restored.query(&[], &[], i64::MIN, i64::MAX, 10, true);
        let lines: Vec<_> = results
            .iter()
            .flat_map(|stream| stream.entries.iter().map(|entry| entry.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn compaction_failure_does_not_fence_journal_writer() {
        let h = harness("compact_retry").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        let compact_tmp = h.journal.wal_path().with_extension("wal.compact.tmp");
        std::fs::create_dir_all(&compact_tmp).unwrap();
        assert!(
            h.journal
                .compact_checkpoint(checkpoint.offset)
                .await
                .is_err()
        );
        std::fs::remove_dir(&compact_tmp).unwrap();

        push(&h, make_push_req(&[("{app=\"new\"}", vec![("new", 200)])])).await;
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();
        let restored = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &restored).unwrap();
        let lines: Vec<_> = restored
            .query(&[], &[], i64::MIN, i64::MAX, 10, true)
            .into_iter()
            .flat_map(|stream| stream.entries.into_iter().map(|entry| entry.line))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn compaction_retry_after_rename_failure_keeps_acknowledged_suffix() {
        let h = harness("compact_rename_retry").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        FAIL_AFTER_COMPACTION_RENAME.store(true, Ordering::Release);
        assert!(
            h.journal
                .compact_checkpoint(checkpoint.offset)
                .await
                .is_err()
        );

        // The writer was reopened after the injected post-rename failure;
        // this append must remain in the replacement WAL before retry.
        push(&h, make_push_req(&[("{app=\"new\"}", vec![("new", 200)])])).await;
        h.journal
            .compact_checkpoint(checkpoint.offset)
            .await
            .unwrap();

        let restored = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &restored).unwrap();
        let lines: Vec<_> = restored
            .query(&[], &[], i64::MIN, i64::MAX, 10, true)
            .into_iter()
            .flat_map(|stream| stream.entries.into_iter().map(|entry| entry.line))
            .collect();
        assert_eq!(lines, vec!["new"]);
    }

    #[tokio::test]
    async fn replay_rolls_back_uncommitted_compaction_before_rename() {
        let h = harness("compact_replay_rollback").await;
        push(&h, make_push_req(&[("{app=\"old\"}", vec![("old", 100)])])).await;
        let checkpoint = h.journal.checkpoint().await.unwrap();
        h.memtable.commit_flush();

        let source_len = std::fs::metadata(h.journal.wal_path()).unwrap().len();
        let state_path = h.journal.wal_path().with_file_name(COMPACTION_STATE_FILE);
        let tmp_path = h.journal.wal_path().with_extension("wal.compact.tmp");
        let state = CompactionState {
            phase: 1,
            offset: checkpoint.offset,
            source_len,
            retained_len: 0,
        };
        write_compaction_state(&state_path, &state, 1).unwrap();
        write_checkpoint(h.journal.ckpt_path(), 0).unwrap();
        std::fs::write(&tmp_path, []).unwrap();

        let restored = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &restored).unwrap();
        assert!(
            restored
                .query(&[], &[], i64::MIN, i64::MAX, 10, true)
                .is_empty()
        );
        assert_eq!(
            read_checkpoint(h.journal.ckpt_path()).unwrap(),
            checkpoint.offset
        );
        assert!(!state_path.exists());
        assert!(!tmp_path.exists());
    }

    #[tokio::test]
    async fn health_turns_false_when_writer_stops() {
        let h = harness("writer_health").await;
        let health = h.journal.healthy.clone();

        drop(h.journal);
        tokio::time::timeout(Duration::from_secs(1), async {
            while health.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("journal health did not reflect writer shutdown");
    }

    #[tokio::test]
    async fn replay_restores_unflushed_data() {
        let h = harness("replay_unflushed").await;
        push(
            &h,
            make_push_req(&[("{app=\"a\"}", vec![("line1", 100), ("line2", 200)])]),
        )
        .await;
        let mt = MemTable::new();
        let (start, end) = replay(h.journal.wal_path(), h.journal.ckpt_path(), &mt).unwrap();
        assert_eq!(start, 0);
        assert!(end > 0);
        let results = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn replay_truncates_crc_corruption_at_tail() {
        let dir = tmp_dir("replay_crc_corruption");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let data = make_push_req(&[("{app=\"a\"}", vec![("line", 100)])]);
        let mut record = Vec::new();
        record.extend_from_slice(&(data.len() as u32).to_le_bytes());
        record.extend_from_slice(&(crc32fast::hash(&data) ^ 1).to_le_bytes());
        record.extend_from_slice(&data);
        std::fs::write(&wal_path, record).unwrap();

        let (start, end) = replay(&wal_path, &ckpt_path, &MemTable::new()).unwrap();

        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn replay_does_not_allocate_declared_length_for_a_partial_tail() {
        let dir = tmp_dir("replay_oversized_partial_tail");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let mut header = Vec::new();
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        std::fs::write(&wal_path, header).unwrap();

        let (start, end) = replay(&wal_path, &ckpt_path, &MemTable::new()).unwrap();

        assert_eq!(start, 0);
        assert_eq!(end, 0);
    }

    #[test]
    fn replay_rejects_crc_corruption_before_valid_records() {
        let dir = tmp_dir("replay_interior_crc_corruption");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        let first = make_push_req(&[("{app=\"a\"}", vec![("bad", 100)])]);
        let second = make_push_req(&[("{app=\"b\"}", vec![("good", 200)])]);
        let mut wal = Vec::new();
        wal.extend_from_slice(&(first.len() as u32).to_le_bytes());
        wal.extend_from_slice(&(crc32fast::hash(&first) ^ 1).to_le_bytes());
        wal.extend_from_slice(&first);
        wal.extend_from_slice(&(second.len() as u32).to_le_bytes());
        wal.extend_from_slice(&crc32fast::hash(&second).to_le_bytes());
        wal.extend_from_slice(&second);
        std::fs::write(&wal_path, wal).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_checkpoint_without_wal() {
        let dir = tmp_dir("checkpoint_without_wal");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        write_checkpoint(&ckpt_path, 128).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_checkpoint_beyond_wal() {
        let dir = tmp_dir("checkpoint_beyond_wal");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        std::fs::write(&wal_path, [0u8; 16]).unwrap();
        write_checkpoint(&ckpt_path, 32).unwrap();

        let result = replay(&wal_path, &ckpt_path, &MemTable::new());

        assert!(result.is_err());
    }

    #[test]
    fn replay_rejects_malformed_checkpoint() {
        let dir = tmp_dir("malformed_checkpoint");
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);
        std::fs::write(&wal_path, []).unwrap();

        for bytes in [&[1u8, 2, 3][..], &[0u8; 9][..]] {
            std::fs::write(&ckpt_path, bytes).unwrap();
            let error = replay(&wal_path, &ckpt_path, &MemTable::new())
                .expect_err("malformed checkpoint must stop recovery");
            assert!(error.contains("exactly 8 bytes"));
        }
    }

    #[test]
    fn spawn_reports_wal_open_failure_synchronously() {
        let dir = tmp_dir("spawn_open_failure");
        std::fs::create_dir_all(dir.join(WAL_FILE)).unwrap();
        let config = Config {
            data_dir: dir,
            ..Config::default()
        };

        let result = Journal::spawn(&config, std::sync::Arc::new(MemTable::new()));

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn checkpoint_clears_memtable_and_persists_offset() {
        let h = harness("ckpt_clear").await;
        push(&h, make_push_req(&[("{app=\"a\"}", vec![("x", 1)])])).await;
        let ckpt = h.journal.checkpoint().await.unwrap();
        // checkpoint는 inner를 비우고 flushing 버퍼로 옮김; unified_query는 여전히 해당 데이터를 본다.
        // flush 완료를 시뮬레이션하기 위해 commit_flush 호출.
        h.memtable.commit_flush();
        h.journal.set_checkpoint(ckpt.offset).unwrap();
        assert_eq!(h.memtable.approximate_size(), 0);

        push(&h, make_push_req(&[("{app=\"b\"}", vec![("y", 2)])])).await;

        let mt = MemTable::new();
        replay(h.journal.wal_path(), h.journal.ckpt_path(), &mt).unwrap();
        let results = mt.query(&[], &[], i64::MIN, i64::MAX, 100, true);
        let total: usize = results.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);
    }
}
