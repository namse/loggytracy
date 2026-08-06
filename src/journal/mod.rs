use std::io::Error as IoError;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use prost014::Message as Prost014Message;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use crate::config::Config;
use crate::memtable::{Labels, LogEntry, MemTable, MemTableSnapshot};
use crate::tenant::TenantId;
use crate::trace::{ExportTraceServiceRequest, TraceMemTable, TraceSpan, normalize_request};

const RECORD_HEADER_SIZE: usize = 8;
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const WAL_FILE: &str = "journal.wal";
const CKPT_FILE: &str = "journal.ckpt";
const COMPACTION_STATE_FILE: &str = "journal.wal.compact.state";
/// `LGY3 | kind | tenant_len:u8 | tenant | payload`.
const TENANT_RECORD_MAGIC: &[u8; 4] = b"LGY3";
const TENANT_RECORD_KIND_LOGS: u8 = 0;
const TENANT_RECORD_KIND_TRACES: u8 = 1;
/// The payload is the OTLP `ExportLogsServiceRequest` as it arrived, the same
/// pattern traces have used from the start: replay re-normalizes instead of
/// the ingest path materializing a second message just so the WAL can hold it.
const TENANT_RECORD_KIND_OTLP_LOGS: u8 = 2;
const TENANT_RECORD_PREFIX_SIZE: usize = TENANT_RECORD_MAGIC.len() + 2;

/// Where a compaction crash is simulated.
///
/// Keyed by WAL path rather than held in a plain flag: tests run in parallel,
/// and a process-wide flag is consumed by whichever compaction reaches it
/// first, which is rarely the one that armed it.
#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompactionFault {
    AfterRename,
    BeforeStateRemoval,
}

#[cfg(test)]
static COMPACTION_FAULTS: std::sync::Mutex<Vec<(PathBuf, CompactionFault)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
fn inject_compaction_fault(wal_path: &Path, fault: CompactionFault) {
    COMPACTION_FAULTS
        .lock()
        .unwrap()
        .push((wal_path.to_path_buf(), fault));
}

#[cfg(test)]
fn take_compaction_fault(wal_path: &Path, fault: CompactionFault) -> bool {
    let mut armed = COMPACTION_FAULTS.lock().unwrap();
    let Some(index) = armed
        .iter()
        .position(|(path, pending)| path == wal_path && *pending == fault)
    else {
        return false;
    };
    armed.swap_remove(index);
    true
}

pub struct CheckpointSnapshot {
    pub offset: u64,
    pub snapshot: Arc<MemTableSnapshot>,
    pub trace_snapshot: Arc<Vec<TraceSpan>>,
}

/// A decoded tenant-framed WAL record: who wrote it, what kind it is, and the
/// original payload.
type TenantRecord<'a> = (TenantId, u8, &'a [u8]);

/// Frame a payload so replay can recover which tenant produced it.
///
/// The tenant is written into the WAL rather than derived at replay because
/// the header it came from is gone by then, and mis-attributing a replayed
/// record would breach isolation after a crash.
fn framed_record_len(tenant: &TenantId, payload: &[u8]) -> usize {
    TENANT_RECORD_PREFIX_SIZE + tenant.as_str().len() + payload.len()
}

/// The framed form as one buffer. Production writes the frame straight into
/// the writer loop's batch buffer; this remains as the reference encoding the
/// journal tests build WAL bytes with.
#[cfg(test)]
fn frame_tenant_record(tenant: &TenantId, kind: u8, payload: &[u8]) -> Vec<u8> {
    let tenant_bytes = tenant.as_str().as_bytes();
    let mut framed =
        Vec::with_capacity(TENANT_RECORD_PREFIX_SIZE + tenant_bytes.len() + payload.len());
    framed.extend_from_slice(TENANT_RECORD_MAGIC);
    framed.push(kind);
    framed.push(tenant_bytes.len() as u8);
    framed.extend_from_slice(tenant_bytes);
    framed.extend_from_slice(payload);
    framed
}

fn decode_tenant_record(data: &[u8]) -> Result<Option<TenantRecord<'_>>, String> {
    if !data.starts_with(TENANT_RECORD_MAGIC) {
        return Ok(None);
    }
    if data.len() < TENANT_RECORD_PREFIX_SIZE {
        return Err("tenant journal record is truncated".to_string());
    }
    let kind = data[TENANT_RECORD_MAGIC.len()];
    let tenant_len = data[TENANT_RECORD_MAGIC.len() + 1] as usize;
    let tenant_end = TENANT_RECORD_PREFIX_SIZE + tenant_len;
    if data.len() < tenant_end {
        return Err("tenant journal record is truncated".to_string());
    }
    let tenant = std::str::from_utf8(&data[TENANT_RECORD_PREFIX_SIZE..tenant_end])
        .map_err(|error| format!("tenant journal record has a non-UTF-8 tenant: {error}"))?;
    let tenant = TenantId::parse(tenant)?;
    Ok(Some((tenant, kind, &data[tenant_end..])))
}

enum JournalCmd {
    Append {
        /// Record kind byte, framed by the writer loop. The payload travels
        /// unframed: framing it here built a prefix+tenant+payload copy of
        /// every export just to copy it again into the batch buffer —
        /// invariant II's "WAL write buffer" copy paid twice.
        kind: u8,
        payload: Vec<u8>,
        tenant: TenantId,
        streams: Vec<(Labels, Vec<LogEntry>)>,
        traces: Vec<TraceSpan>,
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
    u8,
    Vec<u8>,
    TenantId,
    Vec<(Labels, Vec<LogEntry>)>,
    Vec<TraceSpan>,
    oneshot::Sender<Result<(), IoError>>,
);

pub struct Journal {
    tx: mpsc::Sender<JournalCmd>,
    wal_path: PathBuf,
    ckpt_path: PathBuf,
    healthy: Arc<AtomicBool>,
    memtable: Arc<MemTable>,
    trace_memtable: Arc<TraceMemTable>,
    backlog: Arc<WalBacklog>,
}

/// Durable WAL bytes the flush loop has not yet retired.
///
/// Both numbers are already known to the code that changes them, so the ingest
/// gate and `/metrics` read them instead of paying a `stat` plus a checkpoint
/// file read per request.
#[derive(Default)]
pub struct WalBacklog {
    wal_bytes: AtomicU64,
    checkpoint_bytes: AtomicU64,
}

impl WalBacklog {
    fn set_wal_bytes(&self, bytes: u64) {
        self.wal_bytes.store(bytes, Ordering::Release);
    }

    fn set_checkpoint_bytes(&self, bytes: u64) {
        self.checkpoint_bytes.store(bytes, Ordering::Release);
    }

    /// Saturating because the two stores are independent: a checkpoint that
    /// lands between them can briefly exceed the WAL length it refers to.
    pub fn bytes(&self) -> u64 {
        self.wal_bytes
            .load(Ordering::Acquire)
            .saturating_sub(self.checkpoint_bytes.load(Ordering::Acquire))
    }

    /// The WAL file's durable length — dead prefix and live suffix together.
    /// The compaction policy reads this: the backlog alone cannot see a file
    /// whose checkpoint keeps up while its prefix grows without bound.
    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes.load(Ordering::Acquire)
    }
}

include!("writer.rs");
include!("checkpoint.rs");
include!("replay.rs");
include!("compaction.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
