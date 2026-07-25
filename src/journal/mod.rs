use std::io::Error as IoError;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use prost::Message;
use prost014::Message as Prost014Message;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::memtable::{Labels, LogEntry, MemTable, MemTableSnapshot};
use crate::proto::{self, PushRequest};
use crate::tenant::TenantId;
use crate::trace::{ExportTraceServiceRequest, TraceMemTable, TraceSpan, normalize_request};

const RECORD_HEADER_SIZE: usize = 8;
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const WAL_FILE: &str = "journal.wal";
const CKPT_FILE: &str = "journal.ckpt";
const COMPACTION_STATE_FILE: &str = "journal.wal.compact.state";
const COMPACTION_STATE_VERSION: u8 = 1;
/// Pre-tenancy trace record: an OTLP export with no tenant. Replay attributes
/// it to the configured default tenant so an existing WAL still recovers.
const TRACE_RECORD_MAGIC: &[u8; 4] = b"LGY2";
const TRACE_RECORD_VERSION: u8 = 1;
/// Tenant-carrying record:
/// `LGY3 | version | kind | tenant_len:u8 | tenant | payload`.
const TENANT_RECORD_MAGIC: &[u8; 4] = b"LGY3";
const TENANT_RECORD_VERSION: u8 = 1;
const TENANT_RECORD_KIND_LOGS: u8 = 0;
const TENANT_RECORD_KIND_TRACES: u8 = 1;
const TENANT_RECORD_PREFIX_SIZE: usize = TENANT_RECORD_MAGIC.len() + 3;

#[cfg(test)]
static FAIL_AFTER_COMPACTION_RENAME: AtomicBool = AtomicBool::new(false);

pub struct CheckpointSnapshot {
    pub offset: u64,
    pub snapshot: MemTableSnapshot,
    pub trace_snapshot: Vec<TraceSpan>,
}

/// A decoded tenant-framed WAL record: who wrote it, what kind it is, and the
/// original payload.
type TenantRecord<'a> = (TenantId, u8, &'a [u8]);

/// Frame a payload so replay can recover which tenant produced it.
///
/// The tenant is written into the WAL rather than derived at replay because
/// the header it came from is gone by then, and mis-attributing a replayed
/// record would breach isolation after a crash.
fn frame_tenant_record(tenant: &TenantId, kind: u8, payload: &[u8]) -> Vec<u8> {
    let tenant_bytes = tenant.as_str().as_bytes();
    let mut framed =
        Vec::with_capacity(TENANT_RECORD_PREFIX_SIZE + tenant_bytes.len() + payload.len());
    framed.extend_from_slice(TENANT_RECORD_MAGIC);
    framed.push(TENANT_RECORD_VERSION);
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
    let version = data[TENANT_RECORD_MAGIC.len()];
    if version != TENANT_RECORD_VERSION {
        return Err(format!(
            "unsupported tenant journal record version {version}"
        ));
    }
    let kind = data[TENANT_RECORD_MAGIC.len() + 1];
    let tenant_len = data[TENANT_RECORD_MAGIC.len() + 2] as usize;
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
        data: Vec<u8>,
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
    trace_memtable: Arc<TraceMemTable>,
}

include!("writer.rs");
include!("checkpoint.rs");
include!("replay.rs");
include!("compaction.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
