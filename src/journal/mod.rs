use std::collections::HashMap;
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
use crate::memtable::{Labels, LogEntry, MemTable};
use crate::proto::{self, PushRequest};
use crate::trace::{ExportTraceServiceRequest, TraceMemTable, TraceSpan, normalize_request};

const RECORD_HEADER_SIZE: usize = 8;
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const WAL_FILE: &str = "journal.wal";
const CKPT_FILE: &str = "journal.ckpt";
const COMPACTION_STATE_FILE: &str = "journal.wal.compact.state";
const COMPACTION_STATE_VERSION: u8 = 1;
const TRACE_RECORD_MAGIC: &[u8; 4] = b"LGY2";
const TRACE_RECORD_VERSION: u8 = 1;

#[cfg(test)]
static FAIL_AFTER_COMPACTION_RENAME: AtomicBool = AtomicBool::new(false);

pub struct CheckpointSnapshot {
    pub offset: u64,
    pub snapshot: HashMap<Labels, Vec<LogEntry>>,
    pub trace_snapshot: Vec<TraceSpan>,
}

enum JournalCmd {
    Append {
        data: Vec<u8>,
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
