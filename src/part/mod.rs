use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::Result as ParquetResult;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, Length};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::bloom::BloomFilter;
use crate::logql::{LabelMatcher, LineFilter, MatcherOp};
use crate::memtable::{Labels, LogEntry, MemTableSnapshot, QueryResult, StreamResult};
use crate::tenant::TenantId;

pub const DATA_FILE: &str = "data.parquet";
pub const BLOOM_FILE: &str = "bloom.tri";
pub const STREAM_INDEX_FILE: &str = "stream.idx";
pub const META_FILE: &str = "meta.json";
pub const MERGE_TOMBSTONE_FILE: &str = ".merge.tombstone";
/// On-disk layout of `meta.json`.
///
/// Read and rejected before the checksum is verified, because the checksum is
/// computed over the struct and therefore only means anything once both sides
/// agree on what the struct is. Without this a format change surfaces as a
/// checksum mismatch, which reads as corruption rather than as a version the
/// build cannot handle.
pub const PART_META_VERSION: u32 = 1;

const BLOOM_MAGIC_V1: &[u8; 4] = b"BTF1";
const BLOOM_MAGIC_V2: &[u8; 4] = b"BTF2";
const BLOOM_MAGIC_V3: &[u8; 4] = b"BTF3";
const STREAM_MAGIC: &[u8; 4] = b"SIX1";

const EXACT_FIELD_TOKEN_MAGIC: &[u8; 4] = b"FEQ1";
const EXACT_FIELD_SCALAR_SCOPE: u8 = 0;

/// Parquet column holding the row's tenant. It is the leading sort key, so it
/// is also the leading column.
pub const TENANT_COLUMN: &str = "_tenant";

/// The time span a metadata lookup is allowed to see.
///
/// Replaces the bare retention floor that these lookups used to take. Grafana
/// sends `start`/`end` on every label and series call, and answering from the
/// whole history both returns labels that do not exist in the range and reads
/// every part to do it. The retention floor folds into `start_ns`, so one
/// bound expresses both "what the client asked for" and "what the tenant is
/// still entitled to".
#[derive(Clone, Copy, Debug)]
pub struct MetadataWindow {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl MetadataWindow {
    pub fn unbounded() -> Self {
        Self {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
        }
    }

    pub fn clamped_to(self, retention_floor_ns: Option<i64>) -> Self {
        match retention_floor_ns {
            Some(floor_ns) => Self {
                start_ns: self.start_ns.max(floor_ns),
                ..self
            },
            None => self,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.start_ns > self.end_ns
    }

    pub fn contains(&self, timestamp_ns: i64) -> bool {
        timestamp_ns >= self.start_ns && timestamp_ns <= self.end_ns
    }

    pub fn overlaps(&self, min_ts_ns: i64, max_ts_ns: i64) -> bool {
        max_ts_ns >= self.start_ns && min_ts_ns <= self.end_ns
    }
}

pub type StreamMap = BTreeMap<String, BTreeMap<String, RoaringBitmap>>;

#[derive(Clone, Copy)]
struct QueryTimeRange {
    start_ns: i64,
    end_ns: i64,
    include_end: bool,
}

/// A positive exact equality over an entry's structured or parser-visible
/// scalar fields.
///
/// This type deliberately lives below LogQL. Callers may compile eligible
/// pipeline predicates into it without coupling the immutable part format to
/// a particular query AST. Missing field blooms (notably BTF1 parts) always
/// fall back to scanning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactFieldPredicate {
    pub name: String,
    pub value: String,
    /// A preceding parser may also produce this field from the log line. BTF2
    /// indexes parser-visible scalars as well as structured metadata; this bit
    /// remains explicit so older/alternate indexes can conservatively scan.
    pub may_be_extracted: bool,
    /// Whether `value` is a canonical numeric/duration representation. Older
    /// BTF2 indexes contain only raw values and must not prune such queries.
    pub canonical: bool,
}

impl ExactFieldPredicate {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            may_be_extracted: false,
            canonical: false,
        }
    }

    pub fn new_with_extraction(
        name: impl Into<String>,
        value: impl Into<String>,
        may_be_extracted: bool,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            may_be_extracted,
            canonical: false,
        }
    }

    pub fn new_canonical_with_extraction(
        name: impl Into<String>,
        value: impl Into<String>,
        may_be_extracted: bool,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            may_be_extracted,
            canonical: true,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ExactFieldPruning<'a> {
    pub line_filters: &'a [LineFilter],
    pub exact_fields: &'a [ExactFieldPredicate],
}

impl<'a> ExactFieldPruning<'a> {
    pub fn new(line_filters: &'a [LineFilter], exact_fields: &'a [ExactFieldPredicate]) -> Self {
        Self {
            line_filters,
            exact_fields,
        }
    }
}

/// One tenant's contiguous slice of a shared part.
///
/// Rows are sorted by `(tenant, timestamp_ns)` and row groups never straddle
/// a tenant boundary, so a tenant occupies a whole run of row groups. A query
/// is confined to its own run, which is what makes a shared object as
/// fail-closed as a per-tenant file: rows outside the run are not addressable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantSegment {
    pub tenant: TenantId,
    /// Inclusive first row group.
    pub row_group_start: u32,
    /// Exclusive last row group.
    pub row_group_end: u32,
    pub row_count: u64,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

#[derive(Clone, Debug)]
pub struct PartMeta {
    pub id: String,
    pub partition: String,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub row_count: u64,
    pub row_group_count: u32,
    pub row_group_min_ts: Vec<i64>,
    pub row_group_max_ts: Vec<i64>,
    /// Sorted by tenant, non-overlapping, and covering every row group.
    pub tenants: Vec<TenantSegment>,
    /// Memory a full read of this part materializes, recorded at write time.
    /// Merge compares this against its budgets instead of the compressed file
    /// size, which is smaller by whatever zstd achieved on the data.
    pub materialized_bytes: u64,
    pub stream_labels: Vec<String>,
    pub streams: Vec<Labels>,
    integrity: PartIntegrity,
}

impl PartMeta {
    pub fn tenant_segment(&self, tenant: &TenantId) -> Option<&TenantSegment> {
        self.tenants
            .binary_search_by(|segment| segment.tenant.cmp(tenant))
            .ok()
            .map(|index| &self.tenants[index])
    }
}

#[derive(Clone, Debug)]
pub struct Part {
    pub dir: PathBuf,
    pub meta: PartMeta,
}

impl Part {
    pub fn data_path(&self) -> PathBuf {
        self.dir.join(DATA_FILE)
    }
    pub fn bloom_path(&self) -> PathBuf {
        self.dir.join(BLOOM_FILE)
    }
    pub fn stream_index_path(&self) -> PathBuf {
        self.dir.join(STREAM_INDEX_FILE)
    }
    pub fn meta_path(&self) -> PathBuf {
        self.dir.join(META_FILE)
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub tenant: TenantId,
    pub timestamp_ns: i64,
    pub labels: Labels,
    pub line: String,
    pub structured_metadata: Vec<(String, String)>,
}

impl Row {
    /// What this row costs to hold in memory once read back.
    ///
    /// Merge sizes both its group selection and its read budget with this, so
    /// the two are in the same unit by construction. Comparing a compressed
    /// on-disk size against a materialized budget is what made large groups
    /// select successfully and then always fail to read.
    pub fn materialized_bytes(&self) -> u64 {
        let labels_bytes: usize = self
            .labels
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        let metadata_bytes: usize = self
            .structured_metadata
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum();
        (labels_bytes + self.line.len() + metadata_bytes + std::mem::size_of::<Row>()) as u64
    }

    pub fn from_entry(tenant: &TenantId, labels: &Labels, e: &LogEntry) -> Self {
        Self {
            tenant: tenant.clone(),
            timestamp_ns: e.timestamp_ns,
            labels: labels.clone(),
            line: e.line.clone(),
            structured_metadata: e.structured_metadata.clone(),
        }
    }

    /// The part sort key. Timestamp order is preserved *within* a tenant, so
    /// the reader's early termination and row-group time pruning keep working.
    fn sort_key(&self) -> (&str, i64) {
        (self.tenant.as_str(), self.timestamp_ns)
    }
}

include!("format.rs");
include!("indexes.rs");
include!("metadata.rs");
include!("tombstone.rs");
include!("reader.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
