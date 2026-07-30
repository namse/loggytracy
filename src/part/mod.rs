use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
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
use crate::memtable::{
    Labels, LogEntry, MemTableSnapshot, QueryResult, SharedLabels, StreamResult,
};
use crate::tenant::TenantId;

pub const DATA_FILE: &str = "data.parquet";
/// Trigram blooms and the stream index in one file.
///
/// They were two, and a part therefore cost two objects, two round trips on
/// restore and two checksum passes at startup. Startup at 10,099 parts is
/// dominated by exactly that validation (LOAD_RESULTS.md §8), and every object
/// per part is a Class A operation per flush (§9). Nothing ever read one of
/// these without the other, so being two files bought nothing.
pub const INDEX_FILE: &str = "index.bin";
/// Container header: magic plus the version of the layout inside it. Checked
/// before any section is read, so a file from another build is refused rather
/// than parsed as garbage.
pub const INDEX_MAGIC: &[u8; 5] = b"LTIX1";
pub const META_FILE: &str = "meta.json";
pub const MERGE_TOMBSTONE_FILE: &str = ".merge.tombstone";
/// On-disk layout of `meta.json`.
///
/// Read and rejected before the checksum is verified, because the checksum is
/// computed over the struct and therefore only means anything once both sides
/// agree on what the struct is. Without this a format change surfaces as a
/// checksum mismatch, which reads as corruption rather than as a version the
/// build cannot handle.
pub const PART_META_VERSION: u32 = 2;

const BLOOM_MAGIC_V1: &[u8; 4] = b"BTF1";
const BLOOM_MAGIC_V2: &[u8; 4] = b"BTF2";
const BLOOM_MAGIC_V3: &[u8; 4] = b"BTF3";
/// As V3, except a row group that indexed no exact-field token stores a
/// zero-length filter instead of an empty one.
///
/// V3 spent a filter per row group whether or not there was anything to put in
/// it, and `optimal_bits` has a 1024-bit floor, so a row group with no
/// structured metadata and no parser fields still cost 140 bytes on disk and
/// the same resident. Tenant-aligned row groups make that per tenant per part.
const BLOOM_MAGIC_V4: &[u8; 4] = b"BTF4";
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
///
/// Deliberately not a `QueryTimeRange`, and its `end` is deliberately still
/// inclusive. These endpoints answer "did this label exist in the window", not
/// "which rows are in the window", and no measurement establishes what Loki's
/// own bound is here. Closing an existence question one nanosecond wide is the
/// conservative error; a row set is what `query_range` promises exactly.
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

/// The time window a log scan is allowed to return rows from.
///
/// Every level of the read path — the memtable scan, the part-level and
/// row-group-level pruning, and the row-level reject inside a part — used to
/// spell its own comparison out, and four independent comparisons that have to
/// agree is how `end` came to be inclusive on the log path while Loki's
/// `query_range` is `[start, end)`. So the boundary lives here and nothing below
/// the query layer decides it: a site that prunes asks `overlaps`, a site that
/// rejects a row asks `contains`, and the two cannot drift apart. A pruning
/// bound tighter than the row bound silently drops rows, which is the failure
/// mode this type exists to make unrepresentable.
/// The fields are private so no caller can spell the comparison out again from
/// the bounds: the only way to ask whether a timestamp is in the window is to
/// ask this type.
#[derive(Clone, Copy, Debug)]
pub struct QueryTimeRange {
    start_ns: i64,
    end_ns: i64,
    include_end: bool,
}

impl QueryTimeRange {
    /// `[start_ns, end_ns)` — what a Loki log query asks for. Both `query` and
    /// `query_range` include `start` and exclude `end`.
    pub fn half_open(start_ns: i64, end_ns: i64) -> Self {
        Self {
            start_ns,
            end_ns,
            include_end: false,
        }
    }

    /// `[start_ns, end_ns]`.
    ///
    /// Not a log-query window. This is for the scans whose `end` is not a
    /// client-supplied exclusive bound: a metric scan's `end` is its last
    /// evaluation point, and the range evaluator's own window closes on it
    /// (`timestamp_ns <= evaluation_ns`), so a half-open scan would starve the
    /// final point of the row that lands exactly on it.
    pub fn closed(start_ns: i64, end_ns: i64) -> Self {
        Self {
            start_ns,
            end_ns,
            include_end: true,
        }
    }

    /// Every row there is, for the callers that mean "all of them".
    pub fn unbounded() -> Self {
        Self::closed(i64::MIN, i64::MAX)
    }

    /// Whether a row at `timestamp_ns` belongs to the answer.
    pub fn contains(&self, timestamp_ns: i64) -> bool {
        timestamp_ns >= self.start_ns
            && (timestamp_ns < self.end_ns || (self.include_end && timestamp_ns == self.end_ns))
    }

    /// Whether a span `[min_ts_ns, max_ts_ns]` can hold a row this range
    /// contains. Exactly `contains` lifted to a span, so pruning never rejects
    /// a span holding a row the row-level test would have kept.
    pub fn overlaps(&self, min_ts_ns: i64, max_ts_ns: i64) -> bool {
        max_ts_ns >= self.start_ns
            && (min_ts_ns < self.end_ns || (self.include_end && min_ts_ns == self.end_ns))
    }
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
    /// Size of `meta.json` on disk, recorded when it was read.
    ///
    /// Startup parses this file for every part before serving anything, and
    /// `tenants` makes its size scale with tenant breadth, so this is the
    /// concrete cost of the per-tenant index rather than an estimate of it.
    pub meta_bytes: u64,
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
    pub fn index_path(&self) -> PathBuf {
        self.dir.join(INDEX_FILE)
    }
    pub fn meta_path(&self) -> PathBuf {
        self.dir.join(META_FILE)
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub tenant: TenantId,
    pub timestamp_ns: i64,
    pub labels: SharedLabels,
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

    /// The label set is shared with the stream it came from rather than copied.
    /// This clone used to be the whole `BTreeMap` and it is why a flush held
    /// 3.3x the memtable it was materializing.
    pub fn from_entry(tenant: &TenantId, labels: &SharedLabels, e: &LogEntry) -> Self {
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
    /// The full identity of a row, not just its placement.
    ///
    /// `(tenant, timestamp)` is what the layout needs — tenant-aligned row
    /// groups and time order within a tenant. Everything after the timestamp is
    /// a tie-break, which the ordering never cared about, and which makes two
    /// copies of one entry sort adjacent so a duplicate can be recognised.
    fn sort_key(&self) -> (&str, i64, &Labels, &str, &[(String, String)]) {
        (
            self.tenant.as_str(),
            self.timestamp_ns,
            self.labels.as_ref(),
            self.line.as_str(),
            &self.structured_metadata,
        )
    }
}

include!("sink.rs");
include!("format.rs");
include!("indexes.rs");
include!("metadata.rs");
include!("tombstone.rs");
include!("reader.rs");
include!("row_stream.rs");

#[cfg(test)]
mod tests {
    include!("tests.rs");
}
