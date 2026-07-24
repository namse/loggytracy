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
use crate::memtable::{Labels, LogEntry, QueryResult, StreamResult};

pub const DATA_FILE: &str = "data.parquet";
pub const BLOOM_FILE: &str = "bloom.tri";
pub const STREAM_INDEX_FILE: &str = "stream.idx";
pub const META_FILE: &str = "meta.json";
pub const MERGE_TOMBSTONE_FILE: &str = ".merge.tombstone";

const BLOOM_MAGIC_V1: &[u8; 4] = b"BTF1";
const BLOOM_MAGIC_V2: &[u8; 4] = b"BTF2";
const BLOOM_MAGIC_V3: &[u8; 4] = b"BTF3";
const STREAM_MAGIC: &[u8; 4] = b"SIX1";

const EXACT_FIELD_TOKEN_MAGIC: &[u8; 4] = b"FEQ1";
const EXACT_FIELD_SCALAR_SCOPE: u8 = 0;

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
    pub stream_labels: Vec<String>,
    pub streams: Vec<Labels>,
    integrity: PartIntegrity,
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
    pub timestamp_ns: i64,
    pub labels: Labels,
    pub line: String,
    pub structured_metadata: Vec<(String, String)>,
}

impl Row {
    pub fn from_entry(labels: &Labels, e: &LogEntry) -> Self {
        Self {
            timestamp_ns: e.timestamp_ns,
            labels: labels.clone(),
            line: e.line.clone(),
            structured_metadata: e.structured_metadata.clone(),
        }
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
