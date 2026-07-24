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

pub fn partition_of(ts_ns: i64) -> String {
    let secs = ts_ns.div_euclid(1_000_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    dt.format("%Y-%m-%d").to_string()
}

fn gen_part_id(min_ts_ns: i64) -> String {
    let secs = min_ts_ns.div_euclid(1_000_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    format!("{}-{}", dt.format("%Y%m%dT%H%M%S"), uuid::Uuid::new_v4())
}

pub fn rows_from_snapshot(snapshot: &HashMap<Labels, Vec<LogEntry>>) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    for (labels, entries) in snapshot {
        for e in entries {
            rows.push(Row::from_entry(labels, e));
        }
    }
    rows.sort_by_key(|r| r.timestamp_ns);
    rows
}

pub fn flush_rows(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
) -> io::Result<Vec<Part>> {
    flush_rows_internal(rows, parts_root, row_group_size, None)
}

/// Flush rows while carrying a merge tombstone into every committed part.
///
/// The tombstone is written and fsynced inside the temporary part directory
/// before that directory is renamed into the visible partition directory.
/// This makes a visible merged part self-describing even if the process dies
/// immediately after the rename.
pub fn flush_rows_with_merge_tombstone(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
    old_dirs: &[PathBuf],
) -> io::Result<Vec<Part>> {
    flush_rows_internal(rows, parts_root, row_group_size, Some(old_dirs))
}

fn flush_rows_internal(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
    merge_old_dirs: Option<&[PathBuf]>,
) -> io::Result<Vec<Part>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let tmp_root = parts_root.join(".tmp");
    fs::create_dir_all(&tmp_root)?;

    let mut by_partition: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for row in rows {
        let p = partition_of(row.timestamp_ns);
        by_partition.entry(p).or_default().push(row);
    }

    let mut parts = Vec::new();
    let mut committed_dirs: Vec<PathBuf> = Vec::new();
    for (partition, mut part_rows) in by_partition {
        part_rows.sort_by_key(|r| r.timestamp_ns);
        let part_id = gen_part_id(part_rows[0].timestamp_ns);

        let tmp_dir = tmp_root.join(&part_id);
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }
        if let Err(e) = fs::create_dir_all(&tmp_dir) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        let stream_labels = collect_stream_labels(&part_rows);
        if let Err(e) = write_part_files(
            &tmp_dir,
            &part_id,
            &partition,
            &part_rows,
            &stream_labels,
            row_group_size,
        ) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        if let Some(old_dirs) = merge_old_dirs
            && let Err(e) = write_merge_tombstone(&tmp_dir, parts_root, old_dirs)
        {
            let _ = fs::remove_dir_all(&tmp_dir);
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        let final_dir = parts_root.join(&partition).join(&part_id);
        if let Some(parent) = final_dir.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        if final_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
            rollback_committed(&committed_dirs);
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("part dir already exists: {}", final_dir.display()),
            ));
        }
        if let Err(e) = fs::rename(&tmp_dir, &final_dir) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        // rename의 내구성을 보장하기 위해 부모(파티션) 디렉터리와 parts_root를 fsync.
        if let Some(parent) = final_dir.parent()
            && let Err(e) = fsync_dir(parent)
        {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        if let Err(e) = fsync_dir(parts_root) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        committed_dirs.push(final_dir.clone());

        let part = match load_part(&final_dir) {
            Ok(p) => p,
            Err(e) => {
                rollback_committed(&committed_dirs);
                return Err(io::Error::other(e));
            }
        };
        parts.push(part);
    }
    Ok(parts)
}

fn rollback_committed(committed_dirs: &[PathBuf]) {
    for dir in committed_dirs.iter().rev() {
        if dir.exists()
            && let Err(e) = fs::remove_dir_all(dir)
        {
            tracing::warn!(error = %e, ?dir, "rollback: failed to remove committed part dir");
        }
    }
}

fn collect_stream_labels(rows: &[Row]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for r in rows {
        for k in r.labels.keys() {
            set.insert(k.clone());
        }
    }
    set.into_iter().collect()
}

fn write_part_files(
    dir: &Path,
    id: &str,
    partition: &str,
    rows: &[Row],
    stream_labels: &[String],
    row_group_size: usize,
) -> io::Result<()> {
    write_parquet(&dir.join(DATA_FILE), rows, stream_labels, row_group_size)?;
    write_bloom(&dir.join(BLOOM_FILE), rows, row_group_size)?;
    write_stream_index(
        &dir.join(STREAM_INDEX_FILE),
        rows,
        row_group_size,
        stream_labels,
    )?;
    write_meta(
        &dir.join(META_FILE),
        id,
        partition,
        rows,
        row_group_size,
        stream_labels,
    )?;
    Ok(())
}

fn row_group_bounds(n: usize, row_group_size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + row_group_size).min(n);
        out.push((start, end));
        start = end;
    }
    out
}

fn write_parquet(
    path: &Path,
    rows: &[Row],
    stream_labels: &[String],
    row_group_size: usize,
) -> io::Result<()> {
    let mut fields = vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("_msg", DataType::Utf8, false),
    ];
    for label in stream_labels {
        fields.push(Field::new(label, DataType::Utf8, true));
    }
    fields.push(Field::new("structured_metadata", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    let ts: Vec<i64> = rows.iter().map(|r| r.timestamp_ns).collect();
    let msg: Vec<&str> = rows.iter().map(|r| r.line.as_str()).collect();
    let sm: Vec<Option<String>> = rows
        .iter()
        .map(|r| {
            if r.structured_metadata.is_empty() {
                None
            } else {
                serde_json::to_string(&r.structured_metadata).ok()
            }
        })
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(msg)),
    ];
    for label in stream_labels {
        let vals: Vec<Option<&str>> = rows
            .iter()
            .map(|r| r.labels.get(label).map(|s| s.as_str()))
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    columns.push(Arc::new(StringArray::from(sm)));

    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(io::Error::other)?;

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    writer.close().map_err(io::Error::other)?;
    sync_file(path)?;
    Ok(())
}

fn write_bloom(path: &Path, rows: &[Row], row_group_size: usize) -> io::Result<()> {
    let bounds = row_group_bounds(rows.len(), row_group_size);
    let mut buf = Vec::new();
    buf.extend_from_slice(BLOOM_MAGIC_V3);
    buf.extend_from_slice(&(bounds.len() as u32).to_le_bytes());
    for (start, end) in &bounds {
        let mut unique_trigrams: BTreeSet<[u8; 3]> = BTreeSet::new();
        // Count the actual indexed tokens instead of estimating from rows.
        // The second pass keeps the existing bounded-memory insertion path,
        // while sizing the filter for wide structured rows as well as sparse
        // rows.
        let mut exact_capacity = 0usize;
        for row in &rows[*start..*end] {
            for (_name, value) in &row.structured_metadata {
                exact_capacity = exact_capacity
                    .saturating_add(crate::logql::canonical_index_values(value).len());
            }
            for (_name, values) in crate::logql::indexed_parser_fields(&row.line) {
                for value in values {
                    exact_capacity = exact_capacity
                        .saturating_add(crate::logql::canonical_index_values(&value).len());
                }
            }
        }
        let exact_capacity = exact_capacity.max(1);
        let mut exact_fields = BloomFilter::with_capacity(exact_capacity, 0.01);
        for row in &rows[*start..*end] {
            for tri in crate::bloom::trigrams(&row.line) {
                unique_trigrams.insert(tri);
            }
            for (name, value) in &row.structured_metadata {
                for value in crate::logql::canonical_index_values(value) {
                    exact_fields.insert(&encode_exact_field_token(name, &value)?);
                }
            }
            for (name, values) in crate::logql::indexed_parser_fields(&row.line) {
                for value in values {
                    for value in crate::logql::canonical_index_values(&value) {
                        exact_fields.insert(&encode_exact_field_token(&name, &value)?);
                    }
                }
            }
        }
        let estimated_items = unique_trigrams.len().max(1);
        let mut bloom = BloomFilter::with_capacity(estimated_items, 0.01);
        for tri in &unique_trigrams {
            bloom.insert(tri);
        }
        let bytes = bloom.encode();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
        let bytes = exact_fields.encode();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
}

fn encode_exact_field_token(name: &str, value: &str) -> io::Result<Vec<u8>> {
    let name_len = u32::try_from(name.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field name is too large"))?;
    let value_len = u32::try_from(value.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field value is too large"))?;
    let capacity = EXACT_FIELD_TOKEN_MAGIC
        .len()
        .checked_add(1 + 4 + name.len() + 4 + value.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "field token is too large"))?;
    let mut token = Vec::with_capacity(capacity);
    token.extend_from_slice(EXACT_FIELD_TOKEN_MAGIC);
    token.push(EXACT_FIELD_SCALAR_SCOPE);
    token.extend_from_slice(&name_len.to_le_bytes());
    token.extend_from_slice(name.as_bytes());
    token.extend_from_slice(&value_len.to_le_bytes());
    token.extend_from_slice(value.as_bytes());
    Ok(token)
}

fn write_stream_index(
    path: &Path,
    rows: &[Row],
    row_group_size: usize,
    stream_labels: &[String],
) -> io::Result<()> {
    let bounds = row_group_bounds(rows.len(), row_group_size);
    let mut index: StreamMap = BTreeMap::new();
    for (rg, (start, end)) in bounds.iter().enumerate() {
        for row in &rows[*start..*end] {
            for label in stream_labels {
                if let Some(v) = row.labels.get(label) {
                    index
                        .entry(label.clone())
                        .or_default()
                        .entry(v.clone())
                        .or_default()
                        .insert(rg as u32);
                }
            }
        }
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(STREAM_MAGIC);
    let entry_count: usize = index.values().map(|m| m.len()).sum();
    buf.extend_from_slice(&(entry_count as u32).to_le_bytes());
    for (name, values) in &index {
        for (value, bitmap) in values {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
            let value_bytes = value.as_bytes();
            buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(value_bytes);
            let mut bm_bytes = Vec::new();
            bitmap
                .serialize_into(&mut bm_bytes)
                .map_err(io::Error::other)?;
            buf.extend_from_slice(&(bm_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bm_bytes);
        }
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
}

fn write_meta(
    path: &Path,
    id: &str,
    partition: &str,
    rows: &[Row],
    row_group_size: usize,
    stream_labels: &[String],
) -> io::Result<()> {
    let n = rows.len();
    let bounds = row_group_bounds(n, row_group_size);
    let min_ts = rows[0].timestamp_ns;
    let max_ts = rows[n - 1].timestamp_ns;
    let row_group_min_ts: Vec<i64> = bounds
        .iter()
        .map(|(start, _)| rows[*start].timestamp_ns)
        .collect();
    let row_group_max_ts: Vec<i64> = bounds
        .iter()
        .map(|(_, end)| rows[*end - 1].timestamp_ns)
        .collect();

    let mut stream_set: BTreeSet<Labels> = BTreeSet::new();
    for r in rows {
        stream_set.insert(r.labels.clone());
    }
    let streams: Vec<Vec<(String, String)>> = stream_set
        .into_iter()
        .map(|m| m.into_iter().collect())
        .collect();

    let dir = path
        .parent()
        .ok_or_else(|| io::Error::other("metadata path has no parent"))?;
    let integrity = PartIntegrity {
        data_crc32: file_crc32(&dir.join(DATA_FILE))?,
        bloom_crc32: file_crc32(&dir.join(BLOOM_FILE))?,
        stream_index_crc32: file_crc32(&dir.join(STREAM_INDEX_FILE))?,
        metadata_crc32: 0,
    };
    let mut meta = MetaFile {
        id: id.to_string(),
        partition: partition.to_string(),
        min_ts_ns: min_ts,
        max_ts_ns: max_ts,
        row_count: n as u64,
        row_group_count: bounds.len() as u32,
        row_group_min_ts,
        row_group_max_ts,
        stream_labels: stream_labels.to_vec(),
        streams,
        integrity,
    };
    meta.integrity.metadata_crc32 = metadata_crc32(&meta).map_err(io::Error::other)?;
    let s = serde_json::to_string_pretty(&meta).map_err(io::Error::other)?;
    fs::write(path, s)?;
    sync_file(path)?;
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PartIntegrity {
    data_crc32: u32,
    bloom_crc32: u32,
    stream_index_crc32: u32,
    metadata_crc32: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct MetaFile {
    id: String,
    partition: String,
    min_ts_ns: i64,
    max_ts_ns: i64,
    row_count: u64,
    row_group_count: u32,
    row_group_min_ts: Vec<i64>,
    row_group_max_ts: Vec<i64>,
    stream_labels: Vec<String>,
    streams: Vec<Vec<(String, String)>>,
    integrity: PartIntegrity,
}

fn file_crc32(path: &Path) -> io::Result<u32> {
    let mut file = fs::File::open(path)?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize())
}

fn metadata_crc32(meta: &MetaFile) -> Result<u32, String> {
    let mut canonical = meta.clone();
    canonical.integrity.metadata_crc32 = 0;
    let bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    Ok(crc32fast::hash(&bytes))
}

#[derive(Serialize, Deserialize)]
struct MergeTombstone {
    old_dirs: Vec<PathBuf>,
}

pub fn write_merge_tombstone(
    part_dir: &Path,
    parts_root: &Path,
    old_dirs: &[PathBuf],
) -> io::Result<()> {
    let canonical_parts_root = fs::canonicalize(parts_root)?;
    let relative_old_dirs: Vec<PathBuf> = old_dirs
        .iter()
        .map(|old_dir| {
            let canonical_old_dir = fs::canonicalize(old_dir)?;
            canonical_old_dir
                .strip_prefix(&canonical_parts_root)
                .map(Path::to_path_buf)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let tomb = MergeTombstone {
        old_dirs: relative_old_dirs,
    };
    let s = serde_json::to_string(&tomb).map_err(io::Error::other)?;
    let path = part_dir.join(MERGE_TOMBSTONE_FILE);
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, &s)?;
    sync_file(&tmp)?;
    fs::rename(&tmp, &path)?;
    sync_file(&path)?;
    fsync_dir(part_dir)?;
    Ok(())
}

pub fn read_merge_tombstone(part_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let path = part_dir.join(MERGE_TOMBSTONE_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let s = fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let tomb: MergeTombstone = serde_json::from_str(&s).map_err(|e| e.to_string())?;
    for old_dir in &tomb.old_dirs {
        validate_tombstone_part_path(old_dir)?;
    }
    Ok(tomb.old_dirs)
}

pub fn read_merge_tombstone_dirs(
    part_dir: &Path,
    parts_root: &Path,
) -> Result<Vec<PathBuf>, String> {
    let relative_dirs = read_merge_tombstone(part_dir)?;
    let canonical_root = fs::canonicalize(parts_root).map_err(|e| {
        format!(
            "failed to canonicalize parts root {}: {e}",
            parts_root.display()
        )
    })?;

    relative_dirs
        .into_iter()
        .map(|relative_dir| {
            let dir = parts_root.join(&relative_dir);
            let parent = dir
                .parent()
                .ok_or_else(|| format!("part directory has no parent: {}", dir.display()))?;
            let canonical_parent = fs::canonicalize(parent).map_err(|e| {
                format!(
                    "failed to canonicalize tombstone target parent {}: {e}",
                    parent.display()
                )
            })?;
            canonical_parent
                .strip_prefix(&canonical_root)
                .map_err(|_| {
                    format!(
                        "merge tombstone target escapes parts root: {}",
                        dir.display()
                    )
                })?;

            match fs::canonicalize(&dir) {
                Ok(canonical_dir) => {
                    canonical_dir.strip_prefix(&canonical_root).map_err(|_| {
                        format!(
                            "merge tombstone target escapes parts root: {}",
                            dir.display()
                        )
                    })?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "failed to canonicalize tombstone target {}: {error}",
                        dir.display()
                    ));
                }
            }
            Ok(dir)
        })
        .collect()
}

fn validate_tombstone_part_path(path: &Path) -> Result<(), String> {
    let components: Vec<_> = path.components().collect();
    if components.len() != 2
        || components
            .iter()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(format!(
            "invalid merge tombstone part path: {}",
            path.display()
        ));
    }
    Ok(())
}

pub fn remove_merge_tombstone(part_dir: &Path) -> io::Result<()> {
    let path = part_dir.join(MERGE_TOMBSTONE_FILE);
    fs::remove_file(&path)?;
    fsync_dir(part_dir)?;
    Ok(())
}

fn sync_file(path: &Path) -> io::Result<()> {
    let f = fs::File::open(path)?;
    f.sync_all()?;
    let dir = fs::File::open(path.parent().unwrap_or(Path::new(".")))?;
    dir.sync_all()?;
    Ok(())
}

fn canonical_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

pub fn fsync_dir(path: &Path) -> io::Result<()> {
    let dir = fs::File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

pub fn remove_part_dirs(dirs: &[PathBuf]) -> Result<(), String> {
    let mut parents = std::collections::BTreeSet::new();
    let mut first_error = None;

    for dir in dirs {
        match dir.parent() {
            Some(parent) => {
                parents.insert(parent.to_path_buf());
            }
            None => {
                first_error.get_or_insert_with(|| {
                    format!("part directory has no parent: {}", dir.display())
                });
                continue;
            }
        }
        match fs::symlink_metadata(dir) {
            Ok(_) => {
                if let Err(error) = fs::remove_dir_all(dir) {
                    first_error.get_or_insert_with(|| {
                        format!("failed to remove part directory {}: {error}", dir.display())
                    });
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                first_error.get_or_insert_with(|| {
                    format!(
                        "failed to inspect part directory {}: {error}",
                        dir.display()
                    )
                });
            }
        }
    }

    for parent in parents {
        if let Err(error) = fsync_dir(&parent) {
            first_error.get_or_insert_with(|| {
                format!("failed to fsync part parent {}: {error}", parent.display())
            });
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub fn load_part(dir: &Path) -> Result<Part, String> {
    let meta_str = fs::read_to_string(dir.join(META_FILE)).map_err(|e| e.to_string())?;
    let meta_file: MetaFile = serde_json::from_str(&meta_str).map_err(|e| e.to_string())?;
    let actual_metadata_crc = metadata_crc32(&meta_file)?;
    if actual_metadata_crc != meta_file.integrity.metadata_crc32 {
        return Err(format!(
            "metadata checksum mismatch: expected {}, got {}",
            meta_file.integrity.metadata_crc32, actual_metadata_crc
        ));
    }
    validate_meta_file(dir, &meta_file)?;
    let streams: Vec<Labels> = meta_file
        .streams
        .iter()
        .map(|pairs| pairs.iter().cloned().collect())
        .collect();
    let meta = PartMeta {
        id: meta_file.id,
        partition: meta_file.partition,
        min_ts_ns: meta_file.min_ts_ns,
        max_ts_ns: meta_file.max_ts_ns,
        row_count: meta_file.row_count,
        row_group_count: meta_file.row_group_count,
        row_group_min_ts: meta_file.row_group_min_ts,
        row_group_max_ts: meta_file.row_group_max_ts,
        stream_labels: meta_file.stream_labels,
        streams,
        integrity: meta_file.integrity,
    };
    Ok(Part {
        dir: dir.to_path_buf(),
        meta,
    })
}

fn validate_meta_file(dir: &Path, meta: &MetaFile) -> Result<(), String> {
    let dir_id = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid part directory name: {}", dir.display()))?;
    if meta.id != dir_id {
        return Err(format!(
            "part metadata id {} does not match directory {dir_id}",
            meta.id
        ));
    }
    let dir_partition = dir
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid part partition directory: {}", dir.display()))?;
    if meta.partition != dir_partition {
        return Err(format!(
            "part metadata partition {} does not match directory {dir_partition}",
            meta.partition
        ));
    }
    if meta.row_count == 0 || meta.row_group_count == 0 {
        return Err("part metadata must contain at least one row and row group".to_string());
    }
    if meta.min_ts_ns > meta.max_ts_ns
        || partition_of(meta.min_ts_ns) != meta.partition
        || partition_of(meta.max_ts_ns) != meta.partition
    {
        return Err("part metadata has an invalid timestamp range".to_string());
    }
    let row_group_count = meta.row_group_count as usize;
    if meta.row_group_min_ts.len() != row_group_count
        || meta.row_group_max_ts.len() != row_group_count
        || meta.row_group_min_ts.first() != Some(&meta.min_ts_ns)
        || meta.row_group_max_ts.last() != Some(&meta.max_ts_ns)
    {
        return Err("part metadata has inconsistent row-group bounds".to_string());
    }
    for index in 0..row_group_count {
        if meta.row_group_min_ts[index] > meta.row_group_max_ts[index]
            || (index > 0 && meta.row_group_min_ts[index] < meta.row_group_max_ts[index - 1])
        {
            return Err("part metadata row-group bounds are not sorted".to_string());
        }
    }

    let mut expected_labels = BTreeSet::new();
    for stream in &meta.streams {
        let mut names = BTreeSet::new();
        for (name, _) in stream {
            crate::proto::validate_label_name(name)?;
            if !names.insert(name) {
                return Err(format!("duplicate label {name} in part stream metadata"));
            }
            expected_labels.insert(name.clone());
        }
    }
    let actual_labels: BTreeSet<_> = meta.stream_labels.iter().cloned().collect();
    if actual_labels.len() != meta.stream_labels.len() || actual_labels != expected_labels {
        return Err("part stream label metadata is inconsistent".to_string());
    }
    Ok(())
}

pub fn discover_parts(parts_root: &Path) -> Result<Vec<Part>, String> {
    let mut parts = Vec::new();
    let partitions = match fs::read_dir(parts_root) {
        Ok(partitions) => partitions,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(parts),
        Err(error) => {
            return Err(format!(
                "failed to read parts root {}: {error}",
                parts_root.display()
            ));
        }
    };
    let partition_entries: Vec<_> = partitions.collect::<Result<_, _>>().map_err(|e| {
        format!(
            "failed to enumerate parts root {}: {e}",
            parts_root.display()
        )
    })?;

    // Pass 1 only reads and validates merge markers. Deleting while walking
    // the directory tree can erase an intermediate part's tombstone before it
    // is inspected, which would let an older generation reappear. Build the
    // complete replacement graph first and clean it only after its transitive
    // closure is known.
    let mut tombstoned_dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
    let mut invalid_merge_dirs: std::collections::HashSet<PathBuf> =
        std::collections::HashSet::new();
    let mut tombstone_edges: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
    let mut valid_replacements: Vec<PathBuf> = Vec::new();

    for partition_entry in &partition_entries {
        let partition_path = partition_entry.path();
        if !partition_path.is_dir() {
            continue;
        }
        let name = partition_entry.file_name();
        if name == ".tmp" {
            continue;
        }
        let part_entries: Vec<_> = fs::read_dir(&partition_path)
            .map_err(|e| format!("failed to read partition {}: {e}", partition_path.display()))?
            .collect::<Result<_, _>>()
            .map_err(|e| {
                format!(
                    "failed to enumerate partition {}: {e}",
                    partition_path.display()
                )
            })?;

        for part_entry in &part_entries {
            let part_dir = part_entry.path();
            if !part_dir.is_dir() {
                continue;
            }
            if !part_dir.join(META_FILE).exists() {
                continue;
            }
            if !part_dir.join(MERGE_TOMBSTONE_FILE).exists() {
                continue;
            }
            match read_merge_tombstone_dirs(&part_dir, parts_root) {
                Ok(old_dirs) => {
                    let part_key = canonical_path(&part_dir);
                    tombstone_edges.insert(part_key, old_dirs);
                    // Do not delete the old parts until the replacement part
                    // itself can be opened. A corrupt replacement must not
                    // turn a recoverable merge into data loss on restart.
                    let replacement_valid = match load_part(&part_dir).and_then(PartReader::open) {
                        Ok(_) => true,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                ?part_dir,
                                "discover: invalid merge replacement; keeping old parts"
                            );
                            false
                        }
                    };
                    if !replacement_valid {
                        invalid_merge_dirs.insert(canonical_path(&part_dir));
                        continue;
                    }
                    valid_replacements.push(part_dir);
                }
                Err(e) => {
                    // A malformed marker is treated conservatively: skip the
                    // replacement and retain all ordinary parts.
                    invalid_merge_dirs.insert(canonical_path(&part_dir));
                    tracing::warn!(error = %e, ?part_dir, "discover: failed to read merge tombstone; keeping old parts");
                }
            }
        }
    }

    let mut pending_old_dirs = std::collections::VecDeque::new();
    for replacement_dir in &valid_replacements {
        let replacement_key = canonical_path(replacement_dir);
        if let Some(old_dirs) = tombstone_edges.get(&replacement_key) {
            pending_old_dirs.extend(old_dirs.iter().cloned());
        }
    }

    let mut cleanup_dirs = Vec::new();
    while let Some(old_dir) = pending_old_dirs.pop_front() {
        let old_key = canonical_path(&old_dir);
        if !tombstoned_dirs.insert(old_key.clone()) {
            continue;
        }
        cleanup_dirs.push(old_dir);
        if let Some(previous_generation) = tombstone_edges.get(&old_key) {
            pending_old_dirs.extend(previous_generation.iter().cloned());
        }
    }

    if let Err(error) = remove_part_dirs(&cleanup_dirs) {
        tracing::warn!(
            error = %error,
            "discover: tombstoned part cleanup incomplete"
        );
        // Keep all surviving markers. A later restart reconstructs the same
        // closure and retries any paths that could not be removed this time.
    } else {
        for replacement_dir in &valid_replacements {
            if replacement_dir.exists()
                && let Err(error) = remove_merge_tombstone(replacement_dir)
            {
                tracing::warn!(
                    %error,
                    ?replacement_dir,
                    "discover: failed to remove merge tombstone file"
                );
            }
        }
    }

    // Pass 2 loads only the surviving generation. Tombstoned paths remain
    // hidden even when their physical deletion failed.
    for partition_entry in &partition_entries {
        let partition_path = partition_entry.path();
        if !partition_path.is_dir() {
            continue;
        }
        let name = partition_entry.file_name();
        if name == ".tmp" {
            continue;
        }
        let part_entries = fs::read_dir(&partition_path)
            .map_err(|e| format!("failed to read partition {}: {e}", partition_path.display()))?;
        for part_entry in part_entries {
            let part_entry = part_entry.map_err(|e| {
                format!(
                    "failed to enumerate partition {}: {e}",
                    partition_path.display()
                )
            })?;
            let part_dir = part_entry.path();
            if !part_dir.is_dir() {
                continue;
            }
            if !part_dir.exists() {
                continue;
            }
            if !part_dir.join(META_FILE).exists() {
                return Err(format!("part is missing metadata: {}", part_dir.display()));
            }
            let part_key = canonical_path(&part_dir);
            if tombstoned_dirs.contains(&part_key) || invalid_merge_dirs.contains(&part_key) {
                continue;
            }
            let part = load_part(&part_dir)
                .map_err(|e| format!("failed to load part {}: {e}", part_dir.display()))?;
            parts.push(part);
        }
    }
    Ok(parts)
}

pub fn cleanup_tmp(parts_root: &Path) -> Result<(), String> {
    let root_metadata = fs::symlink_metadata(parts_root).map_err(|error| error.to_string())?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(format!(
            "refusing unsafe parts root {}",
            parts_root.display()
        ));
    }
    let canonical_root = fs::canonicalize(parts_root).map_err(|error| error.to_string())?;
    let tmp = parts_root.join(".tmp");
    let metadata = match fs::symlink_metadata(&tmp) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!("refusing unsafe tmp directory {}", tmp.display()));
    }
    let canonical_tmp = fs::canonicalize(&tmp).map_err(|error| error.to_string())?;
    if !canonical_tmp.starts_with(&canonical_root) {
        return Err(format!(
            "tmp directory escapes parts root: {}",
            tmp.display()
        ));
    }
    fs::remove_dir_all(&tmp).map_err(|error| error.to_string())?;
    fsync_dir(parts_root).map_err(|error| error.to_string())
}

#[derive(Clone)]
pub struct PreadReader {
    file: Arc<fs::File>,
    len: u64,
}

impl PreadReader {
    pub fn new(file: fs::File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        Ok(Self {
            file: Arc::new(file),
            len,
        })
    }
}

impl Length for PreadReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for PreadReader {
    type T = PreadCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(PreadCursor {
            file: self.file.clone(),
            pos: start,
            len: self.len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let mut buf = vec![0u8; length];
        self.file.read_exact_at(&mut buf, start).map_err(|e| {
            parquet::errors::ParquetError::from(std::io::Error::new(e.kind(), e.to_string()))
        })?;
        Ok(Bytes::from(buf))
    }
}

pub struct PreadCursor {
    file: Arc<fs::File>,
    pos: u64,
    len: u64,
}

impl Read for PreadCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.len.saturating_sub(self.pos) as usize;
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = buf.len().min(remaining);
        let n = self.file.read_at(&mut buf[..to_read], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

pub struct PartReader {
    part: Part,
    bloom: Vec<BloomFilter>,
    exact_field_bloom: Option<Vec<BloomFilter>>,
    exact_field_bloom_canonical: bool,
    stream_index: StreamMap,
    stream_labels: Vec<String>,
}

struct DecodedBlooms {
    line: Vec<BloomFilter>,
    exact_fields: Option<Vec<BloomFilter>>,
    exact_fields_canonical: bool,
}

fn validate_sidecar_files(part: &Part) -> Result<(), String> {
    let files = [
        (
            BLOOM_FILE,
            part.bloom_path(),
            part.meta.integrity.bloom_crc32,
        ),
        (
            STREAM_INDEX_FILE,
            part.stream_index_path(),
            part.meta.integrity.stream_index_crc32,
        ),
    ];
    for (name, path, expected) in files {
        let actual = file_crc32(&path).map_err(|error| {
            format!(
                "failed to checksum {name} for part {}: {error}",
                part.meta.id
            )
        })?;
        if actual != expected {
            return Err(format!(
                "{name} checksum mismatch for part {}: expected {expected}, got {actual}",
                part.meta.id
            ));
        }
    }
    Ok(())
}

fn open_part_data(
    part: &Part,
    validate_checksum: bool,
) -> Result<(PreadReader, ArrowReaderMetadata), String> {
    if validate_checksum {
        let actual = file_crc32(&part.data_path()).map_err(|error| {
            format!(
                "failed to checksum {DATA_FILE} for part {}: {error}",
                part.meta.id
            )
        })?;
        if actual != part.meta.integrity.data_crc32 {
            return Err(format!(
                "{DATA_FILE} checksum mismatch for part {}: expected {}, got {actual}",
                part.meta.id, part.meta.integrity.data_crc32
            ));
        }
    }

    let data_file =
        PreadReader::new(fs::File::open(part.data_path()).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let arrow_reader_metadata =
        ArrowReaderMetadata::load(&data_file, Default::default()).map_err(|e| e.to_string())?;

    let parquet_rg_count = arrow_reader_metadata.metadata().num_row_groups();
    if parquet_rg_count != part.meta.row_group_count as usize {
        return Err(format!(
            "row group count mismatch for part {}: parquet footer says {}, meta says {}",
            part.meta.id, parquet_rg_count, part.meta.row_group_count
        ));
    }
    let parquet_row_count = arrow_reader_metadata.metadata().file_metadata().num_rows();
    if parquet_row_count != part.meta.row_count as i64 {
        return Err(format!(
            "row count mismatch for part {}: parquet footer says {}, meta says {}",
            part.meta.id, parquet_row_count, part.meta.row_count
        ));
    }
    let mut expected_fields = vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("_msg", DataType::Utf8, false),
    ];
    for label in &part.meta.stream_labels {
        expected_fields.push(Field::new(label, DataType::Utf8, true));
    }
    expected_fields.push(Field::new("structured_metadata", DataType::Utf8, true));
    let expected_schema = Schema::new(expected_fields);
    if arrow_reader_metadata.schema().fields() != expected_schema.fields() {
        return Err(format!(
            "parquet schema does not match metadata for part {}: expected {:?}, got {:?}",
            part.meta.id,
            expected_schema.fields(),
            arrow_reader_metadata.schema().fields()
        ));
    }
    Ok((data_file, arrow_reader_metadata))
}

fn validate_stream_index(part: &Part, index: &StreamMap) -> Result<(), String> {
    let expected: BTreeSet<(String, String)> = part
        .meta
        .streams
        .iter()
        .flat_map(|labels| {
            labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
        })
        .collect();
    let mut indexed = BTreeSet::new();
    for (name, values) in index {
        for (value, bitmap) in values {
            if bitmap.is_empty() || bitmap.iter().any(|rg| rg >= part.meta.row_group_count) {
                return Err(format!(
                    "stream index has invalid row-group bitmap for part {}",
                    part.meta.id
                ));
            }
            indexed.insert((name.clone(), value.clone()));
        }
    }
    if indexed != expected {
        return Err(format!(
            "stream index labels do not match metadata for part {}",
            part.meta.id
        ));
    }
    Ok(())
}

impl PartReader {
    pub fn open(part: Part) -> Result<Self, String> {
        Self::open_internal(part, true)
    }

    /// Opens the metadata and indexes for an object-store cached part. The
    /// Parquet body may have been evicted and is opened only while a query is
    /// actively reading it.
    pub fn open_cached(part: Part) -> Result<Self, String> {
        Self::open_internal(part, false)
    }

    fn open_internal(part: Part, require_data: bool) -> Result<Self, String> {
        validate_sidecar_files(&part)?;
        if part.meta.row_group_count == 0
            || part.meta.row_group_min_ts.len() != part.meta.row_group_count as usize
            || part.meta.row_group_max_ts.len() != part.meta.row_group_count as usize
        {
            return Err(format!(
                "row group metadata length mismatch for part {}",
                part.meta.id
            ));
        }
        let bloom_bytes = fs::read(part.bloom_path()).map_err(|e| e.to_string())?;
        let decoded_blooms = decode_blooms(&bloom_bytes, part.meta.row_group_count as usize)?;
        let stream_index =
            decode_stream_index(&fs::read(part.stream_index_path()).map_err(|e| e.to_string())?)?;
        validate_stream_index(&part, &stream_index)?;
        let stream_labels = part.meta.stream_labels.clone();
        if require_data || part.data_path().exists() {
            open_part_data(&part, true)?;
        }
        Ok(Self {
            part,
            bloom: decoded_blooms.line,
            exact_field_bloom: decoded_blooms.exact_fields,
            exact_field_bloom_canonical: decoded_blooms.exact_fields_canonical,
            stream_index,
            stream_labels,
        })
    }

    pub fn part(&self) -> &Part {
        &self.part
    }

    pub fn meta(&self) -> &PartMeta {
        &self.part.meta
    }

    pub fn label_names(&self) -> &[String] {
        &self.stream_labels
    }

    pub fn label_values(&self, name: &str) -> Vec<String> {
        self.stream_index
            .get(name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn series(&self, matchers: &[LabelMatcher]) -> Vec<Labels> {
        self.part
            .meta
            .streams
            .iter()
            .filter(|labels| matchers.iter().all(|m| m.matches(labels)))
            .cloned()
            .collect()
    }

    pub fn query(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                matchers,
                ExactFieldPruning::new(line_filters, &[]),
                start_ns,
                end_ns,
                limit,
                forward,
                None,
                None,
            )?
            .results)
    }

    /// Uses exact-field predicates only for row-group pruning. Bloom filters
    /// can return false positives, so the caller remains responsible for
    /// evaluating the predicates against each returned entry.
    pub fn query_with_exact_field_pruning(
        &self,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                matchers, pruning, start_ns, end_ns, limit, forward, None, None,
            )?
            .results)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning_and_scan_limit(
        &self,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        self.query_internal(
            matchers,
            pruning.line_filters,
            pruning.exact_fields,
            QueryTimeRange {
                start_ns,
                end_ns,
                include_end: true,
            },
            limit,
            forward,
            scan_limit,
            cancellation,
        )
    }

    pub fn query_all(&self) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_internal(
                &[],
                &[],
                &[],
                QueryTimeRange {
                    start_ns: i64::MIN,
                    end_ns: i64::MAX,
                    include_end: true,
                },
                usize::MAX,
                true,
                None,
                None,
            )?
            .results)
    }

    fn select_row_groups(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        time_range: QueryTimeRange,
    ) -> Vec<u32> {
        self.select_row_groups_with_exact_fields(matchers, line_filters, &[], time_range)
    }

    fn select_row_groups_with_exact_fields(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        time_range: QueryTimeRange,
    ) -> Vec<u32> {
        let mut selected = Vec::with_capacity(self.bloom.len());
        for rg in 0..self.bloom.len() as u32 {
            let rgu = rg as usize;
            if !(self.part.meta.row_group_max_ts[rgu] >= time_range.start_ns
                && (self.part.meta.row_group_min_ts[rgu] < time_range.end_ns
                    || (time_range.include_end
                        && self.part.meta.row_group_min_ts[rgu] == time_range.end_ns)))
            {
                continue;
            }
            if !row_group_matches_index(rg, matchers, &self.stream_index) {
                continue;
            }
            if !self.bloom_prune(rgu, line_filters) {
                continue;
            }
            if !self.exact_field_bloom_prune(rgu, exact_fields) {
                continue;
            }
            selected.push(rg);
        }
        selected
    }

    /// Returns whether any row group can satisfy the catalog-visible portion
    /// of a query. This does not open `data.parquet`, so object-store callers
    /// can use it before deciding which evicted bodies to restore.
    pub fn may_match_exact_fields(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        start_ns: i64,
        end_ns: i64,
    ) -> bool {
        !self
            .select_row_groups_with_exact_fields(
                matchers,
                line_filters,
                exact_fields,
                QueryTimeRange {
                    start_ns,
                    end_ns,
                    include_end: true,
                },
            )
            .is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    fn query_internal(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        time_range: QueryTimeRange,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        let rg_count = self.bloom.len();
        if rg_count == 0 {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
            });
        }
        if limit == 0 {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
            });
        }

        let selected = if exact_fields.is_empty() {
            self.select_row_groups(matchers, line_filters, time_range)
        } else {
            self.select_row_groups_with_exact_fields(
                matchers,
                line_filters,
                exact_fields,
                time_range,
            )
        };
        if selected.is_empty() {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
            });
        }
        let mut sorted_selected = selected.clone();
        sorted_selected.sort_unstable();
        if !forward {
            sorted_selected.reverse();
        }

        let mut collected: Vec<(Labels, LogEntry)> = Vec::new();
        let mut scanned_rows = 0usize;

        let batch_size = scan_limit
            .into_iter()
            .chain(forward.then_some(limit))
            .min()
            .map(|value| value.clamp(1, 1024))
            .unwrap_or(1024);
        'row_groups: for &row_group in &sorted_selected {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                break;
            }
            // Parquet may normalize a multi-row-group selection back to file
            // order. Build one reader per group so backward scans really start
            // at the newest group and can stop once the limit is satisfied.
            let (data_file, arrow_reader_metadata) = open_part_data(&self.part, false)?;
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                data_file,
                arrow_reader_metadata,
            )
            .with_batch_size(batch_size);
            let reader = builder
                .with_row_groups(vec![row_group as usize])
                .build()
                .map_err(|e| e.to_string())?;

            // Parquet yields batches in row order even when a single row
            // group is selected. Buffer only this row group and reverse the
            // batches as well as the rows; reversing rows inside each batch
            // alone would return the oldest batch first for backward scans.
            let mut batches: Vec<_> = reader
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
            if !forward {
                batches.reverse();
            }
            for batch in batches {
                if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    break 'row_groups;
                }
                let rows_to_scan = scan_limit
                    .map(|limit| limit.saturating_sub(scanned_rows).min(batch.num_rows()))
                    .unwrap_or(batch.num_rows());
                scanned_rows = scanned_rows.saturating_add(rows_to_scan);
                let ts = batch.column(0).as_primitive::<Int64Type>();
                let msg = batch.column(1).as_string::<i32>();
                let sm_col_idx = 2 + self.stream_labels.len();
                let sm = batch.column(sm_col_idx).as_string::<i32>();
                let label_cols: Vec<&StringArray> = (0..self.stream_labels.len())
                    .map(|i| batch.column(2 + i).as_string::<i32>())
                    .collect();

                let row_start = if forward {
                    0
                } else {
                    batch.num_rows().saturating_sub(rows_to_scan)
                };
                let row_end = row_start + rows_to_scan;
                let row_indices: Box<dyn Iterator<Item = usize>> = if forward {
                    Box::new(row_start..row_end)
                } else {
                    Box::new((row_start..row_end).rev())
                };
                for i in row_indices {
                    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        break 'row_groups;
                    }
                    let ts_val = ts.value(i);
                    if ts_val < time_range.start_ns
                        || ts_val > time_range.end_ns
                        || (!time_range.include_end && ts_val == time_range.end_ns)
                    {
                        continue;
                    }
                    let mut labels: Labels = BTreeMap::new();
                    for (j, label_name) in self.stream_labels.iter().enumerate() {
                        if !label_cols[j].is_null(i) {
                            labels.insert(label_name.clone(), label_cols[j].value(i).to_string());
                        }
                    }
                    if !matchers.iter().all(|m| m.matches(&labels)) {
                        continue;
                    }
                    let line = msg.value(i).to_string();
                    if !line_filters.iter().all(|f| f.matches(&line)) {
                        continue;
                    }
                    let structured_metadata = if sm.is_null(i) {
                        Vec::new()
                    } else {
                        serde_json::from_str(sm.value(i)).map_err(|error| {
                        format!(
                            "invalid structured metadata in part {} at timestamp {ts_val}: {error}",
                            self.part.meta.id
                        )
                    })?
                    };
                    collected.push((
                        labels,
                        LogEntry {
                            timestamp_ns: ts_val,
                            line,
                            structured_metadata,
                        },
                    ));
                    if forward && collected.len() >= limit {
                        break 'row_groups;
                    }
                }
                if scan_limit.is_some_and(|limit| scanned_rows >= limit) {
                    break 'row_groups;
                }
            }
            if !forward && collected.len() >= limit {
                break;
            }
        }

        if forward {
            collected.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            collected.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        collected.truncate(limit);

        Ok(QueryResult {
            results: group_by_labels(collected),
            scanned_rows,
        })
    }
}

impl PartReader {
    fn bloom_prune(&self, rg: usize, line_filters: &[LineFilter]) -> bool {
        for f in line_filters {
            if let LineFilter::Contains(s) = f
                && !self.bloom[rg].might_contain_substr(s)
            {
                return false;
            }
        }
        true
    }

    fn exact_field_bloom_prune(&self, rg: usize, exact_fields: &[ExactFieldPredicate]) -> bool {
        let Some(blooms) = &self.exact_field_bloom else {
            return true;
        };
        exact_fields.iter().all(|predicate| {
            if predicate.canonical && !self.exact_field_bloom_canonical {
                return true;
            }
            // Stream labels are visible to pipeline field filters, but are
            // intentionally not part of the exact-field bloom. The stream
            // index handles label matchers; skipping this predicate here is
            // required to avoid pruning a row group that contains the label.
            if self
                .stream_labels
                .iter()
                .any(|name| name == &predicate.name)
            {
                return true;
            }
            // Field-filter execution may treat an absent field as an empty
            // string. Absence is not represented in the bloom, so an empty
            // equality cannot safely reject a row group.
            if predicate.value.is_empty() {
                return true;
            }
            encode_exact_field_token(&predicate.name, &predicate.value)
                .map(|token| blooms[rg].contains(&token))
                // An unrepresentable predicate must conservatively scan.
                .unwrap_or(true)
        })
    }
}

fn row_group_matches_index(rg: u32, matchers: &[LabelMatcher], index: &StreamMap) -> bool {
    for m in matchers {
        match m.op {
            MatcherOp::Eq => {
                // {label=""}는 라벨 부재를 매치한다. stream index에는 라벨이
                // 없는 스트림의 entry가 기록되지 않으므로, value가 빈 문자열이면
                // 보수적으로 통과시킨다 (memtable과의 정합성).
                if m.value.is_empty() {
                    continue;
                }
                let Some(values) = index.get(&m.name) else {
                    return false;
                };
                let Some(bitmap) = values.get(&m.value) else {
                    return false;
                };
                if !bitmap.contains(rg) {
                    return false;
                }
            }
            MatcherOp::Neq | MatcherOp::Re | MatcherOp::NRe => {
                // conservative: cannot precisely prune with these ops
            }
        }
    }
    true
}

fn decode_blooms(buf: &[u8], expected_count: usize) -> Result<DecodedBlooms, String> {
    if buf.len() < 8 {
        return Err("bloom file too short".to_string());
    }
    let (has_exact_fields, exact_fields_canonical) = if &buf[0..4] == BLOOM_MAGIC_V1 {
        (false, false)
    } else if &buf[0..4] == BLOOM_MAGIC_V2 {
        (true, false)
    } else if &buf[0..4] == BLOOM_MAGIC_V3 {
        (true, true)
    } else {
        return Err("bloom magic mismatch".to_string());
    };
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if count != expected_count {
        return Err(format!(
            "row group count mismatch: bloom says {count}, metadata says {expected_count}"
        ));
    }
    let mut pos = 8;
    let mut line = Vec::with_capacity(count);
    let mut exact_fields = has_exact_fields.then(|| Vec::with_capacity(count));
    for _ in 0..count {
        line.push(decode_length_prefixed_bloom(buf, &mut pos)?);
        if let Some(exact_fields) = &mut exact_fields {
            exact_fields.push(decode_length_prefixed_bloom(buf, &mut pos)?);
        }
    }
    if pos != buf.len() {
        return Err("bloom file has trailing bytes".to_string());
    }
    Ok(DecodedBlooms {
        line,
        exact_fields,
        exact_fields_canonical,
    })
}

fn decode_length_prefixed_bloom(buf: &[u8], pos: &mut usize) -> Result<BloomFilter, String> {
    let length_end = pos
        .checked_add(4)
        .ok_or_else(|| "bloom length overflow".to_string())?;
    let length_bytes: [u8; 4] = buf
        .get(*pos..length_end)
        .ok_or_else(|| "bloom length truncated".to_string())?
        .try_into()
        .map_err(|_| "bloom length truncated".to_string())?;
    let len = u32::from_le_bytes(length_bytes) as usize;
    *pos = length_end;
    let payload_end = pos
        .checked_add(len)
        .ok_or_else(|| "bloom payload length overflow".to_string())?;
    let payload = buf
        .get(*pos..payload_end)
        .ok_or_else(|| "bloom payload truncated".to_string())?;
    *pos = payload_end;
    BloomFilter::decode(payload)
}

fn decode_stream_index(buf: &[u8]) -> Result<StreamMap, String> {
    if buf.len() < 8 {
        return Err("stream index too short".to_string());
    }
    if &buf[0..4] != STREAM_MAGIC {
        return Err("stream index magic mismatch".to_string());
    }
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let mut pos = 8;
    let mut map: StreamMap = BTreeMap::new();
    for _ in 0..count {
        if pos + 4 > buf.len() {
            return Err("stream index name length truncated".to_string());
        }
        let name_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + name_len > buf.len() {
            return Err("stream index name truncated".to_string());
        }
        let name = std::str::from_utf8(&buf[pos..pos + name_len])
            .map_err(|e| e.to_string())?
            .to_string();
        pos += name_len;
        if pos + 4 > buf.len() {
            return Err("stream index value length truncated".to_string());
        }
        let value_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + value_len > buf.len() {
            return Err("stream index value truncated".to_string());
        }
        let value = std::str::from_utf8(&buf[pos..pos + value_len])
            .map_err(|e| e.to_string())?
            .to_string();
        pos += value_len;
        if pos + 4 > buf.len() {
            return Err("stream index bitmap length truncated".to_string());
        }
        let bm_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + bm_len > buf.len() {
            return Err("stream index bitmap truncated".to_string());
        }
        let bitmap =
            RoaringBitmap::deserialize_from(&buf[pos..pos + bm_len]).map_err(|e| e.to_string())?;
        pos += bm_len;
        map.entry(name).or_default().insert(value, bitmap);
    }
    if pos != buf.len() {
        return Err("stream index has trailing bytes".to_string());
    }
    Ok(map)
}

pub fn group_by_labels(collected: Vec<(Labels, LogEntry)>) -> Vec<StreamResult> {
    let mut grouped: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
    for (labels, entry) in collected {
        grouped.entry(labels).or_default().push(entry);
    }
    grouped
        .into_iter()
        .map(|(labels, entries)| StreamResult { labels, entries })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_btf1(line_blooms: &[BloomFilter]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BLOOM_MAGIC_V1);
        bytes.extend_from_slice(&(line_blooms.len() as u32).to_le_bytes());
        for bloom in line_blooms {
            let encoded = bloom.encode();
            bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&encoded);
        }
        bytes
    }

    fn encode_btf2(line_blooms: &[BloomFilter], exact_blooms: &[BloomFilter]) -> Vec<u8> {
        assert_eq!(line_blooms.len(), exact_blooms.len());
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BLOOM_MAGIC_V2);
        bytes.extend_from_slice(&(line_blooms.len() as u32).to_le_bytes());
        for (line, exact) in line_blooms.iter().zip(exact_blooms) {
            for bloom in [line, exact] {
                let encoded = bloom.encode();
                bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&encoded);
            }
        }
        bytes
    }

    fn rewrite_part_bloom_as_v1(part: &Part) {
        let bytes = fs::read(part.bloom_path()).unwrap();
        let decoded = decode_blooms(&bytes, part.meta.row_group_count as usize).unwrap();
        let legacy = encode_btf1(&decoded.line);
        fs::write(part.bloom_path(), &legacy).unwrap();

        let meta_path = part.meta_path();
        let mut meta: MetaFile =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.integrity.bloom_crc32 = crc32fast::hash(&legacy);
        meta.integrity.metadata_crc32 = 0;
        meta.integrity.metadata_crc32 = metadata_crc32(&meta).unwrap();
        fs::write(meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    }

    fn rewrite_part_bloom_as_v2(part: &Part) {
        let bytes = fs::read(part.bloom_path()).unwrap();
        let decoded = decode_blooms(&bytes, part.meta.row_group_count as usize).unwrap();
        let legacy = encode_btf2(&decoded.line, decoded.exact_fields.as_ref().unwrap());
        fs::write(part.bloom_path(), &legacy).unwrap();

        let meta_path = part.meta_path();
        let mut meta: MetaFile =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta.integrity.bloom_crc32 = crc32fast::hash(&legacy);
        meta.integrity.metadata_crc32 = 0;
        meta.integrity.metadata_crc32 = metadata_crc32(&meta).unwrap();
        fs::write(meta_path, serde_json::to_string_pretty(&meta).unwrap()).unwrap();
    }

    fn make_rows() -> Vec<Row> {
        let mut labels1: Labels = BTreeMap::new();
        labels1.insert("app".to_string(), "test".to_string());
        labels1.insert("host".to_string(), "h1".to_string());
        let mut labels2: Labels = BTreeMap::new();
        labels2.insert("app".to_string(), "other".to_string());
        vec![
            Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: labels1.clone(),
                line: "error connecting to database".to_string(),
                structured_metadata: vec![],
            },
            Row {
                timestamp_ns: 1_700_000_001_000_000_000,
                labels: labels1,
                line: "all good now".to_string(),
                structured_metadata: vec![("trace_id".to_string(), "abc".to_string())],
            },
            Row {
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: labels2,
                line: "other app log line".to_string(),
                structured_metadata: vec![],
            },
        ]
    }

    #[test]
    fn flush_then_query_roundtrip() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows.clone(), &tmp, 2).expect("flush");
        assert_eq!(parts.len(), 1);
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        // all
        let r = reader
            .query(&[], &[], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 3);

        // label matcher app="test"
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "test".to_string()).unwrap();
        let r = reader
            .query(std::slice::from_ref(&m), &[], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);

        // line filter "error"
        let f = LineFilter::Contains("error".to_string());
        let r = reader
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 1);

        // time range
        let r = reader
            .query(
                &[],
                &[],
                1_700_000_001_000_000_000,
                1_700_000_003_000_000_000,
                100,
                true,
            )
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn bloom_prunes_nonexistent_substring() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let f = LineFilter::Contains("zzzzzz-not-present".to_string());
        assert!(
            reader
                .select_row_groups(
                    &[],
                    std::slice::from_ref(&f),
                    QueryTimeRange {
                        start_ns: i64::MIN,
                        end_ns: i64::MAX,
                        include_end: true,
                    },
                )
                .is_empty(),
            "bloom miss must avoid selecting the parquet row group"
        );
        let r = reader
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn btf2_exact_field_bloom_prunes_structured_metadata_by_row_group() {
        let tmp = tempfile_dir();
        let mut rows = make_rows();
        rows[0].structured_metadata = vec![("trace_id".to_string(), "first".to_string())];
        rows[1].structured_metadata = vec![("trace_id".to_string(), "second".to_string())];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        assert_eq!(&fs::read(part.bloom_path()).unwrap()[..4], BLOOM_MAGIC_V3);
        let reader = PartReader::open(part).unwrap();

        let selected = reader.select_row_groups_with_exact_fields(
            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "second")],
            QueryTimeRange {
                start_ns: i64::MIN,
                end_ns: i64::MAX,
                include_end: true,
            },
        );
        assert_eq!(selected, vec![1]);

        // Stream labels are pipeline fields too, but are not in the exact
        // field bloom. Their predicate must therefore remain conservative.
        let app = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "test".to_string()).unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                std::slice::from_ref(&app),
                &[],
                &[ExactFieldPredicate::new("app", "test")],
                QueryTimeRange {
                    start_ns: i64::MIN,
                    end_ns: i64::MAX,
                    include_end: true,
                },
            ),
            vec![0, 1]
        );

        assert!(!reader.may_match_exact_fields(
            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "not-present")],
            i64::MIN,
            i64::MAX,
        ));
        assert!(reader.may_match_exact_fields(
            &[],
            &[],
            &[ExactFieldPredicate::new("missing", "")],
            i64::MIN,
            i64::MAX,
        ));
    }

    #[test]
    fn btf2_exact_field_bloom_indexes_parser_scalars_without_raw_substring_assumptions() {
        let tmp = tempfile_dir();
        let mut rows = make_rows();
        rows[0].line = r#"{"user":"\u0061lice","namespace:key":"value"}"#.to_string();
        rows[1].line = r#"user=bob elapsed=250ms"#.to_string();
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let time_range = QueryTimeRange {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            include_end: true,
        };

        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "user", "alice", true,
                )],
                time_range,
            ),
            vec![0]
        );
        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "user", "bob", true,
                )],
                time_range,
            ),
            vec![1]
        );
        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                &[],
                &[],
                &[ExactFieldPredicate::new_with_extraction(
                    "namespace_key",
                    "value",
                    true,
                )],
                time_range,
            ),
            vec![0]
        );
    }

    #[test]
    fn btf2_exact_field_bloom_indexes_canonical_numeric_and_duration_values() {
        let tmp = tempfile_dir();
        let rows = vec![
            Row {
                timestamp_ns: 1,
                labels: BTreeMap::new(),
                line: r#"{"value":9007199254740992,"elapsed":"1s"}"#.to_string(),
                structured_metadata: vec![],
            },
            Row {
                timestamp_ns: 2,
                labels: BTreeMap::new(),
                line: r#"{"value":9007199254740993,"elapsed":"1000ms"}"#.to_string(),
                structured_metadata: vec![],
            },
        ];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let range = QueryTimeRange {
            start_ns: i64::MIN,
            end_ns: i64::MAX,
            include_end: true,
        };

        let numeric = crate::logql::parse("{} | json | value=9007199254740993").unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                &[],
                &[],
                &numeric.exact_field_predicates(),
                range,
            ),
            vec![1]
        );

        let duration = crate::logql::parse("{} | json | elapsed=1s").unwrap();
        assert_eq!(
            reader.select_row_groups_with_exact_fields(
                &[],
                &[],
                &duration.exact_field_predicates(),
                range,
            ),
            vec![0, 1]
        );
    }

    #[test]
    fn btf2_indexes_scan_typed_equality_conservatively() {
        let tmp = tempfile_dir();
        let rows = vec![
            Row {
                timestamp_ns: 1,
                labels: BTreeMap::new(),
                line: r#"{"value":500.0}"#.to_string(),
                structured_metadata: vec![],
            },
            Row {
                timestamp_ns: 2,
                labels: BTreeMap::new(),
                line: r#"{"value":999}"#.to_string(),
                structured_metadata: vec![],
            },
        ];
        let part = flush_rows(rows, &tmp, 1).unwrap().remove(0);
        rewrite_part_bloom_as_v2(&part);
        let reader = PartReader::open(load_part(&part.dir).unwrap()).unwrap();
        let query = crate::logql::parse("{} | json | value=500").unwrap();
        let selected = reader.select_row_groups_with_exact_fields(
            &[],
            &[],
            &query.exact_field_predicates(),
            QueryTimeRange {
                start_ns: i64::MIN,
                end_ns: i64::MAX,
                include_end: true,
            },
        );
        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn btf1_part_loads_and_exact_fields_fall_back_to_scanning() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 1).unwrap().remove(0);
        rewrite_part_bloom_as_v1(&part);

        let legacy_part = load_part(&part.dir).unwrap();
        let reader = PartReader::open(legacy_part).unwrap();
        assert!(reader.exact_field_bloom.is_none());
        assert!(reader.may_match_exact_fields(
            &[],
            &[],
            &[ExactFieldPredicate::new("trace_id", "not-present")],
            i64::MIN,
            i64::MAX,
        ));
    }

    #[test]
    fn bloom_container_versions_reject_invalid_framing() {
        let bloom = BloomFilter::with_capacity(1, 0.01);
        let legacy = encode_btf1(&[bloom]);
        let decoded = decode_blooms(&legacy, 1).unwrap();
        assert_eq!(decoded.line.len(), 1);
        assert!(decoded.exact_fields.is_none());
        assert!(
            decode_blooms(&legacy, 2)
                .err()
                .unwrap()
                .contains("row group count mismatch")
        );

        let mut trailing = legacy.clone();
        trailing.push(0);
        assert!(
            decode_blooms(&trailing, 1)
                .err()
                .unwrap()
                .contains("trailing bytes")
        );

        let mut unknown = legacy.clone();
        unknown[..4].copy_from_slice(b"BTF9");
        assert!(
            decode_blooms(&unknown, 1)
                .err()
                .unwrap()
                .contains("magic mismatch")
        );

        let mut truncated_v2 = Vec::new();
        truncated_v2.extend_from_slice(BLOOM_MAGIC_V2);
        truncated_v2.extend_from_slice(&1u32.to_le_bytes());
        truncated_v2.extend_from_slice(&legacy[8..]);
        assert!(
            decode_blooms(&truncated_v2, 1)
                .err()
                .unwrap()
                .contains("length truncated")
        );
    }

    #[test]
    fn forward_limit_stops_physical_part_scan() {
        let tmp = tempfile_dir();
        let rows: Vec<Row> = (0..20)
            .map(|timestamp_ns| Row {
                timestamp_ns,
                labels: BTreeMap::new(),
                line: format!("line-{timestamp_ns}"),
                structured_metadata: vec![],
            })
            .collect();
        let part = flush_rows(rows, &tmp, 20).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let result = reader
            .query_with_exact_field_pruning_and_scan_limit(
                &[],
                ExactFieldPruning::new(&[], &[]),
                0,
                19,
                1,
                true,
                Some(100),
                None,
            )
            .unwrap();
        assert_eq!(result.scanned_rows, 1);
        assert_eq!(result.results[0].entries[0].timestamp_ns, 0);
    }

    #[test]
    fn scan_limit_stops_before_collecting_the_rest_of_a_part() {
        let tmp = tempfile_dir();
        let rows: Vec<Row> = (0..20)
            .map(|timestamp_ns| Row {
                timestamp_ns,
                labels: BTreeMap::new(),
                line: format!("line-{timestamp_ns}"),
                structured_metadata: vec![],
            })
            .collect();
        let part = flush_rows(rows, &tmp, 20).unwrap().remove(0);
        let reader = PartReader::open(part).unwrap();
        let result = reader
            .query_with_exact_field_pruning_and_scan_limit(
                &[],
                ExactFieldPruning::new(&[], &[]),
                0,
                19,
                usize::MAX,
                true,
                Some(3),
                None,
            )
            .unwrap();
        assert_eq!(result.scanned_rows, 3);
        assert_eq!(result.results[0].entries.len(), 3);
    }

    #[test]
    fn label_index_prunes_wrong_app() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "missing".to_string()).unwrap();
        assert!(
            reader
                .select_row_groups(
                    std::slice::from_ref(&m),
                    &[],
                    QueryTimeRange {
                        start_ns: i64::MIN,
                        end_ns: i64::MAX,
                        include_end: true,
                    },
                )
                .is_empty(),
            "stream-index miss must avoid selecting the parquet row group"
        );
        let r = reader
            .query(&[m], &[], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn discover_parts_after_flush() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let _ = flush_rows(rows, &tmp, 100).expect("flush");
        let parts = discover_parts(&tmp).unwrap();
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn backward_limit_returns_most_recent() {
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        let rows: Vec<Row> = (0..3_000)
            .map(|i| Row {
                timestamp_ns: 1_700_000_000_000_000_000 + i * 1_000_000_000,
                labels: labels.clone(),
                line: format!("line-{i:04}"),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 3_000).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        let r = reader
            .query(&[], &[], i64::MIN, i64::MAX, 3, false)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-2999", "line-2998", "line-2997"]);

        let r = reader
            .query(&[], &[], i64::MIN, i64::MAX, 3, true)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-0000", "line-0001", "line-0002"]);
    }

    #[test]
    fn series_returns_actual_label_sets() {
        let tmp = tempfile_dir();
        let rows = make_rows();
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        let all = reader.series(&[]);
        assert_eq!(all.len(), 2);
        let app_test: Vec<&Labels> = all
            .iter()
            .filter(|l| l.get("app").map(|v| v.as_str()) == Some("test"))
            .collect();
        assert_eq!(app_test.len(), 1);
        assert_eq!(app_test[0].get("host").map(|s| s.as_str()), Some("h1"));

        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "other".to_string()).unwrap();
        let r = reader.series(&[m]);
        assert_eq!(r.len(), 1);
        assert!(r[0].get("app").map(|v| v.as_str()) == Some("other"));
        assert!(!r[0].contains_key("host"));
    }

    #[test]
    fn concurrent_queries_on_same_part_no_race() {
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "concurrent".to_string());
        let rows: Vec<Row> = (0..50)
            .map(|i| Row {
                timestamp_ns: 1_700_000_000_000_000_000 + i * 1_000_000_000,
                labels: labels.clone(),
                line: format!("concurrent-line-{:02}", i),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 8).expect("flush");
        let reader = Arc::new(PartReader::open(parts.into_iter().next().unwrap()).expect("open"));

        let matcher =
            LabelMatcher::new("app".to_string(), MatcherOp::Eq, "concurrent".to_string()).unwrap();

        let num_threads = 16;
        let queries_per_thread = 50;
        let mut handles = Vec::with_capacity(num_threads);
        for thread_index in 0..num_threads {
            let reader = reader.clone();
            let matcher = matcher.clone();
            handles.push(std::thread::spawn(move || {
                let mut errors = 0u32;
                let mut wrong = 0u32;
                for q in 0..queries_per_thread {
                    let forward = (thread_index + q) % 2 == 0;
                    let limit = 3 + (q % 5);
                    let result = reader
                        .query(
                            std::slice::from_ref(&matcher),
                            &[],
                            i64::MIN,
                            i64::MAX,
                            limit,
                            forward,
                        )
                        .expect("query must not error");
                    let total: usize = result.iter().map(|s| s.entries.len()).sum();
                    if total == 0 {
                        errors += 1;
                    } else if total > limit {
                        wrong += 1;
                    }
                    if forward {
                        let first = result
                            .iter()
                            .flat_map(|s| s.entries.iter())
                            .map(|e| e.timestamp_ns)
                            .next()
                            .unwrap_or(0);
                        let expected_first = 1_700_000_000_000_000_000;
                        if first != expected_first {
                            wrong += 1;
                        }
                    } else {
                        let first = result
                            .iter()
                            .flat_map(|s| s.entries.iter())
                            .map(|e| e.timestamp_ns)
                            .next()
                            .unwrap_or(0);
                        let expected_first = 1_700_000_000_000_000_000 + 49 * 1_000_000_000;
                        if first != expected_first {
                            wrong += 1;
                        }
                    }
                }
                (errors, wrong)
            }));
        }
        let mut total_errors = 0u32;
        let mut total_wrong = 0u32;
        for h in handles {
            let (e, w) = h.join().expect("thread");
            total_errors += e;
            total_wrong += w;
        }
        assert_eq!(
            total_errors, 0,
            "some queries returned empty (race-induced decode failure)"
        );
        assert_eq!(
            total_wrong, 0,
            "some queries returned wrong ordering or count (race-induced corruption)"
        );
    }

    fn tempfile_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "loggytracy-test-{}-{}-{}",
            std::process::id(),
            c,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_tmp_rejects_symlinked_root_and_tmp_directory() {
        use std::os::unix::fs::symlink;

        let outside = tempfile_dir();
        let outside_tmp = outside.join(".tmp");
        fs::create_dir_all(&outside_tmp).unwrap();
        let sentinel = outside_tmp.join("sentinel");
        fs::write(&sentinel, b"must survive").unwrap();

        let link_parent = tempfile_dir();
        let linked_root = link_parent.join("parts");
        symlink(&outside, &linked_root).unwrap();
        let error = cleanup_tmp(&linked_root).unwrap_err();
        assert!(error.contains("unsafe parts root"));
        assert!(sentinel.exists());

        let normal_root = tempfile_dir();
        let linked_tmp = normal_root.join(".tmp");
        symlink(&outside_tmp, &linked_tmp).unwrap();
        let error = cleanup_tmp(&normal_root).unwrap_err();
        assert!(error.contains("unsafe tmp directory"));
        assert!(sentinel.exists());
    }

    #[test]
    fn label_eq_empty_matches_missing_label_in_part() {
        // {app=""}는 라벨 부재를 매치한다. memtable 경로는 이미 그렇게 동작하므로
        // part 경로에서도 보수적으로 통과시켜 정합성을 맞춰야 한다.
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("host".to_string(), "h1".to_string());
        // app 라벨 부재
        let rows: Vec<Row> = vec![Row {
            timestamp_ns: 1_700_000_000_000_000_000,
            labels,
            line: "no app label here".to_string(),
            structured_metadata: vec![],
        }];
        let parts = flush_rows(rows, &tmp, 100).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let m = LabelMatcher::new("app".to_string(), MatcherOp::Eq, "".to_string()).unwrap();
        let r = reader
            .query(&[m], &[], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 1,
            "{{app=\"\"}} should match streams without an app label in part"
        );
    }

    #[test]
    fn load_rejects_metadata_checksum_mismatch() {
        let tmp = tempfile_dir();
        let part = flush_rows(make_rows(), &tmp, 100).expect("flush").remove(0);
        let meta_path = part.meta_path();
        let mut meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        meta["stream_labels"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::String("ghost_label".to_string()));
        fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        assert!(load_part(&part.dir).is_err());
    }

    #[test]
    fn bloom_handles_large_trigram_volume_in_single_row_group() {
        // row group 8192행 × 라인당 수십 trigram → unique 항목이 row 수의 수배~수십배.
        // 이전 구현(capacity=row 수)은 fill ratio ~99%로 도달해 거짓 양성이 발생하지만,
        // 새 구현(unique trigram 수로 capacity)은 존재하지 않는 부분문자열을 정확히 프루닝.
        let tmp = tempfile_dir();
        let mut labels: Labels = BTreeMap::new();
        labels.insert("app".to_string(), "test".to_string());
        let rows: Vec<Row> = (0..8192usize)
            .map(|i| Row {
                timestamp_ns: 1_700_000_000_000_000_000 + (i as i64) * 1_000_000,
                labels: labels.clone(),
                line: format!(
                    "log line index {} some random words here for trigrams unique fragment {}",
                    i, i
                ),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 8192).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");
        let f = LineFilter::Contains("zzzzzz-not-present-substr".to_string());
        let r = reader
            .query(&[], &[f], i64::MIN, i64::MAX, 100, true)
            .expect("q");
        let total: usize = r.iter().map(|s| s.entries.len()).sum();
        assert_eq!(
            total, 0,
            "bloom should prune nonexistent substring even with 8192-row group"
        );
    }

    #[test]
    fn merge_flush_renames_part_with_tombstone_already_present() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let old = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "old".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);

        let merged = flush_rows_with_merge_tombstone(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "merged".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
            std::slice::from_ref(&old.dir),
        )
        .unwrap();
        let new_dir = &merged[0].dir;
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());
        assert_eq!(
            read_merge_tombstone(new_dir).unwrap(),
            vec![old.dir.strip_prefix(&parts_root).unwrap().to_path_buf()]
        );
    }

    #[test]
    fn discover_keeps_old_parts_when_tombstoned_replacement_is_corrupt() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let old = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "old".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let merged = flush_rows_with_merge_tombstone(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "merged".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
            std::slice::from_ref(&old.dir),
        )
        .unwrap();
        let new_dir = &merged[0].dir;
        std::fs::write(new_dir.join(BLOOM_FILE), b"corrupt").unwrap();

        let discovered = discover_parts(&parts_root).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].dir, old.dir);
        assert!(old.dir.exists());
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn merge_tombstone_cleanup_during_discover() {
        // merge에서 새 part rename + tombstone 기록 후 old_dirs 삭제 전 crash 시
        // 재시작 시 discover_parts가 tombstone을 발견해 old_dirs를 정리하고
        // 새 part 1개만 로드하는 것을 검증.
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();

        let mut l1: Labels = BTreeMap::new();
        l1.insert("app".to_string(), "old1".to_string());
        let mut l2: Labels = BTreeMap::new();
        l2.insert("app".to_string(), "old2".to_string());

        let parts1 = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: l1,
                line: "old1 line".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .expect("flush1");
        let parts2 = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: l2,
                line: "old2 line".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .expect("flush2");

        let old_dirs: Vec<PathBuf> = parts1
            .iter()
            .chain(parts2.iter())
            .map(|p| p.dir.clone())
            .collect();

        // 모의 merge: 두 스트림의 rows를 모아 새 part 생성
        let mut l3: Labels = BTreeMap::new();
        l3.insert("app".to_string(), "old1".to_string());
        let mut l4: Labels = BTreeMap::new();
        l4.insert("app".to_string(), "old2".to_string());
        let merged_rows = vec![
            Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: l3,
                line: "old1 line".to_string(),
                structured_metadata: vec![],
            },
            Row {
                timestamp_ns: 1_700_000_002_000_000_000,
                labels: l4,
                line: "old2 line".to_string(),
                structured_metadata: vec![],
            },
        ];
        let merged_parts = flush_rows(merged_rows, &parts_root, 100).expect("flush merged");

        let new_dir = merged_parts[0].dir.clone();
        write_merge_tombstone(&new_dir, &parts_root, &old_dirs).expect("tombstone write");

        // crash 시뮬레이션: old_dirs는 그대로, tombstone은 새 part 디렉터리에 있음.
        for old_dir in &old_dirs {
            assert!(
                old_dir.exists(),
                "old_dirs should still exist before discover"
            );
        }
        assert!(new_dir.join(MERGE_TOMBSTONE_FILE).exists());

        let discovered = discover_parts(&parts_root).unwrap();
        assert_eq!(
            discovered.len(),
            1,
            "tombstone cleanup should leave only the new part"
        );

        for old_dir in &old_dirs {
            assert!(
                !old_dir.exists(),
                "old part dir {} should be removed by tombstone cleanup",
                old_dir.display()
            );
        }
        assert!(
            !new_dir.join(MERGE_TOMBSTONE_FILE).exists(),
            "tombstone file should be removed during discover"
        );
    }

    #[test]
    fn discover_cleans_transitive_merge_tombstone_chain() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let row = |line: &str| Row {
            timestamp_ns: 1_700_000_000_000_000_000,
            labels: BTreeMap::new(),
            line: line.to_string(),
            structured_metadata: vec![],
        };

        let oldest = flush_rows(vec![row("oldest")], &parts_root, 100)
            .unwrap()
            .remove(0);
        let middle = flush_rows_with_merge_tombstone(
            vec![row("middle")],
            &parts_root,
            100,
            std::slice::from_ref(&oldest.dir),
        )
        .unwrap()
        .remove(0);
        let newest = flush_rows_with_merge_tombstone(
            vec![row("newest")],
            &parts_root,
            100,
            std::slice::from_ref(&middle.dir),
        )
        .unwrap()
        .remove(0);

        let discovered = discover_parts(&parts_root).unwrap();

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].meta.id, newest.meta.id);
        assert!(!oldest.dir.exists());
        assert!(!middle.dir.exists());
        assert!(newest.dir.exists());
        assert!(!newest.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn discover_rejects_tombstone_paths_outside_parts_root() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let outside = tmp.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("keep"), b"data").unwrap();
        let replacement = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "replacement".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        std::fs::write(
            replacement.dir.join(MERGE_TOMBSTONE_FILE),
            r#"{"old_dirs":["../../outside"]}"#,
        )
        .unwrap();

        let discovered = discover_parts(&parts_root).unwrap();

        assert!(discovered.is_empty());
        assert!(outside.join("keep").exists());
        assert!(replacement.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }

    #[test]
    fn tombstone_path_validation_rejects_absolute_and_parent_paths() {
        assert!(validate_tombstone_part_path(Path::new("/tmp/part")).is_err());
        assert!(validate_tombstone_part_path(Path::new("partition/../part")).is_err());
        assert!(validate_tombstone_part_path(Path::new("partition/part")).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn tombstone_resolution_rejects_symlink_escape() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        let marker_dir = tmp.join("marker");
        let outside_partition = tmp.join("outside-partition");
        std::fs::create_dir_all(&parts_root).unwrap();
        std::fs::create_dir_all(&marker_dir).unwrap();
        std::fs::create_dir_all(outside_partition.join("part")).unwrap();
        std::os::unix::fs::symlink(&outside_partition, parts_root.join("escape")).unwrap();
        std::fs::write(
            marker_dir.join(MERGE_TOMBSTONE_FILE),
            r#"{"old_dirs":["escape/part"]}"#,
        )
        .unwrap();

        let result = read_merge_tombstone_dirs(&marker_dir, &parts_root);

        assert!(result.is_err());
        assert!(outside_partition.join("part").exists());
    }

    #[test]
    fn discover_retains_tombstone_when_old_part_deletion_fails() {
        let tmp = tempfile_dir();
        let parts_root = tmp.join("parts");
        std::fs::create_dir_all(&parts_root).unwrap();
        let replacement = flush_rows(
            vec![Row {
                timestamp_ns: 1_700_000_000_000_000_000,
                labels: BTreeMap::new(),
                line: "replacement".to_string(),
                structured_metadata: vec![],
            }],
            &parts_root,
            100,
        )
        .unwrap()
        .remove(0);
        let partition = replacement.dir.parent().unwrap();
        let undeletable_as_directory = partition.join("old-part");
        std::fs::write(&undeletable_as_directory, b"not a directory").unwrap();
        let relative = undeletable_as_directory
            .strip_prefix(&parts_root)
            .unwrap()
            .to_string_lossy();
        std::fs::write(
            replacement.dir.join(MERGE_TOMBSTONE_FILE),
            format!(r#"{{"old_dirs":["{relative}"]}}"#),
        )
        .unwrap();

        let discovered = discover_parts(&parts_root).unwrap();

        assert_eq!(discovered.len(), 1);
        assert!(undeletable_as_directory.exists());
        assert!(replacement.dir.join(MERGE_TOMBSTONE_FILE).exists());
    }
}
