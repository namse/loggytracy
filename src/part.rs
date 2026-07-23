use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
use crate::memtable::{Labels, LogEntry, StreamResult};

pub const DATA_FILE: &str = "data.parquet";
pub const BLOOM_FILE: &str = "bloom.tri";
pub const STREAM_INDEX_FILE: &str = "stream.idx";
pub const META_FILE: &str = "meta.json";
pub const MERGE_TOMBSTONE_FILE: &str = ".merge.tombstone";

const BLOOM_MAGIC: &[u8; 4] = b"BTF1";
const STREAM_MAGIC: &[u8; 4] = b"SIX1";

pub type StreamMap = BTreeMap<String, BTreeMap<String, RoaringBitmap>>;

#[derive(Clone, Copy)]
struct QueryTimeRange {
    start_ns: i64,
    end_ns: i64,
    include_end: bool,
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

static PART_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn gen_part_id(min_ts_ns: i64) -> String {
    let secs = min_ts_ns.div_euclid(1_000_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    let c = PART_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{}-{:06x}-{:08x}", dt.format("%Y%m%dT%H%M%S"), c, nanos)
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
    buf.extend_from_slice(BLOOM_MAGIC);
    buf.extend_from_slice(&(bounds.len() as u32).to_le_bytes());
    for (start, end) in &bounds {
        let mut unique_trigrams: BTreeSet<[u8; 3]> = BTreeSet::new();
        for row in &rows[*start..*end] {
            for tri in crate::bloom::trigrams(&row.line) {
                unique_trigrams.insert(tri);
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
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
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

pub fn cleanup_tmp(parts_root: &Path) {
    let tmp = parts_root.join(".tmp");
    if tmp.exists()
        && let Err(e) = fs::remove_dir_all(&tmp)
    {
        tracing::warn!(error = %e, "failed to clean tmp dir");
    }
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
    stream_index: StreamMap,
    stream_labels: Vec<String>,
    data_file: PreadReader,
    arrow_reader_metadata: ArrowReaderMetadata,
}

fn validate_part_files(part: &Part) -> Result<(), String> {
    let files = [
        (DATA_FILE, part.data_path(), part.meta.integrity.data_crc32),
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
        validate_part_files(&part)?;
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
        let bloom = decode_blooms(&bloom_bytes)?;
        if bloom.len() != part.meta.row_group_count as usize {
            return Err(format!(
                "row group count mismatch for part {}: bloom says {}, meta says {}",
                part.meta.id,
                bloom.len(),
                part.meta.row_group_count
            ));
        }
        let stream_index =
            decode_stream_index(&fs::read(part.stream_index_path()).map_err(|e| e.to_string())?)?;
        validate_stream_index(&part, &stream_index)?;
        let stream_labels = part.meta.stream_labels.clone();
        let data_file =
            PreadReader::new(fs::File::open(part.data_path()).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        // parquet footer의 row group 수와 meta.row_group_count 일치 검증.
        // bloom/stream index가 row group 경계를 parquet 실제 row group과 정렬한다는
        // 암묵적 가정에 대한 안전망.
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
        for label in &stream_labels {
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
        Ok(Self {
            part,
            bloom,
            stream_index,
            stream_labels,
            data_file,
            arrow_reader_metadata,
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
        self.query_internal(
            matchers,
            line_filters,
            QueryTimeRange {
                start_ns,
                end_ns,
                include_end: true,
            },
            limit,
            forward,
        )
    }

    pub fn query_all(&self) -> Result<Vec<StreamResult>, String> {
        self.query_internal(
            &[],
            &[],
            QueryTimeRange {
                start_ns: i64::MIN,
                end_ns: i64::MAX,
                include_end: true,
            },
            usize::MAX,
            true,
        )
    }

    fn select_row_groups(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
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
            selected.push(rg);
        }
        selected
    }

    fn query_internal(
        &self,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        time_range: QueryTimeRange,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        let rg_count = self.bloom.len();
        if rg_count == 0 {
            return Ok(Vec::new());
        }

        let selected = self.select_row_groups(matchers, line_filters, time_range);
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let mut sorted_selected = selected.clone();
        sorted_selected.sort_unstable();

        let mut collected: Vec<(Labels, LogEntry)> = Vec::new();

        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.data_file.clone(),
            self.arrow_reader_metadata.clone(),
        );
        let row_group_indices: Vec<usize> = sorted_selected.iter().map(|&rg| rg as usize).collect();
        let reader = builder
            .with_row_groups(row_group_indices)
            .build()
            .map_err(|e| e.to_string())?;

        for maybe_batch in reader {
            let batch = maybe_batch.map_err(|e| e.to_string())?;
            let ts = batch.column(0).as_primitive::<Int64Type>();
            let msg = batch.column(1).as_string::<i32>();
            let sm_col_idx = 2 + self.stream_labels.len();
            let sm = batch.column(sm_col_idx).as_string::<i32>();
            let label_cols: Vec<&StringArray> = (0..self.stream_labels.len())
                .map(|i| batch.column(2 + i).as_string::<i32>())
                .collect();

            for i in 0..batch.num_rows() {
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
            }
        }

        if forward {
            collected.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            collected.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        collected.truncate(limit);

        Ok(group_by_labels(collected))
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

fn decode_blooms(buf: &[u8]) -> Result<Vec<BloomFilter>, String> {
    if buf.len() < 8 {
        return Err("bloom file too short".to_string());
    }
    if &buf[0..4] != BLOOM_MAGIC {
        return Err("bloom magic mismatch".to_string());
    }
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let mut pos = 8;
    let mut blooms = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > buf.len() {
            return Err("bloom length truncated".to_string());
        }
        let len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + len > buf.len() {
            return Err("bloom payload truncated".to_string());
        }
        blooms.push(BloomFilter::decode(&buf[pos..pos + len])?);
        pos += len;
    }
    if pos != buf.len() {
        return Err("bloom file has trailing bytes".to_string());
    }
    Ok(blooms)
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
    if collected.is_empty() {
        return Vec::new();
    }
    let mut results: Vec<StreamResult> = Vec::new();
    for (labels, entry) in collected {
        if let Some(r) = results.iter_mut().find(|r| r.labels == labels) {
            r.entries.push(entry);
        } else {
            results.push(StreamResult {
                labels,
                entries: vec![entry],
            });
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let rows: Vec<Row> = (0..20)
            .map(|i| Row {
                timestamp_ns: 1_700_000_000_000_000_000 + i * 1_000_000_000,
                labels: labels.clone(),
                line: format!("line-{:02}", i),
                structured_metadata: vec![],
            })
            .collect();
        let parts = flush_rows(rows, &tmp, 5).expect("flush");
        let reader = PartReader::open(parts.into_iter().next().unwrap()).expect("open");

        let r = reader
            .query(&[], &[], i64::MIN, i64::MAX, 3, false)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-19", "line-18", "line-17"]);

        let r = reader
            .query(&[], &[], i64::MIN, i64::MAX, 3, true)
            .expect("q");
        let lines: Vec<&str> = r
            .iter()
            .flat_map(|s| s.entries.iter().map(|e| e.line.as_str()))
            .collect();
        assert_eq!(lines, vec!["line-00", "line-01", "line-02"]);
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
