//! The metric part format (M14, issue #8): one flushed batch of series
//! samples as four artifacts, modeled file-for-file on `trace_part.rs` so the
//! object-storage lifecycle generalizes without redesign.
//!
//! * **`data.bin`** — magic `LMS1`, then one Gorilla chunk per series,
//!   concatenated in catalog order. A series' samples are merged (chunks +
//!   out-of-order spill), time-sorted, and re-encoded into a single chunk at
//!   flush, so a part's chunk is always sorted and self-describing.
//! * **`index.bin`** — magic `LMI1`: the tenant segment table and the series
//!   catalog (canonical labels, chunk byte range, sample count, min/max
//!   timestamp). Everything selection needs without touching `data.bin`.
//! * **`series.bloom`** — one bloom over the part's `key\0value` label pair
//!   tokens, for pruning a part on an equality selector without reading
//!   `index.bin`.
//! * **`meta.json`** — identity, time bounds, per-tenant segments with byte
//!   extents (the storage quota's census), and integrity checksums.
//!
//! Parts are tenant-major: each tenant owns a contiguous ordinal range of the
//! catalog, recorded in its segment, so a read can never cross tenants.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::bloom::BloomFilter;
use crate::gorilla;
use crate::part::partition_of;
use crate::series::{SeriesLabels, SeriesSnapshot};
use crate::tenant::TenantId;

pub const SERIES_DATA_FILE: &str = "data.bin";
pub const SERIES_INDEX_FILE: &str = "index.bin";
pub const SERIES_BLOOM_FILE: &str = "series.bloom";
pub const SERIES_META_FILE: &str = "meta.json";

const SERIES_DATA_MAGIC: &[u8; 4] = b"LMS1";
const SERIES_INDEX_MAGIC: &[u8; 4] = b"LMI1";
const SERIES_BLOOM_MAGIC: &[u8; 4] = b"LMB1";

/// The false-positive rate of the label-pair bloom. The trace bloom's 1% is
/// kept: a false positive costs one `index.bin` read, not a scan.
const BLOOM_FPP: f64 = 0.01;

/// One tenant's contiguous run of catalog ordinals in a shared metric part.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesTenantSegment {
    pub tenant: TenantId,
    pub series_start: u32,
    pub series_end: u32,
    pub sample_count: u64,
    /// Where this tenant's chunks sit in `data.bin` — what the storage quota
    /// charges, from the format rather than the evictable local file.
    pub bytes: crate::part::ByteRange,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeriesPartIntegrity {
    data_crc32: u32,
    index_crc32: u32,
    bloom_crc32: u32,
    metadata_crc32: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct SeriesMetaFile {
    id: String,
    partition: String,
    min_ts_ns: i64,
    max_ts_ns: i64,
    series_count: u32,
    sample_count: u64,
    tenants: Vec<SeriesTenantSegment>,
    integrity: SeriesPartIntegrity,
}

#[derive(Clone, Debug)]
pub struct SeriesPartMeta {
    pub id: String,
    pub partition: String,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    pub series_count: u32,
    pub sample_count: u64,
    pub tenants: Vec<SeriesTenantSegment>,
    // Mirrors the trace part: carried so a future re-serialization keeps the
    // checksums beside what they cover.
    #[allow(dead_code)]
    integrity: SeriesPartIntegrity,
}

impl SeriesPartMeta {
    pub fn tenant_segment(&self, tenant: &TenantId) -> Option<&SeriesTenantSegment> {
        self.tenants
            .binary_search_by(|segment| segment.tenant.cmp(tenant))
            .ok()
            .map(|index| &self.tenants[index])
    }

    pub fn overlaps_range(&self, start_ns: i64, end_ns: i64) -> bool {
        self.max_ts_ns >= start_ns && self.min_ts_ns <= end_ns
    }
}

#[derive(Clone, Debug)]
pub struct SeriesPart {
    pub dir: PathBuf,
    pub meta: SeriesPartMeta,
}

impl SeriesPart {
    pub fn data_path(&self) -> PathBuf {
        self.dir.join(SERIES_DATA_FILE)
    }

    pub fn index_path(&self) -> PathBuf {
        self.dir.join(SERIES_INDEX_FILE)
    }

    pub fn bloom_path(&self) -> PathBuf {
        self.dir.join(SERIES_BLOOM_FILE)
    }

    pub fn meta_path(&self) -> PathBuf {
        self.dir.join(SERIES_META_FILE)
    }
}

/// One catalog row: a series and where its chunk lives.
#[derive(Clone, Debug)]
pub struct CatalogEntry {
    pub labels: SeriesLabels,
    pub offset: u64,
    pub length: u64,
    pub sample_count: u32,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
}

impl CatalogEntry {
    pub fn overlaps_range(&self, start_ns: i64, end_ns: i64) -> bool {
        self.max_ts_ns >= start_ns && self.min_ts_ns <= end_ns
    }
}

/// The bloom's token for one label pair. NUL-separated because a label value
/// may contain `=`; keys cannot contain NUL (they pass through attribute-key
/// normalization or are this crate's own reserved names).
pub fn pair_token(key: &str, value: &str) -> Vec<u8> {
    let mut token = Vec::with_capacity(key.len() + 1 + value.len());
    token.extend_from_slice(key.as_bytes());
    token.push(0);
    token.extend_from_slice(value.as_bytes());
    token
}

/// Writes the flushing snapshot without taking ownership of it — the buffer
/// stays shared with the memtable until the flush commits, exactly like the
/// trace writer.
pub fn flush_series_snapshot(
    snapshot: &SeriesSnapshot,
    metrics_root: &Path,
) -> io::Result<Vec<SeriesPart>> {
    if snapshot.is_empty() {
        return Ok(Vec::new());
    }
    // Charged like the log flush: the partition map, the re-encoded chunks and
    // the catalog this builds are the writer's, not its caller's — and one of
    // its callers is compaction.
    let _arena = crate::memprof::enter(crate::memprof::Arena::Flush);
    fs::create_dir_all(metrics_root.join(".tmp"))?;

    // Merge the snapshot's per-series entries (an aborted flush leaves more
    // than one per series), sort, and split by partition. The map is ordered
    // by (partition, tenant, labels), which is exactly the catalog order.
    let mut partitions: BTreeMap<String, PartitionSeries> = BTreeMap::new();
    for (tenant, list) in &snapshot.tenants {
        for series in list {
            let samples = series.sorted_samples().map_err(io::Error::other)?;
            for (ts, value) in samples {
                partitions
                    .entry(partition_of(ts))
                    .or_default()
                    .entry(tenant.clone())
                    .or_default()
                    .entry(series.labels.clone())
                    .or_default()
                    .push((ts, value));
            }
        }
    }

    let mut parts = Vec::new();
    let mut committed_dirs = Vec::new();
    for (partition, tenants) in partitions {
        let id = format!("{}-{}", partition.replace('-', ""), uuid::Uuid::new_v4());
        let tmp_dir = metrics_root.join(".tmp").join(&id);
        let final_dir = metrics_root.join(&partition).join(&id);
        let result = (|| -> io::Result<SeriesPart> {
            if tmp_dir.exists() {
                fs::remove_dir_all(&tmp_dir)?;
            }
            fs::create_dir_all(&tmp_dir)?;
            write_series_part_files(&tmp_dir, &id, &partition, &tenants)?;
            if let Some(parent) = final_dir.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(&tmp_dir, &final_dir)?;
            committed_dirs.push(final_dir.clone());
            sync_dir(final_dir.parent().unwrap_or(metrics_root))?;
            sync_dir(metrics_root)?;
            load_series_part(&final_dir).map_err(io::Error::other)
        })();

        match result {
            Ok(part) => parts.push(part),
            Err(error) => {
                let _ = fs::remove_dir_all(&tmp_dir);
                let cleanup = crate::part::remove_part_dirs(&committed_dirs);
                return Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup_error) => io::Error::other(format!(
                        "metric part flush failed: {error}; rollback failed: {cleanup_error}"
                    )),
                });
            }
        }
    }
    Ok(parts)
}

/// One partition's samples, grouped `tenant -> series -> sorted samples` —
/// which is exactly the catalog order the files are written in.
type PartitionSeries = BTreeMap<TenantId, BTreeMap<SeriesLabels, Vec<(i64, f64)>>>;

fn write_series_part_files(
    dir: &Path,
    id: &str,
    partition: &str,
    tenants: &PartitionSeries,
) -> io::Result<()> {
    let mut data = Vec::new();
    data.extend_from_slice(SERIES_DATA_MAGIC);

    let mut catalog: Vec<CatalogEntry> = Vec::new();
    let mut segments: Vec<SeriesTenantSegment> = Vec::new();
    let mut bloom_tokens: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let mut part_min = i64::MAX;
    let mut part_max = i64::MIN;
    let mut part_samples = 0u64;

    for (tenant, series_map) in tenants {
        let series_start = catalog.len() as u32;
        let segment_bytes_start = data.len() as u64;
        let mut segment_samples = 0u64;
        for (labels, samples) in series_map {
            debug_assert!(
                !samples.is_empty(),
                "the partition split never leaves an empty series"
            );
            let mut encoder = gorilla::Encoder::new();
            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;
            for (ts, value) in samples {
                encoder.append(*ts, *value);
                min_ts = min_ts.min(*ts);
                max_ts = max_ts.max(*ts);
            }
            let chunk = encoder.close();
            let offset = data.len() as u64;
            data.extend_from_slice(&chunk);
            catalog.push(CatalogEntry {
                labels: labels.clone(),
                offset,
                length: chunk.len() as u64,
                sample_count: samples.len() as u32,
                min_ts_ns: min_ts,
                max_ts_ns: max_ts,
            });
            for (key, value) in labels.pairs().map_err(io::Error::other)? {
                bloom_tokens.insert(pair_token(&key, &value));
            }
            part_min = part_min.min(min_ts);
            part_max = part_max.max(max_ts);
            segment_samples += samples.len() as u64;
            part_samples += samples.len() as u64;
        }
        segments.push(SeriesTenantSegment {
            tenant: tenant.clone(),
            series_start,
            series_end: catalog.len() as u32,
            sample_count: segment_samples,
            bytes: crate::part::ByteRange {
                start: segment_bytes_start,
                end: data.len() as u64,
            },
        });
    }

    fs::write(dir.join(SERIES_DATA_FILE), &data)?;
    sync_file(&dir.join(SERIES_DATA_FILE))?;

    let mut index = Vec::new();
    index.extend_from_slice(SERIES_INDEX_MAGIC);
    index.extend_from_slice(&(catalog.len() as u32).to_le_bytes());
    for entry in &catalog {
        index.extend_from_slice(&(entry.labels.as_bytes().len() as u32).to_le_bytes());
        index.extend_from_slice(entry.labels.as_bytes());
        index.extend_from_slice(&entry.offset.to_le_bytes());
        index.extend_from_slice(&entry.length.to_le_bytes());
        index.extend_from_slice(&entry.sample_count.to_le_bytes());
        index.extend_from_slice(&entry.min_ts_ns.to_le_bytes());
        index.extend_from_slice(&entry.max_ts_ns.to_le_bytes());
    }
    fs::write(dir.join(SERIES_INDEX_FILE), &index)?;
    sync_file(&dir.join(SERIES_INDEX_FILE))?;

    let mut bloom = BloomFilter::with_capacity(bloom_tokens.len().max(1), BLOOM_FPP);
    for token in &bloom_tokens {
        bloom.insert(token);
    }
    let mut bloom_bytes = Vec::new();
    bloom_bytes.extend_from_slice(SERIES_BLOOM_MAGIC);
    let encoded = bloom.encode();
    bloom_bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    bloom_bytes.extend_from_slice(&encoded);
    fs::write(dir.join(SERIES_BLOOM_FILE), &bloom_bytes)?;
    sync_file(&dir.join(SERIES_BLOOM_FILE))?;

    let mut meta = SeriesMetaFile {
        id: id.to_string(),
        partition: partition.to_string(),
        min_ts_ns: part_min,
        max_ts_ns: part_max,
        series_count: catalog.len() as u32,
        sample_count: part_samples,
        tenants: segments,
        integrity: SeriesPartIntegrity {
            data_crc32: crc32fast::hash(&data),
            index_crc32: crc32fast::hash(&index),
            bloom_crc32: crc32fast::hash(&bloom_bytes),
            metadata_crc32: 0,
        },
    };
    meta.integrity.metadata_crc32 = metadata_crc32(&meta).map_err(io::Error::other)?;
    let encoded = serde_json::to_vec_pretty(&meta).map_err(io::Error::other)?;
    fs::write(dir.join(SERIES_META_FILE), encoded)?;
    sync_file(&dir.join(SERIES_META_FILE))?;
    Ok(())
}

fn validate_series_tenant_segments(meta: &SeriesMetaFile) -> Result<(), String> {
    if meta.tenants.is_empty() {
        return Err("metric part metadata has no tenant segments".to_string());
    }
    let mut expected_start = 0u32;
    let mut total_samples = 0u64;
    for (index, segment) in meta.tenants.iter().enumerate() {
        if index > 0 && meta.tenants[index - 1].tenant >= segment.tenant {
            return Err("metric tenant segments are not sorted by tenant".to_string());
        }
        if segment.series_start != expected_start
            || segment.series_end <= segment.series_start
            || segment.series_end > meta.series_count
        {
            return Err("metric tenant segments do not tile the catalog".to_string());
        }
        if segment.sample_count == 0 {
            return Err("metric tenant segment is empty".to_string());
        }
        total_samples = total_samples.saturating_add(segment.sample_count);
        expected_start = segment.series_end;
    }
    if expected_start != meta.series_count {
        return Err("metric tenant segments do not cover every series".to_string());
    }
    if total_samples != meta.sample_count {
        return Err(
            "metric tenant segment sample counts do not sum to the part sample count".to_string(),
        );
    }
    Ok(())
}

pub fn load_series_part(dir: &Path) -> Result<SeriesPart, String> {
    let dir_metadata = fs::symlink_metadata(dir).map_err(|error| error.to_string())?;
    if dir_metadata.file_type().is_symlink() {
        return Err(format!(
            "refusing symlinked metric part directory {}",
            dir.display()
        ));
    }
    for file in [
        SERIES_META_FILE,
        SERIES_INDEX_FILE,
        SERIES_BLOOM_FILE,
        SERIES_DATA_FILE,
    ] {
        let path = dir.join(file);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!("refusing symlinked metric file {}", path.display()));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    let bytes = fs::read(dir.join(SERIES_META_FILE)).map_err(|error| error.to_string())?;
    let meta: SeriesMetaFile = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if metadata_crc32(&meta)? != meta.integrity.metadata_crc32 {
        return Err(format!(
            "metric metadata checksum mismatch in {}",
            dir.display()
        ));
    }
    if dir.join(SERIES_DATA_FILE).exists() {
        validate_file_crc(
            &dir.join(SERIES_DATA_FILE),
            meta.integrity.data_crc32,
            "metric data",
        )?;
    }
    validate_file_crc(
        &dir.join(SERIES_INDEX_FILE),
        meta.integrity.index_crc32,
        "metric index",
    )?;
    validate_file_crc(
        &dir.join(SERIES_BLOOM_FILE),
        meta.integrity.bloom_crc32,
        "metric bloom",
    )?;
    validate_series_tenant_segments(&meta)?;
    Ok(SeriesPart {
        dir: dir.to_path_buf(),
        meta: SeriesPartMeta {
            id: meta.id,
            partition: meta.partition,
            min_ts_ns: meta.min_ts_ns,
            max_ts_ns: meta.max_ts_ns,
            series_count: meta.series_count,
            sample_count: meta.sample_count,
            tenants: meta.tenants,
            integrity: meta.integrity,
        },
    })
}

pub fn discover_series_parts(root: &Path) -> Result<Vec<SeriesPart>, String> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut parts = Vec::new();
    for partition in fs::read_dir(root).map_err(|error| error.to_string())? {
        let partition = partition.map_err(|error| error.to_string())?;
        if !partition
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
            || partition.file_name() == ".tmp"
        {
            continue;
        }
        for part_dir in fs::read_dir(partition.path()).map_err(|error| error.to_string())? {
            let part_dir = part_dir.map_err(|error| error.to_string())?;
            if part_dir
                .file_type()
                .map_err(|error| error.to_string())?
                .is_dir()
            {
                parts.push(load_series_part(&part_dir.path())?);
            }
        }
    }
    Ok(parts)
}

pub struct SeriesPartReader {
    part: SeriesPart,
    bloom: BloomFilter,
    catalog: Vec<CatalogEntry>,
}

impl SeriesPartReader {
    pub fn open(part: SeriesPart) -> Result<Self, String> {
        Self::open_internal(part, true)
    }

    /// Open from the catalog artifacts alone: the data body may be evicted to
    /// the object store, and selection must not need it back.
    pub fn open_cached(part: SeriesPart) -> Result<Self, String> {
        Self::open_internal(part, false)
    }

    fn open_internal(part: SeriesPart, require_data: bool) -> Result<Self, String> {
        if require_data && !part.data_path().exists() {
            return Err(format!(
                "metric data body is missing: {}",
                part.data_path().display()
            ));
        }
        // The bloom and the catalog outlive this call — a reader holds both
        // for as long as the registry holds it, offloaded body or not — so
        // they are charged to their own arena rather than to whoever happened
        // to open the part.
        let _arena = crate::memprof::enter(crate::memprof::Arena::SeriesCatalog);
        let bloom_bytes = fs::read(part.bloom_path()).map_err(|error| error.to_string())?;
        let bloom = decode_series_bloom(&bloom_bytes)?;
        let index_bytes = fs::read(part.index_path()).map_err(|error| error.to_string())?;
        let catalog = decode_catalog(&index_bytes, part.meta.series_count as usize)?;
        Ok(Self {
            part,
            bloom,
            catalog,
        })
    }

    pub fn part(&self) -> &SeriesPart {
        &self.part
    }

    /// Whether any series in this part might carry `key=value`. A false
    /// positive costs a catalog walk; a false negative is impossible.
    pub fn may_match_pair(&self, key: &str, value: &str) -> bool {
        self.bloom.contains(&pair_token(key, value))
    }

    /// The tenant's catalog rows — its contiguous ordinal range, so no other
    /// tenant's series are ever offered.
    pub fn tenant_catalog(&self, tenant: &TenantId) -> &[CatalogEntry] {
        match self.part.meta.tenant_segment(tenant) {
            Some(segment) => {
                &self.catalog[segment.series_start as usize..segment.series_end as usize]
            }
            None => &[],
        }
    }

    /// One series' samples, time-sorted as written.
    pub fn read_series(&self, entry: &CatalogEntry) -> Result<Vec<(i64, f64)>, String> {
        let mut file = fs::File::open(self.part.data_path()).map_err(|error| error.to_string())?;
        file.seek(SeekFrom::Start(entry.offset))
            .map_err(|error| error.to_string())?;
        let mut chunk = vec![0u8; entry.length as usize];
        file.read_exact(&mut chunk)
            .map_err(|error| error.to_string())?;
        let samples = gorilla::decode_all(&chunk)?;
        if samples.len() != entry.sample_count as usize {
            return Err(format!(
                "metric chunk in {} decoded {} samples, catalog says {}",
                self.part.meta.id,
                samples.len(),
                entry.sample_count
            ));
        }
        Ok(samples)
    }
}

fn decode_series_bloom(bytes: &[u8]) -> Result<BloomFilter, String> {
    if bytes.len() < 8 || &bytes[..4] != SERIES_BLOOM_MAGIC {
        return Err("metric bloom magic or header mismatch".to_string());
    }
    let length = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let end = 8usize
        .checked_add(length)
        .ok_or_else(|| "metric bloom length overflow".to_string())?;
    if end != bytes.len() {
        return Err("metric bloom length mismatch".to_string());
    }
    BloomFilter::decode(&bytes[8..end])
}

fn decode_catalog(bytes: &[u8], expected_count: usize) -> Result<Vec<CatalogEntry>, String> {
    if bytes.len() < 8 || &bytes[..4] != SERIES_INDEX_MAGIC {
        return Err("metric index magic or header mismatch".to_string());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if count != expected_count {
        return Err(format!(
            "metric index series count mismatch: {count} != {expected_count}"
        ));
    }
    let mut offset = 8usize;
    let mut take = |len: usize| -> Result<&[u8], String> {
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| "metric index is truncated".to_string())?;
        let slice = &bytes[offset..end];
        offset = end;
        Ok(slice)
    };
    let mut catalog = Vec::with_capacity(count);
    for _ in 0..count {
        let labels_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let labels = SeriesLabels::from_canonical(take(labels_len)?.to_vec());
        let chunk_offset = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let chunk_length = u64::from_le_bytes(take(8)?.try_into().unwrap());
        let sample_count = u32::from_le_bytes(take(4)?.try_into().unwrap());
        let min_ts_ns = i64::from_le_bytes(take(8)?.try_into().unwrap());
        let max_ts_ns = i64::from_le_bytes(take(8)?.try_into().unwrap());
        // The canonical bytes come from our own file, but they cross a
        // checksum, not a validator — decode them once so a corrupt entry
        // fails here rather than in a query's rendering.
        labels.pairs()?;
        catalog.push(CatalogEntry {
            labels,
            offset: chunk_offset,
            length: chunk_length,
            sample_count,
            min_ts_ns,
            max_ts_ns,
        });
    }
    if offset != bytes.len() {
        return Err("metric index has trailing bytes".to_string());
    }
    Ok(catalog)
}

fn metadata_crc32(meta: &SeriesMetaFile) -> Result<u32, String> {
    let mut canonical = meta.clone();
    canonical.integrity.metadata_crc32 = 0;
    let bytes = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    Ok(crc32fast::hash(&bytes))
}

fn validate_file_crc(path: &Path, expected: u32, label: &str) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|error| format!("failed to read {label}: {error}"))?;
    if crc32fast::hash(&bytes) != expected {
        return Err(format!("{label} checksum mismatch: {}", path.display()));
    }
    Ok(())
}

fn sync_file(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{METRIC_NAME_LABEL, MetricSample, SampleKind, SeriesMemTable};
    use crate::tenant::test_tenant;

    fn labels(name: &str, instance: &str) -> SeriesLabels {
        SeriesLabels::from_pairs(vec![
            (METRIC_NAME_LABEL.to_string(), name.to_string()),
            ("instance".to_string(), instance.to_string()),
        ])
    }

    fn sample(tenant: &str, series: &SeriesLabels, ts: i64, value: f64) -> MetricSample {
        MetricSample {
            tenant: TenantId::parse(tenant).unwrap(),
            labels: series.clone(),
            ts_ns: ts,
            value,
            kind: SampleKind::Gauge,
            datapoint_index: 0,
        }
    }

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("signy-series-part-{tag}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn a_snapshot_round_trips_through_a_part_with_spill_merged_in_order() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![
            sample("test-tenant", &series, 1_772_000_000_000_000_000, 1.0),
            sample("test-tenant", &series, 1_772_000_020_000_000_000, 3.0),
            // Out of order: lands in the spill, must come back sorted.
            sample("test-tenant", &series, 1_772_000_010_000_000_000, 2.0),
        ]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("roundtrip");
        let parts = flush_series_snapshot(&snapshot, &root).unwrap();
        memtable.commit_flush();
        assert_eq!(parts.len(), 1);
        let reader = SeriesPartReader::open(parts.into_iter().next().unwrap()).unwrap();
        let catalog = reader.tenant_catalog(&test_tenant());
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].sample_count, 3);
        assert_eq!(
            reader.read_series(&catalog[0]).unwrap(),
            vec![
                (1_772_000_000_000_000_000, 1.0),
                (1_772_000_010_000_000_000, 2.0),
                (1_772_000_020_000_000_000, 3.0)
            ]
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn the_bloom_prunes_absent_pairs_and_admits_present_ones() {
        let memtable = SeriesMemTable::new();
        memtable.insert(vec![sample(
            "test-tenant",
            &labels("queue_depth", "a"),
            1_772_000_000_000_000_000,
            1.0,
        )]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("bloom");
        let parts = flush_series_snapshot(&snapshot, &root).unwrap();
        let reader = SeriesPartReader::open(parts.into_iter().next().unwrap()).unwrap();
        assert!(reader.may_match_pair("instance", "a"));
        assert!(reader.may_match_pair(METRIC_NAME_LABEL, "queue_depth"));
        assert!(
            !reader.may_match_pair("instance", "definitely-not-here-9f2c"),
            "a tiny bloom at 1% FPP must reject this"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn tenants_own_contiguous_catalog_ranges_and_reads_cannot_cross() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "shared-instance");
        memtable.insert(vec![
            sample("globex", &series, 1_772_000_000_000_000_000, 9.0),
            sample("acme", &series, 1_772_000_000_000_000_000, 1.0),
        ]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("tenants");
        let parts = flush_series_snapshot(&snapshot, &root).unwrap();
        let reader = SeriesPartReader::open(parts.into_iter().next().unwrap()).unwrap();
        let acme = TenantId::parse("acme").unwrap();
        let globex = TenantId::parse("globex").unwrap();
        let outsider = TenantId::parse("initech").unwrap();
        assert_eq!(reader.tenant_catalog(&acme).len(), 1);
        assert_eq!(reader.tenant_catalog(&globex).len(), 1);
        assert!(reader.tenant_catalog(&outsider).is_empty());
        assert_eq!(
            reader
                .read_series(&reader.tenant_catalog(&acme)[0])
                .unwrap()[0]
                .1,
            1.0
        );
        assert_eq!(
            reader
                .read_series(&reader.tenant_catalog(&globex)[0])
                .unwrap()[0]
                .1,
            9.0
        );
        // The quota census: both tenants have non-empty, disjoint extents.
        let part = reader.part();
        assert_eq!(part.meta.tenants.len(), 2);
        assert!(
            part.meta
                .tenants
                .iter()
                .all(|segment| !segment.bytes.is_empty())
        );
        assert!(part.meta.tenants[0].bytes.end <= part.meta.tenants[1].bytes.start);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn samples_across_a_day_boundary_split_into_two_partitions() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        // 2026-02-25T23:59:50Z and ten seconds later on the 26th.
        let before_midnight = 1_772_063_990_000_000_000;
        let after_midnight = 1_772_064_000_000_000_000;
        memtable.insert(vec![
            sample("test-tenant", &series, before_midnight, 1.0),
            sample("test-tenant", &series, after_midnight, 2.0),
        ]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("partition");
        let parts = flush_series_snapshot(&snapshot, &root).unwrap();
        assert_eq!(parts.len(), 2, "one part per partition");
        let partitions: Vec<&str> = parts
            .iter()
            .map(|part| part.meta.partition.as_str())
            .collect();
        assert_ne!(partitions[0], partitions[1]);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn discovery_reloads_what_flush_wrote_and_catalog_opens_without_the_body() {
        let memtable = SeriesMemTable::new();
        memtable.insert(vec![sample(
            "test-tenant",
            &labels("queue_depth", "a"),
            1_772_000_000_000_000_000,
            1.0,
        )]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("discover");
        let written = flush_series_snapshot(&snapshot, &root).unwrap();
        let discovered = discover_series_parts(&root).unwrap();
        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].meta.id, written[0].meta.id);

        fs::remove_file(discovered[0].data_path()).unwrap();
        let reader =
            SeriesPartReader::open_cached(load_series_part(&discovered[0].dir).unwrap()).unwrap();
        assert!(reader.may_match_pair("instance", "a"));
        assert_eq!(reader.tenant_catalog(&test_tenant()).len(), 1);
        assert!(
            reader
                .read_series(&reader.tenant_catalog(&test_tenant())[0])
                .is_err(),
            "the body is gone; the catalog still answers selection"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_corrupt_artifact_refuses_to_load() {
        let memtable = SeriesMemTable::new();
        memtable.insert(vec![sample(
            "test-tenant",
            &labels("queue_depth", "a"),
            1_772_000_000_000_000_000,
            1.0,
        )]);
        let snapshot = memtable.begin_flush();
        let root = temp_root("corrupt");
        let parts = flush_series_snapshot(&snapshot, &root).unwrap();
        let dir = parts[0].dir.clone();
        let index_path = dir.join(SERIES_INDEX_FILE);
        let mut bytes = fs::read(&index_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&index_path, bytes).unwrap();
        assert!(load_series_part(&dir).unwrap_err().contains("checksum"));
        std::fs::remove_dir_all(&root).ok();
    }
}
