//! The metrics signal's data model: canonical series labels, the one sample
//! kind, and the memtable that buffers samples between the journal and the
//! flush (M14, issue #8).
//!
//! **One sample kind everywhere: `(series labels, timestamp_ns, f64)`.** The
//! five OTLP metric types are decomposed into float series before they reach
//! this module (`series_ingest`); below that boundary there is exactly one
//! encoder (`gorilla`), one buffer shape, and later one part format and one
//! executor.
//!
//! The memtable is the third parallel fork of the log/trace lifecycle, not an
//! abstraction over them, and it implements the same de-facto flush protocol
//! by convention: `begin_flush` / `commit_flush` / `abort_flush(snapshot)` /
//! `is_empty` / `approximate_size` / `tenants`. One structural difference the
//! series shape forces: `begin_flush` moves a series' *samples* out and keeps
//! the series' *state* — the last timestamp and the delta-conversion running
//! total — because a flush that reset the running total would manufacture a
//! counter reset on every flush interval.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::gorilla;
use crate::tenant::TenantId;

/// The reserved label the metric name is stored under, Prometheus's spelling.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// A series identity: the metric name (as `__name__`) plus its sorted label
/// pairs, held in one canonical byte encoding — repeated
/// `u32 LE key length, key, u32 LE value length, value` with pairs sorted by
/// key. The bytes are the identity across the memtable, the index and the
/// parts, so equality is `memcmp` and no surface re-sorts or re-escapes.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeriesLabels(Arc<[u8]>);

impl SeriesLabels {
    /// Canonicalize a pair set. Pairs are sorted by key; on a duplicate key
    /// the *first* occurrence wins, which lets the caller express precedence
    /// by push order (datapoint attributes before promoted resource ones).
    pub fn from_pairs(mut pairs: Vec<(String, String)>) -> Self {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|later, earlier| later.0 == earlier.0);
        let mut bytes = Vec::new();
        for (key, value) in &pairs {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn byte_len(&self) -> usize {
        self.0.len()
    }

    /// Decode back into pairs. The encoding was built from valid pairs, so a
    /// failure here is corruption and is an error, not a lossy render.
    pub fn pairs(&self) -> Result<Vec<(String, String)>, String> {
        let bytes = &self.0;
        let mut pairs = Vec::new();
        let mut at = 0usize;
        let read = |at: &mut usize| -> Result<String, String> {
            let len_end = *at + 4;
            let len_bytes: [u8; 4] = bytes
                .get(*at..len_end)
                .ok_or("series labels truncated")?
                .try_into()
                .map_err(|_| "series labels truncated")?;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let end = len_end + len;
            let text = bytes.get(len_end..end).ok_or("series labels truncated")?;
            *at = end;
            String::from_utf8(text.to_vec()).map_err(|e| format!("series label not UTF-8: {e}"))
        };
        while at < bytes.len() {
            let key = read(&mut at)?;
            let value = read(&mut at)?;
            pairs.push((key, value));
        }
        Ok(pairs)
    }

    /// The `__name__` value, if present.
    pub fn metric_name(&self) -> Option<String> {
        self.pairs()
            .ok()?
            .into_iter()
            .find(|(key, _)| key == METRIC_NAME_LABEL)
            .map(|(_, value)| value)
    }
}

impl std::fmt::Debug for SeriesLabels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.pairs() {
            Ok(pairs) => {
                let rendered: Vec<String> = pairs
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect();
                write!(f, "{{{}}}", rendered.join(","))
            }
            Err(_) => write!(f, "<corrupt series labels>"),
        }
    }
}

/// How a decomposed sample's value relates to the series' stored value.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SampleKind {
    /// Stored as-is: gauges, summary quantiles.
    Gauge,
    /// Already a running total (cumulative sums, cumulative histogram
    /// buckets): stored as-is; `rate` reads the resets out at query time.
    Cumulative,
    /// A delta-temporality increment. The memtable folds it into the series'
    /// running total at insert, so storage only ever holds cumulative values.
    /// Replay reproduces the same totals because the WAL replays in append
    /// order through this same fold.
    Delta,
}

#[derive(Clone, Debug)]
pub struct MetricSample {
    pub tenant: TenantId,
    pub labels: SeriesLabels,
    pub ts_ns: i64,
    pub value: f64,
    pub kind: SampleKind,
}

/// Fixed overhead charged per live series beyond its canonical bytes: map
/// entry, buffer struct, encoder header. An estimate the memory gate audits
/// from outside (`memprof`), not a promise.
const SERIES_OVERHEAD_BYTES: u64 = 160;
const SPILL_SAMPLE_BYTES: u64 = 16;

struct SeriesBuffer {
    /// Chunks returned by aborted flushes, oldest first. Closed streams
    /// cannot be appended to, so they wait here for the next flush.
    closed: Vec<Vec<u8>>,
    open: gorilla::Encoder,
    /// Samples that arrived with a timestamp older than the newest appended
    /// one. The Gorilla stream is append-ordered; the flush merge-sorts this
    /// vector in.
    spill: Vec<(i64, f64)>,
    last_ts: Option<i64>,
    /// Delta-conversion state. Survives `begin_flush` deliberately — see the
    /// module doc.
    running_total: Option<f64>,
}

impl SeriesBuffer {
    fn new() -> Self {
        Self {
            closed: Vec::new(),
            open: gorilla::Encoder::new(),
            spill: Vec::new(),
            last_ts: None,
            running_total: None,
        }
    }

    fn has_samples(&self) -> bool {
        !self.closed.is_empty() || !self.open.is_empty() || !self.spill.is_empty()
    }
}

/// One series' buffered samples as `begin_flush` hands them to the flush:
/// closed chunks oldest-first, plus the out-of-order spill.
pub struct SnapshotSeries {
    pub labels: SeriesLabels,
    pub chunks: Vec<Vec<u8>>,
    pub spill: Vec<(i64, f64)>,
}

impl SnapshotSeries {
    /// Every sample, time-sorted — the form the flush writes and the read
    /// path merges.
    pub fn sorted_samples(&self) -> Result<Vec<(i64, f64)>, String> {
        let mut samples = Vec::new();
        for chunk in &self.chunks {
            samples.extend(gorilla::decode_all(chunk)?);
        }
        samples.extend(self.spill.iter().copied());
        samples.sort_by_key(|(ts, _)| *ts);
        Ok(samples)
    }
}

pub struct SeriesSnapshot {
    pub tenants: BTreeMap<TenantId, Vec<SnapshotSeries>>,
}

impl SeriesSnapshot {
    pub fn is_empty(&self) -> bool {
        self.tenants.values().all(Vec::is_empty)
    }
}

pub struct SeriesMemTable {
    inner: RwLock<BTreeMap<TenantId, HashMap<SeriesLabels, SeriesBuffer>>>,
    flushing: RwLock<Option<Arc<SeriesSnapshot>>>,
    inner_bytes: AtomicU64,
    flushing_bytes: AtomicU64,
}

impl SeriesMemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            flushing: RwLock::new(None),
            inner_bytes: AtomicU64::new(0),
            flushing_bytes: AtomicU64::new(0),
        }
    }

    pub fn insert(&self, samples: Vec<MetricSample>) {
        if samples.is_empty() {
            return;
        }
        let mut added = 0u64;
        let mut inner = self.inner.write();
        for sample in samples {
            let tenant_series = inner.entry(sample.tenant).or_default();
            let buffer = match tenant_series.get_mut(&sample.labels) {
                Some(buffer) => buffer,
                None => {
                    added += sample.labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                    tenant_series
                        .entry(sample.labels.clone())
                        .or_insert_with(SeriesBuffer::new)
                }
            };
            let value = match sample.kind {
                SampleKind::Gauge | SampleKind::Cumulative => sample.value,
                SampleKind::Delta => {
                    let total = buffer.running_total.unwrap_or(0.0) + sample.value;
                    buffer.running_total = Some(total);
                    total
                }
            };
            if buffer.last_ts.is_some_and(|last| sample.ts_ns < last) {
                buffer.spill.push((sample.ts_ns, value));
                added += SPILL_SAMPLE_BYTES;
            } else {
                let before = buffer.open.byte_len();
                buffer.open.append(sample.ts_ns, value);
                added += (buffer.open.byte_len() - before) as u64;
                buffer.last_ts = Some(sample.ts_ns);
            }
        }
        drop(inner);
        self.inner_bytes.fetch_add(added, Ordering::Relaxed);
    }

    /// Move every buffered sample into a snapshot the flush owns, keeping the
    /// per-series state (last timestamp, delta running total) behind. A
    /// previous uncommitted snapshot is folded in, oldest series entries
    /// first, exactly as the log and trace memtables fold theirs.
    pub fn begin_flush(&self) -> Arc<SeriesSnapshot> {
        let mut inner = self.inner.write();
        let mut flushing = self.flushing.write();
        let mut tenants: BTreeMap<TenantId, Vec<SnapshotSeries>> = BTreeMap::new();
        let mut moved = 0u64;
        for (tenant, series_map) in inner.iter_mut() {
            let mut list = Vec::new();
            for (labels, buffer) in series_map.iter_mut() {
                if !buffer.has_samples() {
                    continue;
                }
                let mut chunks = std::mem::take(&mut buffer.closed);
                let open = std::mem::take(&mut buffer.open);
                if !open.is_empty() {
                    chunks.push(open.close());
                }
                let spill = std::mem::take(&mut buffer.spill);
                moved += chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>()
                    + spill.len() as u64 * SPILL_SAMPLE_BYTES;
                list.push(SnapshotSeries {
                    labels: labels.clone(),
                    chunks,
                    spill,
                });
            }
            if !list.is_empty() {
                tenants.insert(tenant.clone(), list);
            }
        }
        // A previous uncommitted snapshot's bytes are already counted in
        // `flushing_bytes`; only its series lists fold in, older entries
        // first.
        if let Some(previous) = flushing.take() {
            let previous = unwrap_snapshot(previous);
            for (tenant, mut list) in previous.tenants {
                let entry = tenants.entry(tenant).or_default();
                list.append(entry);
                *entry = list;
            }
        }
        self.inner_bytes.fetch_sub(moved, Ordering::Relaxed);
        self.flushing_bytes.fetch_add(moved, Ordering::Relaxed);
        let snapshot = Arc::new(SeriesSnapshot { tenants });
        *flushing = Some(snapshot.clone());
        snapshot
    }

    pub fn commit_flush(&self) {
        *self.flushing.write() = None;
        self.flushing_bytes.store(0, Ordering::Relaxed);
    }

    pub fn abort_flush(&self, snapshot: Arc<SeriesSnapshot>) {
        // Cleared first so the unwrap below takes ownership rather than
        // copying, the same order the other memtables use.
        *self.flushing.write() = None;
        let snapshot = unwrap_snapshot(snapshot);
        let mut inner = self.inner.write();
        for (tenant, list) in snapshot.tenants {
            let tenant_series = inner.entry(tenant).or_default();
            for series in list {
                let buffer = tenant_series
                    .entry(series.labels)
                    .or_insert_with(SeriesBuffer::new);
                // The snapshot's samples are older than anything inserted
                // since, so its chunks go to the front and its spill stays
                // spill.
                let mut chunks = series.chunks;
                chunks.append(&mut buffer.closed);
                buffer.closed = chunks;
                buffer.spill.extend(series.spill);
            }
        }
        drop(inner);
        let returned = self.flushing_bytes.swap(0, Ordering::Relaxed);
        self.inner_bytes.fetch_add(returned, Ordering::Relaxed);
    }

    pub fn is_empty(&self) -> bool {
        let has_inner = self
            .inner
            .read()
            .values()
            .any(|series| series.values().any(SeriesBuffer::has_samples));
        if has_inner {
            return false;
        }
        self.flushing
            .read()
            .as_ref()
            .map(|snapshot| snapshot.is_empty())
            .unwrap_or(true)
    }

    /// Every tenant with unflushed samples, the in-flight flush included.
    pub fn tenants(&self) -> BTreeSet<TenantId> {
        let inner = self.inner.read();
        let flushing = self.flushing.read();
        let mut tenants: BTreeSet<TenantId> = inner
            .iter()
            .filter(|(_, series)| series.values().any(SeriesBuffer::has_samples))
            .map(|(tenant, _)| tenant.clone())
            .collect();
        if let Some(snapshot) = flushing.as_ref() {
            tenants.extend(snapshot.tenants.keys().cloned());
        }
        tenants
    }

    pub fn approximate_size(&self) -> usize {
        self.inner_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.flushing_bytes.load(Ordering::Relaxed)) as usize
    }

    /// Live series for a tenant — entries whose state is held, flushed or
    /// not. This is the number `max_active_series` will meter.
    pub fn active_series(&self, tenant: &TenantId) -> usize {
        self.inner
            .read()
            .get(tenant)
            .map(|series| series.len())
            .unwrap_or(0)
    }

    /// A tenant's buffered samples, time-sorted per series — the memtable
    /// half of the read path, and what the replay-equivalence tests compare.
    /// The open encoder is cloned and closed rather than disturbed.
    pub fn sorted_samples(
        &self,
        tenant: &TenantId,
    ) -> Result<BTreeMap<SeriesLabels, Vec<(i64, f64)>>, String> {
        let mut result: BTreeMap<SeriesLabels, Vec<(i64, f64)>> = BTreeMap::new();
        {
            let flushing = self.flushing.read();
            if let Some(snapshot) = flushing.as_ref()
                && let Some(list) = snapshot.tenants.get(tenant)
            {
                for series in list {
                    result
                        .entry(series.labels.clone())
                        .or_default()
                        .extend(series.sorted_samples()?);
                }
            }
        }
        let inner = self.inner.read();
        if let Some(series_map) = inner.get(tenant) {
            for (labels, buffer) in series_map {
                if !buffer.has_samples() {
                    continue;
                }
                let entry = result.entry(labels.clone()).or_default();
                for chunk in &buffer.closed {
                    entry.extend(gorilla::decode_all(chunk)?);
                }
                if !buffer.open.is_empty() {
                    entry.extend(gorilla::decode_all(&buffer.open.clone().close())?);
                }
                entry.extend(buffer.spill.iter().copied());
            }
        }
        for samples in result.values_mut() {
            samples.sort_by_key(|(ts, _)| *ts);
        }
        Ok(result)
    }
}

impl Default for SeriesMemTable {
    fn default() -> Self {
        Self::new()
    }
}

/// See `memtable::unwrap_snapshot`.
fn unwrap_snapshot(snapshot: Arc<SeriesSnapshot>) -> SeriesSnapshot {
    Arc::try_unwrap(snapshot).unwrap_or_else(|shared| SeriesSnapshot {
        tenants: shared
            .tenants
            .iter()
            .map(|(tenant, list)| {
                (
                    tenant.clone(),
                    list.iter()
                        .map(|series| SnapshotSeries {
                            labels: series.labels.clone(),
                            chunks: series.chunks.clone(),
                            spill: series.spill.clone(),
                        })
                        .collect(),
                )
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::test_tenant;

    fn labels(name: &str, instance: &str) -> SeriesLabels {
        SeriesLabels::from_pairs(vec![
            (METRIC_NAME_LABEL.to_string(), name.to_string()),
            ("instance".to_string(), instance.to_string()),
        ])
    }

    fn sample(labels: &SeriesLabels, ts: i64, value: f64, kind: SampleKind) -> MetricSample {
        MetricSample {
            tenant: test_tenant(),
            labels: labels.clone(),
            ts_ns: ts,
            value,
            kind,
        }
    }

    #[test]
    fn canonical_labels_sort_dedup_and_round_trip() {
        let one = SeriesLabels::from_pairs(vec![
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]);
        let same = SeriesLabels::from_pairs(vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]);
        assert_eq!(one, same);
        assert_eq!(
            one.pairs().unwrap(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string())
            ]
        );
        // First occurrence wins on a duplicate key: push order is precedence.
        let precedence = SeriesLabels::from_pairs(vec![
            ("k".to_string(), "datapoint".to_string()),
            ("k".to_string(), "resource".to_string()),
        ]);
        assert_eq!(
            precedence.pairs().unwrap(),
            vec![("k".to_string(), "datapoint".to_string())]
        );
        assert_eq!(labels("up", "a").metric_name().as_deref(), Some("up"));
    }

    #[test]
    fn in_order_samples_land_in_the_chunk_and_older_ones_in_the_spill() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![
            sample(&series, 100, 1.0, SampleKind::Gauge),
            sample(&series, 200, 2.0, SampleKind::Gauge),
            sample(&series, 150, 9.0, SampleKind::Gauge),
            sample(&series, 200, 3.0, SampleKind::Gauge),
        ]);
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(
            sorted.get(&series).unwrap(),
            &vec![(100, 1.0), (150, 9.0), (200, 2.0), (200, 3.0)]
        );
        assert!(memtable.approximate_size() > 0);
    }

    #[test]
    fn delta_samples_accumulate_into_a_running_total() {
        let memtable = SeriesMemTable::new();
        let series = labels("http_requests_total", "a");
        memtable.insert(vec![
            sample(&series, 100, 5.0, SampleKind::Delta),
            sample(&series, 200, 3.0, SampleKind::Delta),
            sample(&series, 300, 2.0, SampleKind::Delta),
        ]);
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(
            sorted.get(&series).unwrap(),
            &vec![(100, 5.0), (200, 8.0), (300, 10.0)]
        );
    }

    #[test]
    fn the_delta_running_total_survives_a_flush_cycle() {
        let memtable = SeriesMemTable::new();
        let series = labels("http_requests_total", "a");
        memtable.insert(vec![sample(&series, 100, 5.0, SampleKind::Delta)]);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        memtable.insert(vec![sample(&series, 200, 3.0, SampleKind::Delta)]);
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(
            sorted.get(&series).unwrap(),
            &vec![(200, 8.0)],
            "a flush must not restart the total: that would be a manufactured counter reset"
        );
        assert_eq!(snapshot.tenants.len(), 1);
    }

    #[test]
    fn begin_flush_moves_samples_and_abort_returns_them_in_order() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        // Both halves are visible mid-flush.
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(sorted.get(&series).unwrap(), &vec![(100, 1.0), (200, 2.0)]);
        memtable.abort_flush(snapshot);
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(sorted.get(&series).unwrap(), &vec![(100, 1.0), (200, 2.0)]);
        assert!(!memtable.is_empty());
        // The next flush carries everything.
        let snapshot = memtable.begin_flush();
        let all: Vec<(i64, f64)> = snapshot.tenants[&test_tenant()]
            .iter()
            .flat_map(|series| series.sorted_samples().unwrap())
            .collect();
        assert_eq!(all, vec![(100, 1.0), (200, 2.0)]);
        memtable.commit_flush();
        assert!(memtable.is_empty());
        assert_eq!(memtable.active_series(&test_tenant()), 1, "state is kept");
    }

    #[test]
    fn an_uncommitted_snapshot_is_folded_into_the_next_flush() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let _first = memtable.begin_flush();
        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        let second = memtable.begin_flush();
        let all: Vec<(i64, f64)> = second.tenants[&test_tenant()]
            .iter()
            .flat_map(|series| series.sorted_samples().unwrap())
            .collect();
        assert_eq!(all, vec![(100, 1.0), (200, 2.0)]);
    }

    #[test]
    fn size_accounting_moves_with_the_flush_and_back_on_abort() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let before = memtable.approximate_size();
        assert!(before > 0);
        let snapshot = memtable.begin_flush();
        assert!(
            memtable.approximate_size() > 0,
            "flushing bytes still count"
        );
        memtable.abort_flush(snapshot);
        assert!(memtable.approximate_size() > 0);
        memtable.begin_flush();
        memtable.commit_flush();
        let after = memtable.approximate_size();
        assert!(
            after < before,
            "committed samples leave the accounting ({after} >= {before})"
        );
    }

    #[test]
    fn tenants_reports_across_active_and_flushing_buffers() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        memtable.begin_flush();
        assert_eq!(memtable.tenants().len(), 1);
        memtable.commit_flush();
        assert!(memtable.tenants().is_empty(), "state alone is not data");
    }
}
