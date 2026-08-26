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
    /// Which OTLP datapoint of its request this sample came from, in the
    /// deterministic traversal order `series_ingest` assigns. The admission
    /// ladder decides per datapoint — a histogram's bucket family is admitted
    /// or refused whole — and the WAL filter drops refused datapoints by this
    /// index, so replay cannot resurrect a refused series.
    pub datapoint_index: u32,
}

/// What the `max_active_series` ladder rung did with one export's samples.
pub struct AdmitOutcome {
    /// Datapoint indices whose series were all admitted. Samples and WAL
    /// bytes for the rest must be dropped by the caller.
    pub admitted: std::collections::HashSet<u32>,
    pub rejected_datapoints: u64,
    pub rejected_samples: u64,
    pub rejected_new_series: u64,
}

impl AdmitOutcome {
    pub fn rejected_any(&self) -> bool {
        self.rejected_datapoints > 0
    }
}

/// The ladder's observability: every rung moves a counter, because a
/// degradation nobody can see is indistinguishable from a bug. Rendered under
/// `loggytracy_*` names by `/metrics`.
#[derive(Default)]
pub struct SeriesCounters {
    /// Live series index entries across all tenants — a gauge.
    pub active_series: AtomicU64,
    pub series_created_total: AtomicU64,
    pub series_evicted_idle_total: AtomicU64,
    /// New series refused at the `max_active_series` boundary.
    pub series_rejected_total: AtomicU64,
    pub metric_datapoints_rejected_total: AtomicU64,
    pub metric_samples_rejected_total: AtomicU64,
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
    counters: SeriesCounters,
}

impl SeriesMemTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            flushing: RwLock::new(None),
            inner_bytes: AtomicU64::new(0),
            flushing_bytes: AtomicU64::new(0),
            counters: SeriesCounters::default(),
        }
    }

    pub fn counters(&self) -> &SeriesCounters {
        &self.counters
    }

    /// The `max_active_series` rung, decided per datapoint under one write
    /// lock so two concurrent exports cannot both reserve the last capacity.
    ///
    /// A datapoint whose series are all known is admitted unconditionally —
    /// steady traffic never notices an explosion. A datapoint needing new
    /// series gets them only if the tenant has capacity; when it does not,
    /// idle series are evicted first (the lazy half of the idle sweep: their
    /// capacity returns exactly when someone asks for it), and only then is
    /// the datapoint refused. Admitted new series are reserved here as empty
    /// state entries, so the reservation holds even though the samples land
    /// later via the journal writer.
    pub fn admit_datapoints(
        &self,
        tenant: &TenantId,
        samples: &[MetricSample],
        max_active: usize,
        idle_cutoff_ns: i64,
    ) -> AdmitOutcome {
        let mut outcome = AdmitOutcome {
            admitted: std::collections::HashSet::new(),
            rejected_datapoints: 0,
            rejected_samples: 0,
            rejected_new_series: 0,
        };
        let mut inner = self.inner.write();
        let tenant_series = inner.entry(tenant.clone()).or_default();
        let mut evicted_for_pressure = false;
        let mut index = 0;
        while index < samples.len() {
            let datapoint = samples[index].datapoint_index;
            let mut end = index;
            while end < samples.len() && samples[end].datapoint_index == datapoint {
                end += 1;
            }
            let group = &samples[index..end];
            let mut new_labels: Vec<&SeriesLabels> = group
                .iter()
                .map(|sample| &sample.labels)
                .filter(|labels| !tenant_series.contains_key(*labels))
                .collect();
            new_labels.sort();
            new_labels.dedup();
            if !new_labels.is_empty() && tenant_series.len() + new_labels.len() > max_active {
                // One eviction pass per export: a second would find nothing
                // new, and the refusal below must not degrade into a scan per
                // datapoint.
                if !evicted_for_pressure {
                    evicted_for_pressure = true;
                    let evicted =
                        Self::evict_idle_locked(tenant_series, idle_cutoff_ns, &self.counters);
                    self.inner_bytes.fetch_sub(evicted, Ordering::Relaxed);
                }
                if tenant_series.len() + new_labels.len() > max_active {
                    outcome.rejected_datapoints += 1;
                    outcome.rejected_samples += group.len() as u64;
                    outcome.rejected_new_series += new_labels.len() as u64;
                    self.counters
                        .series_rejected_total
                        .fetch_add(new_labels.len() as u64, Ordering::Relaxed);
                    self.counters
                        .metric_datapoints_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .metric_samples_rejected_total
                        .fetch_add(group.len() as u64, Ordering::Relaxed);
                    index = end;
                    continue;
                }
            }
            let mut reserved = 0u64;
            for labels in new_labels {
                reserved += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                tenant_series.insert((*labels).clone(), SeriesBuffer::new());
                self.counters
                    .series_created_total
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.active_series.fetch_add(1, Ordering::Relaxed);
            }
            self.inner_bytes.fetch_add(reserved, Ordering::Relaxed);
            outcome.admitted.insert(datapoint);
            index = end;
        }
        outcome
    }

    /// Evict every series whose samples are all flushed and whose newest
    /// sample is older than the cutoff — the idle-timeout rung, called on the
    /// flush cadence and lazily under admission pressure. The evicted series'
    /// history stays in its parts; if it returns it is simply re-created, and
    /// the one artifact — a delta counter restarting — is exactly a counter
    /// reset, which `rate` absorbs.
    pub fn evict_idle(&self, idle_cutoff_ns: i64) -> u64 {
        let mut inner = self.inner.write();
        let mut evicted = 0u64;
        let mut freed = 0u64;
        for tenant_series in inner.values_mut() {
            let before = tenant_series.len() as u64;
            freed += Self::evict_idle_locked(tenant_series, idle_cutoff_ns, &self.counters);
            evicted += before - tenant_series.len() as u64;
        }
        self.inner_bytes.fetch_sub(freed, Ordering::Relaxed);
        evicted
    }

    fn evict_idle_locked(
        tenant_series: &mut HashMap<SeriesLabels, SeriesBuffer>,
        idle_cutoff_ns: i64,
        counters: &SeriesCounters,
    ) -> u64 {
        let mut freed = 0u64;
        let mut evicted = 0u64;
        tenant_series.retain(|labels, buffer| {
            let idle =
                !buffer.has_samples() && buffer.last_ts.is_none_or(|last| last < idle_cutoff_ns);
            if idle {
                freed += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                evicted += 1;
            }
            !idle
        });
        counters
            .series_evicted_idle_total
            .fetch_add(evicted, Ordering::Relaxed);
        let current = counters.active_series.load(Ordering::Relaxed);
        counters
            .active_series
            .fetch_sub(evicted.min(current), Ordering::Relaxed);
        freed
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
                    // Reached by replay, whose WAL was already filtered by
                    // admission; live ingest reserves its entries in
                    // `admit_datapoints` and lands here on the Some arm.
                    added += sample.labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                    self.counters
                        .series_created_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters.active_series.fetch_add(1, Ordering::Relaxed);
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
            datapoint_index: 0,
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

    fn indexed(
        labels: &SeriesLabels,
        ts: i64,
        value: f64,
        kind: SampleKind,
        datapoint: u32,
    ) -> MetricSample {
        MetricSample {
            datapoint_index: datapoint,
            ..sample(labels, ts, value, kind)
        }
    }

    #[test]
    fn admission_refuses_only_datapoints_needing_new_series_past_the_cap() {
        let memtable = SeriesMemTable::new();
        let known = labels("queue_depth", "a");
        let other_known = labels("queue_depth", "b");
        memtable.insert(vec![
            sample(&known, 100, 1.0, SampleKind::Gauge),
            sample(&other_known, 100, 1.0, SampleKind::Gauge),
        ]);
        let fresh = labels("queue_depth", "c");
        let samples = vec![
            indexed(&known, 200, 2.0, SampleKind::Gauge, 0),
            indexed(&fresh, 200, 9.0, SampleKind::Gauge, 1),
        ];
        let outcome = memtable.admit_datapoints(&test_tenant(), &samples, 2, i64::MIN);
        assert!(
            outcome.admitted.contains(&0),
            "the known series' datapoint passes"
        );
        assert!(
            !outcome.admitted.contains(&1),
            "the new series' datapoint is refused"
        );
        assert_eq!(outcome.rejected_datapoints, 1);
        assert_eq!(outcome.rejected_new_series, 1);
        assert_eq!(
            memtable.active_series(&test_tenant()),
            2,
            "nothing was reserved for the refusal"
        );
        assert_eq!(
            memtable
                .counters()
                .series_rejected_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(memtable.counters().active_series.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn idle_flushed_series_return_their_capacity_under_pressure() {
        let memtable = SeriesMemTable::new();
        let old = labels("queue_depth", "old");
        memtable.insert(vec![sample(&old, 100, 1.0, SampleKind::Gauge)]);
        let fresh = labels("queue_depth", "fresh");
        let samples = vec![indexed(&fresh, 1_000, 2.0, SampleKind::Gauge, 0)];
        // Still buffered: the idle series cannot be evicted without losing
        // samples, so the newcomer is refused.
        let outcome = memtable.admit_datapoints(&test_tenant(), &samples, 1, 500);
        assert!(outcome.admitted.is_empty());
        // Flushed: the idle state is evictable, and admission reclaims it.
        memtable.begin_flush();
        memtable.commit_flush();
        let outcome = memtable.admit_datapoints(&test_tenant(), &samples, 1, 500);
        assert!(outcome.admitted.contains(&0));
        assert_eq!(memtable.active_series(&test_tenant()), 1);
        assert_eq!(
            memtable
                .counters()
                .series_evicted_idle_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn a_series_still_fresh_at_the_horizon_is_not_evicted() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 1_000, 1.0, SampleKind::Gauge)]);
        memtable.begin_flush();
        memtable.commit_flush();
        assert_eq!(memtable.evict_idle(500), 0, "newer than the cutoff");
        assert_eq!(memtable.evict_idle(2_000), 1, "idle past the cutoff");
        assert_eq!(memtable.active_series(&test_tenant()), 0);
    }

    #[test]
    fn eviction_resets_the_delta_total_which_is_exactly_a_counter_reset() {
        let memtable = SeriesMemTable::new();
        let series = labels("churn_requests_total", "a");
        memtable.insert(vec![sample(&series, 100, 5.0, SampleKind::Delta)]);
        memtable.begin_flush();
        memtable.commit_flush();
        memtable.evict_idle(i64::MAX);
        memtable.insert(vec![sample(&series, 200, 3.0, SampleKind::Delta)]);
        let sorted = memtable.sorted_samples(&test_tenant()).unwrap();
        assert_eq!(
            sorted.get(&series).unwrap(),
            &vec![(200, 3.0)],
            "the total restarts at the delta — the counter-reset shape rate's \
positive-delta sum is defined to absorb"
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
