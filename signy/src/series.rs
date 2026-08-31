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
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
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

/// A process-local, weak string interner for canonical series identities.
///
/// A metric label set is present in the active memtable and, after a flush,
/// often in several part catalogs at once.  Keeping a separate `Vec<u8>` for
/// each of those copies made catalog memory grow with the number of parts,
/// even though all copies represented the same identity.  The interner shares
/// the immutable `Arc<[u8]>` payload while at least one owner is alive.
///
/// Values are weak on purpose: this is a cache of allocations, not a second
/// owner of every label ever observed.  Dead entries are removed incrementally
/// from the shard that receives new identities, so a churn workload does not
/// turn the optimization into an unbounded label store.  Hashes select a
/// shard only; equality is still checked against the complete canonical bytes,
/// so a hash collision can never merge two series.
struct LabelInterner {
    shards: Vec<Mutex<LabelInternerShard>>,
}

struct LabelInternerShard {
    by_hash: HashMap<u64, std::sync::Weak<[u8]>>,
    registrations_since_sweep: usize,
}

const LABEL_INTERNER_SHARDS: usize = 64;
const LABEL_INTERNER_SWEEP_INTERVAL: usize = 4096;

static LABEL_INTERNER: OnceLock<LabelInterner> = OnceLock::new();

impl LabelInterner {
    fn global() -> &'static Self {
        LABEL_INTERNER.get_or_init(|| LabelInterner {
            shards: (0..LABEL_INTERNER_SHARDS)
                .map(|_| {
                    Mutex::new(LabelInternerShard {
                        by_hash: HashMap::new(),
                        registrations_since_sweep: 0,
                    })
                })
                .collect(),
        })
    }

    fn intern(&self, labels: SeriesLabels) -> SeriesLabels {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        labels.0.hash(&mut hasher);
        let hash = hasher.finish();
        let shard_index = (hash as usize) % self.shards.len();
        let mut shard = self.shards[shard_index].lock();
        // Keep the full byte comparison after the hash lookup.  The hash is a
        // routing aid, never the series identity.
        if let Some(weak) = shard.by_hash.get(&hash) {
            if let Some(existing) = weak.upgrade() {
                if existing.as_ref() == labels.0.as_ref() {
                    return SeriesLabels(existing);
                }
                // A hash collision cannot merge identities.  Keeping the
                // existing entry also means the colliding label remains an
                // ordinary, correctly comparable Arc without allocating a
                // collision bucket for the overwhelmingly common case.
                return labels;
            }
            shard.by_hash.remove(&hash);
        }
        shard
            .by_hash
            .insert(hash, std::sync::Arc::downgrade(&labels.0));
        shard.registrations_since_sweep += 1;
        if shard.registrations_since_sweep >= LABEL_INTERNER_SWEEP_INTERVAL {
            shard.registrations_since_sweep = 0;
            shard.by_hash.retain(|_, weak| weak.strong_count() != 0);
        }
        labels
    }
}

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

    /// Rehydrate from stored canonical bytes (a part's catalog). The bytes
    /// crossed a checksum, not a validator — callers that read them from disk
    /// decode `pairs()` once to fail early on corruption.
    pub fn from_canonical(bytes: Vec<u8>) -> Self {
        Self(bytes.into()).intern()
    }

    /// Return a copy of this identity whose immutable byte payload is shared
    /// with any other live copy in this process.  This is intentionally an
    /// explicit operation rather than part of every `from_pairs` call: label
    /// normalization is a hot ingest path, while only identities retained by
    /// the memtable or part catalog need process-wide sharing.
    pub(crate) fn intern(self) -> Self {
        LabelInterner::global().intern(self)
    }

    #[cfg(test)]
    pub(crate) fn shares_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
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

/// Legacy per-datapoint admission result retained for in-memory callers.
/// Live OTLP ingest uses [`SeriesMemTable::admit_request`] and refuses a
/// complete export under memory/cardinality pressure.
pub struct AdmitOutcome {
    /// Datapoint indices whose series were all admitted. Samples and WAL
    /// bytes for the rest must be dropped by the caller.
    pub admitted: std::collections::HashSet<u32>,
    pub rejected_datapoints: u64,
    pub rejected_samples: u64,
    pub rejected_new_series: u64,
}

/// The series entries reserved by one metric export.
///
/// Admission happens before the journal append, so the entries have to be
/// visible while the append is in flight (otherwise two concurrent exports can
/// both reserve the same last byte).  The writer owns this value after the
/// append is queued.  A successful insert commits it; dropping it on a failed
/// append releases its reference; a failed append additionally removes an
/// empty entry once no other in-flight append still relies on it.
pub struct SeriesAdmission {
    owner: Arc<SeriesMemTable>,
    reserved_series: Vec<(TenantId, SeriesLabels)>,
    committed: bool,
}

impl SeriesAdmission {
    pub fn reserved_series_count(&self) -> usize {
        self.reserved_series.len()
    }

    pub fn commit(mut self) {
        self.owner.commit_admission(&self.reserved_series);
        self.committed = true;
    }
}

impl Drop for SeriesAdmission {
    fn drop(&mut self) {
        if self.committed || self.reserved_series.is_empty() {
            return;
        }
        self.owner.rollback_admission(&self.reserved_series);
    }
}

/// A request-wide admission failure.  Unlike the old per-datapoint ladder,
/// memory/cardinality pressure refuses the complete export.
#[derive(Debug)]
pub struct AdmissionError {
    pub new_series: usize,
    pub active_series: usize,
    pub limit: usize,
}

impl AdmitOutcome {
    pub fn rejected_any(&self) -> bool {
        self.rejected_datapoints > 0
    }
}

/// The ladder's observability: every rung moves a counter, because a
/// degradation nobody can see is indistinguishable from a bug. Rendered under
/// `signy_*` names by `/metrics`.
#[derive(Default)]
pub struct SeriesCounters {
    /// Live series index entries across all tenants — a gauge.
    pub active_series: AtomicU64,
    pub series_created_total: AtomicU64,
    pub series_evicted_idle_total: AtomicU64,
    /// New series refused by the legacy per-datapoint helper.
    pub series_rejected_total: AtomicU64,
    pub metric_datapoints_rejected_total: AtomicU64,
    pub metric_samples_rejected_total: AtomicU64,
    /// Whole exports refused by the process-wide emergency count guard.
    pub metric_cardinality_rejected_total: AtomicU64,
    /// Whole exports refused because the shared memtable byte budget had no
    /// room for their projected sample buffers.
    pub metric_memory_rejected_total: AtomicU64,
}

// The old 160-byte estimate omitted allocator/container overhead.  The M10
// attribution measured roughly 1.7x the estimate, so reserve a conservative
// 320 bytes per live entry.  This is deliberately a byte charge, not a
// cardinality proxy: labels still contribute their actual canonical length.
const SERIES_OVERHEAD_BYTES: u64 = 320;
const SPILL_SAMPLE_BYTES: u64 = 16;
const ADMITTED_SAMPLE_BYTES: u64 = 64;

/// A handle into a tenant's sample-buffer arena.
///
/// `Option<NonZeroU32>` is four bytes, whereas `Option<u32>` is eight: the
/// zero value is reserved for "no samples are buffered".  The arena only
/// contains entries that currently have samples, so a flushed series does not
/// retain three empty `Vec` headers and an empty Gorilla encoder in its index
/// value.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SeriesBufferId(NonZeroU32);

/// State that must survive a successful flush for a series.
///
/// This is intentionally separate from [`SeriesBuffer`].  Most live series in
/// a cardinality workload have already flushed their samples, but still need
/// to remain discoverable in the active-series index.  Keeping the sample
/// vectors inline made those historical identities pay for four empty dynamic
/// containers each.
#[derive(Default)]
struct SeriesState {
    buffer_id: Option<SeriesBufferId>,
    admission_refs: u32,
    flags: u8,
    last_ts: i64,
    admitted_ts: i64,
    buffered_min: i64,
    buffered_max: i64,
    running_total: f64,
}

impl SeriesState {
    const HAS_LAST_TS: u8 = 1;
    const HAS_ADMITTED_TS: u8 = 2;
    const HAS_BUFFERED: u8 = 4;
    const HAS_RUNNING_TOTAL: u8 = 8;

    fn last_ts(&self) -> Option<i64> {
        (self.flags & Self::HAS_LAST_TS != 0).then_some(self.last_ts)
    }

    fn admitted_ts(&self) -> Option<i64> {
        (self.flags & Self::HAS_ADMITTED_TS != 0).then_some(self.admitted_ts)
    }

    fn set_admitted_ts(&mut self, ts_ns: Option<i64>) {
        if let Some(ts_ns) = ts_ns {
            self.admitted_ts = ts_ns;
            self.flags |= Self::HAS_ADMITTED_TS;
        }
    }

    fn running_total(&self) -> Option<f64> {
        (self.flags & Self::HAS_RUNNING_TOTAL != 0).then_some(self.running_total)
    }

    fn set_running_total(&mut self, total: f64) {
        self.running_total = total;
        self.flags |= Self::HAS_RUNNING_TOTAL;
    }

    fn bounds(&self) -> Option<(i64, i64)> {
        (self.flags & Self::HAS_BUFFERED != 0).then_some((self.buffered_min, self.buffered_max))
    }

    fn take_bounds(&mut self) -> Option<(i64, i64)> {
        let bounds = self.bounds();
        self.flags &= !Self::HAS_BUFFERED;
        bounds
    }

    fn observe(&mut self, ts_ns: i64) {
        if let Some((min, max)) = self.bounds() {
            self.buffered_min = min.min(ts_ns);
            self.buffered_max = max.max(ts_ns);
        } else {
            self.buffered_min = ts_ns;
            self.buffered_max = ts_ns;
            self.flags |= Self::HAS_BUFFERED;
        }
    }

    fn absorb(&mut self, bounds: Option<(i64, i64)>) {
        if let Some((min, max)) = bounds {
            self.observe(min);
            self.observe(max);
        }
    }

    fn clear_admitted_after_insert(&mut self) {
        if self.admission_refs == 0 {
            self.flags &= !Self::HAS_ADMITTED_TS;
        }
    }
}

struct SeriesBuffer {
    /// Chunks returned by aborted flushes, oldest first. Closed streams
    /// cannot be appended to, so they wait here for the next flush.
    closed: Vec<Vec<u8>>,
    open: gorilla::Encoder,
    /// Samples that arrived with a timestamp older than the newest appended
    /// one. The Gorilla stream is append-ordered; the flush merge-sorts this
    /// vector in.
    spill: Vec<(i64, f64)>,
}

impl SeriesBuffer {
    fn new() -> Self {
        Self {
            closed: Vec::new(),
            open: gorilla::Encoder::new(),
            spill: Vec::new(),
        }
    }

    fn has_samples(&self) -> bool {
        !self.closed.is_empty() || !self.open.is_empty() || !self.spill.is_empty()
    }
}

/// Per-tenant index and sample arena.  The index's value is compact persistent
/// state; only buffered series have an entry in `buffers`.
#[derive(Default)]
struct TenantSeries {
    states: HashMap<SeriesLabels, SeriesState>,
    buffers: HashMap<SeriesBufferId, SeriesBuffer>,
    next_buffer_id: u32,
}

impl TenantSeries {
    fn alloc_buffer(&mut self) -> SeriesBufferId {
        // A process cannot have four billion simultaneously buffered series;
        // still, skip zero and any live id so the handle remains valid if the
        // counter wraps after a very long-running process.
        let mut raw = self.next_buffer_id;
        loop {
            raw = raw.wrapping_add(1);
            if raw == 0 {
                continue;
            }
            let id = SeriesBufferId(NonZeroU32::new(raw).expect("non-zero buffer id"));
            if !self.buffers.contains_key(&id) {
                self.next_buffer_id = raw;
                self.buffers.insert(id, SeriesBuffer::new());
                return id;
            }
        }
    }

    fn has_samples(&self, state: &SeriesState) -> bool {
        state
            .buffer_id
            .and_then(|id| self.buffers.get(&id))
            .is_some_and(SeriesBuffer::has_samples)
    }
}

/// Subtract without wrapping.
///
/// An accounting slip that went *negative* on a `u64` did not read as a small
/// error: it read as eighteen quintillion buffered bytes, and the ingest gate
/// refused every push against it until the process restarted. A gauge that is
/// a little wrong is a bug to find; a gauge that is `u64::MAX` is an outage,
/// and the two must not be the same failure.
fn saturating_release(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

/// Whether two inclusive timestamp ranges touch.
fn ranges_overlap(bounds: Option<(i64, i64)>, start_ns: i64, end_ns: i64) -> bool {
    // An absent bound cannot prune: metadata that cannot answer must not be
    // able to hide data (the rule `trace_part` states for its row groups).
    bounds.is_none_or(|(min, max)| max >= start_ns && min <= end_ns)
}

/// One series' buffered samples as `begin_flush` hands them to the flush:
/// closed chunks oldest-first, plus the out-of-order spill.
pub struct SnapshotSeries {
    pub labels: SeriesLabels,
    pub chunks: Vec<Vec<u8>>,
    pub spill: Vec<(i64, f64)>,
    /// The timestamps these samples span, carried so an abort can return them
    /// to the buffer without decoding what it is putting back.
    pub bounds: Option<(i64, i64)>,
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
    inner: RwLock<BTreeMap<TenantId, TenantSeries>>,
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

    /// Reserve every series in an export under one write lock.
    ///
    /// `groups` contains the tenant-split pieces of one OTLP export.  The
    /// caller must pass all pieces together: checking them one tenant at a
    /// time would make a multi-tenant request partially accepted under a
    /// global cardinality guard.  The returned reservations are in the same
    /// order as `groups` and are transferred to the journal append items.
    pub fn admit_request(
        self: &Arc<Self>,
        groups: &[(&TenantId, &[MetricSample])],
        max_active: Option<usize>,
        idle_cutoff_ns: i64,
    ) -> Result<Vec<SeriesAdmission>, AdmissionError> {
        let _arena = crate::memprof::enter(crate::memprof::Arena::SeriesMemtable);
        let mut inner = self.inner.write();

        // Preserve the existing lazy idle-eviction behaviour.  It is done
        // before the guard is evaluated and under the same write lock, so a
        // concurrent admission cannot observe a slot that is about to return.
        for tenant_series in inner.values_mut() {
            let freed = Self::evict_idle_locked(tenant_series, idle_cutoff_ns, &self.counters);
            saturating_release(&self.inner_bytes, freed);
        }

        let mut requested: BTreeMap<TenantId, BTreeSet<SeriesLabels>> = BTreeMap::new();
        let mut requested_newest: BTreeMap<(TenantId, SeriesLabels), i64> = BTreeMap::new();
        for (tenant, samples) in groups {
            let labels = requested.entry((*tenant).clone()).or_default();
            for sample in samples.iter() {
                labels.insert(sample.labels.clone());
                requested_newest
                    .entry(((*tenant).clone(), sample.labels.clone()))
                    .and_modify(|newest| *newest = (*newest).max(sample.ts_ns))
                    .or_insert(sample.ts_ns);
            }
        }
        let active = self.counters.active_series.load(Ordering::Relaxed) as usize;
        let new_count = requested
            .iter()
            .map(|(tenant, labels)| {
                let existing = inner.get(tenant).map_or(0, |series| series.states.len());
                labels
                    .iter()
                    .filter(|label| {
                        inner
                            .get(tenant)
                            .is_none_or(|series| !series.states.contains_key(*label))
                    })
                    .count()
                    .min(usize::MAX.saturating_sub(existing))
            })
            .sum::<usize>();
        if let Some(limit) = max_active
            && active.saturating_add(new_count) > limit
        {
            self.counters
                .metric_cardinality_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            self.counters
                .series_rejected_total
                .fetch_add(new_count as u64, Ordering::Relaxed);
            return Err(AdmissionError {
                new_series: new_count,
                active_series: active,
                limit,
            });
        }

        // Make every requested label visible before returning.  This is what
        // serializes concurrent admissions; the samples themselves arrive at
        // the writer only after the WAL fsync.
        for (tenant, labels) in &requested {
            let tenant_series = inner.entry(tenant.clone()).or_default();
            for label in labels {
                if tenant_series.states.contains_key(label) {
                    continue;
                }
                let mut state = SeriesState::default();
                state.set_admitted_ts(
                    requested_newest
                        .get(&(tenant.clone(), label.clone()))
                        .copied(),
                );
                tenant_series.states.insert(label.clone().intern(), state);
                self.counters
                    .series_created_total
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.active_series.fetch_add(1, Ordering::Relaxed);
                self.inner_bytes.fetch_add(
                    label.byte_len() as u64 + SERIES_OVERHEAD_BYTES,
                    Ordering::Relaxed,
                );
            }
        }

        let mut admissions = Vec::with_capacity(groups.len());
        for (tenant, samples) in groups {
            let mut reserved_series = Vec::new();
            let mut seen = BTreeSet::new();
            for sample in samples.iter() {
                if !seen.insert(sample.labels.clone()) {
                    continue;
                }
                if let Some(state) = inner
                    .get_mut(*tenant)
                    .and_then(|series| series.states.get_mut(&sample.labels))
                {
                    // Empty entries are reservations, including one made by
                    // a concurrent request. Hold a reference for this append
                    // so that the other request cannot roll the entry back
                    // while this append is still in flight.
                    if state.buffer_id.is_none() {
                        state.admission_refs = state.admission_refs.saturating_add(1);
                        reserved_series.push(((*tenant).clone(), sample.labels.clone()));
                    }
                }
            }
            admissions.push(SeriesAdmission {
                owner: self.clone(),
                reserved_series,
                committed: false,
            });
        }
        Ok(admissions)
    }

    /// Conservative growth estimate for samples that have been admitted but
    /// not inserted by the journal writer yet.  Existing series still grow a
    /// Gorilla stream, and out-of-order samples use a spill vector; charging
    /// the canonical label allocation plus 64 bytes per sample covers both
    /// without pretending compression is a stable memory contract.  The
    /// normalized request owns one canonical label allocation per sample until
    /// the writer inserts it, so this temporary copy is part of admission too.
    pub fn estimate_sample_bytes(groups: &[(&TenantId, &[MetricSample])]) -> u64 {
        groups
            .iter()
            .flat_map(|(_, samples)| samples.iter())
            .map(|sample| (sample.labels.byte_len() as u64).saturating_add(ADMITTED_SAMPLE_BYTES))
            .sum()
    }

    fn commit_admission(&self, reserved_series: &[(TenantId, SeriesLabels)]) {
        let mut inner = self.inner.write();
        for (tenant, labels) in reserved_series {
            let Some(state) = inner
                .get_mut(tenant)
                .and_then(|series| series.states.get_mut(labels))
            else {
                continue;
            };
            state.admission_refs = state.admission_refs.saturating_sub(1);
        }
    }

    fn rollback_admission(&self, reserved_series: &[(TenantId, SeriesLabels)]) {
        let mut inner = self.inner.write();
        let mut freed = 0u64;
        let mut removed = 0u64;
        for (tenant, labels) in reserved_series {
            let Some(series) = inner.get_mut(tenant) else {
                continue;
            };
            let remove = if let Some(state) = series.states.get_mut(labels) {
                state.admission_refs = state.admission_refs.saturating_sub(1);
                state.buffer_id.is_none() && state.admission_refs == 0
            } else {
                false
            };
            if remove {
                if let Some(state) = series.states.remove(labels)
                    && let Some(id) = state.buffer_id
                {
                    series.buffers.remove(&id);
                }
                freed = freed.saturating_add(labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES);
                removed += 1;
            }
        }
        saturating_release(&self.inner_bytes, freed);
        let current = self.counters.active_series.load(Ordering::Relaxed);
        self.counters
            .active_series
            .fetch_sub(removed.min(current), Ordering::Relaxed);
    }

    /// Legacy per-datapoint helper, retained for callers that use the series
    /// table directly.  Transport ingest uses [`Self::admit_request`], whose
    /// memory/cardinality decision is whole-export and process-wide.
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
        // A reservation allocates the entry that outlives this call: the
        // canonical label bytes and the `SeriesBuffer` that holds the open
        // Gorilla stream. Charged where it lives rather than to the ingest
        // decode that happened to trigger it.
        let _arena = crate::memprof::enter(crate::memprof::Arena::SeriesMemtable);
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
                .filter(|labels| !tenant_series.states.contains_key(*labels))
                .collect();
            new_labels.sort();
            new_labels.dedup();
            if !new_labels.is_empty() && tenant_series.states.len() + new_labels.len() > max_active
            {
                // One eviction pass per export: a second would find nothing
                // new, and the refusal below must not degrade into a scan per
                // datapoint.
                if !evicted_for_pressure {
                    evicted_for_pressure = true;
                    let evicted =
                        Self::evict_idle_locked(tenant_series, idle_cutoff_ns, &self.counters);
                    saturating_release(&self.inner_bytes, evicted);
                }
                if tenant_series.states.len() + new_labels.len() > max_active {
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
            let newest = group.iter().map(|sample| sample.ts_ns).max();
            let mut reserved = 0u64;
            for labels in new_labels {
                reserved += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                let mut state = SeriesState::default();
                state.set_admitted_ts(newest);
                tenant_series
                    .states
                    .insert((*labels).clone().intern(), state);
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
            let before = tenant_series.states.len() as u64;
            freed += Self::evict_idle_locked(tenant_series, idle_cutoff_ns, &self.counters);
            evicted += before - tenant_series.states.len() as u64;
        }
        saturating_release(&self.inner_bytes, freed);
        evicted
    }

    fn evict_idle_locked(
        tenant_series: &mut TenantSeries,
        idle_cutoff_ns: i64,
        counters: &SeriesCounters,
    ) -> u64 {
        let mut freed = 0u64;
        let mut evicted = 0u64;
        let idle: Vec<(SeriesLabels, Option<SeriesBufferId>)> = tenant_series
            .states
            .iter()
            .filter_map(|(labels, state)| {
                let has_samples = tenant_series.has_samples(state);
                let idle = !has_samples
                    && state.last_ts().is_none_or(|last| last < idle_cutoff_ns)
                    && state
                        .admitted_ts()
                        .is_none_or(|admitted| admitted < idle_cutoff_ns)
                    && state.admission_refs == 0;
                idle.then(|| (labels.clone(), state.buffer_id))
            })
            .collect();
        for (labels, buffer_id) in idle {
            if let Some(state) = tenant_series.states.remove(&labels)
                && let Some(id) = state.buffer_id.or(buffer_id)
            {
                tenant_series.buffers.remove(&id);
            }
            freed += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
            evicted += 1;
        }
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
        // Same arena as the reservation: what grows here is the open stream
        // and the spill vector of an entry that stays until it is evicted.
        let _arena = crate::memprof::enter(crate::memprof::Arena::SeriesMemtable);
        let mut added = 0u64;
        let mut inner = self.inner.write();
        for sample in samples {
            let MetricSample {
                tenant,
                labels,
                ts_ns,
                value: raw_value,
                kind,
                ..
            } = sample;
            let tenant_series = inner.entry(tenant).or_default();
            if !tenant_series.states.contains_key(&labels) {
                // Reached by replay, whose WAL was already filtered by
                // admission; live ingest reserves its entries in
                // `admit_request` and lands here on the existing state arm
                // unless another request rolled an uncommitted reservation
                // back after this request was admitted.
                added += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                self.counters
                    .series_created_total
                    .fetch_add(1, Ordering::Relaxed);
                self.counters.active_series.fetch_add(1, Ordering::Relaxed);
                tenant_series
                    .states
                    .insert(labels.clone().intern(), SeriesState::default());
            }
            let buffer_id = if let Some(id) = tenant_series
                .states
                .get(&labels)
                .and_then(|state| state.buffer_id)
            {
                id
            } else {
                let id = tenant_series.alloc_buffer();
                tenant_series
                    .states
                    .get_mut(&labels)
                    .expect("series state inserted before buffer")
                    .buffer_id = Some(id);
                id
            };
            let value = {
                let state = tenant_series
                    .states
                    .get_mut(&labels)
                    .expect("series state exists for every sample");
                match kind {
                    SampleKind::Gauge | SampleKind::Cumulative => raw_value,
                    SampleKind::Delta => {
                        let total = state.running_total().unwrap_or(0.0) + raw_value;
                        state.set_running_total(total);
                        total
                    }
                }
            };
            let last_ts = tenant_series
                .states
                .get(&labels)
                .and_then(SeriesState::last_ts);
            let buffer = tenant_series
                .buffers
                .get_mut(&buffer_id)
                .expect("series state points at its sample buffer");
            if last_ts.is_some_and(|last| ts_ns < last) {
                buffer.spill.push((ts_ns, value));
                added += SPILL_SAMPLE_BYTES;
            } else {
                let before = buffer.open.byte_len();
                buffer.open.append(ts_ns, value);
                added += (buffer.open.byte_len() - before) as u64;
            }
            let state = tenant_series
                .states
                .get_mut(&labels)
                .expect("series state exists for every sample");
            if last_ts.is_none_or(|last| ts_ns >= last) {
                state.last_ts = ts_ns;
                state.flags |= SeriesState::HAS_LAST_TS;
            }
            state.clear_admitted_after_insert();
            state.observe(ts_ns);
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
            for (labels, state) in series_map.states.iter_mut() {
                let Some(buffer_id) = state.buffer_id else {
                    continue;
                };
                let Some(mut buffer) = series_map.buffers.remove(&buffer_id) else {
                    state.buffer_id = None;
                    continue;
                };
                if !buffer.has_samples() {
                    state.buffer_id = None;
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
                state.buffer_id = None;
                list.push(SnapshotSeries {
                    labels: labels.clone(),
                    chunks,
                    spill,
                    bounds: state.take_bounds(),
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
        saturating_release(&self.inner_bytes, moved);
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
        // Bytes for entries this abort has to re-create. A series whose
        // samples were mid-flush can have been evicted in between — its
        // state left the accounting when it did, and putting the entry back
        // without putting the bytes back is what made a later eviction
        // subtract what was never added.
        let mut restored_state = 0u64;
        for (tenant, list) in snapshot.tenants {
            let tenant_series = inner.entry(tenant).or_default();
            for series in list {
                if !tenant_series.states.contains_key(&series.labels) {
                    restored_state += series.labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                    tenant_series
                        .states
                        .insert(series.labels.clone().intern(), SeriesState::default());
                }
                let buffer_id = if let Some(id) = tenant_series
                    .states
                    .get(&series.labels)
                    .and_then(|state| state.buffer_id)
                {
                    id
                } else {
                    let id = tenant_series.alloc_buffer();
                    tenant_series
                        .states
                        .get_mut(&series.labels)
                        .expect("series state inserted before buffer")
                        .buffer_id = Some(id);
                    id
                };
                let buffer = tenant_series
                    .buffers
                    .get_mut(&buffer_id)
                    .expect("series state points at its sample buffer");
                // The snapshot's samples are older than anything inserted
                // since, so its chunks go to the front and its spill stays
                // spill.
                let mut chunks = series.chunks;
                chunks.append(&mut buffer.closed);
                buffer.closed = chunks;
                buffer.spill.extend(series.spill);
                tenant_series
                    .states
                    .get_mut(&series.labels)
                    .expect("series state exists for aborted snapshot")
                    .absorb(series.bounds);
            }
        }
        drop(inner);
        let returned = self.flushing_bytes.swap(0, Ordering::Relaxed);
        self.inner_bytes
            .fetch_add(returned.saturating_add(restored_state), Ordering::Relaxed);
    }

    pub fn is_empty(&self) -> bool {
        let has_inner = self.inner.read().values().any(|series| {
            series
                .states
                .values()
                .any(|state| series.has_samples(state))
        });
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
            .filter(|(_, series)| {
                series
                    .states
                    .values()
                    .any(|state| series.has_samples(state))
            })
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
    /// not. The emergency count guard meters the process-wide sum.
    pub fn active_series(&self, tenant: &TenantId) -> usize {
        self.inner
            .read()
            .get(tenant)
            .map(|series| series.states.len())
            .unwrap_or(0)
    }

    /// Every live series identity a tenant holds — the memtable half of
    /// selection and discovery. Entries whose samples have all flushed still
    /// appear: their state is live, and their history answers from parts.
    pub fn series_labels(&self, tenant: &TenantId) -> Vec<SeriesLabels> {
        self.inner
            .read()
            .get(tenant)
            .map(|series| series.states.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The tenant's series whose *buffered* samples reach `[start_ns,
    /// end_ns]`, the in-flight flush included. Answered from the recorded
    /// bounds alone, so a discovery request never decodes a chunk — and a
    /// series whose samples have all been flushed is left to the part
    /// catalogs, which is where they now are.
    pub fn series_labels_in_range(
        &self,
        tenant: &TenantId,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<SeriesLabels> {
        let mut labels: BTreeSet<SeriesLabels> = BTreeSet::new();
        if let Some(snapshot) = self.flushing.read().as_ref()
            && let Some(list) = snapshot.tenants.get(tenant)
        {
            for series in list {
                if ranges_overlap(series.bounds, start_ns, end_ns) {
                    labels.insert(series.labels.clone());
                }
            }
        }
        if let Some(series) = self.inner.read().get(tenant) {
            for (key, state) in &series.states {
                if series.has_samples(state) && ranges_overlap(state.bounds(), start_ns, end_ns) {
                    labels.insert(key.clone());
                }
            }
        }
        labels.into_iter().collect()
    }

    /// One series' buffered samples, time-sorted — the per-series read the
    /// executor merges with the part chunks. The open encoder is cloned and
    /// closed rather than disturbed.
    pub fn sorted_samples_of(
        &self,
        tenant: &TenantId,
        labels: &SeriesLabels,
    ) -> Result<Vec<(i64, f64)>, String> {
        let mut samples = Vec::new();
        {
            let flushing = self.flushing.read();
            if let Some(snapshot) = flushing.as_ref()
                && let Some(list) = snapshot.tenants.get(tenant)
            {
                for series in list {
                    if series.labels == *labels {
                        samples.extend(series.sorted_samples()?);
                    }
                }
            }
        }
        let inner = self.inner.read();
        if let Some(series) = inner.get(tenant)
            && let Some(state) = series.states.get(labels)
            && let Some(buffer_id) = state.buffer_id
            && let Some(buffer) = series.buffers.get(&buffer_id)
        {
            for chunk in &buffer.closed {
                samples.extend(gorilla::decode_all(chunk)?);
            }
            if !buffer.open.is_empty() {
                samples.extend(gorilla::decode_all(&buffer.open.clone().close())?);
            }
            samples.extend(buffer.spill.iter().copied());
        }
        drop(inner);
        samples.sort_by_key(|(ts, _)| *ts);
        Ok(samples)
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
            for (labels, state) in &series_map.states {
                let Some(buffer_id) = state.buffer_id else {
                    continue;
                };
                let Some(buffer) = series_map.buffers.get(&buffer_id) else {
                    continue;
                };
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
                            bounds: series.bounds,
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
    fn flushed_series_state_does_not_inline_sample_storage() {
        assert!(
            std::mem::size_of::<SeriesState>() < std::mem::size_of::<SeriesBuffer>(),
            "persistent series state must stay smaller than the sample buffer"
        );
        assert!(
            std::mem::size_of::<SeriesState>() <= 64,
            "series index values are expected to remain compact"
        );
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
    fn live_canonical_labels_share_their_payload_without_changing_identity() {
        let labels = labels("up", "a");
        let retained = labels.clone().intern();
        let from_part = SeriesLabels::from_canonical(labels.as_bytes().to_vec());
        assert!(Arc::ptr_eq(&retained.0, &from_part.0));
        assert_eq!(retained, from_part);

        let different = SeriesLabels::from_pairs(vec![
            (METRIC_NAME_LABEL.to_string(), "up".to_string()),
            ("instance".to_string(), "b".to_string()),
        ])
        .intern();
        assert!(!Arc::ptr_eq(&retained.0, &different.0));
        assert_ne!(retained, different);
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
    fn request_admission_is_global_and_refuses_all_tenants_atomically() {
        let memtable = Arc::new(SeriesMemTable::new());
        let first_tenant = test_tenant();
        let second_tenant = TenantId::parse("other").unwrap();
        let first_labels = labels("queue_depth", "first");
        let second_labels = labels("queue_depth", "second");
        let mut first = sample(&first_labels, 100, 1.0, SampleKind::Gauge);
        let mut second = sample(&second_labels, 100, 1.0, SampleKind::Gauge);
        first.tenant = first_tenant.clone();
        second.tenant = second_tenant.clone();
        let groups = vec![
            (&first_tenant, std::slice::from_ref(&first)),
            (&second_tenant, std::slice::from_ref(&second)),
        ];
        let error = match memtable.admit_request(&groups, Some(1), i64::MIN) {
            Ok(_) => panic!("the process-wide guard counts both tenants"),
            Err(error) => error,
        };
        assert_eq!(error.new_series, 2);
        assert_eq!(memtable.active_series(&first_tenant), 0);
        assert_eq!(memtable.active_series(&second_tenant), 0);
    }

    #[test]
    fn dropped_request_admission_rolls_back_reserved_series() {
        let memtable = Arc::new(SeriesMemTable::new());
        let tenant = test_tenant();
        let labels = labels("queue_depth", "reserved");
        let sample = sample(&labels, 100, 1.0, SampleKind::Gauge);
        let groups = vec![(&tenant, std::slice::from_ref(&sample))];
        let admissions = memtable
            .admit_request(&groups, None, i64::MIN)
            .expect("the empty table has room");
        assert_eq!(memtable.active_series(&tenant), 1);
        drop(admissions);
        assert_eq!(
            memtable.active_series(&tenant),
            0,
            "a journal enqueue failure must not leave a phantom series"
        );
        assert_eq!(memtable.approximate_size(), 0);
    }

    #[test]
    fn concurrent_empty_reservations_keep_each_other_alive() {
        let memtable = Arc::new(SeriesMemTable::new());
        let tenant = test_tenant();
        let labels = labels("queue_depth", "shared");
        let sample = sample(&labels, 100, 1.0, SampleKind::Gauge);
        let groups = vec![(&tenant, std::slice::from_ref(&sample))];

        let first = memtable
            .admit_request(&groups, None, i64::MIN)
            .expect("the first request has room");
        let second = memtable
            .admit_request(&groups, None, i64::MAX)
            .expect("the second request shares the reserved entry");
        assert_eq!(memtable.active_series(&tenant), 1);

        // The first request failed after the second had already observed its
        // empty reservation. Its rollback must release only its own ref.
        drop(first);
        assert_eq!(memtable.active_series(&tenant), 1);
        assert!(memtable.approximate_size() > 0);

        drop(second);
        assert_eq!(memtable.active_series(&tenant), 0);
        assert_eq!(memtable.approximate_size(), 0);
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

    /// The defect the published bed run found: a series evicted while its
    /// samples were mid-flush is re-created by the abort, and the entry came
    /// back without its state bytes — so the *next* eviction subtracted what
    /// was never added, wrapped the `u64`, and the ingest gate refused every
    /// push against an eighteen-quintillion-byte memtable.
    #[test]
    fn an_abort_that_recreates_an_evicted_series_keeps_the_accounting_sound() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "a");
        memtable.insert(vec![sample(&series, 1_000, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        // The samples are in the snapshot, so the entry looks idle and the
        // sweep takes it.
        assert_eq!(memtable.evict_idle(i64::MAX), 1);
        assert_eq!(memtable.active_series(&test_tenant()), 0);
        // The flush then fails and puts the samples back.
        memtable.abort_flush(snapshot);
        assert_eq!(memtable.active_series(&test_tenant()), 1);
        let after_abort = memtable.approximate_size();
        assert!(after_abort > 0);

        // Flushing and evicting for real must not take the accounting below
        // zero.
        memtable.begin_flush();
        memtable.commit_flush();
        assert_eq!(memtable.evict_idle(i64::MAX), 1);
        assert_eq!(
            memtable.approximate_size(),
            0,
            "every byte added has been released exactly once"
        );
    }

    /// Belt as well as braces: even if some future accounting slips, a
    /// release past zero must clamp rather than wrap — the gauge is allowed
    /// to be wrong, the ingest gate is not allowed to read `u64::MAX`.
    #[test]
    fn releasing_more_bytes_than_were_taken_clamps_at_zero() {
        let counter = AtomicU64::new(100);
        saturating_release(&counter, 40);
        assert_eq!(counter.load(Ordering::Relaxed), 60);
        saturating_release(&counter, 1_000);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
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
