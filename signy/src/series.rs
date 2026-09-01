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

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasher, Hash, Hasher};
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

impl Borrow<[u8]> for SeriesLabels {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

/// A process-local, weak string interner for canonical catalog identities.
///
/// Part catalogs often contain the same label set in several parts. Keeping a
/// separate `Vec<u8>` for each catalog copy made catalog memory grow with the
/// number of parts, even though all copies represented the same identity. The
/// interner shares the immutable `Arc<[u8]>` payload while at least one catalog
/// owner is alive. Active series deliberately do not register here: their
/// index owns its canonical bytes directly and does not need a process-wide
/// map entry on the cardinality hot path.
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

/// Entries one shard will hold. The pool is a cache of allocations, and a
/// cache has to be bounded or it becomes the thing it was avoiding: a
/// cardinality burst retires millions of identities at once, and remembering
/// each of them costs a bucket of its own — at ten million series the pool
/// would cost more than the index entries whose retirement filled it. The
/// bound keeps the hit that pays, a steady population that keeps reporting,
/// and lets a burst overflow past it. Sixty-four shards of this hold rather
/// more than the comparison bed's active population.
const LABEL_INTERNER_SHARD_CAPACITY: usize = 8_192;

static LABEL_INTERNER: OnceLock<LabelInterner> = OnceLock::new();

impl LabelInternerShard {
    /// Remember one payload, unless this shard is full. A full shard drops
    /// its dead entries first; if the live ones alone fill it, the caller
    /// keeps an ordinary unshared `Arc` — correct, merely not deduplicated.
    fn remember(&mut self, hash: u64, payload: &std::sync::Arc<[u8]>) {
        if self.by_hash.len() >= LABEL_INTERNER_SHARD_CAPACITY {
            self.by_hash.retain(|_, weak| weak.strong_count() != 0);
            self.registrations_since_sweep = 0;
            if self.by_hash.len() >= LABEL_INTERNER_SHARD_CAPACITY {
                return;
            }
        }
        self.by_hash
            .insert(hash, std::sync::Arc::downgrade(payload));
        self.registrations_since_sweep += 1;
        if self.registrations_since_sweep >= LABEL_INTERNER_SWEEP_INTERVAL {
            self.registrations_since_sweep = 0;
            self.by_hash.retain(|_, weak| weak.strong_count() != 0);
        }
    }
}

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

    fn hash(bytes: &[u8]) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        hasher.finish()
    }

    fn lookup(&self, bytes: &[u8]) -> Option<SeriesLabels> {
        let hash = Self::hash(bytes);
        let shard_index = (hash as usize) % self.shards.len();
        let shard = self.shards[shard_index].lock();
        // Keep the full byte comparison after the hash lookup.  The hash is a
        // routing aid, never the series identity.
        if let Some(weak) = shard.by_hash.get(&hash)
            && let Some(existing) = weak.upgrade()
            && existing.as_ref() == bytes
        {
            return Some(SeriesLabels(existing));
        }
        // A hash collision cannot merge identities. Keeping the existing
        // entry also means the colliding label remains an ordinary, correctly
        // comparable Arc without allocating a collision bucket for the
        // overwhelmingly common case.
        None
    }

    /// Intern a label that came from a catalog. The weak pool is deliberately
    /// populated only by catalog misses, never by active-series admission.
    fn intern_catalog(&self, labels: SeriesLabels) -> SeriesLabels {
        if let Some(existing) = self.lookup(&labels.0) {
            return existing;
        }
        let hash = Self::hash(&labels.0);
        let shard_index = (hash as usize) % self.shards.len();
        let mut shard = self.shards[shard_index].lock();
        if let Some(weak) = shard.by_hash.get(&hash)
            && let Some(existing) = weak.upgrade()
        {
            if existing.as_ref() == labels.0.as_ref() {
                return SeriesLabels(existing);
            }
            // A collision is not an identity match; leave the existing entry
            // intact and keep this label as an ordinary Arc.
            return labels;
        }
        shard.remember(hash, &labels.0);
        labels
    }

    /// Reuse an existing catalog allocation, but do not add a fresh active
    /// series to the pool. This keeps the pool proportional to historical
    /// catalog-only labels rather than cardinality.
    fn reuse_catalog_or_self(&self, labels: SeriesLabels) -> SeriesLabels {
        self.lookup(&labels.0).unwrap_or(labels)
    }

    fn register_evicted(&self, labels: &SeriesLabels) {
        let hash = Self::hash(&labels.0);
        let shard_index = (hash as usize) % self.shards.len();
        let mut shard = self.shards[shard_index].lock();
        if let Some(weak) = shard.by_hash.get(&hash)
            && let Some(existing) = weak.upgrade()
        {
            if existing.as_ref() == labels.0.as_ref() {
                return;
            }
            return;
        }
        shard.remember(hash, &labels.0);
    }

    /// Aggregate the weak interner's map population.  The interner is sharded
    /// only to keep registration contention low; the diagnostic is deliberately
    /// process-wide and sums the shards so a capacity probe can compare it to
    /// the series index without exposing a shard dimension.
    fn stats(&self) -> (usize, usize) {
        self.shards
            .iter()
            .fold((0usize, 0usize), |(len, capacity), shard| {
                let shard = shard.lock();
                (
                    len.saturating_add(shard.by_hash.len()),
                    capacity.saturating_add(shard.by_hash.capacity()),
                )
            })
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
        LabelInterner::global().intern_catalog(self)
    }

    pub(crate) fn reuse_catalog_or_self(self) -> Self {
        LabelInterner::global().reuse_catalog_or_self(self)
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
    /// Series whose index state was retired the moment their samples became
    /// durable, rather than at the idle horizon.
    pub series_retired_flushed_total: AtomicU64,
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

/// Point-in-time shape of the metric index and sample arena.
///
/// These are intentionally structural observations rather than admission
/// inputs: `/metrics` and the disposable capacity probe can show which map or
/// buffer representation accounts for memory without changing the production
/// 429 policy or the persisted series format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SeriesMemoryStats {
    pub states_len: usize,
    pub states_capacity: usize,
    pub buffers_len: usize,
    pub buffers_capacity: usize,
    pub empty_buffers: usize,
    pub inline_buffers: usize,
    pub stream_buffers: usize,
    pub flushing_series: usize,
    pub flushing_tenants: usize,
    pub interner_len: usize,
    pub interner_capacity: usize,
}

// The old 160-byte estimate omitted allocator/container overhead.  The M10
// attribution measured roughly 1.7x the estimate, so reserve a conservative
// 320 bytes per live entry.  This is deliberately a byte charge, not a
// cardinality proxy: labels still contribute their actual canonical length.
const SERIES_OVERHEAD_BYTES: u64 = 320;
const SPILL_SAMPLE_BYTES: u64 = 16;
const ADMITTED_SAMPLE_BYTES: u64 = 64;
/// The temporary accounting charge for a series whose only buffered sample
/// still fits in the inline representation.  This is the timestamp and the
/// f64 value; a one-sample series does not need a Gorilla writer or spill
/// vector until it either receives another sample or crosses a flush
/// boundary.
const INLINE_SAMPLE_BYTES: u64 = 16;

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
    flags: u8,
    last_ts: i64,
    running_total: f64,
}

impl SeriesState {
    const HAS_LAST_TS: u8 = 1;
    const HAS_RUNNING_TOTAL: u8 = 2;

    fn last_ts(&self) -> Option<i64> {
        (self.flags & Self::HAS_LAST_TS != 0).then_some(self.last_ts)
    }

    fn running_total(&self) -> Option<f64> {
        (self.flags & Self::HAS_RUNNING_TOTAL != 0).then_some(self.running_total)
    }

    fn set_running_total(&mut self, total: f64) {
        self.running_total = total;
        self.flags |= Self::HAS_RUNNING_TOTAL;
    }
}

/// A transient admission reservation.  It lives beside the persistent series
/// state so an empty state entry does not pay for request-only coordination
/// fields after the append has completed.
#[derive(Default)]
struct ReservationState {
    refs: u32,
    admitted_ts: i64,
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
    bounds: Option<(i64, i64)>,
}

impl SeriesBuffer {
    fn new() -> Self {
        Self {
            closed: Vec::new(),
            open: gorilla::Encoder::new(),
            spill: Vec::new(),
            bounds: None,
        }
    }

    fn has_samples(&self) -> bool {
        !self.closed.is_empty() || !self.open.is_empty() || !self.spill.is_empty()
    }

    fn accounted_bytes(&self) -> u64 {
        let open = if self.open.is_empty() {
            0
        } else {
            self.open.byte_len() as u64
        };
        self.closed
            .iter()
            .map(|chunk| chunk.len() as u64)
            .sum::<u64>()
            .saturating_add(open)
            .saturating_add(self.spill.len() as u64 * SPILL_SAMPLE_BYTES)
    }

    fn observe(&mut self, ts_ns: i64) {
        if let Some((min, max)) = self.bounds {
            self.bounds = Some((min.min(ts_ns), max.max(ts_ns)));
        } else {
            self.bounds = Some((ts_ns, ts_ns));
        }
    }
}

/// Storage for one live series' samples.
///
/// The common cardinality shape is one sample per series.  Keeping that
/// sample here avoids constructing the three `Vec` headers and an encoder
/// that a Gorilla stream carries.  A second sample promotes the value to the
/// normal stream, and flush/abort do the same before crossing the existing
/// chunk boundary.  The stream itself remains boxed so the enum's inline size
/// is just the larger of the two scalar sample fields and a pointer.
enum SeriesBufferStorage {
    Empty,
    Inline { ts_ns: i64, value: f64 },
    Stream(Box<SeriesBuffer>),
}

impl SeriesBufferStorage {
    fn has_samples(&self) -> bool {
        match self {
            Self::Empty => false,
            Self::Inline { .. } => true,
            Self::Stream(stream) => stream.has_samples(),
        }
    }

    fn accounted_bytes(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Inline { .. } => INLINE_SAMPLE_BYTES,
            Self::Stream(stream) => stream.accounted_bytes(),
        }
    }

    fn bounds(&self) -> Option<(i64, i64)> {
        match self {
            Self::Empty => None,
            Self::Inline { ts_ns, .. } => Some((*ts_ns, *ts_ns)),
            Self::Stream(stream) => stream.bounds,
        }
    }

    fn observe(&mut self, ts_ns: i64) {
        match self {
            Self::Empty => {}
            Self::Inline { .. } => {}
            Self::Stream(stream) => stream.observe(ts_ns),
        }
    }

    fn absorb(&mut self, bounds: Option<(i64, i64)>) {
        let Some((min, max)) = bounds else {
            return;
        };
        self.observe(min);
        self.observe(max);
    }

    fn take_bounds(&mut self) -> Option<(i64, i64)> {
        match self {
            Self::Empty => None,
            Self::Inline { ts_ns, .. } => {
                let bounds = Some((*ts_ns, *ts_ns));
                *self = Self::Empty;
                bounds
            }
            Self::Stream(stream) => stream.bounds.take(),
        }
    }

    /// Promote this value to the existing Gorilla stream representation.
    /// Empty is useful only as the freshly allocated arena slot; callers that
    /// need a sample always invoke this after checking `has_samples` or while
    /// inserting one.
    fn into_stream(self, last_ts: Option<i64>) -> SeriesBufferStorage {
        match self {
            Self::Empty => Self::Stream(Box::new(SeriesBuffer::new())),
            Self::Inline { ts_ns, value } => {
                let mut stream = SeriesBuffer::new();
                stream.observe(ts_ns);
                if last_ts.is_some_and(|last| ts_ns < last) {
                    stream.spill.push((ts_ns, value));
                } else {
                    stream.open.append(ts_ns, value);
                }
                Self::Stream(Box::new(stream))
            }
            Self::Stream(stream) => Self::Stream(stream),
        }
    }

    /// Append a sample, promoting an inline first sample when needed.  The
    /// returned value is the change in the same byte estimate used by the
    /// memtable's accounting counters.
    fn append(&mut self, last_ts: Option<i64>, ts_ns: i64, value: f64) -> u64 {
        let before = self.accounted_bytes();
        if let Self::Inline {
            ts_ns: first_ts,
            value: first_value,
        } = self
        {
            let first_ts = *first_ts;
            let first_value = *first_value;
            let mut stream = SeriesBuffer::new();
            stream.observe(first_ts);
            if last_ts.is_some_and(|last| first_ts < last) {
                stream.spill.push((first_ts, first_value));
            } else {
                stream.open.append(first_ts, first_value);
            }
            *self = Self::Stream(Box::new(stream));
        }

        match self {
            Self::Empty => {
                *self = Self::Inline { ts_ns, value };
            }
            Self::Inline { .. } => unreachable!("inline storage was promoted above"),
            Self::Stream(stream) => {
                if last_ts.is_some_and(|last| ts_ns < last) {
                    stream.spill.push((ts_ns, value));
                } else {
                    stream.open.append(ts_ns, value);
                }
            }
        }
        self.observe(ts_ns);
        self.accounted_bytes().saturating_sub(before)
    }

    /// Add this storage's samples to a caller-owned sorted-sample scratch
    /// vector without changing the storage.
    fn extend_samples(&self, samples: &mut Vec<(i64, f64)>) -> Result<(), String> {
        match self {
            Self::Empty => {}
            Self::Inline { ts_ns, value } => samples.push((*ts_ns, *value)),
            Self::Stream(stream) => {
                for chunk in &stream.closed {
                    samples.extend(gorilla::decode_all(chunk)?);
                }
                if !stream.open.is_empty() {
                    samples.extend(gorilla::decode_all(&stream.open.clone().close())?);
                }
                samples.extend(stream.spill.iter().copied());
            }
        }
        Ok(())
    }

    /// Restore a snapshot in front of samples that arrived while its flush
    /// was in flight.  Abort deliberately promotes an inline value: once the
    /// two generations share a buffer, the existing stream/chunk machinery
    /// is the single ordering representation again.
    fn prepend_aborted(
        &mut self,
        mut chunks: Vec<Vec<u8>>,
        mut spill: Vec<(i64, f64)>,
        current_last_ts: Option<i64>,
    ) {
        match self {
            Self::Empty => {
                // A flush promotes a lone inline sample solely to cross the
                // on-disk chunk format. If abort has no concurrent sample,
                // decode that one internally-produced chunk and put it back
                // inline, avoiding a boxed stream plus three dynamic headers.
                if chunks.len() == 1
                    && spill.is_empty()
                    && let Ok(mut decoded) = gorilla::decode_all(&chunks[0])
                    && decoded.len() == 1
                {
                    let (ts_ns, value) = decoded.pop().expect("singleton decoded above");
                    *self = Self::Inline { ts_ns, value };
                } else {
                    *self = Self::Stream(Box::new(SeriesBuffer {
                        closed: chunks,
                        open: gorilla::Encoder::new(),
                        spill,
                        bounds: None,
                    }));
                }
            }
            Self::Inline { ts_ns, value } => {
                let current = (*ts_ns, *value);
                let mut stream = SeriesBuffer {
                    closed: chunks,
                    open: gorilla::Encoder::new(),
                    spill: Vec::new(),
                    bounds: None,
                };
                stream.observe(current.0);
                // This is the same test insert used when the inline sample
                // was first recorded.  An older concurrent sample belongs to
                // spill; an in-order one belongs in the open stream.
                if current_last_ts.is_some_and(|last| current.0 < last) {
                    stream.spill.push(current);
                } else {
                    stream.open.append(current.0, current.1);
                }
                stream.spill.append(&mut spill);
                *self = Self::Stream(Box::new(stream));
            }
            Self::Stream(stream) => {
                chunks.append(&mut stream.closed);
                stream.closed = chunks;
                stream.spill.append(&mut spill);
            }
        }
    }
}

const SERIES_STATE_SHARDS: usize = 64;

/// Bucket capacity a shard keeps without question. Below this the table is
/// too small for its allocation to matter and rebuilding it only costs a
/// rehash.
const MIN_SHARD_CAPACITY: usize = 64;

/// A layout-sharded series index. Each shard is an ordinary HashMap; shards
/// are not locks and all callers still hold the tenant-wide memtable lock.
/// Splitting growth across maps prevents one 7-million-entry rehash from
/// allocating a second table of the entire index while the old table is live.
/// Full canonical labels remain the map keys and equality is still checked by
/// HashMap, so the routing hash is never an identity shortcut.
struct SeriesStates {
    shards: [HashMap<SeriesLabels, SeriesState>; SERIES_STATE_SHARDS],
    route: RandomState,
    len: usize,
}

impl Default for SeriesStates {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| HashMap::new()),
            route: RandomState::new(),
            len: 0,
        }
    }
}

impl SeriesStates {
    fn shard_for<Q: ?Sized + Hash>(&self, key: &Q) -> usize {
        (self.route.hash_one(key) as usize) & (SERIES_STATE_SHARDS - 1)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.shards.iter().map(HashMap::capacity).sum::<usize>()
    }

    fn contains_key(&self, labels: &SeriesLabels) -> bool {
        self.shards[self.shard_for(labels)].contains_key(labels)
    }

    fn get(&self, labels: &SeriesLabels) -> Option<&SeriesState> {
        self.shards[self.shard_for(labels)].get(labels)
    }

    fn get_mut(&mut self, labels: &SeriesLabels) -> Option<&mut SeriesState> {
        let shard = self.shard_for(labels);
        self.shards[shard].get_mut(labels)
    }

    fn get_key_value<Q: ?Sized + Hash + Eq>(&self, key: &Q) -> Option<(&SeriesLabels, &SeriesState)>
    where
        SeriesLabels: Borrow<Q>,
    {
        let shard = self.shard_for(key);
        self.shards[shard].get_key_value(key)
    }

    fn insert(&mut self, labels: SeriesLabels, state: SeriesState) -> Option<SeriesState> {
        let shard = self.shard_for(&labels);
        let previous = self.shards[shard].insert(labels, state);
        if previous.is_none() {
            self.len += 1;
        }
        previous
    }

    fn remove(&mut self, labels: &SeriesLabels) -> Option<SeriesState> {
        let shard = self.shard_for(labels);
        let previous = self.shards[shard].remove(labels);
        if previous.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        previous
    }

    /// Return bucket capacity a burst has left behind. `HashMap::remove`
    /// keeps the table it grew, so a retirement that empties an index would
    /// otherwise hold the cardinality peak's allocation for the process'
    /// life. The hysteresis is what keeps a steady workload from paying a
    /// shrink and a regrow on every flush: only a shard holding at least four
    /// times the entries it needs is rebuilt, and it is rebuilt to twice its
    /// live population rather than to the minimum.
    fn shrink_slack(&mut self) {
        for shard in &mut self.shards {
            let len = shard.len();
            if shard.capacity() > len.saturating_mul(4).max(MIN_SHARD_CAPACITY) {
                shard.shrink_to(len.saturating_mul(2));
            }
        }
    }

    fn keys(&self) -> impl Iterator<Item = &SeriesLabels> {
        self.shards.iter().flat_map(|shard| shard.keys())
    }

    fn values(&self) -> impl Iterator<Item = &SeriesState> {
        self.shards.iter().flat_map(|shard| shard.values())
    }

    fn iter(&self) -> impl Iterator<Item = (&SeriesLabels, &SeriesState)> {
        self.shards.iter().flat_map(|shard| shard.iter())
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&SeriesLabels, &mut SeriesState)> {
        self.shards.iter_mut().flat_map(|shard| shard.iter_mut())
    }
}

/// Per-tenant index and sample arena.  The index's value is compact persistent
/// state; only buffered series have an entry in `buffers`.
#[derive(Default)]
struct TenantSeries {
    states: SeriesStates,
    buffers: HashMap<SeriesBufferId, SeriesBufferStorage>,
    reservations: HashMap<SeriesLabels, ReservationState>,
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
                self.buffers.insert(id, SeriesBufferStorage::Empty);
                return id;
            }
        }
    }

    fn has_samples(&self, state: &SeriesState) -> bool {
        state
            .buffer_id
            .and_then(|id| self.buffers.get(&id))
            .is_some_and(SeriesBufferStorage::has_samples)
    }

    fn reserve(&mut self, labels: &SeriesLabels, admitted_ts: i64) {
        match self.reservations.entry(labels.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(ReservationState {
                    refs: 1,
                    admitted_ts,
                });
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let reservation = entry.get_mut();
                reservation.refs = reservation.refs.saturating_add(1);
                reservation.admitted_ts = reservation.admitted_ts.max(admitted_ts);
            }
        }
    }

    /// Record the timestamp used by the legacy synchronous admission helper
    /// without creating an in-flight reference. Its caller inserts the
    /// samples directly; the timestamp only prevents an idle sweep from
    /// evicting the newly admitted empty state before that insert lands.
    fn mark_admitted(&mut self, labels: &SeriesLabels, admitted_ts: i64) {
        let reservation = self.reservations.entry(labels.clone()).or_default();
        reservation.admitted_ts = reservation.admitted_ts.max(admitted_ts);
    }

    fn clear_reservation_after_insert(&mut self, labels: &SeriesLabels) {
        if self
            .reservations
            .get(labels)
            .is_some_and(|reservation| reservation.refs == 0)
        {
            self.reservations.remove(labels);
        }
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

    /// Resolve a bounded batch of canonical catalog labels against the live
    /// tenant index. The returned keys clone only their `Arc` payload, so an
    /// active label does not allocate a second byte buffer. Keeping one read
    /// guard for the batch avoids a lock round-trip per catalog row.
    pub(crate) fn resolve_live_label_batch(
        &self,
        tenant: &TenantId,
        canonical: &[&[u8]],
    ) -> Vec<Option<SeriesLabels>> {
        let inner = self.inner.read();
        let Some(series) = inner.get(tenant) else {
            return vec![None; canonical.len()];
        };
        canonical
            .iter()
            .map(|bytes| {
                series
                    .states
                    .get_key_value(*bytes)
                    .map(|(labels, _)| labels.clone())
            })
            .collect()
    }

    /// Current container populations used by the metric index and its sample
    /// arena.  The snapshot takes the same read locks as query/discovery and
    /// never walks encoded samples, so an operator scrape can inspect the
    /// shape even while a large flush is in flight.
    pub fn memory_stats(&self) -> SeriesMemoryStats {
        let (
            states_len,
            states_capacity,
            buffers_len,
            buffers_capacity,
            empty_buffers,
            inline_buffers,
            stream_buffers,
        ) = {
            let inner = self.inner.read();
            inner.values().fold(
                (0usize, 0usize, 0usize, 0usize, 0usize, 0usize, 0usize),
                |(
                    states_len,
                    states_capacity,
                    buffers_len,
                    buffers_capacity,
                    empty_buffers,
                    inline_buffers,
                    stream_buffers,
                ),
                 tenant| {
                    let (empty, inline, stream) = tenant.buffers.values().fold(
                        (0usize, 0usize, 0usize),
                        |(empty, inline, stream), buffer| match buffer {
                            SeriesBufferStorage::Empty => (empty + 1, inline, stream),
                            SeriesBufferStorage::Inline { .. } => (empty, inline + 1, stream),
                            SeriesBufferStorage::Stream(_) => (empty, inline, stream + 1),
                        },
                    );
                    (
                        states_len.saturating_add(tenant.states.len()),
                        states_capacity.saturating_add(tenant.states.capacity()),
                        buffers_len.saturating_add(tenant.buffers.len()),
                        buffers_capacity.saturating_add(tenant.buffers.capacity()),
                        empty_buffers.saturating_add(empty),
                        inline_buffers.saturating_add(inline),
                        stream_buffers.saturating_add(stream),
                    )
                },
            )
        };
        let (flushing_series, flushing_tenants) = self
            .flushing
            .read()
            .as_ref()
            .map(|snapshot| {
                (
                    snapshot.tenants.values().map(Vec::len).sum::<usize>(),
                    snapshot.tenants.len(),
                )
            })
            .unwrap_or_default();
        let (interner_len, interner_capacity) = LabelInterner::global().stats();
        SeriesMemoryStats {
            states_len,
            states_capacity,
            buffers_len,
            buffers_capacity,
            empty_buffers,
            inline_buffers,
            stream_buffers,
            flushing_series,
            flushing_tenants,
            interner_len,
            interner_capacity,
        }
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
                            .is_none_or(|series| !series.states.contains_key(label))
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
                tenant_series.states.insert(
                    label.clone().reuse_catalog_or_self(),
                    SeriesState::default(),
                );
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
                let should_reserve = inner
                    .get(*tenant)
                    .and_then(|series| series.states.get(&sample.labels))
                    .is_some_and(|state| state.buffer_id.is_none());
                if should_reserve {
                    // Empty entries are reservations, including one made by
                    // a concurrent request. Hold a reference for this append
                    // so that the other request cannot roll the entry back
                    // while this append is still in flight.
                    let admitted_ts = requested_newest
                        .get(&((*tenant).clone(), sample.labels.clone()))
                        .copied()
                        .unwrap_or(i64::MIN);
                    inner
                        .get_mut(*tenant)
                        .expect("tenant series exists")
                        .reserve(&sample.labels, admitted_ts);
                    reserved_series.push(((*tenant).clone(), sample.labels.clone()));
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
            let Some(series) = inner.get_mut(tenant) else {
                continue;
            };
            let Some(reservation) = series.reservations.get_mut(labels) else {
                continue;
            };
            reservation.refs = reservation.refs.saturating_sub(1);
            // A successful insert has already attached a buffer. Once the
            // final writer commits, no request-only timestamp needs to remain
            // in the tenant map. If commit arrives first, retain it until
            // insert clears it so an idle sweep cannot evict the empty
            // reservation before its WAL record lands.
            if reservation.refs == 0
                && series
                    .states
                    .get(labels)
                    .is_some_and(|state| state.buffer_id.is_some())
            {
                series.reservations.remove(labels);
            }
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
            let refs = if let Some(reservation) = series.reservations.get_mut(labels) {
                reservation.refs = reservation.refs.saturating_sub(1);
                reservation.refs
            } else {
                0
            };
            let state_has_buffer = series
                .states
                .get(labels)
                .is_some_and(|state| state.buffer_id.is_some());
            let remove_state = series
                .states
                .get(labels)
                .is_some_and(|state| state.buffer_id.is_none() && refs == 0);
            if remove_state {
                series.states.remove(labels);
                freed = freed.saturating_add(labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES);
                removed += 1;
            }
            if refs == 0 && (state_has_buffer || remove_state) {
                series.reservations.remove(labels);
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
                .filter(|labels| !tenant_series.states.contains_key(labels))
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
                tenant_series.states.insert(
                    (*labels).clone().reuse_catalog_or_self(),
                    SeriesState::default(),
                );
                if let Some(admitted_ts) = newest {
                    tenant_series.mark_admitted(labels, admitted_ts);
                }
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
                    && tenant_series
                        .reservations
                        .get(labels)
                        .is_none_or(|reservation| {
                            reservation.refs == 0 && reservation.admitted_ts < idle_cutoff_ns
                        });
                idle.then(|| (labels.clone(), state.buffer_id))
            })
            .collect();
        for (labels, buffer_id) in idle {
            // Keep only a weak hint for catalogs that still own the payload;
            // the pool never becomes an owner of evicted series.
            LabelInterner::global().register_evicted(&labels);
            if let Some(state) = tenant_series.states.remove(&labels)
                && let Some(id) = state.buffer_id.or(buffer_id)
            {
                tenant_series.buffers.remove(&id);
            }
            tenant_series.reservations.remove(&labels);
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
                tenant_series.states.insert(
                    labels.clone().reuse_catalog_or_self(),
                    SeriesState::default(),
                );
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
            added += buffer.append(last_ts, ts_ns, value);
            {
                let state = tenant_series
                    .states
                    .get_mut(&labels)
                    .expect("series state exists for every sample");
                if last_ts.is_none_or(|last| ts_ns >= last) {
                    state.last_ts = ts_ns;
                    state.flags |= SeriesState::HAS_LAST_TS;
                }
            }
            tenant_series.clear_reservation_after_insert(&labels);
        }
        self.inner_bytes.fetch_add(added, Ordering::Relaxed);
        drop(inner);
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
        let mut snapshot_bytes = 0u64;
        for (tenant, series_map) in inner.iter_mut() {
            let mut list = Vec::new();
            // The arena is only needed while samples are live.  Taking the
            // map before draining it releases the large bucket allocation at
            // the end of this flush instead of retaining cardinality-sized
            // capacity on every tenant's otherwise compact state index.
            let mut buffers = std::mem::take(&mut series_map.buffers);
            for (labels, state) in series_map.states.iter_mut() {
                let Some(buffer_id) = state.buffer_id else {
                    continue;
                };
                let Some(buffer) = buffers.remove(&buffer_id) else {
                    state.buffer_id = None;
                    continue;
                };
                if !buffer.has_samples() {
                    state.buffer_id = None;
                    continue;
                }
                // A one-sample value has no stream to move yet.  Flushes use
                // the established chunk format, so promote it here before
                // taking the stream fields below.
                let accounted = buffer.accounted_bytes();
                let mut buffer = buffer.into_stream(state.last_ts());
                let bounds = buffer.take_bounds();
                let mut buffer = match buffer {
                    SeriesBufferStorage::Stream(stream) => *stream,
                    SeriesBufferStorage::Empty | SeriesBufferStorage::Inline { .. } => {
                        unreachable!("flush promotion must produce a stream")
                    }
                };
                let mut chunks = std::mem::take(&mut buffer.closed);
                let open = std::mem::take(&mut buffer.open);
                if !open.is_empty() {
                    chunks.push(open.close());
                }
                let spill = std::mem::take(&mut buffer.spill);
                moved += accounted;
                snapshot_bytes += chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>()
                    + spill.len() as u64 * SPILL_SAMPLE_BYTES;
                state.buffer_id = None;
                list.push(SnapshotSeries {
                    labels: labels.clone(),
                    chunks,
                    spill,
                    bounds,
                });
            }
            debug_assert!(
                buffers.is_empty(),
                "every allocated sample buffer must be referenced by a live state"
            );
            // `buffers` is dropped here.  `series_map.buffers` is already a
            // fresh empty map, so its capacity remains zero until the next
            // sample actually needs an arena slot.
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
        self.flushing_bytes
            .fetch_add(snapshot_bytes, Ordering::Relaxed);
        let snapshot = Arc::new(SeriesSnapshot { tenants });
        *flushing = Some(snapshot.clone());
        snapshot
    }

    pub fn commit_flush(&self) {
        *self.flushing.write() = None;
        self.flushing_bytes.store(0, Ordering::Relaxed);
    }

    /// Drop the index entries of series whose samples have just become
    /// durable.
    ///
    /// A flushed series keeps an index entry for two reasons and no others:
    /// the delta running total, which nothing on disk can reconstruct, and
    /// `last_ts`, which routes a late sample into the spill vector of the
    /// buffer it would otherwise share with in-order ones. The second reason
    /// ends at a part boundary — the read path sorts and de-duplicates across
    /// parts, and the compactor merges their samples by timestamp — so a
    /// gauge or cumulative series that has just been written is holding a
    /// bucket for nothing. On a cardinality burst that is nearly the whole
    /// index: the 10-million probe carried 6.25 million states of which at
    /// most 1.09 million had a sample buffer.
    ///
    /// Called after the visibility transition and never inside it. The parts
    /// carrying these series are registered first, so no query can observe an
    /// interval where an identity is in neither place, and a burst-sized
    /// retirement does not stall the queries queued behind the lifecycle
    /// lock.
    ///
    /// What this returns is the bucket, not usually the labels. A catalog
    /// opened against the live memtable shares its `Arc`, so the canonical
    /// bytes leave only when the catalog stops owning them too.
    pub fn retire_flushed(&self, snapshot: &SeriesSnapshot) -> u64 {
        let mut inner = self.inner.write();
        let mut freed = 0u64;
        let mut retired = 0u64;
        for (tenant, list) in &snapshot.tenants {
            let Some(tenant_series) = inner.get_mut(tenant) else {
                continue;
            };
            for series in list {
                let labels = &series.labels;
                let Some(state) = tenant_series.states.get(labels) else {
                    continue;
                };
                // Three things make an entry more than history. Samples that
                // arrived while the flush was in flight; a delta total that
                // storage cannot rebuild; and an admission still in flight —
                // whatever its reference count, because removing the state a
                // reservation belongs to would strand it in a map that only
                // eviction walks, and eviction walks states.
                let retire = !tenant_series.has_samples(state)
                    && state.running_total().is_none()
                    && !tenant_series.reservations.contains_key(labels);
                let buffer_id = state.buffer_id;
                if !retire {
                    continue;
                }
                // Keep a weak hint, exactly as the idle sweep does: the
                // catalog that now owns these bytes can hand them straight
                // back if the series reports again.
                LabelInterner::global().register_evicted(labels);
                if let Some(state) = tenant_series.states.remove(labels)
                    && let Some(id) = state.buffer_id.or(buffer_id)
                {
                    tenant_series.buffers.remove(&id);
                }
                freed += labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                retired += 1;
            }
            tenant_series.states.shrink_slack();
        }
        saturating_release(&self.inner_bytes, freed);
        let current = self.counters.active_series.load(Ordering::Relaxed);
        self.counters
            .active_series
            .fetch_sub(retired.min(current), Ordering::Relaxed);
        self.counters
            .series_retired_flushed_total
            .fetch_add(retired, Ordering::Relaxed);
        drop(inner);
        retired
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
        let mut restored_buffer = 0u64;
        for (tenant, list) in snapshot.tenants {
            let tenant_series = inner.entry(tenant).or_default();
            for series in list {
                if !tenant_series.states.contains_key(&series.labels) {
                    restored_state += series.labels.byte_len() as u64 + SERIES_OVERHEAD_BYTES;
                    tenant_series.states.insert(
                        series.labels.clone().reuse_catalog_or_self(),
                        SeriesState::default(),
                    );
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
                let current_last_ts = tenant_series
                    .states
                    .get(&series.labels)
                    .and_then(SeriesState::last_ts);
                let buffer = tenant_series
                    .buffers
                    .get_mut(&buffer_id)
                    .expect("series state points at its sample buffer");
                let before = buffer.accounted_bytes();
                // The snapshot's samples are older than anything inserted
                // since, so its chunks go to the front and its spill stays
                // spill.  `prepend_aborted` promotes an inline concurrent
                // sample to the stream while retaining the insertion-order
                // distinction between its open stream and spill.
                buffer.prepend_aborted(series.chunks, series.spill, current_last_ts);
                restored_buffer =
                    restored_buffer.saturating_add(buffer.accounted_bytes().saturating_sub(before));
                buffer.absorb(series.bounds);
            }
        }
        // Recompute the live-buffer delta around the restore so an inline
        // concurrent sample is charged at 16 bytes before abort and at its
        // promoted Gorilla size afterwards; adding the snapshot bytes
        // separately would double-count that conversion.
        self.flushing_bytes.store(0, Ordering::Relaxed);
        self.inner_bytes.fetch_add(
            restored_state.saturating_add(restored_buffer),
            Ordering::Relaxed,
        );
        // Keep the accounting publication inside the same critical section as
        // the state restoration.  A concurrent begin/eviction must not observe
        // the restored maps before the bytes that back them are visible.
        drop(inner);
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
            for (key, state) in series.states.iter() {
                let bounds = state
                    .buffer_id
                    .and_then(|id| series.buffers.get(&id))
                    .and_then(SeriesBufferStorage::bounds);
                if series.has_samples(state) && ranges_overlap(bounds, start_ns, end_ns) {
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
            buffer.extend_samples(&mut samples)?;
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
            for (labels, state) in series_map.states.iter() {
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
                buffer.extend_samples(entry)?;
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
            std::mem::size_of::<SeriesState>() <= 24,
            "persistent series state must fit in 24 bytes (got {})",
            std::mem::size_of::<SeriesState>()
        );
    }

    #[test]
    fn sharded_series_states_keep_full_key_lookup_and_iteration() {
        let mut states = SeriesStates::default();
        let series_labels: Vec<_> = (0..1_000)
            .map(|index| labels("queue_depth", &format!("shard-{index}")))
            .collect();
        for (index, label) in series_labels.iter().enumerate() {
            states.insert(
                label.clone(),
                SeriesState {
                    last_ts: index as i64,
                    flags: SeriesState::HAS_LAST_TS,
                    ..SeriesState::default()
                },
            );
        }
        assert_eq!(states.len(), series_labels.len());
        assert!(states.capacity() >= states.len());
        for label in &series_labels {
            let (stored, state) = states
                .get_key_value(label.as_bytes())
                .expect("borrowed canonical bytes find their full key");
            assert_eq!(stored, label);
            assert!(state.last_ts().is_some());
        }
        assert_eq!(states.iter().count(), series_labels.len());
        assert_eq!(states.values().count(), series_labels.len());
        let removed = states
            .remove(&series_labels[17])
            .expect("present before removal");
        assert_eq!(removed.last_ts(), Some(17));
        assert_eq!(states.len(), series_labels.len() - 1);
        states.insert(series_labels[17].clone(), SeriesState::default());
        assert_eq!(states.len(), series_labels.len());
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
    fn a_first_sample_stays_inline_until_the_second_sample_promotes_it() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "inline");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);

        {
            let inner = memtable.inner.read();
            let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
            let state = tenant_series
                .states
                .get(&series)
                .expect("series was inserted");
            let buffer_id = state.buffer_id.expect("sample has a buffer");
            assert!(matches!(
                tenant_series.buffers.get(&buffer_id),
                Some(SeriesBufferStorage::Inline {
                    ts_ns: 100,
                    value: 1.0
                })
            ));
        }
        assert_eq!(
            memtable.approximate_size(),
            series.byte_len() + SERIES_OVERHEAD_BYTES as usize + INLINE_SAMPLE_BYTES as usize
        );

        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        let inner = memtable.inner.read();
        let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
        let state = tenant_series
            .states
            .get(&series)
            .expect("series was inserted");
        let buffer_id = state.buffer_id.expect("second sample has a buffer");
        assert!(matches!(
            tenant_series.buffers.get(&buffer_id),
            Some(SeriesBufferStorage::Stream(_))
        ));
        drop(inner);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(100, 1.0), (200, 2.0)]
        );
    }

    #[test]
    fn memory_stats_reports_inline_stream_and_flushing_populations() {
        let memtable = SeriesMemTable::new();
        let inline = labels("queue_depth", "stats-inline");
        let stream = labels("queue_depth", "stats-stream");
        memtable.insert(vec![sample(&inline, 100, 1.0, SampleKind::Gauge)]);
        memtable.insert(vec![
            sample(&stream, 100, 1.0, SampleKind::Gauge),
            sample(&stream, 200, 2.0, SampleKind::Gauge),
        ]);

        let stats = memtable.memory_stats();
        assert_eq!(stats.states_len, 2);
        assert!(stats.states_capacity >= stats.states_len);
        assert_eq!(stats.buffers_len, 2);
        assert!(stats.buffers_capacity >= stats.buffers_len);
        assert_eq!(stats.empty_buffers, 0);
        assert_eq!(stats.inline_buffers, 1);
        assert_eq!(stats.stream_buffers, 1);
        assert_eq!(stats.flushing_series, 0);
        assert_eq!(stats.flushing_tenants, 0);
        assert!(stats.interner_len > 0);
        assert!(stats.interner_capacity >= stats.interner_len);

        let snapshot = memtable.begin_flush();
        let stats = memtable.memory_stats();
        assert_eq!(stats.buffers_len, 0);
        assert_eq!(stats.buffers_capacity, 0);
        assert_eq!(stats.inline_buffers, 0);
        assert_eq!(stats.stream_buffers, 0);
        assert_eq!(stats.flushing_series, 2);
        assert_eq!(stats.flushing_tenants, 1);
        memtable.commit_flush();
        assert_eq!(memtable.memory_stats().flushing_series, 0);
        drop(snapshot);
    }

    #[test]
    fn begin_flush_releases_drained_buffer_capacity_and_keeps_state_reusable() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "drained-arena");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        assert!(memtable.memory_stats().buffers_capacity > 0);

        let snapshot = memtable.begin_flush();
        let stats = memtable.memory_stats();
        assert_eq!(stats.buffers_len, 0);
        assert_eq!(stats.buffers_capacity, 0);
        assert_eq!(memtable.active_series(&test_tenant()), 1);
        assert_eq!(memtable.series_labels(&test_tenant()), vec![series.clone()]);
        assert_eq!(
            snapshot.tenants[&test_tenant()][0]
                .sorted_samples()
                .unwrap(),
            vec![(100, 1.0)]
        );

        memtable.commit_flush();
        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        assert!(memtable.memory_stats().buffers_capacity > 0);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(200, 2.0)]
        );
    }

    #[test]
    fn a_single_inline_sample_promotes_to_the_existing_chunk_on_flush() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "flush-inline");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let state_bytes = series.byte_len() + SERIES_OVERHEAD_BYTES as usize;

        let snapshot = memtable.begin_flush();
        let flushed = &snapshot.tenants[&test_tenant()][0];
        assert_eq!(
            gorilla::decode_all(&flushed.chunks[0]).unwrap(),
            vec![(100, 1.0)]
        );
        assert!(flushed.spill.is_empty());
        assert_eq!(
            memtable.approximate_size(),
            state_bytes + flushed.chunks[0].len()
        );
        memtable.commit_flush();
        assert_eq!(memtable.approximate_size(), state_bytes);
    }

    #[test]
    fn abort_promotes_a_concurrent_inline_sample_and_preserves_order() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "abort-inline");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let state_bytes = series.byte_len() + SERIES_OVERHEAD_BYTES as usize;
        let snapshot = memtable.begin_flush();
        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        assert_eq!(memtable.approximate_size(), state_bytes + 20 + 16);

        {
            let inner = memtable.inner.read();
            let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
            let state = tenant_series
                .states
                .get(&series)
                .expect("series was inserted");
            let buffer_id = state.buffer_id.expect("concurrent sample has a buffer");
            assert!(matches!(
                tenant_series.buffers.get(&buffer_id),
                Some(SeriesBufferStorage::Inline {
                    ts_ns: 200,
                    value: 2.0
                })
            ));
        }

        memtable.abort_flush(snapshot);
        // The snapshot's 20-byte chunk and the concurrent inline sample are
        // now one stream (20-byte closed chunk + 20-byte open chunk).
        assert_eq!(memtable.approximate_size(), state_bytes + 40);
        let inner = memtable.inner.read();
        let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
        let state = tenant_series
            .states
            .get(&series)
            .expect("series was inserted");
        let buffer_id = state.buffer_id.expect("aborted samples have a buffer");
        assert!(matches!(
            tenant_series.buffers.get(&buffer_id),
            Some(SeriesBufferStorage::Stream(_))
        ));
        drop(inner);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(100, 1.0), (200, 2.0)]
        );
    }

    #[test]
    fn a_lone_older_sample_promotes_to_spill_on_flush() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "older-inline");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        memtable.begin_flush();
        memtable.commit_flush();

        memtable.insert(vec![sample(&series, 50, 2.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        let flushed = &snapshot.tenants[&test_tenant()][0];
        assert!(flushed.chunks.is_empty());
        assert_eq!(flushed.spill, vec![(50, 2.0)]);
        memtable.commit_flush();
    }

    #[test]
    fn buffer_bounds_track_out_of_order_and_equal_timestamps() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "bounds");
        memtable.insert(vec![
            sample(&series, 200, 2.0, SampleKind::Gauge),
            sample(&series, 100, 1.0, SampleKind::Gauge),
            sample(&series, 200, 3.0, SampleKind::Gauge),
        ]);

        let inner = memtable.inner.read();
        let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
        let state = tenant_series.states.get(&series).expect("series exists");
        let buffer_id = state.buffer_id.expect("samples have a buffer");
        assert_eq!(
            tenant_series
                .buffers
                .get(&buffer_id)
                .expect("buffer exists")
                .bounds(),
            Some((100, 200))
        );
        drop(inner);

        assert_eq!(
            memtable.series_labels_in_range(&test_tenant(), 200, 200),
            vec![series.clone()]
        );
        let snapshot = memtable.begin_flush();
        assert_eq!(snapshot.tenants[&test_tenant()][0].bounds, Some((100, 200)));
        memtable.commit_flush();
    }

    #[test]
    fn abort_absorbs_snapshot_bounds_with_concurrent_samples() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "abort-bounds");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.insert(vec![
            sample(&series, 300, 3.0, SampleKind::Gauge),
            sample(&series, 200, 2.0, SampleKind::Gauge),
            sample(&series, 300, 4.0, SampleKind::Gauge),
        ]);
        memtable.abort_flush(snapshot);

        let inner = memtable.inner.read();
        let tenant_series = inner.get(&test_tenant()).expect("tenant was inserted");
        let state = tenant_series.states.get(&series).expect("series exists");
        let buffer_id = state.buffer_id.expect("aborted samples have a buffer");
        assert_eq!(
            tenant_series
                .buffers
                .get(&buffer_id)
                .expect("buffer exists")
                .bounds(),
            Some((100, 300))
        );
        drop(inner);
        assert_eq!(
            memtable.series_labels_in_range(&test_tenant(), 100, 100),
            vec![series.clone()]
        );
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(100, 1.0), (200, 2.0), (300, 3.0), (300, 4.0)]
        );
    }

    #[test]
    fn old_admission_timestamp_cannot_replace_a_newer_reservation() {
        let memtable = Arc::new(SeriesMemTable::new());
        let tenant = test_tenant();
        let series = labels("queue_depth", "reservation-ts");
        let newer = sample(&series, 1_000, 1.0, SampleKind::Gauge);
        let older = sample(&series, -100, 2.0, SampleKind::Gauge);
        let newer_groups = vec![(&tenant, std::slice::from_ref(&newer))];
        let older_groups = vec![(&tenant, std::slice::from_ref(&older))];
        let first = memtable
            .admit_request(&newer_groups, None, i64::MIN)
            .unwrap()
            .pop()
            .unwrap();
        let second = memtable
            .admit_request(&older_groups, None, i64::MIN)
            .unwrap()
            .pop()
            .unwrap();
        drop(first);
        second.commit();

        // The older overlapping request must not lower the timestamp that
        // protects the empty reservation from an idle sweep.
        assert_eq!(memtable.evict_idle(500), 0);
        assert_eq!(memtable.evict_idle(2_000), 1);
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
    fn aborting_a_singleton_flush_restores_inline_storage() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "singleton-abort");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        assert_eq!(memtable.memory_stats().stream_buffers, 0);
        memtable.abort_flush(snapshot);
        let stats = memtable.memory_stats();
        assert_eq!(stats.inline_buffers, 1);
        assert_eq!(stats.stream_buffers, 0);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(100, 1.0)]
        );
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
        let state_bytes = series.byte_len() + SERIES_OVERHEAD_BYTES as usize;
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let before = memtable.approximate_size();
        assert_eq!(before, state_bytes + INLINE_SAMPLE_BYTES as usize);
        let snapshot = memtable.begin_flush();
        assert_eq!(
            memtable.approximate_size(),
            state_bytes + snapshot.tenants[&test_tenant()][0].chunks[0].len(),
            "begin_flush moves the sample charge to flushing"
        );
        memtable.abort_flush(snapshot);
        assert_eq!(
            memtable.approximate_size(),
            state_bytes + INLINE_SAMPLE_BYTES as usize,
            "abort_flush restores exactly the inline sample charge"
        );
        let second = memtable.begin_flush();
        assert_eq!(
            memtable.approximate_size(),
            state_bytes + second.tenants[&test_tenant()][0].chunks[0].len(),
            "a second begin_flush does not duplicate bytes"
        );
        memtable.commit_flush();
        assert_eq!(memtable.approximate_size(), state_bytes);
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
    fn the_label_pool_stops_growing_before_it_costs_more_than_it_saves() {
        let memtable = SeriesMemTable::new();
        let burst: Vec<MetricSample> = (0..40_000)
            .map(|index| {
                sample(
                    &labels("queue_depth", &format!("pool-burst-{index}")),
                    100,
                    1.0,
                    SampleKind::Gauge,
                )
            })
            .collect();
        memtable.insert(burst);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        assert_eq!(memtable.retire_flushed(&snapshot), 40_000);

        // Every retired identity offers itself to the pool. The pool is a
        // cache of allocations, so it takes what it is sized for and lets the
        // rest go: an entry per burst series would cost more than the index
        // buckets the retirement just returned.
        let (len, _) = LabelInterner::global().stats();
        assert!(
            len <= LABEL_INTERNER_SHARDS * LABEL_INTERNER_SHARD_CAPACITY,
            "the pool grew past its bound: {len} entries"
        );
    }

    #[test]
    fn a_flushed_gauge_leaves_the_index_and_takes_its_charge_with_it() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "retired");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        assert_eq!(memtable.active_series(&test_tenant()), 1);

        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        assert_eq!(
            memtable.retire_flushed(&snapshot),
            1,
            "a gauge whose samples are durable keeps nothing the index can serve"
        );

        assert_eq!(memtable.active_series(&test_tenant()), 0);
        assert!(memtable.series_labels(&test_tenant()).is_empty());
        assert_eq!(
            memtable.approximate_size(),
            0,
            "the charge leaves with the entry, exactly as the idle sweep's does"
        );
        assert!(memtable.is_empty());
    }

    #[test]
    fn a_delta_series_keeps_its_index_entry_through_retirement() {
        let memtable = SeriesMemTable::new();
        let series = labels("http_requests_total", "delta");
        memtable.insert(vec![sample(&series, 100, 5.0, SampleKind::Delta)]);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();

        assert_eq!(
            memtable.retire_flushed(&snapshot),
            0,
            "storage cannot rebuild a running total, so its entry is not history"
        );
        memtable.insert(vec![sample(&series, 200, 3.0, SampleKind::Delta)]);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(200, 8.0)],
            "retiring a delta series would manufacture a counter reset"
        );
    }

    #[test]
    fn a_sample_that_lands_during_the_flush_keeps_its_series_resident() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "concurrent");
        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        memtable.commit_flush();

        assert_eq!(memtable.retire_flushed(&snapshot), 0);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(200, 2.0)],
            "the sample that arrived mid-flush is still buffered and still owned"
        );
    }

    #[test]
    fn retirement_leaves_a_series_with_an_admission_in_flight_alone() {
        let memtable = Arc::new(SeriesMemTable::new());
        let tenant = test_tenant();
        let series = labels("queue_depth", "in-flight");
        let pending = sample(&series, 300, 3.0, SampleKind::Gauge);
        let groups = vec![(&tenant, std::slice::from_ref(&pending))];

        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        let admission = memtable
            .admit_request(&groups, None, i64::MIN)
            .expect("the empty table has room");

        assert_eq!(
            memtable.retire_flushed(&snapshot),
            0,
            "removing the state would strand a reservation in a map only eviction walks"
        );
        admission.into_iter().for_each(SeriesAdmission::commit);
        memtable.insert(vec![pending]);
        assert_eq!(
            memtable.sorted_samples(&tenant).unwrap()[&series],
            vec![(300, 3.0)]
        );
    }

    #[test]
    fn retirement_returns_the_bucket_capacity_a_burst_grew() {
        let memtable = SeriesMemTable::new();
        let burst: Vec<MetricSample> = (0..20_000)
            .map(|index| {
                sample(
                    &labels("queue_depth", &format!("burst-{index}")),
                    100,
                    1.0,
                    SampleKind::Gauge,
                )
            })
            .collect();
        memtable.insert(burst);
        let grown = memtable.memory_stats().states_capacity;
        assert!(grown >= 20_000);

        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        assert_eq!(memtable.retire_flushed(&snapshot), 20_000);

        let stats = memtable.memory_stats();
        assert_eq!(stats.states_len, 0);
        assert!(
            stats.states_capacity * 4 < grown,
            "a burst's table must not outlive the burst: {} of {grown} buckets remain",
            stats.states_capacity
        );
    }

    #[test]
    fn a_retired_series_that_reports_again_is_charged_exactly_once() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "returning");
        let state_bytes = (series.byte_len() + SERIES_OVERHEAD_BYTES as usize) as u64;

        memtable.insert(vec![sample(&series, 100, 1.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        memtable.retire_flushed(&snapshot);
        assert_eq!(memtable.approximate_size(), 0);

        memtable.insert(vec![sample(&series, 200, 2.0, SampleKind::Gauge)]);
        assert_eq!(memtable.active_series(&test_tenant()), 1);
        assert_eq!(
            memtable.approximate_size() as u64,
            state_bytes + INLINE_SAMPLE_BYTES,
            "the returning series is a new entry and is charged like one"
        );
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(200, 2.0)]
        );
    }

    #[test]
    fn a_sample_older_than_a_retired_series_last_flush_is_still_kept() {
        let memtable = SeriesMemTable::new();
        let series = labels("queue_depth", "late");
        memtable.insert(vec![sample(&series, 500, 5.0, SampleKind::Gauge)]);
        let snapshot = memtable.begin_flush();
        memtable.commit_flush();
        memtable.retire_flushed(&snapshot);

        // `last_ts` went with the entry, so this sample opens a fresh buffer
        // rather than landing in a spill vector. Nothing is lost: it reaches
        // its own part, and both the read path and the compactor merge parts
        // by timestamp.
        memtable.insert(vec![sample(&series, 400, 4.0, SampleKind::Gauge)]);
        assert_eq!(
            memtable.sorted_samples(&test_tenant()).unwrap()[&series],
            vec![(400, 4.0)]
        );
        assert_eq!(
            snapshot.tenants[&test_tenant()][0]
                .sorted_samples()
                .unwrap(),
            vec![(500, 5.0)]
        );
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
        assert_eq!(
            memtable.evict_idle(2_000),
            1,
            "legacy admission must not leave a permanent reservation"
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
