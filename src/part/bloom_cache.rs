/// The bloom half of a part's sidecar, made evictable.
///
/// The 24-hour soak died at t≈1834 s with `part_sidecar_resident_bytes` at
/// 630 MiB and climbing (todo.md, the soak section): every open part kept its
/// line and exact-field window blooms resident forever — ~2 MiB per part
/// since the 0.1% FPP widening — and the live part count under a real
/// retention window is proportional to ingest rate times that window, so the
/// one term the declared budget did not cover grew with data held rather
/// than with work done. The blooms are durable in `index.bin`
/// (`docs/VISION.md` scoped exactly this fix), so the resident copy is a
/// cache: eviction is a re-read, not a loss.
///
/// What stays resident is the stream index — tens of kilobytes per part
/// against the blooms' megabytes — because the infallible metadata paths
/// (`label_names`, `label_values`, `series`) read it, and turning those into
/// I/O-fallible answers for the small half of the bytes buys nothing.
///
/// Shape: one process-wide byte total and LRU registry over every reader's
/// slot, unlike the row-group cache's per-reader maps — a reader owns
/// exactly one bloom entry, so "evict your own least-recently-used" would
/// never evict anything. The byte total is authoritative in the registry:
/// every add and subtract happens under its lock, and a slot's data and its
/// registry entry are reconciled by re-checking the entry before a deferred
/// clear, so an eviction racing a reinstall skips rather than double-counts.
///
/// 0 = unbounded: the pre-eviction behaviour, and the default when no memory
/// budget is declared. The startup wiring sets it before any registry loads
/// parts.
static BLOOM_CACHE_BUDGET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BLOOM_CACHE_TOTAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BLOOM_SLOT_NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

struct BloomCacheEntry {
    slot: std::sync::Weak<BloomSlot>,
    bytes: u64,
    last_touch: u64,
}

#[derive(Default)]
struct BloomCacheRegistry {
    entries: HashMap<u64, BloomCacheEntry>,
    clock: u64,
}

static BLOOM_CACHE_REGISTRY: std::sync::OnceLock<std::sync::Mutex<BloomCacheRegistry>> =
    std::sync::OnceLock::new();

fn bloom_cache_registry() -> &'static std::sync::Mutex<BloomCacheRegistry> {
    BLOOM_CACHE_REGISTRY.get_or_init(|| std::sync::Mutex::new(BloomCacheRegistry::default()))
}

/// Startup wiring, called before the registries load their parts.
pub fn configure_bloom_cache(budget: Option<u64>) {
    BLOOM_CACHE_BUDGET.store(
        budget.unwrap_or(0),
        std::sync::atomic::Ordering::Release,
    );
}

/// The process-wide resident bloom total, for the metrics endpoint.
pub fn bloom_cache_bytes() -> u64 {
    BLOOM_CACHE_TOTAL.load(std::sync::atomic::Ordering::Acquire)
}

/// One reader's bloom residency. The reader keeps the `Arc`; the registry
/// keeps a `Weak` so a dropped reader cannot be chosen as a victim into a
/// leak.
pub(crate) struct BloomSlot {
    id: u64,
    data: std::sync::Mutex<Option<Arc<DecodedBlooms>>>,
}

impl BloomSlot {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            id: BLOOM_SLOT_NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            data: std::sync::Mutex::new(None),
        })
    }

    /// The resident blooms, touched for LRU — or `None`, meaning the caller
    /// pays the re-read.
    pub(crate) fn get(&self) -> Option<Arc<DecodedBlooms>> {
        let data = self.data.lock().expect("bloom slot poisoned").clone()?;
        let mut reg = bloom_cache_registry()
            .lock()
            .expect("bloom registry poisoned");
        reg.clock += 1;
        let clock = reg.clock;
        if let Some(entry) = reg.entries.get_mut(&self.id) {
            entry.last_touch = clock;
        }
        Some(data)
    }

    /// Install freshly decoded blooms and settle the budget: while the total
    /// is over it, the least-recently-used *other* slots are cleared. The
    /// installing slot is never its own victim — a part being queried right
    /// now is by definition the most recently used.
    pub(crate) fn install(self: &Arc<Self>, blooms: Arc<DecodedBlooms>, bytes: u64) {
        *self.data.lock().expect("bloom slot poisoned") = Some(blooms);
        let mut victims: Vec<Arc<BloomSlot>> = Vec::new();
        {
            let mut reg = bloom_cache_registry()
                .lock()
                .expect("bloom registry poisoned");
            reg.clock += 1;
            let clock = reg.clock;
            // A concurrent duplicate decode or an eviction racing this
            // reinstall both land here: the old entry's bytes leave the
            // total before the new ones join it, so the total never counts
            // one slot twice and never goes negative.
            if let Some(old) = reg.entries.remove(&self.id) {
                BLOOM_CACHE_TOTAL.fetch_sub(old.bytes, std::sync::atomic::Ordering::AcqRel);
            }
            BLOOM_CACHE_TOTAL.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
            reg.entries.insert(
                self.id,
                BloomCacheEntry {
                    slot: Arc::downgrade(self),
                    bytes,
                    last_touch: clock,
                },
            );
            let budget = BLOOM_CACHE_BUDGET.load(std::sync::atomic::Ordering::Acquire);
            if budget > 0 {
                while BLOOM_CACHE_TOTAL.load(std::sync::atomic::Ordering::Acquire) > budget
                    && reg.entries.len() > 1
                {
                    let Some(victim_id) = reg
                        .entries
                        .iter()
                        .filter(|(id, _)| **id != self.id)
                        .min_by_key(|(_, entry)| entry.last_touch)
                        .map(|(id, _)| *id)
                    else {
                        break;
                    };
                    let entry = reg.entries.remove(&victim_id).expect("chosen from the map");
                    BLOOM_CACHE_TOTAL.fetch_sub(entry.bytes, std::sync::atomic::Ordering::AcqRel);
                    if let Some(slot) = entry.slot.upgrade() {
                        victims.push(slot);
                    }
                }
            }
        }
        // Outside the registry lock, and re-checked under it: a victim that
        // reinstalled between selection and here has a fresh entry and keeps
        // its data. The Arc a running query may still hold keeps its memory
        // until that query ends — the same bounded transient the row-group
        // cache accepts.
        for slot in victims {
            let mut data = slot.data.lock().expect("bloom slot poisoned");
            let reg = bloom_cache_registry()
                .lock()
                .expect("bloom registry poisoned");
            if !reg.entries.contains_key(&slot.id) {
                *data = None;
            }
        }
    }

    /// Deterministic removal on reader drop — a merged-away or
    /// retention-deleted part gives its bytes back immediately rather than
    /// waiting to be chosen as a victim.
    pub(crate) fn remove(&self) {
        let mut data = self.data.lock().expect("bloom slot poisoned");
        let mut reg = bloom_cache_registry()
            .lock()
            .expect("bloom registry poisoned");
        if let Some(entry) = reg.entries.remove(&self.id) {
            BLOOM_CACHE_TOTAL.fetch_sub(entry.bytes, std::sync::atomic::Ordering::AcqRel);
        }
        *data = None;
    }
}
