/// Decoded row groups, kept for the next scan.
///
/// The per-admitted-group constant the rare shapes pay is the wide reader's
/// build — dictionary pages and column-chunk setup for every projected
/// column, priced at ~1.5 ms per group even when the selection keeps four
/// rows. A part is immutable, so a group decoded once can serve every later
/// scan; the cache holds exactly what a completed whole-group decode already
/// produced (`RecordBatch` clones are `Arc` refcounts — filling costs no
/// second decode), and it dies with its `PartReader`, which is dropped on
/// unregister and replace — no invalidation code at all.
///
/// The budget is one process-wide byte counter shared by every reader
/// (`LayoutTotals`' shape). A reader that pushes the total past the budget
/// evicts its own least-recently-used groups first; strict global LRU across
/// readers is deliberately not built — eviction here only decides which
/// *speedup* to keep.
static GLOBAL_BYTES: std::sync::OnceLock<Arc<std::sync::atomic::AtomicU64>> =
    std::sync::OnceLock::new();
/// 0 = off. Plain atomic so startup sets it before any reader opens and
/// tests can flip it without a set-once fight.
static GLOBAL_BUDGET: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn global_bytes() -> Arc<std::sync::atomic::AtomicU64> {
    GLOBAL_BYTES
        .get_or_init(|| Arc::new(std::sync::atomic::AtomicU64::new(0)))
        .clone()
}

/// Startup wiring: the budget every subsequently-opened reader's cache runs
/// under. Called before the registries load their parts.
pub fn configure_row_group_cache(budget: Option<u64>) {
    GLOBAL_BUDGET.store(budget.unwrap_or(0), std::sync::atomic::Ordering::Release);
}

/// The process-wide resident total, for the metrics endpoint.
pub fn row_group_cache_bytes() -> u64 {
    global_bytes().load(std::sync::atomic::Ordering::Acquire)
}

pub(crate) struct GroupCache {
    inner: std::sync::Mutex<GroupCacheInner>,
    shared_bytes: Arc<std::sync::atomic::AtomicU64>,
    budget_bytes: Option<u64>,
}

#[derive(Default)]
struct GroupCacheInner {
    groups: HashMap<u32, CachedGroup>,
    /// Monotonic touch counter; smallest = least recently used.
    clock: u64,
}

struct CachedGroup {
    batches: Arc<Vec<RecordBatch>>,
    bytes: u64,
    last_touch: u64,
}

/// A cached whole-group decode: the batches and each batch's group-absolute
/// starting row, so a group-absolute `RowSelection` index maps straight to
/// `(batch, batch-relative row)`.
pub(crate) struct CachedGroupRead {
    pub batches: Arc<Vec<RecordBatch>>,
    pub offsets: Vec<usize>,
    pub total_rows: usize,
    pub bytes: u64,
}

impl GroupCache {
    pub(crate) fn new(
        shared_bytes: Arc<std::sync::atomic::AtomicU64>,
        budget_bytes: Option<u64>,
    ) -> Self {
        Self {
            inner: std::sync::Mutex::new(GroupCacheInner::default()),
            shared_bytes,
            budget_bytes,
        }
    }

    /// The cache a production reader gets: the global counter and whatever
    /// budget startup configured (0 = disabled).
    pub(crate) fn from_global() -> Self {
        let budget = GLOBAL_BUDGET.load(std::sync::atomic::Ordering::Acquire);
        Self::new(global_bytes(), (budget > 0).then_some(budget))
    }

    pub(crate) fn enabled(&self) -> bool {
        self.budget_bytes.is_some()
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes_for_test(&self) -> u64 {
        self.inner
            .lock()
            .expect("group cache poisoned")
            .groups
            .values()
            .map(|group| group.bytes)
            .sum()
    }

    pub(crate) fn get(&self, rgu: u32) -> Option<CachedGroupRead> {
        let mut inner = self.inner.lock().expect("group cache poisoned");
        inner.clock += 1;
        let clock = inner.clock;
        let group = inner.groups.get_mut(&rgu)?;
        group.last_touch = clock;
        Some(read_of(group))
    }

    /// Insert a completed whole-group decode. The batches must cover the
    /// group's rows exactly, in order — the caller only inserts when the
    /// decode ran with no selection and reached the group's end.
    pub(crate) fn insert(&self, rgu: u32, batches: Vec<RecordBatch>) {
        let Some(budget) = self.budget_bytes else {
            return;
        };
        let _arena = crate::memprof::enter(crate::memprof::Arena::RowGroupCache);
        let bytes: u64 = batches
            .iter()
            .map(|batch| batch.get_array_memory_size() as u64)
            .sum();
        if bytes > budget {
            return;
        }
        let mut inner = self.inner.lock().expect("group cache poisoned");
        if inner.groups.contains_key(&rgu) {
            return;
        }
        inner.clock += 1;
        let clock = inner.clock;
        inner.groups.insert(
            rgu,
            CachedGroup {
                batches: Arc::new(batches),
                bytes,
                last_touch: clock,
            },
        );
        let mut total = self
            .shared_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(bytes);
        // Over budget: this reader gives back its own least-recently-used
        // groups. Another reader may hold the bulk of the total; it will do
        // the same the next time it inserts.
        while total > budget && inner.groups.len() > 1 {
            let Some((&victim, _)) = inner
                .groups
                .iter()
                .min_by_key(|(_, group)| group.last_touch)
            else {
                break;
            };
            if victim == rgu && inner.groups.len() == 1 {
                break;
            }
            let removed = inner.groups.remove(&victim).expect("chosen from the map");
            self.shared_bytes
                .fetch_sub(removed.bytes, std::sync::atomic::Ordering::AcqRel);
            total = total.saturating_sub(removed.bytes);
        }
    }

}

impl Drop for GroupCache {
    fn drop(&mut self) {
        let held: u64 = self
            .inner
            .get_mut()
            .map(|inner| inner.groups.values().map(|group| group.bytes).sum())
            .unwrap_or(0);
        if held > 0 {
            self.shared_bytes
                .fetch_sub(held, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

fn read_of(group: &CachedGroup) -> CachedGroupRead {
    let mut offsets = Vec::with_capacity(group.batches.len());
    let mut at = 0usize;
    for batch in group.batches.iter() {
        offsets.push(at);
        at += batch.num_rows();
    }
    CachedGroupRead {
        batches: group.batches.clone(),
        offsets,
        total_rows: at,
        bytes: group.bytes,
    }
}
