use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
#[cfg(test)]
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::object_storage::{ObjectStorage, TENANT_POLICY_PREFIX};
use crate::part::PartMeta;
use crate::tenant::TenantId;
use crate::trace_part::TracePartMeta;

const POLICY_FILE_SUFFIX: &str = ".json";
const POLICY_TEMP_SUFFIX: &str = ".json.tmp";

/// How long one tenant's data is kept.
///
/// A tenant the control plane has never pushed has no `TenantRetention` at
/// all, which is deliberately different from [`TenantRetention::Infinite`]:
/// both keep the data, but only the second one is an answer the control plane
/// actually gave.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantRetention {
    Finite(Duration),
    Infinite,
}

/// How fast one tenant may write to this instance.
///
/// A *rate*, deliberately, and not the monthly volume budget the plan sells.
/// A monthly budget is platform-wide state — several instances can serve the
/// same tenant, and the month outlives any of them — so the control plane is
/// the only place that can hold it. What an instance can enforce on its own is
/// the share of itself it hands to one tenant, and that is what protects the
/// other tenants on it from a neighbour's burst.
///
/// The numbers are not decided here. Plans differ, they change after launch,
/// and a value compiled in or read from this instance's environment could be
/// neither per-tenant nor changed without a restart. This is the field the
/// control plane pushes into; [`TenantIngestRate`] is only its shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TenantIngestRate {
    Unlimited,
    /// Zero is a legal value and means the tenant may not write at all, the
    /// same way `retention: "0"` is how a tenant is deleted rather than a
    /// degenerate case of keeping data.
    BytesPerSecond(u64),
}

/// What the control plane pushed for one tenant.
///
/// The raw strings are kept as they arrived so a `GET` returns what was sent.
/// The control plane is the only authority here: nothing rewrites, caps or
/// otherwise reinterprets the values it pushed.
#[derive(Clone)]
struct PolicyEntry {
    retention: TenantRetention,
    raw: String,
    /// `None` for a policy pushed before ingest rates existed, which is left
    /// distinct from an explicit `unlimited` for the same reason an unknown
    /// tenant is distinct from an infinite one: only one of them is an answer
    /// the control plane gave. An absent rate falls back to the configured
    /// default for tenants nothing is known about.
    ingest_rate: Option<TenantIngestRate>,
    raw_ingest_rate: Option<String>,
    updated_at: SystemTime,
}

/// One tenant's policy as the admin endpoints report it.
pub struct PolicyView {
    pub retention: String,
    pub ingest_rate: Option<String>,
    pub updated_at: SystemTime,
}

/// Every policy this instance knows, read by queries, retention and merge.
pub struct PolicyMap {
    entries: BTreeMap<TenantId, PolicyEntry>,
}

impl PolicyMap {
    pub fn retention(&self, tenant: &TenantId) -> Option<TenantRetention> {
        self.entries.get(tenant).map(|entry| entry.retention)
    }

    /// The rate the control plane pushed for this tenant, or `None` when it
    /// has said nothing — in which case the caller applies the configured
    /// default rather than inventing one here.
    pub fn ingest_rate(&self, tenant: &TenantId) -> Option<TenantIngestRate> {
        self.entries.get(tenant).and_then(|entry| entry.ingest_rate)
    }

    pub fn view(&self, tenant: &TenantId) -> Option<PolicyView> {
        self.entries.get(tenant).map(|entry| PolicyView {
            retention: entry.raw.clone(),
            ingest_rate: entry.raw_ingest_rate.clone(),
            updated_at: entry.updated_at,
        })
    }

    pub fn tenant_count(&self) -> usize {
        self.entries.len()
    }

    pub fn infinite_tenant_count(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| matches!(entry.retention, TenantRetention::Infinite))
            .count()
    }

    /// Age of the newest policy on this pod. It gates nothing; it tells an
    /// operator whether the control plane is still talking to this instance.
    /// Restored from the stored `updated_at`, so a restart does not reset it.
    ///
    /// Only meaningful once at least one policy exists: with an empty map there
    /// is no newest push and this reads zero, so an alert on it must be paired
    /// with `known_tenants > 0`. The "never pushed at all" state is the
    /// unknown-tenant gauge's to report, not this one's.
    pub fn newest_push_age(&self, now: SystemTime) -> Duration {
        self.entries
            .values()
            .map(|entry| entry.updated_at)
            .max()
            .map(|newest| now.duration_since(newest).unwrap_or_default())
            .unwrap_or_default()
    }
}

/// Per-tenant cutoffs resolved against one instant.
///
/// Every decision below reads `meta.json` only: the tenant index already
/// carries each tenant's row-group range, row count and timestamp bounds, so
/// no object body is downloaded to decide what has expired.
#[derive(Clone)]
pub struct Cutoffs {
    policies: Arc<PolicyMap>,
    now_ns: i64,
}

impl Cutoffs {
    /// Oldest timestamp still retained for `tenant`, or `None` when nothing
    /// expires — an unknown tenant or an explicitly infinite one.
    pub fn cutoff_ns(&self, tenant: &TenantId) -> Option<i64> {
        match self.policies.retention(tenant)? {
            TenantRetention::Infinite => None,
            TenantRetention::Finite(period) => Some(
                self.now_ns
                    .saturating_sub(period.as_nanos().min(i64::MAX as u128) as i64),
            ),
        }
    }

    pub fn is_expired(&self, tenant: &TenantId, timestamp_ns: i64) -> bool {
        self.cutoff_ns(tenant)
            .is_some_and(|cutoff_ns| timestamp_ns < cutoff_ns)
    }

    /// Whether every tenant in the part has expired, which is the free
    /// whole-part deletion case.
    pub fn log_part_fully_expired(&self, meta: &PartMeta) -> bool {
        !meta.tenants.is_empty()
            && meta
                .tenants
                .iter()
                .all(|segment| self.is_expired(&segment.tenant, segment.max_ts_ns))
    }

    /// Rows the part holds for tenants whose whole segment has expired.
    pub fn expired_log_rows(&self, meta: &PartMeta) -> u64 {
        meta.tenants
            .iter()
            .filter(|segment| self.is_expired(&segment.tenant, segment.max_ts_ns))
            .map(|segment| segment.row_count)
            .sum()
    }

    /// Expired share of a part's rows, used to decide whether reclaiming them
    /// is worth one rewrite.
    pub fn expired_log_row_fraction(&self, meta: &PartMeta) -> f64 {
        if meta.row_count == 0 {
            return 0.0;
        }
        self.expired_log_rows(meta) as f64 / meta.row_count as f64
    }

    /// Whether the part still holds rows for a tenant at zero retention.
    ///
    /// Zero retention is how a tenant is deleted, so that path ignores
    /// `retention_rewrite_threshold`: any part still holding the tenant's rows
    /// is rewritten, which turns "the rows may survive in a large part
    /// indefinitely" into "the next few merge ticks" without job tracking.
    pub fn holds_zero_retention_rows(&self, meta: &PartMeta) -> bool {
        meta.tenants
            .iter()
            .any(|segment| segment.row_count > 0 && self.is_zero_retention(&segment.tenant))
    }

    fn is_zero_retention(&self, tenant: &TenantId) -> bool {
        matches!(
            self.policies.retention(tenant),
            Some(TenantRetention::Finite(period)) if period.is_zero()
        )
    }

    /// Trace tenant segments carry no timestamps of their own, so a segment's
    /// bound comes from the row groups it owns.
    pub fn trace_part_fully_expired(&self, meta: &TracePartMeta) -> bool {
        if meta.tenants.is_empty() {
            return false;
        }
        meta.tenants.iter().all(|segment| {
            let groups = segment.row_group_start as usize..segment.row_group_end as usize;
            let segment_max = meta
                .row_group_max_ts
                .get(groups)
                .and_then(|bounds| bounds.iter().max().copied())
                .unwrap_or(meta.max_ts_ns);
            self.is_expired(&segment.tenant, segment_max)
        })
    }
}

#[derive(Default)]
pub struct TenantPolicyMetrics {
    pub push_accepted: AtomicU64,
    /// A policy change the instance refused to make: a malformed body, an
    /// unparseable retention value, or a tenant id that is not one. Counts
    /// only requests that meant to change something, so a bad `GET` does not
    /// look like a control plane pushing garbage.
    pub push_rejected: AtomicU64,
    pub push_persist_errors: AtomicU64,
    /// Admin requests that failed the bearer check, on any of the three
    /// methods. Separate from `push_rejected` because the two ask different
    /// questions: one is "is the control plane broken", the other is "is
    /// someone knocking".
    pub admin_unauthorized: AtomicU64,
}

/// Why a policy change did not take effect.
#[derive(Debug)]
pub enum PolicyError {
    /// The request is malformed. Nothing was stored, so the control plane must
    /// fix the request rather than retry it.
    Invalid(String),
    /// The policy could not be made durable. Nothing was applied, and the
    /// control plane owns the retry.
    Persist(String),
}

/// Where the policy objects live.
///
/// One object per tenant, so a push is a single blind write: no
/// read-modify-write, no CAS, and no contention between two tenants pushed at
/// the same time.
enum PolicyStore {
    Remote(Arc<ObjectStorage>),
    /// With no object store configured the same files live under the data
    /// directory, written temp-file-then-rename like the rest of local state.
    Local(PathBuf),
}

#[derive(Serialize, Deserialize)]
struct PolicyDocument {
    retention: String,
    /// Absent in every policy stored before ingest rates existed. `default`
    /// rather than required, because a stored policy that suddenly fails to
    /// deserialize is a fatal boot — the same trap `meta.json` fell into
    /// before it had a version field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ingest_rate: Option<String>,
    updated_at: String,
}

impl PolicyStore {
    async fn put(&self, tenant: &TenantId, document: &PolicyDocument) -> Result<(), String> {
        let body = serde_json::to_vec(document)
            .map_err(|error| format!("failed to encode the tenant policy: {error}"))?;
        match self {
            Self::Remote(storage) => storage.put_tenant_policy(tenant.as_str(), body).await,
            Self::Local(root) => {
                let root = root.clone();
                let file_name = format!("{tenant}{POLICY_FILE_SUFFIX}");
                let temp_name = format!("{tenant}{POLICY_TEMP_SUFFIX}");
                tokio::task::spawn_blocking(move || {
                    write_local_policy(&root, &file_name, &temp_name, &body)
                })
                .await
                .map_err(|error| format!("tenant policy write task failed: {error}"))?
            }
        }
    }

    async fn delete(&self, tenant: &TenantId) -> Result<(), String> {
        match self {
            Self::Remote(storage) => storage.delete_tenant_policy(tenant.as_str()).await,
            Self::Local(root) => {
                let path = root.join(format!("{tenant}{POLICY_FILE_SUFFIX}"));
                let root = root.clone();
                tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
                    Ok(()) => crate::part::fsync_dir(&root).map_err(|error| error.to_string()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(format!(
                        "failed to delete the tenant policy {}: {error}",
                        path.display()
                    )),
                })
                .await
                .map_err(|error| format!("tenant policy delete task failed: {error}"))?
            }
        }
    }

    /// Every stored policy. A failure is fatal at boot, so this never returns a
    /// partial map: booting with a silently empty one would unclamp every query
    /// and hand back data a downgrade had already hidden.
    async fn load_all(&self) -> Result<BTreeMap<TenantId, PolicyEntry>, String> {
        let documents = match self {
            Self::Remote(storage) => storage.load_tenant_policies().await?,
            Self::Local(root) => {
                let root = root.clone();
                tokio::task::spawn_blocking(move || read_local_policies(&root))
                    .await
                    .map_err(|error| format!("tenant policy load task failed: {error}"))??
            }
        };
        let mut entries = BTreeMap::new();
        for (file_name, body) in documents {
            // A temp file is an expected crash leftover; anything else under
            // this prefix is unexplained, and guessing is exactly what the
            // fatal load exists to prevent.
            if file_name.ends_with(POLICY_TEMP_SUFFIX) {
                continue;
            }
            let raw_tenant = file_name.strip_suffix(POLICY_FILE_SUFFIX).ok_or_else(|| {
                format!("unexpected object {file_name:?} under {TENANT_POLICY_PREFIX}")
            })?;
            let tenant = TenantId::parse(raw_tenant)
                .map_err(|error| format!("invalid tenant policy file {file_name:?}: {error}"))?;
            let document: PolicyDocument = serde_json::from_slice(&body)
                .map_err(|error| format!("invalid tenant policy {file_name:?}: {error}"))?;
            let retention = parse_retention(&document.retention).map_err(|error| {
                format!(
                    "invalid retention {:?} in {file_name:?}: {error}",
                    document.retention
                )
            })?;
            let ingest_rate = document
                .ingest_rate
                .as_deref()
                .map(|raw| {
                    parse_ingest_rate(raw).map_err(|error| {
                        format!("invalid ingest_rate {raw:?} in {file_name:?}: {error}")
                    })
                })
                .transpose()?;
            let updated_at = parse_timestamp(&document.updated_at).map_err(|error| {
                format!(
                    "invalid updated_at {:?} in {file_name:?}: {error}",
                    document.updated_at
                )
            })?;
            entries.insert(
                tenant,
                PolicyEntry {
                    retention,
                    raw: document.retention,
                    ingest_rate,
                    raw_ingest_rate: document.ingest_rate,
                    updated_at,
                },
            );
        }
        Ok(entries)
    }
}

fn write_local_policy(
    root: &Path,
    file_name: &str,
    temp_name: &str,
    body: &[u8],
) -> Result<(), String> {
    use std::io::Write;

    std::fs::create_dir_all(root).map_err(|error| {
        format!(
            "failed to create the tenant policy directory {}: {error}",
            root.display()
        )
    })?;
    let temporary = root.join(temp_name);
    let path = root.join(file_name);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| error.to_string())?;
    file.write_all(body).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())?;
    std::fs::rename(&temporary, &path).map_err(|error| error.to_string())?;
    crate::part::fsync_dir(root).map_err(|error| error.to_string())
}

fn read_local_policies(root: &Path) -> Result<Vec<(String, Vec<u8>)>, String> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        // Nothing has ever been pushed on this machine. Distinct from a read
        // failure: an empty policy set is a valid state, an unreadable one is
        // not.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to read the tenant policy directory {}: {error}",
                root.display()
            ));
        }
    };
    let mut documents = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        let file_name = entry
            .file_name()
            .into_string()
            .map_err(|name| format!("non-UTF-8 tenant policy file {name:?}"))?;
        let body = std::fs::read(entry.path())
            .map_err(|error| format!("failed to read tenant policy {file_name:?}: {error}"))?;
        documents.push((file_name, body));
    }
    Ok(documents)
}

/// The tenant→retention map, pushed one tenant at a time by the control plane.
///
/// loggytracy never calls out: the control plane learns immediately whether its
/// change took effect, and owns the retry when it did not. Nothing here is on
/// the ingest hot path, and a tenant that was never pushed keeps its data
/// forever — loggytracy never invents a deletion.
pub struct TenantPolicy {
    store: Option<PolicyStore>,
    policies: RwLock<Arc<PolicyMap>>,
    /// Serializes a write with the map swap that follows it. Two pushes for the
    /// same tenant are blind writes, so without this the store and the in-memory
    /// map could end up disagreeing about which one landed last.
    write_lock: tokio::sync::Mutex<()>,
    /// Stamps `updated_at` and resolves cutoffs. Injected because those two
    /// decide what a tenant may read: `query_floor_ns` clamps every read path
    /// to this clock, so a test that cannot move it cannot exercise a retention
    /// boundary except by writing data in the past and hoping.
    clock: Arc<crate::clock::Clock>,
    pub metrics: TenantPolicyMetrics,
}

impl TenantPolicy {
    /// A policy that is switched off: every tenant keeps everything, and the
    /// admin routes are not mounted.
    pub fn disabled() -> Self {
        Self::with_entries(None, BTreeMap::new())
    }

    fn with_entries(store: Option<PolicyStore>, entries: BTreeMap<TenantId, PolicyEntry>) -> Self {
        Self::with_entries_and_clock(store, entries, crate::clock::Clock::system())
    }

    fn with_entries_and_clock(
        store: Option<PolicyStore>,
        entries: BTreeMap<TenantId, PolicyEntry>,
        clock: Arc<crate::clock::Clock>,
    ) -> Self {
        Self {
            store,
            policies: RwLock::new(Arc::new(PolicyMap { entries })),
            write_lock: tokio::sync::Mutex::new(()),
            clock,
            metrics: TenantPolicyMetrics::default(),
        }
    }

    /// A policy whose clock the test controls, so retention boundaries can be
    /// crossed by moving time instead of by backdating data.
    #[cfg(test)]
    pub fn enabled_with_clock(clock: Arc<crate::clock::Clock>) -> Self {
        Self::with_entries_and_clock(
            Some(PolicyStore::Remote(Arc::new(ObjectStorage::in_memory()))),
            BTreeMap::new(),
            clock,
        )
    }

    #[cfg(test)]
    pub fn clock(&self) -> &Arc<crate::clock::Clock> {
        &self.clock
    }

    /// Load every stored policy before the workers start. A failure is fatal,
    /// the same class as a manifest that cannot be read.
    pub async fn load(
        config: &Config,
        object_storage: Option<Arc<ObjectStorage>>,
    ) -> Result<Self, String> {
        let store = match object_storage {
            Some(storage) => PolicyStore::Remote(storage),
            None => PolicyStore::Local(config.data_dir.join(TENANT_POLICY_PREFIX)),
        };
        let entries = store.load_all().await?;
        if config.tenant_policy_token.is_none() {
            // Without a token nothing clamps queries, so every row a downgrade
            // or a deletion had made invisible becomes readable again — and
            // with the admin routes unmounted there is no surface on which an
            // operator would notice. A forgotten environment variable must not
            // be able to undelete data, so this is fatal rather than a warning.
            if !entries.is_empty() {
                return Err(format!(
                    "{} stored tenant retention policies were found but \
LOGGYTRACY_TENANT_POLICY_TOKEN is not set; starting without it would unclamp every query and \
resurrect data those policies had already expired",
                    entries.len()
                ));
            }
            return Ok(Self::disabled());
        }
        tracing::info!(
            tenants = entries.len(),
            "loaded per-tenant retention policies"
        );
        Ok(Self::with_entries(Some(store), entries))
    }

    pub fn is_enabled(&self) -> bool {
        self.store.is_some()
    }

    /// The current map, or `None` when per-tenant retention is off. `None` must
    /// always mean "delete nothing".
    pub fn snapshot(&self) -> Option<Arc<PolicyMap>> {
        self.is_enabled()
            .then(|| self.policies.read().unwrap().clone())
    }

    pub fn cutoffs_at(&self, now_ns: i64) -> Option<Cutoffs> {
        Some(Cutoffs {
            policies: self.snapshot()?,
            now_ns,
        })
    }

    pub fn cutoffs_now(&self) -> Option<Cutoffs> {
        self.cutoffs_at(self.clock.now_ns())
    }

    /// Oldest timestamp `tenant` may still read. `None` leaves the requested
    /// range untouched, which is the fail-open behaviour every read path wants
    /// for a tenant the control plane has said nothing about.
    pub fn query_floor_ns(&self, tenant: &TenantId) -> Option<i64> {
        self.cutoffs_now()?.cutoff_ns(tenant)
    }

    pub fn view(&self, tenant: &TenantId) -> Option<PolicyView> {
        self.snapshot()?.view(tenant)
    }

    /// Store one tenant's retention, then apply it.
    ///
    /// The order is the whole point of the push shape: a success is a promise
    /// that the policy survives a restart, so the control plane's retry loop
    /// terminates on a real guarantee.
    /// A push carries the whole policy, so omitting `ingest_rate` clears it
    /// rather than leaving the previous value in place. Merging instead would
    /// make the stored state depend on the order of pushes, and a control
    /// plane retrying a push it believes to be complete would silently keep a
    /// rate it thinks it removed.
    pub async fn push(
        &self,
        tenant: &TenantId,
        raw: &str,
        raw_ingest_rate: Option<&str>,
    ) -> Result<PolicyView, PolicyError> {
        let Some(store) = &self.store else {
            return Err(PolicyError::Invalid(
                "per-tenant retention is not enabled".to_string(),
            ));
        };
        let retention = match parse_retention(raw) {
            Ok(retention) => retention,
            Err(error) => {
                self.metrics.push_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(PolicyError::Invalid(error));
            }
        };
        let ingest_rate = match raw_ingest_rate.map(parse_ingest_rate).transpose() {
            Ok(rate) => rate,
            Err(error) => {
                self.metrics.push_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(PolicyError::Invalid(error));
            }
        };
        let raw = raw.trim().to_string();
        let raw_ingest_rate = raw_ingest_rate.map(|rate| rate.trim().to_string());

        // Stamped under the lock, not on arrival: two concurrent pushes for the
        // same tenant commit in lock order, and a timestamp taken before the
        // wait could let the one that commits last store the older `updated_at`
        // and walk the push age backwards.
        let _guard = self.write_lock.lock().await;
        let updated_at = self.clock.now();
        let document = PolicyDocument {
            retention: raw.clone(),
            ingest_rate: raw_ingest_rate.clone(),
            updated_at: format_timestamp(updated_at),
        };
        if let Err(error) = store.put(tenant, &document).await {
            self.metrics
                .push_persist_errors
                .fetch_add(1, Ordering::Relaxed);
            return Err(PolicyError::Persist(error));
        }
        self.mutate(|entries| {
            entries.insert(
                tenant.clone(),
                PolicyEntry {
                    retention,
                    raw: raw.clone(),
                    ingest_rate,
                    raw_ingest_rate: raw_ingest_rate.clone(),
                    updated_at,
                },
            );
        });
        self.metrics.push_accepted.fetch_add(1, Ordering::Relaxed);
        Ok(PolicyView {
            retention: raw,
            ingest_rate: raw_ingest_rate,
            updated_at,
        })
    }

    /// The rate the control plane pushed for `tenant`, or `None` when it has
    /// said nothing about it — including when per-tenant policy is off
    /// entirely. The caller applies the configured default in that case, so
    /// that "policy disabled" and "tenant unknown" behave identically instead
    /// of one of them silently meaning unlimited.
    pub fn ingest_rate(&self, tenant: &TenantId) -> Option<TenantIngestRate> {
        self.snapshot()?.ingest_rate(tenant)
    }

    /// Return the tenant to *unknown*, which keeps its data forever. This is
    /// not tenant deletion; that is `retention: "0"`.
    pub async fn remove(&self, tenant: &TenantId) -> Result<(), PolicyError> {
        let Some(store) = &self.store else {
            return Err(PolicyError::Invalid(
                "per-tenant retention is not enabled".to_string(),
            ));
        };
        let _guard = self.write_lock.lock().await;
        if let Err(error) = store.delete(tenant).await {
            self.metrics
                .push_persist_errors
                .fetch_add(1, Ordering::Relaxed);
            return Err(PolicyError::Persist(error));
        }
        self.mutate(|entries| {
            entries.remove(tenant);
        });
        self.metrics.push_accepted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn record_rejected_push(&self) {
        self.metrics.push_rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_unauthorized(&self) {
        self.metrics
            .admin_unauthorized
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Copy-insert-swap. Pushes are rare, so paying a map copy per push keeps
    /// every query read to one `Arc` clone.
    fn mutate(&self, change: impl FnOnce(&mut BTreeMap<TenantId, PolicyEntry>)) {
        let mut policies = self.policies.write().unwrap();
        let mut entries = policies.entries.clone();
        change(&mut entries);
        *policies = Arc::new(PolicyMap { entries });
    }

    #[cfg(test)]
    pub fn install_for_test(&self, retentions: BTreeMap<TenantId, TenantRetention>) {
        self.mutate(|entries| {
            entries.clear();
            for (tenant, retention) in retentions {
                let raw = match retention {
                    TenantRetention::Infinite => "infinite".to_string(),
                    TenantRetention::Finite(period) => format!("{}ms", period.as_millis()),
                };
                entries.insert(
                    tenant,
                    PolicyEntry {
                        retention,
                        raw,
                        ingest_rate: None,
                        raw_ingest_rate: None,
                        updated_at: SystemTime::now(),
                    },
                );
            }
        });
    }

    #[cfg(test)]
    pub fn enabled_for_test() -> Self {
        Self::with_entries(
            Some(PolicyStore::Remote(Arc::new(ObjectStorage::in_memory()))),
            BTreeMap::new(),
        )
    }

    #[cfg(test)]
    pub fn for_test_with_store(store: Arc<ObjectStorage>) -> Self {
        Self::with_entries(Some(PolicyStore::Remote(store)), BTreeMap::new())
    }

    #[cfg(test)]
    pub fn for_test_with_local_store(root: PathBuf) -> Self {
        Self::with_entries(Some(PolicyStore::Local(root)), BTreeMap::new())
    }
}

/// Only tests reach for this now; production code reads the clock its
/// `TenantPolicy` or `AppState` was built with.
#[cfg(test)]
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn format_timestamp(at: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at).to_rfc3339()
}

fn parse_timestamp(raw: &str) -> Result<SystemTime, String> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|parsed| SystemTime::from(parsed.with_timezone(&chrono::Utc)))
        .map_err(|error| format!("{error}"))
}

/// Prometheus-style duration, or the literal `infinite`. Untrusted input.
pub fn parse_retention(raw: &str) -> Result<TenantRetention, String> {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("infinite") {
        return Ok(TenantRetention::Infinite);
    }
    if value == "0" {
        // Deleting a tenant is `retention: "0"`: the cutoff lands at now, so
        // queries empty immediately and every part holding the tenant becomes
        // eligible for rewrite regardless of the rewrite threshold.
        return Ok(TenantRetention::Finite(Duration::ZERO));
    }
    let (number, unit_nanos) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_000_000u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('d') {
        (number, 24 * 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('w') {
        (number, 7 * 24 * 60 * 60 * 1_000_000_000u64)
    } else if let Some(number) = value.strip_suffix('y') {
        (number, 365 * 24 * 60 * 60 * 1_000_000_000u64)
    } else {
        return Err(
            "expected a duration such as 90m, 24h, 7d, or the literal \"infinite\"".to_string(),
        );
    };
    let number: u64 = number.trim().parse().map_err(|error| format!("{error}"))?;
    let nanos = number
        .checked_mul(unit_nanos)
        .ok_or_else(|| "duration overflow".to_string())?;
    Ok(TenantRetention::Finite(Duration::from_nanos(nanos)))
}

/// Parses an ingest rate: a byte size per second, or the literal `unlimited`.
///
/// `"0"` is accepted and means the tenant may not write, mirroring
/// `retention: "0"`. The unit is bytes per second and is not optional in the
/// suffix-less case for the same reason a bare number is not a duration: a
/// value whose unit has to be guessed is a value that will eventually be
/// guessed wrong.
pub fn parse_ingest_rate(raw: &str) -> Result<TenantIngestRate, String> {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("unlimited") {
        return Ok(TenantIngestRate::Unlimited);
    }
    let value = value.strip_suffix("/s").map(str::trim_end).unwrap_or(value);
    let (number, unit) = if let Some(number) = value.strip_suffix("KiB") {
        (number, 1024u64)
    } else if let Some(number) = value.strip_suffix("MiB") {
        (number, 1024 * 1024u64)
    } else if let Some(number) = value.strip_suffix("GiB") {
        (number, 1024 * 1024 * 1024u64)
    } else if let Some(number) = value.strip_suffix('B') {
        (number, 1u64)
    } else {
        (value, 1u64)
    };
    let number: u64 = number.trim().parse().map_err(|_| {
        "expected a byte rate such as 512KiB/s, 4MiB/s, 0, or the literal \"unlimited\"".to_string()
    })?;
    let bytes = number
        .checked_mul(unit)
        .ok_or_else(|| "ingest rate overflow".to_string())?;
    Ok(TenantIngestRate::BytesPerSecond(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(raw: &str) -> TenantId {
        TenantId::parse(raw).expect("valid tenant id")
    }

    fn days(count: u64) -> Duration {
        Duration::from_secs(count * 24 * 60 * 60)
    }

    #[test]
    fn parses_prometheus_durations_and_the_infinite_literal() {
        assert_eq!(
            parse_retention("30d").unwrap(),
            TenantRetention::Finite(days(30))
        );
        assert_eq!(
            parse_retention("90m").unwrap(),
            TenantRetention::Finite(Duration::from_secs(90 * 60))
        );
        assert_eq!(
            parse_retention("infinite").unwrap(),
            TenantRetention::Infinite
        );
        assert_eq!(
            parse_retention("0").unwrap(),
            TenantRetention::Finite(Duration::ZERO)
        );
        assert!(parse_retention("7").is_err());
        assert!(parse_retention("forever").is_err());
        assert!(parse_retention("-1d").is_err());
    }

    #[test]
    fn an_unknown_tenant_never_expires() {
        let policy = TenantPolicy::enabled_for_test();
        assert_eq!(policy.query_floor_ns(&tenant("acme")), None);

        policy.install_for_test(
            [(
                tenant("acme"),
                TenantRetention::Finite(Duration::from_nanos(100)),
            )]
            .into_iter()
            .collect(),
        );
        let cutoffs = policy.cutoffs_at(1_000).unwrap();
        assert_eq!(cutoffs.cutoff_ns(&tenant("acme")), Some(900));
        assert!(cutoffs.is_expired(&tenant("acme"), 899));
        assert!(!cutoffs.is_expired(&tenant("acme"), 900));
        assert_eq!(cutoffs.cutoff_ns(&tenant("hobby")), None);
        assert!(!cutoffs.is_expired(&tenant("hobby"), i64::MIN));
    }

    #[test]
    fn a_disabled_policy_never_expires_anything() {
        let policy = TenantPolicy::disabled();
        assert!(!policy.is_enabled());
        assert!(policy.cutoffs_now().is_none());
        assert!(policy.snapshot().is_none());
        assert_eq!(policy.query_floor_ns(&tenant("acme")), None);
    }

    #[tokio::test]
    async fn a_push_is_durable_and_survives_a_restart() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = TenantPolicy::for_test_with_store(storage.clone());
        policy.push(&tenant("acme"), "30d", None).await.unwrap();
        policy
            .push(&tenant("intern"), "infinite", None)
            .await
            .unwrap();
        // A downgrade taken before the restart must still be in force after it.
        policy.push(&tenant("acme"), "7d", None).await.unwrap();
        assert_eq!(
            policy.metrics.push_accepted.load(Ordering::Relaxed),
            3,
            "every push is accepted"
        );

        let config = Config {
            tenant_policy_token: Some("secret".to_string()),
            retention_period: None,
            ..Config::default()
        };
        let restarted = TenantPolicy::load(&config, Some(storage)).await.unwrap();
        let map = restarted.snapshot().unwrap();
        assert_eq!(map.tenant_count(), 2);
        assert_eq!(
            map.retention(&tenant("acme")),
            Some(TenantRetention::Finite(days(7))),
            "the newest policy wins, not the first one stored"
        );
        assert_eq!(
            map.retention(&tenant("intern")),
            Some(TenantRetention::Infinite)
        );
        assert_eq!(map.view(&tenant("acme")).unwrap().retention, "7d");
    }

    #[test]
    fn zero_retention_expires_everything_and_forces_a_rewrite() {
        let policy = TenantPolicy::enabled_for_test();
        policy.install_for_test(
            [
                (tenant("acme"), TenantRetention::Finite(Duration::ZERO)),
                (tenant("beta"), TenantRetention::Infinite),
            ]
            .into_iter()
            .collect(),
        );
        let cutoffs = policy.cutoffs_at(1_000).unwrap();
        // The cutoff sits at now, so every query for the tenant empties from
        // the next request onward.
        assert_eq!(cutoffs.cutoff_ns(&tenant("acme")), Some(1_000));
        assert!(cutoffs.is_expired(&tenant("acme"), 999));
        assert!(!cutoffs.is_expired(&tenant("beta"), i64::MIN));
        assert!(cutoffs.is_zero_retention(&tenant("acme")));
        assert!(!cutoffs.is_zero_retention(&tenant("beta")));
        assert!(!cutoffs.is_zero_retention(&tenant("hobby")));
    }

    #[tokio::test]
    async fn a_local_store_round_trips_without_an_object_store() {
        let data_dir = tempdir("tenant-policy-local");
        let dir = data_dir.join(TENANT_POLICY_PREFIX);
        let policy = TenantPolicy::for_test_with_local_store(dir.clone());
        policy.push(&tenant("acme"), "7d", None).await.unwrap();
        assert_eq!(
            policy.snapshot().unwrap().retention(&tenant("acme")),
            Some(TenantRetention::Finite(days(7)))
        );

        let config = Config {
            tenant_policy_token: Some("secret".to_string()),
            retention_period: None,
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let restarted = TenantPolicy::load(&config, None).await.unwrap();
        assert_eq!(
            restarted.snapshot().unwrap().retention(&tenant("acme")),
            Some(TenantRetention::Finite(days(7)))
        );

        // A crash leftover is expected and ignored; anything else is not.
        std::fs::write(dir.join("acme.json.tmp"), b"garbage").unwrap();
        assert!(TenantPolicy::load(&config, None).await.is_ok());
        std::fs::write(dir.join("acme.txt"), b"garbage").unwrap();
        assert!(TenantPolicy::load(&config, None).await.is_err());
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    #[tokio::test]
    async fn an_unreadable_policy_object_fails_the_load() {
        let storage = Arc::new(ObjectStorage::in_memory());
        storage
            .put_tenant_policy("acme", b"{\"retention\":\"soon\"".to_vec())
            .await
            .unwrap();
        let config = Config {
            tenant_policy_token: Some("secret".to_string()),
            retention_period: None,
            ..Config::default()
        };
        assert!(TenantPolicy::load(&config, Some(storage)).await.is_err());
    }

    /// What retention actually promises is a boundary in time, and the previous
    /// tests could only approach it by writing data with backdated timestamps
    /// and asserting the far side. That leaves the edge itself — the case a
    /// user notices — unexercised.
    ///
    /// With the clock injected the data stays put and time moves, which is the
    /// real situation, and the assertion can sit exactly on the boundary.
    #[tokio::test]
    async fn a_retention_boundary_is_crossed_by_time_passing() {
        let start_ns = 1_800_000_000_000_000_000i64;
        let clock = crate::clock::Clock::fixed(start_ns);
        let policy = TenantPolicy::enabled_with_clock(clock.clone());
        policy.push(&tenant("acme"), "7d", None).await.unwrap();

        let seven_days = Duration::from_secs(7 * 24 * 60 * 60);
        let floor = || policy.query_floor_ns(&tenant("acme")).expect("finite");

        // A row written now sits exactly at the floor's far side.
        assert_eq!(floor(), start_ns - seven_days.as_nanos() as i64);

        // One nanosecond before the boundary: still readable.
        clock.advance(seven_days);
        assert_eq!(floor(), start_ns);
        assert!(
            !policy
                .cutoffs_now()
                .unwrap()
                .is_expired(&tenant("acme"), start_ns),
            "a row exactly at the cutoff is retained"
        );

        // One nanosecond past it: gone.
        clock.advance(Duration::from_nanos(1));
        assert!(
            policy
                .cutoffs_now()
                .unwrap()
                .is_expired(&tenant("acme"), start_ns),
            "one nanosecond past the cutoff must expire"
        );
    }

    /// An upgrade applies at deletion time, not write time. Stated that way it
    /// is a claim about the clock, so moving the clock is how to check it.
    #[tokio::test]
    async fn an_upgrade_rescues_data_the_old_plan_had_already_passed() {
        let start_ns = 1_800_000_000_000_000_000i64;
        let clock = crate::clock::Clock::fixed(start_ns);
        let policy = TenantPolicy::enabled_with_clock(clock.clone());
        policy.push(&tenant("acme"), "1d", None).await.unwrap();

        clock.advance(Duration::from_secs(2 * 24 * 60 * 60));
        assert!(
            policy
                .cutoffs_now()
                .unwrap()
                .is_expired(&tenant("acme"), start_ns),
            "two days in, a one-day plan has passed this row"
        );

        // The control plane upgrades the tenant. The row is still on disk, and
        // the new plan covers it again.
        policy.push(&tenant("acme"), "30d", None).await.unwrap();
        assert!(
            !policy
                .cutoffs_now()
                .unwrap()
                .is_expired(&tenant("acme"), start_ns),
            "the upgrade must apply to data written under the old plan"
        );
    }

    /// Retention `0` is how a tenant is deleted, and until a rewrite reclaims
    /// the rows the only thing hiding them is the query clamp. Dropping the
    /// token drops the clamp, so a boot that would do that has to fail.
    #[tokio::test]
    async fn dropping_the_token_with_stored_policies_fails_the_boot() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let with_token = Config {
            tenant_policy_token: Some("secret".to_string()),
            retention_period: None,
            ..Config::default()
        };
        TenantPolicy::load(&with_token, Some(storage.clone()))
            .await
            .expect("an empty store boots")
            .push(&tenant("acme"), "0", None)
            .await
            .expect("the deletion is stored");

        let without_token = Config {
            tenant_policy_token: None,
            ..with_token.clone()
        };
        let error = match TenantPolicy::load(&without_token, Some(storage.clone())).await {
            Err(error) => error,
            Ok(_) => panic!("a stored policy without a token must not boot"),
        };
        assert!(error.contains("TENANT_POLICY_TOKEN"), "{error}");

        // With the policy withdrawn there is nothing left to unclamp, so the
        // guard stops applying: it protects stored decisions, not the token.
        TenantPolicy::load(&with_token, Some(storage.clone()))
            .await
            .unwrap()
            .remove(&tenant("acme"))
            .await
            .unwrap();
        assert!(
            TenantPolicy::load(&without_token, Some(storage))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_failing_store_changes_neither_the_map_nor_the_object() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = TenantPolicy::for_test_with_store(storage.clone());
        policy.push(&tenant("acme"), "30d", None).await.unwrap();

        // A malformed value is rejected before anything is written.
        assert!(matches!(
            policy.push(&tenant("acme"), "soon", None).await,
            Err(PolicyError::Invalid(_))
        ));
        assert_eq!(policy.metrics.push_rejected.load(Ordering::Relaxed), 1);
        assert_eq!(
            policy.snapshot().unwrap().retention(&tenant("acme")),
            Some(TenantRetention::Finite(days(30))),
            "a rejected push leaves the previous policy in force"
        );
        assert_eq!(storage.load_tenant_policies().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_delete_returns_the_tenant_to_unknown() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = TenantPolicy::for_test_with_store(storage.clone());
        policy.push(&tenant("acme"), "1ms", None).await.unwrap();
        assert!(policy.query_floor_ns(&tenant("acme")).is_some());

        policy.remove(&tenant("acme")).await.unwrap();
        assert_eq!(
            policy.query_floor_ns(&tenant("acme")),
            None,
            "an unknown tenant is never clamped"
        );
        assert!(storage.load_tenant_policies().await.unwrap().is_empty());
        // Deleting an unknown tenant is a no-op, not an error.
        policy.remove(&tenant("hobby")).await.unwrap();
    }

    /// The control plane is the only authority on retention. A pushed value is
    /// applied exactly as sent, and the two ways of saying "keep forever" —
    /// an explicit `infinite` and never having been pushed — must produce the
    /// same retention, or asking for preservation would cost data that staying
    /// silent would have kept.
    #[tokio::test]
    async fn nothing_reinterprets_a_pushed_value() {
        let storage = Arc::new(ObjectStorage::in_memory());
        let policy = TenantPolicy::for_test_with_store(storage);
        policy.push(&tenant("acme"), "3650d", None).await.unwrap();
        policy
            .push(&tenant("intern"), "infinite", None)
            .await
            .unwrap();

        let map = policy.snapshot().unwrap();
        assert_eq!(
            map.retention(&tenant("acme")),
            Some(TenantRetention::Finite(days(3650))),
            "a long finite plan is honoured as sent"
        );
        assert_eq!(map.view(&tenant("acme")).unwrap().retention, "3650d");
        assert_eq!(
            map.retention(&tenant("intern")),
            Some(TenantRetention::Infinite)
        );
        assert_eq!(map.infinite_tenant_count(), 1);

        let cutoffs = policy.cutoffs_at(1_000).unwrap();
        assert_eq!(
            cutoffs.cutoff_ns(&tenant("intern")),
            cutoffs.cutoff_ns(&tenant("never_pushed")),
            "an explicit infinite keeps exactly as much as never being pushed"
        );
    }

    fn tempdir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "loggytracy-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
