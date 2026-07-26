/// Immutable part objects plus a compare-and-swap manifest.
///
/// `prefix` is the path component of LOGGYTRACY_OBJECT_STORE_URL. Credentials
/// and endpoint settings are consumed by object_store from the process
/// environment (AWS_*, OBJECT_STORE_* and backend-specific keys).
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    manifest_update: tokio::sync::Mutex<()>,
    /// The local filesystem backend does not implement conditional updates.
    /// It is only exposed as a single-process development backend, where the
    /// process-local mutex plus LocalFileSystem's staged rename gives us an
    /// atomic replacement.
    local_manifest_overwrite: bool,
}

/// Coordinates all mutations of the local object-store cache. Readers hold a
/// shared guard while their Parquet files are open; restore and eviction hold
/// an exclusive guard.
pub struct RemoteCache {
    pub storage: Arc<ObjectStorage>,
    pub parts_root: PathBuf,
    // The low bit is health; the remaining bits are a failure generation.
    // A success may clear a failure only if the operation started in the
    // current generation, preventing an older concurrent restore/publish from
    // overwriting a newer failure.
    remote_state: Arc<AtomicU64>,
    cache_healthy: Arc<AtomicBool>,
}

impl RemoteCache {
    pub fn new(storage: Arc<ObjectStorage>, parts_root: PathBuf) -> Self {
        Self {
            storage,
            parts_root,
            remote_state: Arc::new(AtomicU64::new(1)),
            cache_healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.is_remote_healthy() && self.is_cache_healthy()
    }

    pub fn is_remote_healthy(&self) -> bool {
        self.remote_state.load(Ordering::Acquire) & 1 == 1
    }

    pub fn is_cache_healthy(&self) -> bool {
        self.cache_healthy.load(Ordering::Acquire)
    }

    pub fn mark_remote_healthy(&self) {
        let epoch = self.remote_operation_epoch();
        self.mark_remote_healthy_since(epoch);
    }

    pub fn mark_remote_unhealthy(&self) {
        self.remote_state
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                Some((((state >> 1).saturating_add(1)) << 1) & !1)
            })
            .ok();
    }

    pub fn remote_operation_epoch(&self) -> u64 {
        self.remote_state.load(Ordering::Acquire) >> 1
    }

    pub fn mark_remote_healthy_since(&self, epoch: u64) {
        let unhealthy = epoch << 1;
        let healthy = unhealthy | 1;
        let _ = self.remote_state.compare_exchange(
            unhealthy,
            healthy,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn mark_cache_healthy(&self) {
        self.cache_healthy.store(true, Ordering::Release);
    }

    pub fn mark_cache_unhealthy(&self) {
        self.cache_healthy.store(false, Ordering::Release);
    }

    pub fn trace_parts_root(&self) -> PathBuf {
        self.parts_root
            .parent()
            .map(|parent| parent.join("traces"))
            .unwrap_or_else(|| PathBuf::from("traces"))
    }

    #[cfg(test)]
    pub fn mark_unhealthy(&self) {
        self.mark_remote_unhealthy();
    }
}

impl ObjectStorage {
    pub fn from_url(url: &str) -> Result<Self, String> {
        let url =
            url::Url::parse(url).map_err(|error| format!("invalid object-store URL: {error}"))?;
        let local_manifest_overwrite = url.scheme() == "file";
        if local_manifest_overwrite {
            tracing::warn!(
                %url,
                "object store is a local filesystem path: manifest updates use overwrite instead \
of compare-and-swap and are only safe for a single process on a local disk. Do not use this for \
production or on shared/network storage."
            );
        }
        let options = normalized_object_store_options(std::env::vars());
        let (store, prefix) = object_store::parse_url_opts(&url, options)
            .map_err(|error| format!("failed to configure object store: {error}"))?;
        // Tier B load gate: wrap the real store with seeded latency/fault
        // injection only when the load knobs are present. Absent the knobs this
        // is a no-op and the object-store construction path is unchanged.
        let store: Arc<dyn ObjectStore> = match fault_store::FaultConfig::from_env()? {
            Some(config) => Arc::new(fault_store::LatencyFaultStore::new(Arc::from(store), config)),
            None => Arc::from(store),
        };
        Ok(Self {
            store,
            prefix,
            manifest_update: tokio::sync::Mutex::new(()),
            local_manifest_overwrite,
        })
    }

    #[cfg(test)]
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: ObjectPath::from("loggytracy-test"),
            manifest_update: tokio::sync::Mutex::new(()),
            local_manifest_overwrite: false,
        }
    }

    /// An in-memory store whose every write fails, for the paths that must
    /// report a failure rather than apply a change that is not durable.
    #[cfg(test)]
    pub fn in_memory_with_failing_writes() -> Self {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        Self {
            store: Arc::new(fault_store::LatencyFaultStore::new(
                inner,
                fault_store::FaultConfig::for_test(0, 0, 0, 1.0, 1),
            )),
            prefix: ObjectPath::from("loggytracy-test"),
            manifest_update: tokio::sync::Mutex::new(()),
            local_manifest_overwrite: false,
        }
    }

    /// Wrap an arbitrary object store, used by tests to inject fault-injecting
    /// backends. The store is treated as a full conditional-put backend (no
    /// local overwrite shortcut), exercising the real CAS manifest path.
    #[cfg(test)]
    pub fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: ObjectPath::from(prefix),
            manifest_update: tokio::sync::Mutex::new(()),
            local_manifest_overwrite: false,
        }
    }

    fn path(&self, relative: &str) -> ObjectPath {
        if self.prefix.as_ref().is_empty() {
            ObjectPath::from(relative)
        } else {
            ObjectPath::from(format!("{}/{relative}", self.prefix.as_ref()))
        }
    }

    fn manifest_path(&self) -> ObjectPath {
        self.path(MANIFEST_FILE)
    }

    fn trace_manifest_path(&self) -> ObjectPath {
        self.path(TRACE_MANIFEST_FILE)
    }

    fn part_path(&self, part: &ManifestPart, file: &str) -> ObjectPath {
        self.path(&format!("parts/{}/{}/{}", part.partition, part.id, file))
    }

    fn tenant_policy_path(&self, tenant: &str) -> ObjectPath {
        self.path(&format!("{TENANT_POLICY_PREFIX}/{tenant}.json"))
    }

    fn trace_part_path(&self, part: &TraceManifestPart, file: &str) -> ObjectPath {
        self.path(&format!(
            "trace_parts/{}/{}/{}",
            part.partition, part.id, file
        ))
    }

    async fn load_manifest_versioned(&self) -> Result<LoadedManifest, String> {
        let path = self.manifest_path();
        match self.store.get(&path).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| format!("failed to read manifest body: {error}"))?;
                let manifest: Manifest = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid object-store manifest: {error}"))?;
                if manifest.format_version != MANIFEST_FORMAT_VERSION {
                    return Err(format!(
                        "unsupported manifest format version {}",
                        manifest.format_version
                    ));
                }
                validate_manifest(&manifest)?;
                Ok(LoadedManifest { manifest, version })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(LoadedManifest {
                manifest: Manifest::default(),
                version: None,
            }),
            Err(error) => Err(format!("failed to load object-store manifest: {error}")),
        }
    }

    pub async fn load_manifest(&self) -> Result<Manifest, String> {
        Ok(self.load_manifest_versioned().await?.manifest)
    }

    async fn load_trace_manifest_versioned(
        &self,
    ) -> Result<(TraceManifest, Option<UpdateVersion>), String> {
        match self.store.get(&self.trace_manifest_path()).await {
            Ok(result) => {
                let version = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| format!("failed to read trace manifest body: {error}"))?;
                let manifest: TraceManifest = serde_json::from_slice(&bytes)
                    .map_err(|error| format!("invalid trace object-store manifest: {error}"))?;
                if manifest.format_version != MANIFEST_FORMAT_VERSION {
                    return Err(format!(
                        "unsupported trace manifest format version {}",
                        manifest.format_version
                    ));
                }
                validate_trace_manifest(&manifest)?;
                Ok((manifest, version))
            }
            Err(object_store::Error::NotFound { .. }) => Ok((TraceManifest::default(), None)),
            Err(error) => Err(format!(
                "failed to load trace object-store manifest: {error}"
            )),
        }
    }

    pub async fn load_trace_manifest(&self) -> Result<TraceManifest, String> {
        Ok(self.load_trace_manifest_versioned().await?.0)
    }

    pub async fn publish_trace_parts(&self, added: &[TracePart]) -> Result<TraceManifest, String> {
        for part in added {
            let id = part.meta.id.clone();
            TracePartReader::open(part.clone())
                .map_err(|error| format!("refusing to publish invalid trace part {id}: {error}"))?;
        }
        for part in added {
            self.upload_trace_part(part).await?;
        }

        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_trace_manifest_versioned().await?;
            let mut next = loaded.clone();
            for part in added {
                let descriptor = TraceManifestPart {
                    id: part.meta.id.clone(),
                    partition: part.meta.partition.clone(),
                };
                if let Some(existing) = next.parts.iter().find(|item| item.id == descriptor.id) {
                    if existing != &descriptor {
                        return Err(format!(
                            "trace manifest part ID collision: {}",
                            descriptor.id
                        ));
                    }
                } else {
                    next.parts.push(descriptor);
                }
            }
            next.parts.sort_by(|left, right| {
                (&left.partition, &left.id).cmp(&(&right.partition, &right.id))
            });
            if next.parts == loaded.parts {
                return Ok(loaded);
            }
            next.generation = loaded
                .generation
                .checked_add(1)
                .ok_or_else(|| "trace manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode trace manifest: {error}"))?;
            let mode = match version {
                Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            };
            match self
                .store
                .put_opts(
                    &self.trace_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update trace manifest: {error}")),
            }
        }
        Err("trace manifest compare-and-swap retry limit exceeded".to_string())
    }

    /// Removes trace descriptors from the manifest using the same CAS
    /// semantics as publication. The immutable objects are deleted only after
    /// the manifest no longer exposes them.
    pub async fn remove_trace_parts(
        &self,
        removed: &[TraceManifestPart],
    ) -> Result<TraceManifest, String> {
        if removed.is_empty() {
            return self.load_trace_manifest().await;
        }
        let removed_ids: HashSet<&str> = removed.iter().map(|part| part.id.as_str()).collect();
        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let (loaded, version) = self.load_trace_manifest_versioned().await?;
            let present = loaded
                .parts
                .iter()
                .filter(|part| removed_ids.contains(part.id.as_str()))
                .count();
            // Removal is the only thing this writes, so it is idempotent per
            // id: a batch that mixes ids an earlier tick already removed with
            // ids that have only just expired removes what is left rather than
            // failing, which is what keeps a retry from wedging forever.
            if present == 0 {
                return Ok(loaded);
            }
            let mut next = loaded.clone();
            next.parts
                .retain(|part| !removed_ids.contains(part.id.as_str()));
            next.generation = loaded
                .generation
                .checked_add(1)
                .ok_or_else(|| "trace manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode trace manifest: {error}"))?;
            let mode = match version {
                Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            };
            match self
                .store
                .put_opts(
                    &self.trace_manifest_path(),
                    body.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(next),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update trace manifest: {error}")),
            }
        }
        Err("trace manifest retention CAS retry limit exceeded".to_string())
    }

    pub async fn delete_part_objects(&self, parts: &[ManifestPart]) -> Result<(), String> {
        for part in parts {
            for file in PART_FILES {
                match self.store.delete(&self.part_path(part, file)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to delete remote part {}/{} file {file}: {error}",
                            part.partition, part.id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn delete_trace_part_objects(
        &self,
        parts: &[TraceManifestPart],
    ) -> Result<(), String> {
        for part in parts {
            for file in TRACE_PART_FILES {
                match self.store.delete(&self.trace_part_path(part, file)).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to delete remote trace part {}/{} file {file}: {error}",
                            part.partition, part.id
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// One tenant's retention policy, written blind.
    ///
    /// One object per tenant is what lets a push be a single unconditional
    /// write: no read-modify-write, no CAS, and no contention between two
    /// tenants pushed concurrently. Ordering between two pushes for the *same*
    /// tenant is the caller's problem, and `TenantPolicy` serializes them.
    pub async fn put_tenant_policy(&self, tenant: &str, body: Vec<u8>) -> Result<(), String> {
        self.store
            .put(&self.tenant_policy_path(tenant), body.into())
            .await
            .map(|_| ())
            .map_err(|error| format!("failed to store the policy for tenant {tenant}: {error}"))
    }

    pub async fn delete_tenant_policy(&self, tenant: &str) -> Result<(), String> {
        match self.store.delete(&self.tenant_policy_path(tenant)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(format!(
                "failed to delete the policy for tenant {tenant}: {error}"
            )),
        }
    }

    /// Every stored policy, as `(file name, body)`. Read once at startup; a
    /// failure here is fatal, so it never returns a partial listing.
    pub async fn load_tenant_policies(&self) -> Result<Vec<(String, Vec<u8>)>, String> {
        use futures_util::StreamExt;

        let prefix = self.path(TENANT_POLICY_PREFIX);
        let mut locations = Vec::new();
        let mut stream = self.store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            let meta =
                item.map_err(|error| format!("failed to list the tenant policies: {error}"))?;
            locations.push(meta.location);
        }
        let mut policies = Vec::new();
        for location in locations {
            let name = location
                .filename()
                .ok_or_else(|| format!("tenant policy object {location} has no file name"))?
                .to_string();
            let bytes = self
                .store
                .get(&location)
                .await
                .map_err(|error| format!("failed to read tenant policy {location}: {error}"))?
                .bytes()
                .await
                .map_err(|error| format!("failed to read tenant policy {location}: {error}"))?;
            policies.push((name, bytes.to_vec()));
        }
        Ok(policies)
    }

    /// Deletes immutable objects that are absent from both manifests only
    /// after a grace period. Retention removes manifest visibility first; this
    /// pass is the crash-safe, delayed physical garbage collector.
    pub async fn garbage_collect_orphans(
        &self,
        grace_period: std::time::Duration,
    ) -> Result<usize, String> {
        use futures_util::StreamExt;

        let manifest = self.load_manifest().await?;
        let trace_manifest = self.load_trace_manifest().await?;
        let mut active = HashSet::new();
        for part in &manifest.parts {
            for file in PART_FILES {
                active.insert(self.part_path(part, file).to_string());
            }
        }
        for part in &trace_manifest.parts {
            for file in TRACE_PART_FILES {
                active.insert(self.trace_part_path(part, file).to_string());
            }
        }
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(grace_period)
                .map_err(|error| format!("invalid garbage-collection grace period: {error}"))?;
        let mut candidates = Vec::new();
        for prefix in [self.path("parts"), self.path("trace_parts")] {
            let mut stream = self.store.list(Some(&prefix));
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|error| format!("failed to list object store: {error}"))?;
                if meta.last_modified < cutoff && !active.contains(meta.location.to_string().as_str())
                {
                    candidates.push(meta.location);
                }
            }
        }
        let mut removed = 0;
        for location in candidates {
            match self.store.delete(&location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => removed += 1,
                Err(error) => {
                    return Err(format!(
                        "failed to delete orphan object {}: {error}",
                        location
                    ));
                }
            }
        }
        Ok(removed)
    }

}
