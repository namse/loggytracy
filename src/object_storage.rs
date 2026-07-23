use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};

use crate::part::{self, BLOOM_FILE, DATA_FILE, META_FILE, Part, STREAM_INDEX_FILE};

const MANIFEST_FILE: &str = "manifest.json";
const UPLOAD_MARKER_FILE: &str = ".object-store-uploading";
const PART_FILES: [&str; 4] = [DATA_FILE, BLOOM_FILE, STREAM_INDEX_FILE, META_FILE];
const CATALOG_FILES: [&str; 3] = [BLOOM_FILE, STREAM_INDEX_FILE, META_FILE];
const MANIFEST_FORMAT_VERSION: u32 = 1;
const MAX_CAS_ATTEMPTS: usize = 16;

fn normalized_object_store_options<I>(variables: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    let variables: Vec<_> = variables.into_iter().collect();
    let mut options = BTreeMap::new();

    // object_store's config-key parser expects lowercase names even though
    // process environment variables conventionally use uppercase names.
    for (key, value) in &variables {
        if key.starts_with("AWS_") {
            options.insert(key.to_ascii_lowercase(), value.clone());
        }
    }
    // Explicit OBJECT_STORE_* values override the corresponding AWS value.
    // Both OBJECT_STORE_ENDPOINT and OBJECT_STORE_AWS_ENDPOINT are accepted.
    for (key, value) in &variables {
        if let Some(key) = key.strip_prefix("OBJECT_STORE_") {
            options.insert(key.to_ascii_lowercase(), value.clone());
        }
    }
    options.into_iter().collect()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestPart {
    pub id: String,
    pub partition: String,
}

impl From<&Part> for ManifestPart {
    fn from(part: &Part) -> Self {
        Self {
            id: part.meta.id.clone(),
            partition: part.meta.partition.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub generation: u64,
    pub parts: Vec<ManifestPart>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            generation: 0,
            parts: Vec::new(),
        }
    }
}

struct LoadedManifest {
    manifest: Manifest,
    version: Option<UpdateVersion>,
}

struct LocalMergeGroup {
    old_ids: Vec<String>,
    added: Vec<Part>,
}

fn validate_cache_root(root: &Path) -> Result<PathBuf, String> {
    let metadata = std::fs::symlink_metadata(root).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!("refusing unsafe cache root {}", root.display()));
    }
    std::fs::canonicalize(root).map_err(|error| error.to_string())
}

fn ensure_safe_directory_chain(root: &Path, components: &[&str]) -> Result<PathBuf, String> {
    std::fs::create_dir_all(root).map_err(|error| error.to_string())?;
    let canonical_root = validate_cache_root(root)?;
    let mut current = root.to_path_buf();
    for component in components {
        if !is_safe_path_component(component) {
            return Err(format!("unsafe cache path component {component:?}"));
        }
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "refusing symlinked cache directory {}",
                    current.display()
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "cache path is not a directory: {}",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|error| {
                    format!(
                        "failed to create cache directory {}: {error}",
                        current.display()
                    )
                })?;
            }
            Err(error) => return Err(error.to_string()),
        }
        let canonical = std::fs::canonicalize(&current).map_err(|error| error.to_string())?;
        if !canonical.starts_with(&canonical_root) {
            return Err(format!(
                "cache directory escapes root: {}",
                current.display()
            ));
        }
    }
    Ok(current)
}

fn ensure_existing_cache_dir(canonical_root: &Path, path: &Path) -> Result<(), String> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "refusing unsafe cache directory {}",
            path.display()
        ));
    }
    let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
    if !canonical.starts_with(canonical_root) {
        return Err(format!("cache directory escapes root: {}", path.display()));
    }
    Ok(())
}

fn cache_part_dir(parts_root: &Path, descriptor: &ManifestPart) -> Result<PathBuf, String> {
    let dir = ensure_safe_directory_chain(parts_root, &[&descriptor.partition, &descriptor.id])?;
    for file in [
        DATA_FILE,
        BLOOM_FILE,
        STREAM_INDEX_FILE,
        META_FILE,
        part::MERGE_TOMBSTONE_FILE,
        UPLOAD_MARKER_FILE,
        ".access",
    ] {
        match std::fs::symlink_metadata(dir.join(file)) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "refusing symlinked cache file {}",
                    dir.join(file).display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(dir)
}

fn validate_cache_tree_no_symlinks(parts_root: &Path) -> Result<(), String> {
    std::fs::create_dir_all(parts_root).map_err(|error| error.to_string())?;
    let canonical_root = validate_cache_root(parts_root)?;
    for partition in std::fs::read_dir(parts_root).map_err(|error| error.to_string())? {
        let partition = partition.map_err(|error| error.to_string())?;
        let metadata =
            std::fs::symlink_metadata(partition.path()).map_err(|error| error.to_string())?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "refusing symlinked cache partition {}",
                partition.path().display()
            ));
        }
        if !metadata.is_dir() {
            continue;
        }
        ensure_existing_cache_dir(&canonical_root, &partition.path())?;
        for entry in std::fs::read_dir(partition.path()).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let metadata =
                std::fs::symlink_metadata(entry.path()).map_err(|error| error.to_string())?;
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "refusing symlinked cache directory {}",
                    entry.path().display()
                ));
            }
            if metadata.is_dir() {
                ensure_existing_cache_dir(&canonical_root, &entry.path())?;
                for child in std::fs::read_dir(entry.path()).map_err(|error| error.to_string())? {
                    let child = child.map_err(|error| error.to_string())?;
                    let child_metadata = std::fs::symlink_metadata(child.path())
                        .map_err(|error| error.to_string())?;
                    if child_metadata.file_type().is_symlink() {
                        return Err(format!(
                            "refusing symlinked cache file {}",
                            child.path().display()
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn open_manifest_part(
    dir: &Path,
    descriptor: &ManifestPart,
    require_data: bool,
) -> Result<crate::part::PartReader, String> {
    let part = part::load_part(dir)?;
    if part.meta.id != descriptor.id || part.meta.partition != descriptor.partition {
        return Err(format!(
            "cached part metadata does not match manifest descriptor {}/{}",
            descriptor.partition, descriptor.id
        ));
    }
    if require_data {
        crate::part::PartReader::open(part)
    } else {
        crate::part::PartReader::open_cached(part)
    }
}

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
}

impl RemoteCache {
    pub fn new(storage: Arc<ObjectStorage>, parts_root: PathBuf) -> Self {
        Self {
            storage,
            parts_root,
        }
    }
}

impl ObjectStorage {
    pub fn from_url(url: &str) -> Result<Self, String> {
        let url =
            url::Url::parse(url).map_err(|error| format!("invalid object-store URL: {error}"))?;
        let local_manifest_overwrite = url.scheme() == "file";
        let options = normalized_object_store_options(std::env::vars());
        let (store, prefix) = object_store::parse_url_opts(&url, options)
            .map_err(|error| format!("failed to configure object store: {error}"))?;
        Ok(Self {
            store: Arc::from(store),
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

    fn part_path(&self, part: &ManifestPart, file: &str) -> ObjectPath {
        self.path(&format!("parts/{}/{}/{}", part.partition, part.id, file))
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

    /// Publishes local parts that are not yet visible in the manifest.
    /// `upload_part` verifies existing immutable keys, so this also resumes an
    /// upload interrupted before the manifest CAS without accepting unrelated
    /// bytes under the same part ID.
    pub async fn publish_local_only_parts(
        &self,
        local_parts: &[Part],
        manifest: &Manifest,
    ) -> Result<Manifest, String> {
        let active_ids: HashSet<&str> =
            manifest.parts.iter().map(|part| part.id.as_str()).collect();
        let mut unpublished = Vec::new();
        for part in local_parts {
            if active_ids.contains(part.meta.id.as_str()) {
                remove_upload_marker(part)?;
                continue;
            }
            let marker = part.dir.join(UPLOAD_MARKER_FILE);
            if marker.exists() {
                unpublished.push(part.clone());
                continue;
            }

            let descriptor = ManifestPart::from(part);
            let mut remote_files = 0usize;
            for file in PART_FILES {
                match self.store.head(&self.part_path(&descriptor, file)).await {
                    Ok(_) => remote_files += 1,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "failed to inspect remote part {} file {file}: {error}",
                            part.meta.id
                        ));
                    }
                }
            }
            if remote_files == PART_FILES.len() {
                tracing::warn!(
                    part_id = %part.meta.id,
                    "local part is absent from the manifest but has a complete remote object set; preserving it as a stale generation"
                );
                continue;
            }
            write_upload_marker(part)?;
            unpublished.push(part.clone());
        }
        if unpublished.is_empty() {
            Ok(manifest.clone())
        } else {
            let published = self.publish(&unpublished, &[]).await?;
            for part in &unpublished {
                remove_upload_marker(part)?;
            }
            Ok(published)
        }
    }

    /// Uploads immutable part files, then atomically adds/removes their IDs in
    /// the manifest. Uploaded objects that lose a CAS race are harmless and
    /// will be collected by a future retention pass.
    pub async fn publish(
        &self,
        added: &[Part],
        removed_ids: &[String],
    ) -> Result<Manifest, String> {
        // The manifest is the source of truth. Never make a part visible
        // remotely until the exact local files have passed the same read-back
        // validation required by the registry.
        for part in added {
            let id = part.meta.id.clone();
            crate::part::PartReader::open(part.clone())
                .map_err(|error| format!("refusing to publish invalid part {id}: {error}"))?;
        }
        // Persist intent before the first immutable object is written. If the
        // process dies after the last object upload but before the manifest
        // CAS, startup can distinguish this transaction from an inactive old
        // generation and safely finish it.
        for part in added {
            write_upload_marker(part)?;
        }
        for part in added {
            self.upload_part(part).await?;
        }

        let _guard = self.manifest_update.lock().await;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let loaded = self.load_manifest_versioned().await?;
            let mut next = loaded.manifest.clone();
            let removed: HashSet<&str> = removed_ids.iter().map(String::as_str).collect();

            // A CAS retry may observe that another writer already replaced
            // one or more of our merge inputs. Reapplying this replacement
            // would retain both writers' outputs and duplicate every row.
            // Accept only an intact input set, or the exact idempotent state
            // produced by an earlier successful attempt whose response was
            // lost.
            if !removed.is_empty() {
                let present_removed = loaded
                    .manifest
                    .parts
                    .iter()
                    .filter(|part| removed.contains(part.id.as_str()))
                    .count();
                let all_added_present = added.iter().all(|part| {
                    let descriptor = ManifestPart::from(part);
                    loaded
                        .manifest
                        .parts
                        .iter()
                        .any(|existing| existing == &descriptor)
                });
                if present_removed == 0 && all_added_present {
                    remove_upload_markers_best_effort(added);
                    return Ok(loaded.manifest);
                }
                if present_removed != removed.len() {
                    return Err(format!(
                        "manifest replacement conflict: expected {} input parts, found {present_removed}",
                        removed.len()
                    ));
                }
            }
            next.parts
                .retain(|part| !removed.contains(part.id.as_str()));
            for part in added.iter().map(ManifestPart::from) {
                if let Some(existing) = next.parts.iter().find(|item| item.id == part.id) {
                    if existing != &part {
                        return Err(format!("manifest part ID collision: {}", part.id));
                    }
                } else {
                    next.parts.push(part);
                }
            }
            next.parts.sort_by(|left, right| {
                (&left.partition, &left.id).cmp(&(&right.partition, &right.id))
            });
            if next.parts == loaded.manifest.parts {
                remove_upload_markers_best_effort(added);
                return Ok(loaded.manifest);
            }
            next.generation = loaded
                .manifest
                .generation
                .checked_add(1)
                .ok_or_else(|| "manifest generation overflow".to_string())?;
            let body = serde_json::to_vec_pretty(&next)
                .map_err(|error| format!("failed to encode manifest: {error}"))?;
            let mode = match loaded.version {
                Some(_) if self.local_manifest_overwrite => PutMode::Overwrite,
                Some(version) => PutMode::Update(version),
                None => PutMode::Create,
            };
            let options = PutOptions {
                mode,
                ..Default::default()
            };
            match self
                .store
                .put_opts(&self.manifest_path(), body.into(), options)
                .await
            {
                Ok(_) => {
                    remove_upload_markers_best_effort(added);
                    return Ok(next);
                }
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::AlreadyExists { .. }) => continue,
                Err(error) => return Err(format!("failed to update manifest: {error}")),
            }
        }
        Err("manifest compare-and-swap retry limit exceeded".to_string())
    }

    async fn upload_part(&self, part: &Part) -> Result<(), String> {
        let descriptor = ManifestPart::from(part);
        for file in PART_FILES {
            let local_path = part.dir.join(file);
            let metadata = std::fs::symlink_metadata(&local_path).map_err(|error| {
                format!(
                    "failed to inspect part file {}: {error}",
                    local_path.display()
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!(
                    "refusing unsafe part file {}",
                    local_path.display()
                ));
            }
            let bytes = tokio::fs::read(&local_path).await.map_err(|error| {
                format!("failed to read part file {}: {error}", local_path.display())
            })?;
            let options = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match self
                .store
                .put_opts(
                    &self.part_path(&descriptor, file),
                    bytes.clone().into(),
                    options,
                )
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let remote = self
                        .store
                        .get(&self.part_path(&descriptor, file))
                        .await
                        .map_err(|error| {
                            format!(
                                "failed to verify existing part {} file {file}: {error}",
                                part.meta.id
                            )
                        })?
                        .bytes()
                        .await
                        .map_err(|error| {
                            format!(
                                "failed to read existing part {} file {file}: {error}",
                                part.meta.id
                            )
                        })?;
                    if remote.as_ref() != bytes.as_slice() {
                        return Err(format!(
                            "immutable object collision for part {} file {file}",
                            part.meta.id
                        ));
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to upload part {} file {file}: {error}",
                        part.meta.id
                    ));
                }
            }
        }
        Ok(())
    }

    /// Repairs the manifest and local catalog after crashes, including a
    /// local-only to object-store transition. An initially empty manifest is
    /// seeded with the final locally recovered generation in one CAS. Once a
    /// remote generation exists, interrupted merge tombstones are replayed
    /// oldest-first before local cleanup.
    pub async fn reconcile_local_cache(&self, parts_root: &Path) -> Result<Manifest, String> {
        let initial = self.restore_catalog(parts_root).await?;
        validate_cache_tree_no_symlinks(parts_root)?;
        let migrating_from_local_only = initial.generation == 0 && initial.parts.is_empty();

        let tombstoned_active_ids: HashSet<String> = initial
            .parts
            .iter()
            .filter(|descriptor| {
                parts_root
                    .join(&descriptor.partition)
                    .join(&descriptor.id)
                    .join(part::MERGE_TOMBSTONE_FILE)
                    .exists()
            })
            .map(|descriptor| descriptor.id.clone())
            .collect();
        if !tombstoned_active_ids.is_empty() {
            self.restore_parts(parts_root, &tombstoned_active_ids)
                .await?;
        }

        let groups = collect_local_merge_groups(parts_root)?;
        let valid_outputs: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.added.iter().map(|part| part.meta.id.as_str()))
            .collect();
        for active_id in &tombstoned_active_ids {
            if !valid_outputs.contains(active_id.as_str()) {
                return Err(format!(
                    "active manifest part {active_id} has an invalid local merge tombstone replacement"
                ));
            }
        }

        let produced_ids: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.added.iter().map(|part| part.meta.id.as_str()))
            .collect();
        let merge_order = topological_merge_order(&groups)?;
        if migrating_from_local_only {
            // Reject overlapping roots before local cleanup: they describe
            // competing histories whose rows cannot be combined safely.
            let mut root_inputs = HashSet::new();
            for group in &groups {
                let is_root = group
                    .old_ids
                    .iter()
                    .all(|old_id| !produced_ids.contains(old_id.as_str()));
                if !is_root {
                    continue;
                }
                for old_id in &group.old_ids {
                    if !root_inputs.insert(old_id.as_str()) {
                        return Err(format!(
                            "overlapping local-only merge roots contain input part {old_id}"
                        ));
                    }
                }
            }

            // Resolve every local tombstone first, then publish the complete
            // final frontier in one CAS. This handles unbalanced trees such as
            // A+B -> M followed by M+C -> N without needing C to appear in an
            // intermediate remote generation. A crash before the CAS leaves
            // durable local outputs to retry; a crash after it leaves the
            // whole active generation visible.
            let local_parts = part::discover_parts(parts_root)?;
            self.publish_local_only_parts(&local_parts, &initial)
                .await?;
            return self.restore_catalog(parts_root).await;
        }

        for index in merge_order {
            let manifest = self.load_manifest().await?;
            let active: HashSet<&str> =
                manifest.parts.iter().map(|part| part.id.as_str()).collect();
            let active_inputs = groups[index]
                .old_ids
                .iter()
                .filter(|id| active.contains(id.as_str()))
                .count();
            let outputs_active = groups[index].added.iter().all(|part| {
                let descriptor = ManifestPart::from(part);
                manifest.parts.iter().any(|active| active == &descriptor)
            });

            if outputs_active && active_inputs == 0 {
                // The manifest CAS completed before the previous process
                // stopped. The local tombstone is only pending cleanup.
                continue;
            }
            if active_inputs == groups[index].old_ids.len() {
                self.publish(&groups[index].added, &groups[index].old_ids)
                    .await?;
                continue;
            }
            if merge_group_reaches_active_output(index, &groups, &active) {
                // A newer local merge in the same tombstone chain is already
                // active. Replaying this ancestor would temporarily resurrect
                // rows that the active descendant replaced.
                continue;
            }
            return Err(format!(
                "local merge replacement conflict: expected {} active input parts, found {active_inputs}",
                groups[index].old_ids.len()
            ));
        }

        // The manifest now durably describes every valid merge transaction,
        // so it is safe for the existing tombstone recovery to remove old
        // local generations and their markers.
        let local_parts = part::discover_parts(parts_root)?;
        let manifest = self.load_manifest().await?;
        self.publish_local_only_parts(&local_parts, &manifest)
            .await?;
        self.restore_catalog(parts_root).await
    }

    /// Restores only the small metadata/index catalog needed to plan queries.
    /// Parquet bodies remain remote until a query or merge selects them.
    pub async fn restore_catalog(&self, parts_root: &Path) -> Result<Manifest, String> {
        let manifest = self.load_manifest().await?;
        tokio::fs::create_dir_all(parts_root)
            .await
            .map_err(|error| error.to_string())?;
        validate_cache_root(parts_root)?;
        for descriptor in &manifest.parts {
            let final_dir = cache_part_dir(parts_root, descriptor)?;
            if open_manifest_part(&final_dir, descriptor, false).is_ok() {
                continue;
            }
            self.download_part(descriptor, parts_root, false).await?;
        }
        Ok(manifest)
    }

    /// Restores the Parquet bodies for a selected set of parts. The caller
    /// must hold the cache's exclusive operation lock.
    pub async fn restore_parts(
        &self,
        parts_root: &Path,
        ids: &HashSet<String>,
    ) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let manifest = self.load_manifest().await?;
        let mut restored = HashSet::new();
        for descriptor in &manifest.parts {
            if !ids.contains(&descriptor.id) {
                continue;
            }
            let final_dir = cache_part_dir(parts_root, descriptor)?;
            if open_manifest_part(&final_dir, descriptor, true).is_err() {
                self.download_part(descriptor, parts_root, true).await?;
            }
            restored.insert(descriptor.id.clone());
        }
        let missing: Vec<_> = ids.difference(&restored).cloned().collect();
        if !missing.is_empty() {
            return Err(format!(
                "parts are no longer present in the object-store manifest: {}",
                missing.join(", ")
            ));
        }
        Ok(())
    }

    async fn download_part(
        &self,
        descriptor: &ManifestPart,
        parts_root: &Path,
        include_data: bool,
    ) -> Result<(), String> {
        let final_dir = cache_part_dir(parts_root, descriptor)?;
        let local_tombstone = std::fs::read(final_dir.join(part::MERGE_TOMBSTONE_FILE)).ok();
        let tmp_dir = ensure_safe_directory_chain(
            parts_root,
            &[".tmp", "remote", &descriptor.partition, &descriptor.id],
        )?;
        std::fs::remove_dir_all(&tmp_dir).map_err(|error| error.to_string())?;
        std::fs::create_dir(&tmp_dir).map_err(|error| error.to_string())?;
        let files: &[&str] = if include_data {
            &PART_FILES
        } else {
            &CATALOG_FILES
        };
        for &file in files {
            let result = self
                .store
                .get(&self.part_path(descriptor, file))
                .await
                .map_err(|error| {
                    format!(
                        "failed to download part {} file {file}: {error}",
                        descriptor.id
                    )
                })?;
            let bytes = result.bytes().await.map_err(|error| {
                format!("failed to read part {} file {file}: {error}", descriptor.id)
            })?;
            tokio::fs::write(tmp_dir.join(file), bytes)
                .await
                .map_err(|error| {
                    format!(
                        "failed to cache part {} file {file}: {error}",
                        descriptor.id
                    )
                })?;
        }
        let downloaded =
            open_manifest_part(&tmp_dir, descriptor, include_data).map_err(|error| {
                format!(
                    "downloaded part {} failed validation: {error}",
                    descriptor.id
                )
            })?;
        drop(downloaded);

        if let Some(tombstone) = local_tombstone {
            let marker = tmp_dir.join(part::MERGE_TOMBSTONE_FILE);
            std::fs::write(&marker, tombstone).map_err(|error| error.to_string())?;
            std::fs::File::open(&marker)
                .and_then(|file| file.sync_all())
                .map_err(|error| error.to_string())?;
            part::fsync_dir(&tmp_dir).map_err(|error| error.to_string())?;
        }

        std::fs::remove_dir_all(&final_dir).map_err(|error| error.to_string())?;
        std::fs::rename(&tmp_dir, &final_dir)
            .map_err(|error| format!("failed to commit cached part {}: {error}", descriptor.id))?;
        Ok(())
    }

    pub fn evict_cache(
        &self,
        parts_root: &Path,
        max_bytes: u64,
        eligible_part_ids: &HashSet<String>,
    ) -> Result<u64, String> {
        let mut cached = Vec::new();
        let mut total = 0u64;
        let canonical_root = match std::fs::symlink_metadata(parts_root) {
            Ok(_) => validate_cache_root(parts_root)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.to_string()),
        };
        let partitions = match std::fs::read_dir(parts_root) {
            Ok(entries) => entries,
            Err(error) => return Err(error.to_string()),
        };
        for partition in partitions {
            let partition = partition.map_err(|error| error.to_string())?;
            if partition.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let partition_metadata =
                std::fs::symlink_metadata(partition.path()).map_err(|error| error.to_string())?;
            if partition_metadata.file_type().is_symlink() {
                return Err(format!(
                    "refusing symlinked cache partition {}",
                    partition.path().display()
                ));
            }
            if !partition_metadata.is_dir() {
                continue;
            }
            ensure_existing_cache_dir(&canonical_root, &partition.path())?;
            for entry in std::fs::read_dir(partition.path()).map_err(|error| error.to_string())? {
                let entry = entry.map_err(|error| error.to_string())?;
                let entry_metadata =
                    std::fs::symlink_metadata(entry.path()).map_err(|error| error.to_string())?;
                if entry_metadata.file_type().is_symlink() {
                    return Err(format!(
                        "refusing symlinked cache part {}",
                        entry.path().display()
                    ));
                }
                if !entry_metadata.is_dir() {
                    continue;
                }
                ensure_existing_cache_dir(&canonical_root, &entry.path())?;
                let id = entry.file_name().to_string_lossy().into_owned();
                if !eligible_part_ids.contains(&id) {
                    // Absence from the current registry does not prove that a
                    // directory is disposable. It may be local-only data from
                    // a period when object storage was disabled, or an
                    // interrupted transaction requiring operator recovery.
                    // Cache eviction only removes bodies belonging to active
                    // manifest parts; retention can clean confirmed stale
                    // generations separately.
                    continue;
                }
                // The bound applies to evictable Parquet bodies. Metadata,
                // bloom filters, and stream indexes form the small persistent
                // catalog required to plan selective restores.
                let data_path = entry.path().join(DATA_FILE);
                let bytes = match std::fs::symlink_metadata(&data_path) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(format!(
                            "refusing symlinked cache data file {}",
                            data_path.display()
                        ));
                    }
                    Ok(metadata) if metadata.is_file() => metadata.len(),
                    Ok(_) => {
                        return Err(format!(
                            "cache data path is not a file: {}",
                            data_path.display()
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                    Err(error) => return Err(error.to_string()),
                };
                let access_path = entry.path().join(".access");
                let accessed = match std::fs::symlink_metadata(&access_path) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(format!(
                            "refusing symlinked cache access marker {}",
                            access_path.display()
                        ));
                    }
                    Ok(metadata) => metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        entry_metadata.modified().unwrap_or(std::time::UNIX_EPOCH)
                    }
                    Err(error) => return Err(error.to_string()),
                };
                total = total.saturating_add(bytes);
                if bytes > 0 {
                    cached.push((accessed, bytes, entry.path()));
                }
            }
        }
        cached.sort_by_key(|(accessed, _, _)| *accessed);
        for (_, bytes, dir) in cached {
            if total <= max_bytes {
                break;
            }
            let data_path = dir.join(DATA_FILE);
            match std::fs::remove_file(&data_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(format!(
                        "failed to evict cached data file {}: {error}",
                        data_path.display()
                    ));
                }
            }
            let _ = std::fs::remove_file(dir.join(".access"));
            part::fsync_dir(&dir).map_err(|error| error.to_string())?;
            total = total.saturating_sub(bytes);
        }
        Ok(total)
    }
}

fn write_upload_marker(part: &Part) -> Result<(), String> {
    let marker = part.dir.join(UPLOAD_MARKER_FILE);
    for _ in 0..3 {
        match std::fs::symlink_metadata(&marker) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "refusing symlinked upload marker {}",
                    marker.display()
                ));
            }
            Ok(metadata) if metadata.is_file() => {
                return part::fsync_dir(&part.dir).map_err(|error| error.to_string());
            }
            Ok(_) => {
                return Err(format!(
                    "upload marker path is not a file: {}",
                    marker.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&marker)
                {
                    Ok(file) => {
                        file.sync_all().map_err(|error| error.to_string())?;
                        return part::fsync_dir(&part.dir).map_err(|error| error.to_string());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err(format!(
        "upload marker changed repeatedly while creating {}",
        marker.display()
    ))
}

fn remove_upload_marker(part: &Part) -> Result<(), String> {
    let marker = part.dir.join(UPLOAD_MARKER_FILE);
    match std::fs::symlink_metadata(&marker) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "refusing symlinked upload marker {}",
                marker.display()
            ));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(format!(
                "upload marker path is not a file: {}",
                marker.display()
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.to_string()),
    }
    match std::fs::remove_file(&marker) {
        Ok(()) => part::fsync_dir(&part.dir).map_err(|error| error.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn remove_upload_markers_best_effort(parts: &[Part]) {
    for part in parts {
        if let Err(error) = remove_upload_marker(part) {
            // The manifest is already committed. Reporting publication as a
            // failure here would cause callers to retry durable rows. Startup
            // can safely remove a leftover marker from an active part.
            tracing::warn!(%error, part_id = %part.meta.id, "failed to remove committed upload marker");
        }
    }
}

fn collect_local_merge_groups(parts_root: &Path) -> Result<Vec<LocalMergeGroup>, String> {
    let mut candidates: BTreeMap<Vec<String>, Vec<PathBuf>> = BTreeMap::new();
    let partitions = match std::fs::read_dir(parts_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.to_string()),
    };
    for partition in partitions {
        let partition = partition.map_err(|error| error.to_string())?;
        if !partition.path().is_dir() || partition.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        for entry in std::fs::read_dir(partition.path()).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let dir = entry.path();
            if !dir.is_dir() || !dir.join(part::MERGE_TOMBSTONE_FILE).exists() {
                continue;
            }
            let old_dirs = match part::read_merge_tombstone_dirs(&dir, parts_root) {
                Ok(old_dirs) => old_dirs,
                Err(error) => {
                    tracing::warn!(%error, ?dir, "ignoring malformed local merge tombstone");
                    continue;
                }
            };
            let mut old_ids: Vec<String> = old_dirs
                .iter()
                .map(|old_dir| {
                    old_dir
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            format!("invalid merge input directory {}", old_dir.display())
                        })
                })
                .collect::<Result<_, _>>()?;
            old_ids.sort();
            old_ids.dedup();
            if old_ids.is_empty() {
                tracing::warn!(?dir, "ignoring merge tombstone without inputs");
                continue;
            }
            candidates.entry(old_ids).or_default().push(dir);
        }
    }

    let mut groups = Vec::new();
    for (old_ids, dirs) in candidates {
        // Current merges operate within one day partition and therefore emit
        // one replacement part. Two directories naming the same input set
        // are competing attempts, not parts of one transaction; selecting or
        // combining them would duplicate rows.
        if dirs.len() != 1 {
            return Err(format!(
                "multiple local merge replacements name the same inputs: {}",
                old_ids.join(", ")
            ));
        }
        let dir = &dirs[0];
        let replacement = match part::load_part(dir).and_then(crate::part::PartReader::open) {
            Ok(reader) => reader.part().clone(),
            Err(error) => {
                tracing::warn!(%error, ?dir, "local merge replacement is invalid; retaining inputs");
                continue;
            }
        };
        groups.push(LocalMergeGroup {
            old_ids,
            added: vec![replacement],
        });
    }
    Ok(groups)
}

fn topological_merge_order(groups: &[LocalMergeGroup]) -> Result<Vec<usize>, String> {
    let mut producer = HashMap::new();
    for (index, group) in groups.iter().enumerate() {
        for part in &group.added {
            if producer.insert(part.meta.id.as_str(), index).is_some() {
                return Err(format!(
                    "duplicate local merge replacement ID {}",
                    part.meta.id
                ));
            }
        }
    }

    let mut indegree = vec![0usize; groups.len()];
    let mut dependents = vec![Vec::new(); groups.len()];
    for (index, group) in groups.iter().enumerate() {
        let mut parents = HashSet::new();
        for old_id in &group.old_ids {
            if let Some(&parent) = producer.get(old_id.as_str())
                && parents.insert(parent)
            {
                indegree[index] += 1;
                dependents[parent].push(index);
            }
        }
    }

    let mut ready: VecDeque<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, &degree)| (degree == 0).then_some(index))
        .collect();
    let mut ordered = Vec::with_capacity(groups.len());
    while let Some(index) = ready.pop_front() {
        ordered.push(index);
        for &dependent in &dependents[index] {
            indegree[dependent] -= 1;
            if indegree[dependent] == 0 {
                ready.push_back(dependent);
            }
        }
    }
    if ordered.len() != groups.len() {
        return Err("local merge tombstones contain a cycle".to_string());
    }
    Ok(ordered)
}

fn merge_group_reaches_active_output(
    start: usize,
    groups: &[LocalMergeGroup],
    active_ids: &HashSet<&str>,
) -> bool {
    let mut pending: VecDeque<&str> = groups[start]
        .added
        .iter()
        .map(|part| part.meta.id.as_str())
        .collect();
    let mut visited = HashSet::new();

    while let Some(id) = pending.pop_front() {
        if !visited.insert(id) {
            continue;
        }
        if active_ids.contains(id) {
            return true;
        }
        for group in groups {
            if group.old_ids.iter().any(|old_id| old_id == id) {
                pending.extend(group.added.iter().map(|part| part.meta.id.as_str()));
            }
        }
    }
    false
}

fn validate_manifest(manifest: &Manifest) -> Result<(), String> {
    let mut ids = HashSet::new();
    for part in &manifest.parts {
        if !is_safe_path_component(&part.id)
            || !is_safe_path_component(&part.partition)
            || !ids.insert(part.id.as_str())
        {
            return Err("manifest contains an invalid or duplicate part".to_string());
        }
    }
    Ok(())
}

fn is_safe_path_component(value: &str) -> bool {
    let mut components = Path::new(value).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Labels;
    use crate::part::Row;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "loggytracy-object-store-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn row(line: &str) -> Row {
        let labels: Labels = [("app".to_string(), "remote".to_string())]
            .into_iter()
            .collect();
        Row {
            timestamp_ns: 1_700_000_000_000_000_000,
            labels,
            line: line.to_string(),
            structured_metadata: Vec::new(),
        }
    }

    #[test]
    fn object_store_environment_keys_are_normalized_and_explicit_values_win() {
        let options: BTreeMap<_, _> = normalized_object_store_options([
            ("AWS_ACCESS_KEY_ID".to_string(), "aws-key".to_string()),
            ("AWS_REGION".to_string(), "ap-northeast-2".to_string()),
            (
                "OBJECT_STORE_AWS_ACCESS_KEY_ID".to_string(),
                "explicit-key".to_string(),
            ),
            (
                "OBJECT_STORE_ENDPOINT".to_string(),
                "http://minio:9000".to_string(),
            ),
            ("UNRELATED".to_string(), "ignored".to_string()),
        ])
        .into_iter()
        .collect();

        assert_eq!(options.get("aws_access_key_id").unwrap(), "explicit-key");
        assert_eq!(options.get("aws_region").unwrap(), "ap-northeast-2");
        assert_eq!(options.get("endpoint").unwrap(), "http://minio:9000");
        assert!(!options.contains_key("unrelated"));
    }

    #[tokio::test]
    async fn restores_queryable_part_after_cache_is_deleted() {
        let storage = ObjectStorage::in_memory();
        let source = temp_dir("source").join("parts");
        let parts = part::flush_rows(vec![row("from object store")], &source, 100).unwrap();
        storage.publish(&parts, &[]).await.unwrap();

        std::fs::remove_dir_all(&source).unwrap();
        let manifest = storage.restore_catalog(&source).await.unwrap();
        assert!(
            !source
                .join(&manifest.parts[0].partition)
                .join(&manifest.parts[0].id)
                .join(DATA_FILE)
                .exists()
        );
        let ids = manifest.parts.iter().map(|part| part.id.clone()).collect();
        storage.restore_parts(&source, &ids).await.unwrap();
        let registry =
            crate::part_registry::PartRegistry::load_from_manifest(&source, &manifest).unwrap();
        let result = registry
            .query(&[], &[], i64::MIN, i64::MAX, 10, true)
            .unwrap();
        assert_eq!(result[0].entries[0].line, "from object store");
    }

    #[tokio::test]
    async fn manifest_replace_is_atomic_and_increments_generation() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("replace").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        let first = storage.publish(&old, &[]).await.unwrap();
        assert_eq!(first.generation, 1);

        let old_id = old[0].meta.id.clone();
        let mut replacement_row = row("new");
        replacement_row.timestamp_ns += 1;
        let new = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        let second = storage.publish(&new, &[old_id]).await.unwrap();
        assert_eq!(second.generation, 2);
        assert_eq!(second.parts.len(), 1);
        assert_eq!(second.parts[0].id, new[0].meta.id);
    }

    #[tokio::test]
    async fn competing_replacement_cannot_publish_duplicate_rows() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("replacement-conflict").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut first_row = row("first replacement");
        first_row.timestamp_ns += 1;
        let first = part::flush_rows(vec![first_row], &root, 100).unwrap();
        storage
            .publish(&first, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        // Repeating the exact operation is idempotent, but a different output
        // for the already-consumed input generation is a conflict.
        storage
            .publish(&first, std::slice::from_ref(&old_id))
            .await
            .unwrap();
        let mut second_row = row("second replacement");
        second_row.timestamp_ns += 2;
        let second = part::flush_rows(vec![second_row], &root, 100).unwrap();
        let error = storage
            .publish(&second, std::slice::from_ref(&old_id))
            .await
            .unwrap_err();
        assert!(error.contains("manifest replacement conflict"));

        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, first[0].meta.id);
    }

    #[tokio::test]
    async fn reconciliation_publishes_local_only_parts_without_resurrecting_stale_parts() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut replacement_row = row("replacement");
        replacement_row.timestamp_ns += 1;
        let replacement = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        storage
            .publish(&replacement, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        let mut local_row = row("written while object store was disabled");
        local_row.timestamp_ns += 2;
        let local_only = part::flush_rows(vec![local_row], &root, 100).unwrap();
        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();

        let ids: HashSet<_> = reconciled
            .parts
            .iter()
            .map(|part| part.id.as_str())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(replacement[0].meta.id.as_str()));
        assert!(ids.contains(local_only[0].meta.id.as_str()));
        assert!(!ids.contains(old_id.as_str()));
    }

    #[tokio::test]
    async fn reconciliation_finishes_an_interrupted_merge_replacement() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-uncommitted-merge").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_dirs: Vec<_> = old.iter().map(|part| part.dir.clone()).collect();

        let mut replacement_row = row("uncommitted replacement");
        replacement_row.timestamp_ns += 1;
        let replacement =
            part::flush_rows_with_merge_tombstone(vec![replacement_row], &root, 100, &old_dirs)
                .unwrap();
        assert!(replacement[0].dir.join(part::MERGE_TOMBSTONE_FILE).exists());

        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(reconciled.parts.len(), 1);
        assert_eq!(reconciled.parts[0].id, replacement[0].meta.id);
        assert!(!old[0].dir.exists());
        assert!(!replacement[0].dir.join(part::MERGE_TOMBSTONE_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_a_local_only_merge_into_an_empty_manifest() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-local-only-merge").join("parts");
        let old = part::flush_rows(vec![row("old local")], &root, 100).unwrap();
        let mut replacement_row = row("merged while local only");
        replacement_row.timestamp_ns += 1;
        let replacement = part::flush_rows_with_merge_tombstone(
            vec![replacement_row],
            &root,
            100,
            &[old[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, replacement[0].meta.id);
        assert!(!old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_an_unbalanced_local_only_merge_tree() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-unbalanced-local-merge").join("parts");
        let first = part::flush_rows(vec![row("first")], &root, 100).unwrap();
        let second = part::flush_rows(vec![row("second")], &root, 100).unwrap();
        let third = part::flush_rows(vec![row("third")], &root, 100).unwrap();
        let middle = part::flush_rows_with_merge_tombstone(
            vec![row("middle")],
            &root,
            100,
            &[first[0].dir.clone(), second[0].dir.clone()],
        )
        .unwrap();
        let newest = part::flush_rows_with_merge_tombstone(
            vec![row("newest")],
            &root,
            100,
            &[middle[0].dir.clone(), third[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.generation, 1);
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, newest[0].meta.id);
        assert!(!first[0].dir.exists());
        assert!(!second[0].dir.exists());
        assert!(!third[0].dir.exists());
        assert!(!middle[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_migrates_independent_local_only_merge_trees() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-independent-local-merges").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();

        let first = part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone()],
        )
        .unwrap();
        let second = part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[second_old[0].dir.clone()],
        )
        .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        let active: HashSet<_> = manifest.parts.iter().map(|part| part.id.as_str()).collect();
        assert_eq!(
            manifest.generation, 1,
            "all roots must be seeded by one CAS"
        );
        assert_eq!(active.len(), 2);
        assert!(active.contains(first[0].meta.id.as_str()));
        assert!(active.contains(second[0].meta.id.as_str()));
        assert!(!first_old[0].dir.exists());
        assert!(!second_old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_after_atomic_local_only_root_seed() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-after-root-seed").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();
        let first = part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone()],
        )
        .unwrap();
        let second = part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[second_old[0].dir.clone()],
        )
        .unwrap();

        // State after the atomic root manifest CAS but before local tombstone
        // cleanup, equivalent to a crash at that reconciliation boundary.
        let seeded = storage
            .publish(&[first[0].clone(), second[0].clone()], &[])
            .await
            .unwrap();
        assert_eq!(seeded.generation, 1);
        assert!(first_old[0].dir.exists());
        assert!(second_old[0].dir.exists());

        let reconciled = storage.reconcile_local_cache(&root).await.unwrap();
        let active: HashSet<_> = reconciled
            .parts
            .iter()
            .map(|part| part.id.as_str())
            .collect();
        assert_eq!(active.len(), 2);
        assert!(active.contains(first[0].meta.id.as_str()));
        assert!(active.contains(second[0].meta.id.as_str()));
        assert!(!first_old[0].dir.exists());
        assert!(!second_old[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_rejects_overlapping_local_only_merge_roots_before_publish() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-overlapping-local-merges").join("parts");
        let first_old = part::flush_rows(vec![row("first old")], &root, 100).unwrap();
        let shared_old = part::flush_rows(vec![row("shared old")], &root, 100).unwrap();
        let second_old = part::flush_rows(vec![row("second old")], &root, 100).unwrap();

        part::flush_rows_with_merge_tombstone(
            vec![row("first replacement")],
            &root,
            100,
            &[first_old[0].dir.clone(), shared_old[0].dir.clone()],
        )
        .unwrap();
        part::flush_rows_with_merge_tombstone(
            vec![row("second replacement")],
            &root,
            100,
            &[shared_old[0].dir.clone(), second_old[0].dir.clone()],
        )
        .unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("overlapping local-only merge roots"));
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[tokio::test]
    async fn reconciliation_rejects_a_stale_competing_merge_replacement() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-competing-merge").join("parts");
        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        storage.publish(&old, &[]).await.unwrap();
        let old_id = old[0].meta.id.clone();

        let mut winner_row = row("remote winner");
        winner_row.timestamp_ns += 1;
        let winner = part::flush_rows(vec![winner_row], &root, 100).unwrap();
        storage
            .publish(&winner, std::slice::from_ref(&old_id))
            .await
            .unwrap();

        let mut loser_row = row("stale local loser");
        loser_row.timestamp_ns += 2;
        let old_dirs = vec![old[0].dir.clone()];
        let loser =
            part::flush_rows_with_merge_tombstone(vec![loser_row], &root, 100, &old_dirs).unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("local merge replacement conflict"));

        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, winner[0].meta.id);
        assert!(loser[0].dir.exists(), "ambiguous local data was deleted");
    }

    #[tokio::test]
    async fn reconciliation_accepts_an_already_active_merge_chain_descendant() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-active-descendant").join("parts");
        let oldest = part::flush_rows(vec![row("oldest")], &root, 100).unwrap();
        storage.publish(&oldest, &[]).await.unwrap();

        let mut middle_row = row("middle");
        middle_row.timestamp_ns += 1;
        let middle = part::flush_rows_with_merge_tombstone(
            vec![middle_row],
            &root,
            100,
            &[oldest[0].dir.clone()],
        )
        .unwrap();
        storage
            .publish(&middle, &[oldest[0].meta.id.clone()])
            .await
            .unwrap();

        let mut newest_row = row("newest");
        newest_row.timestamp_ns += 2;
        let newest = part::flush_rows_with_merge_tombstone(
            vec![newest_row],
            &root,
            100,
            &[middle[0].dir.clone()],
        )
        .unwrap();
        storage
            .publish(&newest, &[middle[0].meta.id.clone()])
            .await
            .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, newest[0].meta.id);
        assert!(!oldest[0].dir.exists());
        assert!(!middle[0].dir.exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_a_partial_local_part_upload() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-partial-upload").join("parts");
        let local = part::flush_rows(vec![row("partial upload")], &root, 100).unwrap();
        let descriptor = ManifestPart::from(&local[0]);
        let data = tokio::fs::read(local[0].data_path()).await.unwrap();
        storage
            .store
            .put_opts(
                &storage.part_path(&descriptor, DATA_FILE),
                data.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();

        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, local[0].meta.id);
        assert!(!local[0].dir.join(UPLOAD_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_resumes_a_fully_uploaded_uncommitted_part() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-complete-upload").join("parts");
        let local = part::flush_rows(vec![row("uploaded before crash")], &root, 100).unwrap();

        // State after upload_part completed but before the manifest CAS.
        write_upload_marker(&local[0]).unwrap();
        storage.upload_part(&local[0]).await.unwrap();
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        assert_eq!(manifest.parts.len(), 1);
        assert_eq!(manifest.parts[0].id, local[0].meta.id);
        assert!(!local[0].dir.join(UPLOAD_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn reconciliation_repairs_corrupt_active_catalog_before_local_scan() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("reconcile-corrupt-catalog").join("parts");
        let local = part::flush_rows(vec![row("repair catalog")], &root, 100).unwrap();
        storage.publish(&local, &[]).await.unwrap();
        std::fs::write(local[0].meta_path(), b"not json").unwrap();

        let manifest = storage.reconcile_local_cache(&root).await.unwrap();
        let registry =
            crate::part_registry::PartRegistry::load_from_manifest(&root, &manifest).unwrap();

        assert_eq!(registry.part_count(), 1);
    }

    #[tokio::test]
    async fn publish_rejects_different_bytes_under_an_existing_part_key() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("immutable-collision").join("parts");
        let local = part::flush_rows(vec![row("original")], &root, 100).unwrap();
        storage.publish(&local, &[]).await.unwrap();
        let descriptor = ManifestPart::from(&local[0]);
        storage
            .store
            .put_opts(
                &storage.part_path(&descriptor, DATA_FILE),
                bytes::Bytes::from_static(b"different").into(),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let error = storage.publish(&local, &[]).await.unwrap_err();

        assert!(error.contains("immutable object collision"));
        assert!(
            local[0].dir.join(UPLOAD_MARKER_FILE).exists(),
            "publication intent must be durable before object upload"
        );
    }

    #[tokio::test]
    async fn file_backend_can_update_an_existing_manifest() {
        let remote = temp_dir("file-backend");
        let url = url::Url::from_directory_path(&remote).unwrap();
        let storage = ObjectStorage::from_url(url.as_str()).unwrap();
        let root = temp_dir("file-backend-parts").join("parts");

        let old = part::flush_rows(vec![row("old")], &root, 100).unwrap();
        assert_eq!(storage.publish(&old, &[]).await.unwrap().generation, 1);

        let old_id = old[0].meta.id.clone();
        let mut replacement_row = row("new");
        replacement_row.timestamp_ns += 1;
        let new = part::flush_rows(vec![replacement_row], &root, 100).unwrap();
        let manifest = storage.publish(&new, &[old_id]).await.unwrap();

        assert_eq!(manifest.generation, 2);
        assert_eq!(storage.load_manifest().await.unwrap().generation, 2);
        assert_eq!(manifest.parts[0].id, new[0].meta.id);
    }

    #[tokio::test]
    async fn invalid_part_is_rejected_before_manifest_update() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("invalid-publish").join("parts");
        let parts = part::flush_rows(vec![row("corrupt me")], &root, 100).unwrap();
        std::fs::write(parts[0].data_path(), b"not parquet").unwrap();

        let error = storage.publish(&parts, &[]).await.unwrap_err();

        assert!(error.contains("refusing to publish invalid part"));
        let manifest = storage.load_manifest().await.unwrap();
        assert_eq!(manifest.generation, 0);
        assert!(manifest.parts.is_empty());
    }

    #[test]
    fn eviction_respects_cache_limit() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("evict").join("parts");
        let parts = part::flush_rows(vec![row("evict me")], &root, 100).unwrap();
        let eligible = [parts[0].meta.id.clone()].into_iter().collect();
        assert_eq!(storage.evict_cache(&root, 0, &eligible).unwrap(), 0);
        assert!(!parts[0].data_path().exists());
        assert!(parts[0].meta_path().exists());
        assert!(parts[0].bloom_path().exists());
    }

    #[test]
    fn eviction_preserves_parts_absent_from_the_registry() {
        let storage = ObjectStorage::in_memory();
        let root = temp_dir("evict-stale").join("parts");
        let active = part::flush_rows(vec![row("active")], &root, 100).unwrap();
        let mut stale_row = row("stale");
        stale_row.timestamp_ns += 1;
        let stale = part::flush_rows(vec![stale_row], &root, 100).unwrap();
        let active_bytes = std::fs::metadata(active[0].data_path()).unwrap().len();
        let eligible = [active[0].meta.id.clone()].into_iter().collect();

        assert_eq!(
            storage.evict_cache(&root, u64::MAX, &eligible).unwrap(),
            active_bytes
        );
        assert!(active[0].dir.exists());
        assert!(stale[0].dir.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cache_restore_and_eviction_reject_symlinked_partitions() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let source = temp_dir("symlink-source").join("parts");
        let parts = part::flush_rows(vec![row("remote")], &source, 100).unwrap();
        let manifest = storage.publish(&parts, &[]).await.unwrap();
        let descriptor = &manifest.parts[0];

        let cache = temp_dir("symlink-cache").join("parts");
        std::fs::create_dir_all(&cache).unwrap();
        let outside = temp_dir("symlink-outside");
        let outside_part = outside.join(&descriptor.id);
        std::fs::create_dir_all(&outside_part).unwrap();
        let outside_data = outside_part.join(DATA_FILE);
        std::fs::write(&outside_data, b"must survive").unwrap();
        symlink(&outside, cache.join(&descriptor.partition)).unwrap();

        let restore_error = storage.restore_catalog(&cache).await.unwrap_err();
        assert!(restore_error.contains("symlinked cache directory"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        let eligible = [descriptor.id.clone()].into_iter().collect();
        let eviction_error = storage.evict_cache(&cache, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("symlinked cache partition"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        let empty_storage = ObjectStorage::in_memory();
        let migration_error = empty_storage
            .reconcile_local_cache(&cache)
            .await
            .unwrap_err();
        assert!(migration_error.contains("symlinked cache partition"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");

        std::fs::remove_file(cache.join(&descriptor.partition)).unwrap();
        let cache_part = cache.join(&descriptor.partition).join(&descriptor.id);
        std::fs::create_dir_all(&cache_part).unwrap();
        symlink(&outside_data, cache_part.join(DATA_FILE)).unwrap();
        let file_error = storage.restore_catalog(&cache).await.unwrap_err();
        assert!(file_error.contains("symlinked cache file"));
        let eviction_error = storage.evict_cache(&cache, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("symlinked cache data file"));
        assert_eq!(std::fs::read(&outside_data).unwrap(), b"must survive");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconciliation_rejects_a_symlinked_upload_marker() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("symlink-upload-marker").join("parts");
        let parts = part::flush_rows(vec![row("local")], &root, 100).unwrap();
        let outside = temp_dir("symlink-marker-target").join("outside.txt");
        std::fs::write(&outside, b"must survive").unwrap();
        symlink(&outside, parts[0].dir.join(UPLOAD_MARKER_FILE)).unwrap();

        let error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(error.contains("symlinked cache file"));
        let publish_error = storage.publish(&parts, &[]).await.unwrap_err();
        assert!(publish_error.contains("symlinked upload marker"));
        assert_eq!(std::fs::read(&outside).unwrap(), b"must survive");
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn migration_and_upload_reject_symlinked_immutable_part_files() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let root = temp_dir("symlink-immutable-part").join("parts");
        let parts = part::flush_rows(vec![row("local")], &root, 100).unwrap();
        let outside = temp_dir("symlink-immutable-target").join(DATA_FILE);
        std::fs::rename(parts[0].data_path(), &outside).unwrap();
        let expected = std::fs::read(&outside).unwrap();
        symlink(&outside, parts[0].data_path()).unwrap();

        let migration_error = storage.reconcile_local_cache(&root).await.unwrap_err();
        assert!(migration_error.contains("symlinked cache file"));
        assert_eq!(std::fs::read(&outside).unwrap(), expected);
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());

        let upload_error = storage.publish(&parts, &[]).await.unwrap_err();
        assert!(upload_error.contains("unsafe part file"));
        assert_eq!(std::fs::read(&outside).unwrap(), expected);
        assert!(storage.load_manifest().await.unwrap().parts.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restore_reconcile_and_eviction_reject_a_symlinked_cache_root() {
        use std::os::unix::fs::symlink;

        let storage = ObjectStorage::in_memory();
        let outside_root = temp_dir("symlink-cache-root-outside").join("parts");
        let parts = part::flush_rows(vec![row("outside")], &outside_root, 100).unwrap();
        storage.publish(&parts, &[]).await.unwrap();
        let outside_data = parts[0].data_path();
        let link_parent = temp_dir("symlink-cache-root-link");
        let cache_link = link_parent.join("parts");
        symlink(&outside_root, &cache_link).unwrap();

        let restore_error = storage.restore_catalog(&cache_link).await.unwrap_err();
        assert!(restore_error.contains("unsafe cache root"));
        assert!(outside_data.exists());

        let eligible = [parts[0].meta.id.clone()].into_iter().collect();
        let eviction_error = storage.evict_cache(&cache_link, 0, &eligible).unwrap_err();
        assert!(eviction_error.contains("unsafe cache root"));
        assert!(outside_data.exists());

        let empty_storage = ObjectStorage::in_memory();
        let reconcile_error = empty_storage
            .reconcile_local_cache(&cache_link)
            .await
            .unwrap_err();
        assert!(reconcile_error.contains("unsafe cache root"));
        assert!(outside_data.exists());
    }

    #[test]
    fn manifest_rejects_relative_path_components() {
        for value in [".", "..", "a/b", ""] {
            let manifest = Manifest {
                format_version: MANIFEST_FORMAT_VERSION,
                generation: 1,
                parts: vec![ManifestPart {
                    id: value.to_string(),
                    partition: "2026-01-01".to_string(),
                }],
            };
            assert!(validate_manifest(&manifest).is_err(), "accepted {value:?}");
        }
    }
}
