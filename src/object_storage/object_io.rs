impl ObjectStorage {
    async fn upload_trace_part(&self, part: &TracePart) -> Result<(), String> {
        let descriptor = TraceManifestPart {
            id: part.meta.id.clone(),
            partition: part.meta.partition.clone(),
        };
        for file in TRACE_PART_FILES {
            let local_path = part.dir.join(file);
            let metadata = std::fs::symlink_metadata(&local_path).map_err(|error| {
                format!(
                    "failed to inspect trace part file {}: {error}",
                    local_path.display()
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!(
                    "refusing unsafe trace part file {}",
                    local_path.display()
                ));
            }
            let bytes = tokio::fs::read(&local_path).await.map_err(|error| {
                format!(
                    "failed to read trace part file {}: {error}",
                    local_path.display()
                )
            })?;
            match self
                .store
                .put_opts(
                    &self.trace_part_path(&descriptor, file),
                    bytes.clone().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let remote = self
                        .store
                        .get(&self.trace_part_path(&descriptor, file))
                        .await
                        .map_err(|error| {
                            format!("failed to verify existing trace object: {error}")
                        })?
                        .bytes()
                        .await
                        .map_err(|error| {
                            format!("failed to read existing trace object: {error}")
                        })?;
                    if remote.as_ref() != bytes.as_slice() {
                        return Err(format!(
                            "immutable object collision for trace part {} file {file}",
                            part.meta.id
                        ));
                    }
                }
                Err(error) => {
                    return Err(format!(
                        "failed to upload trace part {} file {file}: {error}",
                        part.meta.id
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn restore_trace_catalog(&self, traces_root: &Path) -> Result<TraceManifest, String> {
        let manifest = self.load_trace_manifest().await?;
        std::fs::create_dir_all(traces_root).map_err(|error| error.to_string())?;
        for descriptor in &manifest.parts {
            let dir = trace_cache_part_dir(traces_root, descriptor)?;
            if crate::trace_part::load_trace_part(&dir)
                .ok()
                .and_then(|part| TracePartReader::open_cached(part).ok())
                .is_some()
            {
                continue;
            }
            self.download_trace_part(descriptor, traces_root, false)
                .await?;
        }
        Ok(manifest)
    }

    /// Restores the remote trace catalog and publishes trace parts left by a
    /// local-only run or by a crash before the trace manifest CAS. Trace parts
    /// are immutable, so retrying an interrupted upload is safe: existing
    /// remote objects are verified byte-for-byte by `upload_trace_part`.
    pub async fn reconcile_trace_local_cache(
        &self,
        traces_root: &Path,
    ) -> Result<TraceManifest, String> {
        let manifest = self.restore_trace_catalog(traces_root).await?;
        validate_cache_tree_no_symlinks(traces_root)?;
        let active: HashMap<&str, &TraceManifestPart> = manifest
            .parts
            .iter()
            .map(|part| (part.id.as_str(), part))
            .collect();
        let mut unpublished = Vec::new();
        for part in discover_trace_parts(traces_root)? {
            let descriptor = TraceManifestPart {
                id: part.meta.id.clone(),
                partition: part.meta.partition.clone(),
            };
            if let Some(existing) = active.get(descriptor.id.as_str()) {
                if **existing != descriptor {
                    return Err(format!(
                        "local trace part {} conflicts with the remote manifest",
                        descriptor.id
                    ));
                }
                continue;
            }
            TracePartReader::open(part.clone()).map_err(|error| {
                format!(
                    "local trace part {} is not fully cached and is absent from the remote manifest: {error}",
                    descriptor.id
                )
            })?;
            unpublished.push(part);
        }
        if !unpublished.is_empty() {
            self.publish_trace_parts(&unpublished).await?;
        }
        self.restore_trace_catalog(traces_root).await
    }

    pub async fn restore_trace_parts(
        &self,
        traces_root: &Path,
        ids: &HashSet<String>,
    ) -> Result<(), String> {
        if ids.is_empty() {
            return Ok(());
        }
        let manifest = self.load_trace_manifest().await?;
        let mut restored = HashSet::new();
        for descriptor in &manifest.parts {
            if !ids.contains(&descriptor.id) {
                continue;
            }
            let dir = trace_cache_part_dir(traces_root, descriptor)?;
            if crate::trace_part::load_trace_part(&dir)
                .ok()
                .and_then(|part| TracePartReader::open(part).ok())
                .is_none()
            {
                self.download_trace_part(descriptor, traces_root, true)
                    .await?;
            }
            restored.insert(descriptor.id.clone());
        }
        let missing: Vec<_> = ids.difference(&restored).cloned().collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "trace parts are no longer present in the object-store manifest: {}",
                missing.join(", ")
            ))
        }
    }

    async fn download_trace_part(
        &self,
        descriptor: &TraceManifestPart,
        traces_root: &Path,
        include_data: bool,
    ) -> Result<(), String> {
        let final_dir = trace_cache_part_dir(traces_root, descriptor)?;
        let temp_dir = ensure_safe_directory_chain(
            traces_root,
            &[".tmp", "remote", &descriptor.partition, &descriptor.id],
        )?;
        std::fs::remove_dir_all(&temp_dir).map_err(|error| error.to_string())?;
        std::fs::create_dir(&temp_dir).map_err(|error| error.to_string())?;
        let files: &[&str] = if include_data {
            &TRACE_PART_FILES
        } else {
            &TRACE_CATALOG_FILES
        };
        for file in files {
            let bytes = self
                .store
                .get(&self.trace_part_path(descriptor, file))
                .await
                .map_err(|error| {
                    format!(
                        "failed to download trace part {} file {file}: {error}",
                        descriptor.id
                    )
                })?
                .bytes()
                .await
                .map_err(|error| {
                    format!(
                        "failed to read trace part {} file {file}: {error}",
                        descriptor.id
                    )
                })?;
            tokio::fs::write(temp_dir.join(file), bytes)
                .await
                .map_err(|error| {
                    format!(
                        "failed to cache trace part {} file {file}: {error}",
                        descriptor.id
                    )
                })?;
        }
        let downloaded = crate::trace_part::load_trace_part(&temp_dir)?;
        if downloaded.meta.id != descriptor.id || downloaded.meta.partition != descriptor.partition
        {
            return Err(format!(
                "downloaded trace part {} metadata mismatch",
                descriptor.id
            ));
        }
        TracePartReader::open_cached(downloaded)?;
        if include_data {
            TracePartReader::open(crate::trace_part::load_trace_part(&temp_dir)?)?;
        }
        if final_dir.exists() {
            std::fs::remove_dir_all(&final_dir).map_err(|error| error.to_string())?;
        }
        if let Some(parent) = final_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        std::fs::rename(&temp_dir, &final_dir).map_err(|error| error.to_string())?;
        Ok(())
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
                // The intact-input-set rule protects a *replacement*: there is
                // an output that must not be retained alongside another
                // writer's. A pure removal produces no output, so deleting
                // whichever subset is still present reaches the same end state.
                // Requiring an intact set here would wedge retention forever
                // once a batch mixes ids an earlier tick already removed with
                // ids that have only just expired.
                if !added.is_empty() && present_removed != removed.len() {
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

}

