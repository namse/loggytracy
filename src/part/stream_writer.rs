/// Writes a part without ever holding it.
///
/// The batch path materializes the whole part as a `Vec<Row>` before a byte is
/// written, and [`docs/MEMORY_ATTRIBUTION.md`] measured what that costs merge:
/// **829 of 847 live megabytes** at the settle peak, across **7.2 million live
/// allocations**, which is why the budget gate is red at 2 GiB.
///
/// Nothing about the format required that. Every file a part is made of is
/// produced a row group at a time — Parquet flushes per row group already, the
/// blooms are per row group, the stream index addresses row groups by ordinal,
/// and `meta.json`'s extremes, tenant segments and counts all accumulate. The
/// one thing that genuinely has to be known before the first row group is the
/// **schema**, because it names a column per stream label — and a merge can
/// read that from its inputs' `meta.json` without touching a row.
///
/// So this holds one row group and a set of counters. The output is the same
/// bytes the batch path writes, and it is the same functions that write them:
/// `encode_group_blooms` and `row_group_batch` are shared, so the two cannot
/// drift into producing different parts.
///
/// **Rows must arrive sorted by `(tenant, timestamp_ns, …)` and deduplicated.**
/// [`MergedRows`] is what guarantees that. A row group is cut when the tenant
/// changes or `row_group_size` is reached, which is exactly what
/// `row_group_bounds` computes for the batch path.
pub struct StreamingPartWriter {
    schema: Arc<Schema>,
    writer: ArrowWriter<fs::File>,
    data_path: PathBuf,
    stream_labels: Vec<String>,
    /// The keys this part stores as `_sm:` columns, chosen before the first
    /// row: a merge sums its inputs' recorded per-key row counts and takes the
    /// same top-N `select_metadata_columns` takes — deterministic without
    /// reading a row, exactly like the stream-label union. The counts are
    /// re-counted during `push`, so keys whose rows retention or a delete
    /// dropped do not inflate the next merge's choice.
    metadata_keys: Vec<String>,
    metadata_counts: BTreeMap<String, u64>,
    row_group_size: usize,
    group: Vec<Row>,

    bloom_sections: Vec<Vec<u8>>,
    index: BTreeMap<String, BTreeMap<String, RoaringBitmap>>,
    streams: BTreeSet<SharedLabels>,

    row_count: u64,
    min_ts_ns: i64,
    max_ts_ns: i64,
    materialized_bytes: u64,
    row_group_min_ts: Vec<i64>,
    row_group_max_ts: Vec<i64>,
    row_group_rows: Vec<u32>,
    row_group_ts_monotonic: Vec<bool>,
    tenants: Vec<TenantSegment>,
}

impl StreamingPartWriter {
    pub fn create(
        dir: &Path,
        stream_labels: Vec<String>,
        metadata_keys: Vec<String>,
        row_group_size: usize,
    ) -> io::Result<Self> {
        let schema = part_schema(&stream_labels, &metadata_keys);
        let data_path = dir.join(DATA_FILE);
        let file = fs::File::create(&data_path)?;
        let props = part_writer_properties(row_group_size);
        let writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(io::Error::other)?;
        Ok(Self {
            schema,
            writer,
            data_path,
            stream_labels,
            metadata_keys,
            metadata_counts: BTreeMap::new(),
            row_group_size,
            group: Vec::new(),
            bloom_sections: Vec::new(),
            index: BTreeMap::new(),
            streams: BTreeSet::new(),
            row_count: 0,
            min_ts_ns: i64::MAX,
            max_ts_ns: i64::MIN,
            materialized_bytes: 0,
            row_group_min_ts: Vec::new(),
            row_group_max_ts: Vec::new(),
            row_group_rows: Vec::new(),
            row_group_ts_monotonic: Vec::new(),
            tenants: Vec::new(),
        })
    }

    pub fn push(&mut self, row: Row) -> io::Result<()> {
        // The same rule `row_group_bounds` applies to a slice: one tenant per
        // group, cut on size. Not on stream change — that was measured worse,
        // and the selectivity comes from the sort order rather than the cut.
        let cut = self
            .group
            .first()
            .is_some_and(|first| first.tenant != row.tenant)
            || self.group.len() >= self.row_group_size;
        if cut {
            self.flush_group()?;
        }
        self.row_count += 1;
        self.min_ts_ns = self.min_ts_ns.min(row.timestamp_ns);
        self.max_ts_ns = self.max_ts_ns.max(row.timestamp_ns);
        self.materialized_bytes = self
            .materialized_bytes
            .saturating_add(row.materialized_bytes());
        if !self.streams.contains(&row.labels) {
            self.streams.insert(row.labels.clone());
        }
        for (name, _) in &row.structured_metadata {
            if self.metadata_keys.binary_search(name).is_ok() {
                *self.metadata_counts.entry(name.clone()).or_default() += 1;
            }
        }
        self.group.push(row);
        Ok(())
    }

    fn flush_group(&mut self) -> io::Result<()> {
        if self.group.is_empty() {
            return Ok(());
        }
        let ordinal = self.row_group_min_ts.len() as u32;
        let batch = row_group_batch(&self.schema, &self.group, &self.stream_labels, &self.metadata_keys)?;
        self.writer.write(&batch).map_err(io::Error::other)?;
        // The sidecars address row groups by ordinal, so a flush per batch pins
        // the boundary rather than letting the writer pick one that straddles a
        // tenant. Same reason the batch path flushes here.
        self.writer.flush().map_err(io::Error::other)?;
        self.bloom_sections.push(encode_group_blooms(&self.group)?);

        for row in &self.group {
            for label in &self.stream_labels {
                let Some(value) = row.labels.get(label) else {
                    continue;
                };
                // Owned keys, unlike the batch path, because the rows are gone
                // after this call. Cloned only when a name or value is seen for
                // the first time, so the cost is per distinct stream rather
                // than per row.
                match self.index.get_mut(label.as_str()) {
                    Some(values) => match values.get_mut(value.as_str()) {
                        Some(bitmap) => {
                            bitmap.insert(ordinal);
                        }
                        None => {
                            let mut bitmap = RoaringBitmap::new();
                            bitmap.insert(ordinal);
                            values.insert(value.clone(), bitmap);
                        }
                    },
                    None => {
                        let mut bitmap = RoaringBitmap::new();
                        bitmap.insert(ordinal);
                        self.index
                            .entry(label.clone())
                            .or_default()
                            .insert(value.clone(), bitmap);
                    }
                }
            }
        }

        let first = self.group.first().expect("checked non-empty");
        // Scanned rather than taken from the ends: rows are ordered by stream
        // before time, so a group holding two streams does not start at its own
        // minimum.
        let min_ts_ns = self
            .group
            .iter()
            .map(|row| row.timestamp_ns)
            .min()
            .unwrap_or_default();
        let max_ts_ns = self
            .group
            .iter()
            .map(|row| row.timestamp_ns)
            .max()
            .unwrap_or_default();
        self.row_group_min_ts.push(min_ts_ns);
        self.row_group_max_ts.push(max_ts_ns);
        self.row_group_rows.push(self.group.len() as u32);
        self.row_group_ts_monotonic.push(
            self.group
                .windows(2)
                .all(|pair| pair[0].timestamp_ns <= pair[1].timestamp_ns),
        );
        match self.tenants.last_mut() {
            Some(segment) if segment.tenant == first.tenant => {
                segment.row_group_end = ordinal + 1;
                segment.row_count += self.group.len() as u64;
                segment.min_ts_ns = segment.min_ts_ns.min(min_ts_ns);
                segment.max_ts_ns = segment.max_ts_ns.max(max_ts_ns);
            }
            _ => self.tenants.push(TenantSegment {
                tenant: first.tenant.clone(),
                row_group_start: ordinal,
                row_group_end: ordinal + 1,
                row_count: self.group.len() as u64,
                min_ts_ns,
                max_ts_ns,
            }),
        }

        self.group.clear();
        Ok(())
    }

    /// Whether any row was ever pushed. An empty stream must not leave a part.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn min_ts_ns(&self) -> i64 {
        self.min_ts_ns
    }

    pub fn finish(mut self, dir: &Path, id: &str, partition: &str) -> io::Result<()> {
        self.flush_group()?;
        self.writer.close().map_err(io::Error::other)?;
        sync_file(&self.data_path)?;

        let mut blooms = Vec::new();
        blooms.extend_from_slice(BLOOM_MAGIC);
        blooms.extend_from_slice(&(self.bloom_sections.len() as u32).to_le_bytes());
        for section in &self.bloom_sections {
            blooms.extend_from_slice(section);
        }
        let entry_count: usize = self.index.values().map(|values| values.len()).sum();
        let entries = self
            .index
            .iter()
            .flat_map(|(name, values)| {
                values
                    .iter()
                    .map(move |(value, bitmap)| (name.as_str(), value.as_str(), bitmap))
            })
            .collect::<Vec<_>>();
        let streams = serialize_stream_index(entry_count, entries)?;
        write_index_sections(&dir.join(INDEX_FILE), &blooms, &streams)?;

        let integrity = PartIntegrity {
            data_crc32: file_crc32(&self.data_path)?,
            index_crc32: file_crc32(&dir.join(INDEX_FILE))?,
            metadata_crc32: 0,
        };
        let mut meta = MetaFile {
            id: id.to_string(),
            partition: partition.to_string(),
            min_ts_ns: if self.row_count == 0 {
                0
            } else {
                self.min_ts_ns
            },
            max_ts_ns: if self.row_count == 0 {
                0
            } else {
                self.max_ts_ns
            },
            row_count: self.row_count,
            row_group_count: self.row_group_min_ts.len() as u32,
            row_group_min_ts: self.row_group_min_ts,
            row_group_max_ts: self.row_group_max_ts,
            row_group_rows: self.row_group_rows,
            row_group_ts_monotonic: self.row_group_ts_monotonic,
            tenants: self.tenants,
            materialized_bytes: self.materialized_bytes,
            // Every declared key stays listed, even at a count of zero:
            // the Parquet file carries its (all-null) column either way, and
            // `meta.json`'s key list is what the schema check rebuilds. Same
            // superset behavior as `stream_labels` after retention drops a
            // stream.
            metadata_columns: self
                .metadata_keys
                .iter()
                .map(|key| {
                    (
                        key.clone(),
                        self.metadata_counts.get(key).copied().unwrap_or(0),
                    )
                })
                .collect(),
            stream_labels: self.stream_labels,
            streams: self
                .streams
                .iter()
                .map(|labels| {
                    labels
                        .iter()
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect()
                })
                .collect(),
            integrity,
        };
        meta.integrity.metadata_crc32 = metadata_crc32(&meta).map_err(io::Error::other)?;
        let encoded = serde_json::to_string(&meta).map_err(io::Error::other)?;
        fs::write(dir.join(META_FILE), encoded)?;
        sync_file(&dir.join(META_FILE))?;
        Ok(())
    }
}
