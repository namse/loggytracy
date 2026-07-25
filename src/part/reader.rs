#[derive(Clone)]
pub struct PreadReader {
    file: Arc<fs::File>,
    len: u64,
}

impl PreadReader {
    pub fn new(file: fs::File) -> io::Result<Self> {
        let len = file.metadata()?.len();
        Ok(Self {
            file: Arc::new(file),
            len,
        })
    }
}

impl Length for PreadReader {
    fn len(&self) -> u64 {
        self.len
    }
}

impl ChunkReader for PreadReader {
    type T = PreadCursor;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(PreadCursor {
            file: self.file.clone(),
            pos: start,
            len: self.len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let mut buf = vec![0u8; length];
        self.file.read_exact_at(&mut buf, start).map_err(|e| {
            parquet::errors::ParquetError::from(std::io::Error::new(e.kind(), e.to_string()))
        })?;
        Ok(Bytes::from(buf))
    }
}

pub struct PreadCursor {
    file: Arc<fs::File>,
    pos: u64,
    len: u64,
}

impl Read for PreadCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.len.saturating_sub(self.pos) as usize;
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = buf.len().min(remaining);
        let n = self.file.read_at(&mut buf[..to_read], self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

pub struct PartReader {
    part: Part,
    bloom: Vec<BloomFilter>,
    exact_field_bloom: Option<Vec<BloomFilter>>,
    exact_field_bloom_canonical: bool,
    stream_index: StreamMap,
    stream_labels: Vec<String>,
}

struct DecodedBlooms {
    line: Vec<BloomFilter>,
    exact_fields: Option<Vec<BloomFilter>>,
    exact_fields_canonical: bool,
}

fn validate_sidecar_files(part: &Part) -> Result<(), String> {
    let files = [
        (
            BLOOM_FILE,
            part.bloom_path(),
            part.meta.integrity.bloom_crc32,
        ),
        (
            STREAM_INDEX_FILE,
            part.stream_index_path(),
            part.meta.integrity.stream_index_crc32,
        ),
    ];
    for (name, path, expected) in files {
        let actual = file_crc32(&path).map_err(|error| {
            format!(
                "failed to checksum {name} for part {}: {error}",
                part.meta.id
            )
        })?;
        if actual != expected {
            return Err(format!(
                "{name} checksum mismatch for part {}: expected {expected}, got {actual}",
                part.meta.id
            ));
        }
    }
    Ok(())
}

fn open_part_data(
    part: &Part,
    validate_checksum: bool,
) -> Result<(PreadReader, ArrowReaderMetadata), String> {
    if validate_checksum {
        let actual = file_crc32(&part.data_path()).map_err(|error| {
            format!(
                "failed to checksum {DATA_FILE} for part {}: {error}",
                part.meta.id
            )
        })?;
        if actual != part.meta.integrity.data_crc32 {
            return Err(format!(
                "{DATA_FILE} checksum mismatch for part {}: expected {}, got {actual}",
                part.meta.id, part.meta.integrity.data_crc32
            ));
        }
    }

    let data_file =
        PreadReader::new(fs::File::open(part.data_path()).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let arrow_reader_metadata =
        ArrowReaderMetadata::load(&data_file, Default::default()).map_err(|e| e.to_string())?;

    let parquet_rg_count = arrow_reader_metadata.metadata().num_row_groups();
    if parquet_rg_count != part.meta.row_group_count as usize {
        return Err(format!(
            "row group count mismatch for part {}: parquet footer says {}, meta says {}",
            part.meta.id, parquet_rg_count, part.meta.row_group_count
        ));
    }
    let parquet_row_count = arrow_reader_metadata.metadata().file_metadata().num_rows();
    if parquet_row_count != part.meta.row_count as i64 {
        return Err(format!(
            "row count mismatch for part {}: parquet footer says {}, meta says {}",
            part.meta.id, parquet_row_count, part.meta.row_count
        ));
    }
    let expected_schema = part_schema(&part.meta.stream_labels);
    if arrow_reader_metadata.schema().fields() != expected_schema.fields() {
        return Err(format!(
            "parquet schema does not match metadata for part {}: expected {:?}, got {:?}",
            part.meta.id,
            expected_schema.fields(),
            arrow_reader_metadata.schema().fields()
        ));
    }
    Ok((data_file, arrow_reader_metadata))
}

fn validate_stream_index(part: &Part, index: &StreamMap) -> Result<(), String> {
    let expected: BTreeSet<(String, String)> = part
        .meta
        .streams
        .iter()
        .flat_map(|labels| {
            labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
        })
        .collect();
    let mut indexed = BTreeSet::new();
    for (name, values) in index {
        for (value, bitmap) in values {
            if bitmap.is_empty() || bitmap.iter().any(|rg| rg >= part.meta.row_group_count) {
                return Err(format!(
                    "stream index has invalid row-group bitmap for part {}",
                    part.meta.id
                ));
            }
            indexed.insert((name.clone(), value.clone()));
        }
    }
    if indexed != expected {
        return Err(format!(
            "stream index labels do not match metadata for part {}",
            part.meta.id
        ));
    }
    Ok(())
}

impl PartReader {
    pub fn open(part: Part) -> Result<Self, String> {
        Self::open_internal(part, true)
    }

    /// Opens the metadata and indexes for an object-store cached part. The
    /// Parquet body may have been evicted and is opened only while a query is
    /// actively reading it.
    pub fn open_cached(part: Part) -> Result<Self, String> {
        Self::open_internal(part, false)
    }

    fn open_internal(part: Part, require_data: bool) -> Result<Self, String> {
        validate_sidecar_files(&part)?;
        if part.meta.row_group_count == 0
            || part.meta.row_group_min_ts.len() != part.meta.row_group_count as usize
            || part.meta.row_group_max_ts.len() != part.meta.row_group_count as usize
        {
            return Err(format!(
                "row group metadata length mismatch for part {}",
                part.meta.id
            ));
        }
        let bloom_bytes = fs::read(part.bloom_path()).map_err(|e| e.to_string())?;
        let decoded_blooms = decode_blooms(&bloom_bytes, part.meta.row_group_count as usize)?;
        let stream_index =
            decode_stream_index(&fs::read(part.stream_index_path()).map_err(|e| e.to_string())?)?;
        validate_stream_index(&part, &stream_index)?;
        let stream_labels = part.meta.stream_labels.clone();
        if require_data || part.data_path().exists() {
            open_part_data(&part, true)?;
        }
        Ok(Self {
            part,
            bloom: decoded_blooms.line,
            exact_field_bloom: decoded_blooms.exact_fields,
            exact_field_bloom_canonical: decoded_blooms.exact_fields_canonical,
            stream_index,
            stream_labels,
        })
    }

    pub fn part(&self) -> &Part {
        &self.part
    }

    pub fn meta(&self) -> &PartMeta {
        &self.part.meta
    }

    /// The row groups a tenant may address, or `None` when the part holds no
    /// rows for it. Every read path funnels through this, so a tenant cannot
    /// reach another tenant's rows even if a matcher or filter would select
    /// them.
    fn tenant_row_groups(&self, tenant: &TenantId) -> Option<std::ops::Range<u32>> {
        self.part
            .meta
            .tenant_segment(tenant)
            .map(|segment| segment.row_group_start..segment.row_group_end)
    }

    /// Label names a tenant can see. Derived from the stream index restricted
    /// to the tenant's row groups rather than the part-wide label list.
    pub fn label_names(&self, tenant: &TenantId) -> Vec<String> {
        let Some(groups) = self.tenant_row_groups(tenant) else {
            return Vec::new();
        };
        self.stream_index
            .iter()
            .filter(|(_, values)| {
                values
                    .values()
                    .any(|bitmap| bitmap.iter().any(|rg| groups.contains(&rg)))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Label names present anywhere in the part, for internal use by callers
    /// that already know they are not serving a tenant query (schema checks).
    pub fn all_label_names(&self) -> &[String] {
        &self.stream_labels
    }

    pub fn label_values(&self, tenant: &TenantId, name: &str) -> Vec<String> {
        let Some(groups) = self.tenant_row_groups(tenant) else {
            return Vec::new();
        };
        self.stream_index
            .get(name)
            .map(|values| {
                values
                    .iter()
                    .filter(|(_, bitmap)| bitmap.iter().any(|rg| groups.contains(&rg)))
                    .map(|(value, _)| value.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn series(&self, tenant: &TenantId, matchers: &[LabelMatcher]) -> Vec<Labels> {
        let Some(groups) = self.tenant_row_groups(tenant) else {
            return Vec::new();
        };
        self.part
            .meta
            .streams
            .iter()
            .filter(|labels| matchers.iter().all(|m| m.matches(labels)))
            .filter(|labels| self.stream_occurs_in(labels, &groups))
            .cloned()
            .collect()
    }

    /// Whether a stream's labels are indexed in any of `groups`. The part-wide
    /// stream list is shared by all tenants, so it has to be filtered through
    /// the row-group posting lists before it can be returned.
    fn stream_occurs_in(&self, labels: &Labels, groups: &std::ops::Range<u32>) -> bool {
        if labels.is_empty() {
            return true;
        }
        labels.iter().all(|(name, value)| {
            self.stream_index
                .get(name)
                .and_then(|values| values.get(value))
                .is_some_and(|bitmap| bitmap.iter().any(|rg| groups.contains(&rg)))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                tenant,
                matchers,
                ExactFieldPruning::new(line_filters, &[]),
                start_ns,
                end_ns,
                limit,
                forward,
                None,
                None,
            )?
            .results)
    }

    /// Uses exact-field predicates only for row-group pruning. Bloom filters
    /// can return false positives, so the caller remains responsible for
    /// evaluating the predicates against each returned entry.
    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
    ) -> Result<Vec<StreamResult>, String> {
        Ok(self
            .query_with_exact_field_pruning_and_scan_limit(
                tenant, matchers, pruning, start_ns, end_ns, limit, forward, None, None,
            )?
            .results)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning_and_scan_limit(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        self.query_with_exact_field_pruning_and_scan_limits(
            tenant,
            matchers,
            pruning,
            start_ns,
            end_ns,
            limit,
            forward,
            scan_limit,
            None,
            cancellation,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_with_exact_field_pruning_and_scan_limits(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        pruning: ExactFieldPruning<'_>,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        scan_bytes_limit: Option<u64>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        self.query_internal(
            tenant,
            matchers,
            pruning.line_filters,
            pruning.exact_fields,
            QueryTimeRange {
                start_ns,
                end_ns,
                include_end: true,
            },
            limit,
            forward,
            scan_limit,
            scan_bytes_limit,
            cancellation,
        )
    }

    /// Every row in the part, tenant by tenant. Merge is the only caller: it
    /// rewrites a part and therefore has to see all tenants, so this returns
    /// `Row`s that carry their own tenant instead of `StreamResult`s that do
    /// not.
    pub fn read_all_rows(&self, scan_bytes_limit: Option<u64>) -> Result<Vec<Row>, String> {
        let mut rows = Vec::new();
        for segment in &self.part.meta.tenants {
            let result = self.query_internal(
                &segment.tenant,
                &[],
                &[],
                &[],
                QueryTimeRange {
                    start_ns: i64::MIN,
                    end_ns: i64::MAX,
                    include_end: true,
                },
                usize::MAX,
                true,
                None,
                scan_bytes_limit,
                None,
            )?;
            for stream in result.results {
                for entry in stream.entries {
                    rows.push(Row::from_entry(&segment.tenant, &stream.labels, &entry));
                }
            }
        }
        Ok(rows)
    }

    fn select_row_groups(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        time_range: QueryTimeRange,
    ) -> Vec<u32> {
        self.select_row_groups_with_exact_fields(tenant, matchers, line_filters, &[], time_range)
    }

    fn select_row_groups_with_exact_fields(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        time_range: QueryTimeRange,
    ) -> Vec<u32> {
        let Some(groups) = self.tenant_row_groups(tenant) else {
            return Vec::new();
        };
        let mut selected = Vec::with_capacity(groups.len());
        for rg in groups {
            let rgu = rg as usize;
            if !(self.part.meta.row_group_max_ts[rgu] >= time_range.start_ns
                && (self.part.meta.row_group_min_ts[rgu] < time_range.end_ns
                    || (time_range.include_end
                        && self.part.meta.row_group_min_ts[rgu] == time_range.end_ns)))
            {
                continue;
            }
            if !row_group_matches_index(rg, matchers, &self.stream_index) {
                continue;
            }
            if !self.bloom_prune(rgu, line_filters) {
                continue;
            }
            if !self.exact_field_bloom_prune(rgu, exact_fields) {
                continue;
            }
            selected.push(rg);
        }
        selected
    }

    /// Returns whether any row group can satisfy the catalog-visible portion
    /// of a query. This does not open `data.parquet`, so object-store callers
    /// can use it before deciding which evicted bodies to restore.
    pub fn may_match_exact_fields(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        start_ns: i64,
        end_ns: i64,
    ) -> bool {
        !self
            .select_row_groups_with_exact_fields(
                tenant,
                matchers,
                line_filters,
                exact_fields,
                QueryTimeRange {
                    start_ns,
                    end_ns,
                    include_end: true,
                },
            )
            .is_empty()
    }

    #[allow(clippy::too_many_arguments)]
    fn query_internal(
        &self,
        tenant: &TenantId,
        matchers: &[LabelMatcher],
        line_filters: &[LineFilter],
        exact_fields: &[ExactFieldPredicate],
        time_range: QueryTimeRange,
        limit: usize,
        forward: bool,
        scan_limit: Option<usize>,
        scan_bytes_limit: Option<u64>,
        cancellation: Option<&AtomicBool>,
    ) -> Result<QueryResult, String> {
        let rg_count = self.bloom.len();
        if rg_count == 0 {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
                scanned_bytes: 0,
            });
        }
        if limit == 0 {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
                scanned_bytes: 0,
            });
        }

        let selected = if exact_fields.is_empty() {
            self.select_row_groups(tenant, matchers, line_filters, time_range)
        } else {
            self.select_row_groups_with_exact_fields(
                tenant,
                matchers,
                line_filters,
                exact_fields,
                time_range,
            )
        };
        if selected.is_empty() {
            return Ok(QueryResult {
                results: Vec::new(),
                scanned_rows: 0,
                scanned_bytes: 0,
            });
        }
        let mut sorted_selected = selected.clone();
        sorted_selected.sort_unstable();
        if !forward {
            sorted_selected.reverse();
        }

        let mut collected: Vec<(Labels, LogEntry)> = Vec::new();
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0u64;

        let batch_size = scan_limit
            .into_iter()
            .chain(forward.then_some(limit))
            .min()
            .map(|value| value.clamp(1, 1024))
            .unwrap_or(1024);
        'row_groups: for &row_group in &sorted_selected {
            if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                break;
            }
            // Parquet may normalize a multi-row-group selection back to file
            // order. Build one reader per group so backward scans really start
            // at the newest group and can stop once the limit is satisfied.
            let (data_file, arrow_reader_metadata) = open_part_data(&self.part, false)?;
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                data_file,
                arrow_reader_metadata,
            )
            .with_batch_size(batch_size);
            let reader = builder
                .with_row_groups(vec![row_group as usize])
                .build()
                .map_err(|e| e.to_string())?;

            // Parquet yields batches in row order even when a single row
            // group is selected. Buffer only this row group and reverse the
            // batches as well as the rows; reversing rows inside each batch
            // alone would return the oldest batch first for backward scans.
            let mut batches: Vec<_> = reader
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
            if !forward {
                batches.reverse();
            }
            for batch in batches {
                if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                    break 'row_groups;
                }
                let batch_bytes = batch.get_array_memory_size() as u64;
                scanned_bytes = scanned_bytes.saturating_add(batch_bytes);
                if scan_bytes_limit.is_some_and(|limit| scanned_bytes > limit) {
                    return Err(format!(
                        "query exceeds the maximum of {} scanned bytes",
                        scan_bytes_limit.unwrap_or_default()
                    ));
                }
                let rows_to_scan = scan_limit
                    .map(|limit| limit.saturating_sub(scanned_rows).min(batch.num_rows()))
                    .unwrap_or(batch.num_rows());
                scanned_rows = scanned_rows.saturating_add(rows_to_scan);
                let row_tenant = batch.column(0).as_string::<i32>();
                let ts = batch.column(1).as_primitive::<Int64Type>();
                let msg = batch.column(2).as_string::<i32>();
                let sm_col_idx = 3 + self.stream_labels.len();
                let sm = batch.column(sm_col_idx).as_string::<i32>();
                let label_cols: Vec<&StringArray> = (0..self.stream_labels.len())
                    .map(|label_index| batch.column(3 + label_index).as_string::<i32>())
                    .collect();

                let row_start = if forward {
                    0
                } else {
                    batch.num_rows().saturating_sub(rows_to_scan)
                };
                let row_end = row_start + rows_to_scan;
                let row_indices: Box<dyn Iterator<Item = usize>> = if forward {
                    Box::new(row_start..row_end)
                } else {
                    Box::new((row_start..row_end).rev())
                };
                for i in row_indices {
                    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
                        break 'row_groups;
                    }
                    // Row groups are tenant-aligned, so this never rejects a
                    // row in a well-formed part. It is kept so isolation does
                    // not depend on `meta.json` alone: a metadata bug becomes
                    // an empty result rather than a cross-tenant read.
                    if row_tenant.value(i) != tenant.as_str() {
                        return Err(format!(
                            "part {} row group {row_group} contains rows outside tenant {tenant}",
                            self.part.meta.id
                        ));
                    }
                    let ts_val = ts.value(i);
                    if ts_val < time_range.start_ns
                        || ts_val > time_range.end_ns
                        || (!time_range.include_end && ts_val == time_range.end_ns)
                    {
                        continue;
                    }
                    let mut labels: Labels = BTreeMap::new();
                    for (j, label_name) in self.stream_labels.iter().enumerate() {
                        if !label_cols[j].is_null(i) {
                            labels.insert(label_name.clone(), label_cols[j].value(i).to_string());
                        }
                    }
                    if !matchers.iter().all(|m| m.matches(&labels)) {
                        continue;
                    }
                    let line = msg.value(i).to_string();
                    if !line_filters.iter().all(|f| f.matches(&line)) {
                        continue;
                    }
                    let structured_metadata = if sm.is_null(i) {
                        Vec::new()
                    } else {
                        serde_json::from_str(sm.value(i)).map_err(|error| {
                        format!(
                            "invalid structured metadata in part {} at timestamp {ts_val}: {error}",
                            self.part.meta.id
                        )
                    })?
                    };
                    collected.push((
                        labels,
                        LogEntry {
                            timestamp_ns: ts_val,
                            line,
                            structured_metadata,
                        },
                    ));
                    if forward && collected.len() >= limit {
                        break 'row_groups;
                    }
                }
                if scan_limit.is_some_and(|limit| scanned_rows >= limit) {
                    break 'row_groups;
                }
            }
            if !forward && collected.len() >= limit {
                break;
            }
        }

        if forward {
            collected.sort_by_key(|e| e.1.timestamp_ns);
        } else {
            collected.sort_by_key(|e| std::cmp::Reverse(e.1.timestamp_ns));
        }
        collected.truncate(limit);

        Ok(QueryResult {
            results: group_by_labels(collected),
            scanned_rows,
            scanned_bytes,
        })
    }
}

impl PartReader {
    fn bloom_prune(&self, rg: usize, line_filters: &[LineFilter]) -> bool {
        for f in line_filters {
            if let LineFilter::Contains(s) = f
                && !self.bloom[rg].might_contain_substr(s)
            {
                return false;
            }
        }
        true
    }

    fn exact_field_bloom_prune(&self, rg: usize, exact_fields: &[ExactFieldPredicate]) -> bool {
        let Some(blooms) = &self.exact_field_bloom else {
            return true;
        };
        exact_fields.iter().all(|predicate| {
            if predicate.canonical && !self.exact_field_bloom_canonical {
                return true;
            }
            // Stream labels are visible to pipeline field filters, but are
            // intentionally not part of the exact-field bloom. The stream
            // index handles label matchers; skipping this predicate here is
            // required to avoid pruning a row group that contains the label.
            if self
                .stream_labels
                .iter()
                .any(|name| name == &predicate.name)
            {
                return true;
            }
            // Field-filter execution may treat an absent field as an empty
            // string. Absence is not represented in the bloom, so an empty
            // equality cannot safely reject a row group.
            if predicate.value.is_empty() {
                return true;
            }
            encode_exact_field_token(&predicate.name, &predicate.value)
                .map(|token| blooms[rg].contains(&token))
                // An unrepresentable predicate must conservatively scan.
                .unwrap_or(true)
        })
    }
}

fn row_group_matches_index(rg: u32, matchers: &[LabelMatcher], index: &StreamMap) -> bool {
    for m in matchers {
        match m.op {
            MatcherOp::Eq => {
                // {label=""}는 라벨 부재를 매치한다. stream index에는 라벨이
                // 없는 스트림의 entry가 기록되지 않으므로, value가 빈 문자열이면
                // 보수적으로 통과시킨다 (memtable과의 정합성).
                if m.value.is_empty() {
                    continue;
                }
                let Some(values) = index.get(&m.name) else {
                    return false;
                };
                let Some(bitmap) = values.get(&m.value) else {
                    return false;
                };
                if !bitmap.contains(rg) {
                    return false;
                }
            }
            MatcherOp::Neq | MatcherOp::Re | MatcherOp::NRe => {
                // conservative: cannot precisely prune with these ops
            }
        }
    }
    true
}

fn decode_blooms(buf: &[u8], expected_count: usize) -> Result<DecodedBlooms, String> {
    if buf.len() < 8 {
        return Err("bloom file too short".to_string());
    }
    let (has_exact_fields, exact_fields_canonical) = if &buf[0..4] == BLOOM_MAGIC_V1 {
        (false, false)
    } else if &buf[0..4] == BLOOM_MAGIC_V2 {
        (true, false)
    } else if &buf[0..4] == BLOOM_MAGIC_V3 {
        (true, true)
    } else {
        return Err("bloom magic mismatch".to_string());
    };
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if count != expected_count {
        return Err(format!(
            "row group count mismatch: bloom says {count}, metadata says {expected_count}"
        ));
    }
    let mut pos = 8;
    let mut line = Vec::with_capacity(count);
    let mut exact_fields = has_exact_fields.then(|| Vec::with_capacity(count));
    for _ in 0..count {
        line.push(decode_length_prefixed_bloom(buf, &mut pos)?);
        if let Some(exact_fields) = &mut exact_fields {
            exact_fields.push(decode_length_prefixed_bloom(buf, &mut pos)?);
        }
    }
    if pos != buf.len() {
        return Err("bloom file has trailing bytes".to_string());
    }
    Ok(DecodedBlooms {
        line,
        exact_fields,
        exact_fields_canonical,
    })
}

fn decode_length_prefixed_bloom(buf: &[u8], pos: &mut usize) -> Result<BloomFilter, String> {
    let length_end = pos
        .checked_add(4)
        .ok_or_else(|| "bloom length overflow".to_string())?;
    let length_bytes: [u8; 4] = buf
        .get(*pos..length_end)
        .ok_or_else(|| "bloom length truncated".to_string())?
        .try_into()
        .map_err(|_| "bloom length truncated".to_string())?;
    let len = u32::from_le_bytes(length_bytes) as usize;
    *pos = length_end;
    let payload_end = pos
        .checked_add(len)
        .ok_or_else(|| "bloom payload length overflow".to_string())?;
    let payload = buf
        .get(*pos..payload_end)
        .ok_or_else(|| "bloom payload truncated".to_string())?;
    *pos = payload_end;
    BloomFilter::decode(payload)
}

fn decode_stream_index(buf: &[u8]) -> Result<StreamMap, String> {
    if buf.len() < 8 {
        return Err("stream index too short".to_string());
    }
    if &buf[0..4] != STREAM_MAGIC {
        return Err("stream index magic mismatch".to_string());
    }
    let count = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let mut pos = 8;
    let mut map: StreamMap = BTreeMap::new();
    for _ in 0..count {
        if pos + 4 > buf.len() {
            return Err("stream index name length truncated".to_string());
        }
        let name_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + name_len > buf.len() {
            return Err("stream index name truncated".to_string());
        }
        let name = std::str::from_utf8(&buf[pos..pos + name_len])
            .map_err(|e| e.to_string())?
            .to_string();
        pos += name_len;
        if pos + 4 > buf.len() {
            return Err("stream index value length truncated".to_string());
        }
        let value_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + value_len > buf.len() {
            return Err("stream index value truncated".to_string());
        }
        let value = std::str::from_utf8(&buf[pos..pos + value_len])
            .map_err(|e| e.to_string())?
            .to_string();
        pos += value_len;
        if pos + 4 > buf.len() {
            return Err("stream index bitmap length truncated".to_string());
        }
        let bm_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + bm_len > buf.len() {
            return Err("stream index bitmap truncated".to_string());
        }
        let bitmap =
            RoaringBitmap::deserialize_from(&buf[pos..pos + bm_len]).map_err(|e| e.to_string())?;
        pos += bm_len;
        map.entry(name).or_default().insert(value, bitmap);
    }
    if pos != buf.len() {
        return Err("stream index has trailing bytes".to_string());
    }
    Ok(map)
}

pub fn group_by_labels(collected: Vec<(Labels, LogEntry)>) -> Vec<StreamResult> {
    let mut grouped: BTreeMap<Labels, Vec<LogEntry>> = BTreeMap::new();
    for (labels, entry) in collected {
        grouped.entry(labels).or_default().push(entry);
    }
    grouped
        .into_iter()
        .map(|(labels, entries)| StreamResult { labels, entries })
        .collect()
}
