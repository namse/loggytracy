pub fn partition_of(ts_ns: i64) -> String {
    let secs = ts_ns.div_euclid(1_000_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    dt.format("%Y-%m-%d").to_string()
}

fn gen_part_id(min_ts_ns: i64) -> String {
    let secs = min_ts_ns.div_euclid(1_000_000_000);
    let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
    format!("{}-{}", dt.format("%Y%m%dT%H%M%S"), uuid::Uuid::new_v4())
}

pub fn rows_from_snapshot(snapshot: &MemTableSnapshot) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    for (tenant, streams) in snapshot {
        for (labels, entries) in streams {
            for e in entries {
                rows.push(Row::from_entry(tenant, labels, e));
            }
        }
    }
    sort_rows(&mut rows);
    rows
}

/// Sorts into layout order and drops entries that are copies of one another.
///
/// Delivery is at-least-once. A push that was durably written but whose
/// response never reached the client is retried, and a crash between a flush
/// and its checkpoint replays a WAL suffix that is already in a part — both
/// produce a second copy of an entry that is identical in every field. Loki
/// resolves this the same way: within a stream, one timestamp and one line is
/// one entry.
///
/// What is given up is two genuinely distinct entries that share a stream, a
/// nanosecond, a line and its metadata. At nanosecond resolution that is a
/// collision of things already indistinguishable to a reader.
///
/// Every part is written through here — flush, merge and the retention rewrite
/// — so a duplicate that a flush could not see (its twin is in an older part)
/// is removed the first time the two are merged.
fn sort_rows(rows: &mut Vec<Row>) {
    rows.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
    rows.dedup_by(|left, right| left.sort_key() == right.sort_key());
}

pub fn flush_rows(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
) -> io::Result<Vec<Part>> {
    flush_rows_internal(rows, parts_root, row_group_size, None)
}

/// Flush rows while carrying a merge tombstone into every committed part.
///
/// The tombstone is written and fsynced inside the temporary part directory
/// before that directory is renamed into the visible partition directory.
/// This makes a visible merged part self-describing even if the process dies
/// immediately after the rename.
pub fn flush_rows_with_merge_tombstone(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
    old_dirs: &[PathBuf],
) -> io::Result<Vec<Part>> {
    flush_rows_internal(rows, parts_root, row_group_size, Some(old_dirs))
}

/// Write one part from a sorted, deduplicated row stream.
///
/// The merge counterpart of [`flush_rows_with_merge_tombstone`], and the reason
/// [`StreamingPartWriter`] exists: the batch form holds the whole group as a
/// `Vec<Row>` first, which `docs/MEMORY_ATTRIBUTION.md` measured at 829 of 847
/// live megabytes at the settle peak.
///
/// `stream_labels` and `partition` come from the inputs' `meta.json`, not from
/// the rows, because the schema names a column per stream label and has to
/// exist before the first row group. That makes the label set a **superset**:
/// a label whose only rows were dropped by retention or a delete request still
/// gets a column, all null. It costs a compressed empty column rather than a
/// second pass over the rows to find out, and no read path can see the
/// difference — an absent value prunes exactly as a missing column does.
///
/// A stream that turns out to be empty leaves no part and no tombstone, the
/// same as flushing an empty `Vec`.
#[allow(clippy::too_many_arguments)]
pub fn flush_row_stream_with_merge_tombstone(
    rows: &mut MergedRows,
    keep: &mut dyn FnMut(&Row) -> bool,
    parts_root: &Path,
    partition: &str,
    stream_labels: Vec<String>,
    metadata_keys: Vec<String>,
    parsed_keys: Vec<String>,
    row_group_size: usize,
    old_dirs: &[PathBuf],
) -> io::Result<(Vec<Part>, u64, u64)> {
    let tmp_root = parts_root.join(".tmp");
    fs::create_dir_all(&tmp_root)?;
    let staging = tmp_root.join(format!("merge-{}", uuid::Uuid::new_v4()));
    if staging.exists() {
        let _ = fs::remove_dir_all(&staging);
    }
    fs::create_dir_all(&staging)?;

    let mut writer =
        StreamingPartWriter::create(&staging, stream_labels, metadata_keys, parsed_keys, row_group_size)?;
    let mut dropped = 0u64;
    let mut kept = 0u64;
    loop {
        let row = match rows.next_row() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(io::Error::other(error));
            }
        };
        if !keep(&row) {
            dropped += 1;
            continue;
        }
        kept += 1;
        if let Err(error) = writer.push(row) {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
    }
    if writer.is_empty() {
        let _ = fs::remove_dir_all(&staging);
        return Ok((Vec::new(), dropped, kept));
    }

    let part_id = gen_part_id(writer.min_ts_ns());
    if let Err(error) = writer.finish(&staging, &part_id, partition) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    if let Err(error) = write_merge_tombstone(&staging, parts_root, old_dirs) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }

    let final_dir = parts_root.join(partition).join(&part_id);
    if let Some(parent) = final_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    if final_dir.exists() {
        let _ = fs::remove_dir_all(&staging);
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("part dir already exists: {}", final_dir.display()),
        ));
    }
    fs::rename(&staging, &final_dir)?;
    if let Some(parent) = final_dir.parent() {
        fsync_dir(parent)?;
    }
    fsync_dir(parts_root)?;
    let part = load_part(&final_dir).map_err(|error| {
        rollback_committed(std::slice::from_ref(&final_dir));
        io::Error::other(error)
    })?;
    Ok((vec![part], dropped, kept))
}

fn flush_rows_internal(
    rows: Vec<Row>,
    parts_root: &Path,
    row_group_size: usize,
    merge_old_dirs: Option<&[PathBuf]>,
) -> io::Result<Vec<Part>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let tmp_root = parts_root.join(".tmp");
    fs::create_dir_all(&tmp_root)?;

    let mut by_partition: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for row in rows {
        let p = partition_of(row.timestamp_ns);
        by_partition.entry(p).or_default().push(row);
    }

    let mut parts = Vec::new();
    let mut committed_dirs: Vec<PathBuf> = Vec::new();
    for (partition, mut part_rows) in by_partition {
        sort_rows(&mut part_rows);
        let part_id = gen_part_id(
            part_rows
                .iter()
                .map(|row| row.timestamp_ns)
                .min()
                .unwrap_or_default(),
        );

        let tmp_dir = tmp_root.join(&part_id);
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }
        if let Err(e) = fs::create_dir_all(&tmp_dir) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        let stream_labels = collect_stream_labels(&part_rows);
        let metadata_columns = select_metadata_columns(metadata_column_counts(&part_rows));
        let parsed_rows = parse_rows(&part_rows);
        let parsed_columns = select_metadata_columns(parsed_column_counts(&parsed_rows));
        if let Err(e) = write_part_files(
            &tmp_dir,
            &part_id,
            &partition,
            &part_rows,
            &parsed_rows,
            &stream_labels,
            &metadata_columns,
            &parsed_columns,
            row_group_size,
        ) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        if let Some(old_dirs) = merge_old_dirs
            && let Err(e) = write_merge_tombstone(&tmp_dir, parts_root, old_dirs)
        {
            let _ = fs::remove_dir_all(&tmp_dir);
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        let final_dir = parts_root.join(&partition).join(&part_id);
        if let Some(parent) = final_dir.parent()
            && let Err(e) = fs::create_dir_all(parent)
        {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        if final_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
            rollback_committed(&committed_dirs);
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("part dir already exists: {}", final_dir.display()),
            ));
        }
        if let Err(e) = fs::rename(&tmp_dir, &final_dir) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        // Fsync the parent (partition) directory and parts_root to make the rename durable.
        if let Some(parent) = final_dir.parent()
            && let Err(e) = fsync_dir(parent)
        {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        if let Err(e) = fsync_dir(parts_root) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }
        committed_dirs.push(final_dir.clone());

        let part = match load_part(&final_dir) {
            Ok(p) => p,
            Err(e) => {
                rollback_committed(&committed_dirs);
                return Err(io::Error::other(e));
            }
        };
        parts.push(part);
    }
    Ok(parts)
}

fn rollback_committed(committed_dirs: &[PathBuf]) {
    for dir in committed_dirs.iter().rev() {
        if dir.exists()
            && let Err(e) = fs::remove_dir_all(dir)
        {
            tracing::warn!(error = %e, ?dir, "rollback: failed to remove committed part dir");
        }
    }
}

fn collect_stream_labels(rows: &[Row]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for r in rows {
        for k in r.labels.keys() {
            set.insert(k.clone());
        }
    }
    set.into_iter().collect()
}

/// How many rows of the part carry each metadata key.
fn metadata_column_counts(rows: &[Row]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for row in rows {
        for (name, _) in &row.structured_metadata {
            *counts.entry(name.clone()).or_default() += 1;
        }
    }
    counts
}

/// One `| json` extraction per row, shared by the column-key selection, the
/// column fill and nothing else — the parse was the whole added write cost of
/// the `_pf:` columns, and running it once instead of twice is half of it
/// back.
fn parse_rows(rows: &[Row]) -> Vec<Option<BTreeMap<String, String>>> {
    rows.iter()
        .map(|row| crate::logql::parsed_json_fields(&row.line))
        .collect()
}

/// How many rows' lines `| json` would extract each field from.
fn parsed_column_counts(parsed: &[Option<BTreeMap<String, String>>]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for fields in parsed.iter().flatten() {
        for name in fields.keys() {
            *counts.entry(name.clone()).or_default() += 1;
        }
    }
    counts
}

/// Which metadata keys become columns in this part: the `MAX_METADATA_COLUMNS`
/// most frequent, returned **sorted by key** with their row counts.
///
/// Frequency rather than first-N-alphabetical, so an adversarial tenant
/// churning key names cannot push `trace_id` out of a column; ties break on
/// the key so the choice is deterministic. The counts travel into `meta.json`
/// because a merge must choose its output's columns *before* reading a row —
/// the same constraint that makes `stream_labels` a union of the inputs' metas
/// — and summing recorded counts is what keeps that choice deterministic too.
/// Keys past the cap stay in the residual blob column; the invariant the read
/// path relies on is that a columnized key never also appears in the residual.
pub(crate) fn select_metadata_columns(counts: BTreeMap<String, u64>) -> Vec<(String, u64)> {
    let mut by_count: Vec<(String, u64)> = counts.into_iter().collect();
    by_count.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    by_count.truncate(MAX_METADATA_COLUMNS);
    by_count.sort_by(|a, b| a.0.cmp(&b.0));
    by_count
}

#[allow(clippy::too_many_arguments)]
fn write_part_files(
    dir: &Path,
    id: &str,
    partition: &str,
    rows: &[Row],
    parsed_rows: &[Option<BTreeMap<String, String>>],
    stream_labels: &[String],
    metadata_columns: &[(String, u64)],
    parsed_columns: &[(String, u64)],
    row_group_size: usize,
) -> io::Result<()> {
    write_parquet(
        &dir.join(DATA_FILE),
        rows,
        parsed_rows,
        stream_labels,
        metadata_columns,
        parsed_columns,
        row_group_size,
    )?;
    write_index(&dir.join(INDEX_FILE), rows, row_group_size, stream_labels)?;
    write_meta(
        &dir.join(META_FILE),
        id,
        partition,
        rows,
        row_group_size,
        stream_labels,
        metadata_columns,
        parsed_columns,
    )?;
    Ok(())
}

/// Row-group boundaries for a `(tenant, labels, timestamp)`-sorted row set.
///
/// A row group never spans two tenants, and holds a contiguous run of whole
/// streams rather than one stream each.
///
/// Cutting on every stream change was tried and measured worse: with 128
/// streams across 8 parts it turned roughly 3 row groups per part into 128, all
/// far under `row_group_size`, and the per-group cost swamped the pruning win —
/// `label_only` forward went from 2.04 ms to 5.19 ms while reading half the
/// rows. The selectivity does not come from the cut; it comes from the sort
/// order. Rows ordered by stream before time make a stream contiguous, so it
/// touches one or two groups instead of all of them, and the groups stay the
/// size they were.
fn row_group_bounds(rows: &[Row], row_group_size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut segment_start = 0usize;
    while segment_start < rows.len() {
        let mut segment_end = segment_start;
        while segment_end < rows.len() && rows[segment_end].tenant == rows[segment_start].tenant {
            segment_end += 1;
        }
        let mut start = segment_start;
        while start < segment_end {
            let end = (start + row_group_size).min(segment_end);
            out.push((start, end));
            start = end;
        }
        segment_start = segment_end;
    }
    out
}

/// The Parquet column a metadata key is stored in.
///
/// The `_sm:` prefix keeps the metadata namespace disjoint from the label
/// columns that precede it in the schema: `:` cannot appear in a validated
/// label name, and metadata keys are arbitrary strings (OTLP attributes keep
/// their dots), so the mapping is injective in both directions.
fn metadata_column_name(key: &str) -> String {
    format!("_sm:{key}")
}

/// The Parquet column a `| json`-extracted field is stored in. Same
/// injectivity argument as [`metadata_column_name`].
fn parsed_column_name(key: &str) -> String {
    format!("_pf:{key}")
}

fn part_schema(
    stream_labels: &[String],
    metadata_keys: &[String],
    parsed_keys: &[String],
) -> Arc<Schema> {
    let mut fields = vec![
        Field::new(TENANT_COLUMN, DataType::Utf8, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("_msg", DataType::Utf8, false),
    ];
    for label in stream_labels {
        fields.push(Field::new(label, DataType::Utf8, true));
    }
    for key in metadata_keys {
        fields.push(Field::new(metadata_column_name(key), DataType::Utf8, true));
    }
    for key in parsed_keys {
        fields.push(Field::new(parsed_column_name(key), DataType::Utf8, true));
    }
    fields.push(Field::new("structured_metadata", DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

fn row_group_batch(
    schema: &Arc<Schema>,
    rows: &[Row],
    parsed_rows: &[Option<BTreeMap<String, String>>],
    stream_labels: &[String],
    metadata_keys: &[String],
    parsed_keys: &[String],
) -> io::Result<RecordBatch> {
    let tenants: Vec<&str> = rows.iter().map(|r| r.tenant.as_str()).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.timestamp_ns).collect();
    let msg: Vec<&str> = rows.iter().map(|r| r.line.as_str()).collect();
    // The residual: only the pairs whose key did not make a column. Most rows
    // have none, so the common write path runs no serde at all — and the read
    // path depends on the split being exact: a columnized key must never also
    // appear in a row's residual.
    let sm: Vec<Option<String>> = rows
        .iter()
        .map(|r| {
            debug_assert!(
                r.structured_metadata
                    .windows(2)
                    .all(|pair| pair[0].0 < pair[1].0),
                "structured metadata must be canonical before it reaches a part"
            );
            let residual: Vec<&(String, String)> = r
                .structured_metadata
                .iter()
                .filter(|(name, _)| metadata_keys.binary_search(name).is_err())
                .collect();
            if residual.is_empty() {
                None
            } else {
                serde_json::to_string(&residual).ok()
            }
        })
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(tenants)),
        Arc::new(Int64Array::from(ts)),
        Arc::new(StringArray::from(msg)),
    ];
    for label in stream_labels {
        let vals: Vec<Option<&str>> = rows
            .iter()
            .map(|r| r.labels.get(label).map(|s| s.as_str()))
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    for key in metadata_keys {
        let vals: Vec<Option<&str>> = rows
            .iter()
            .map(|r| {
                r.structured_metadata
                    .binary_search_by(|(name, _)| name.as_str().cmp(key))
                    .ok()
                    .map(|index| r.structured_metadata[index].1.as_str())
            })
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    for key in parsed_keys {
        let vals: Vec<Option<&str>> = parsed_rows
            .iter()
            .map(|fields| {
                fields
                    .as_ref()
                    .and_then(|fields| fields.get(key))
                    .map(String::as_str)
            })
            .collect();
        columns.push(Arc::new(StringArray::from(vals)));
    }
    columns.push(Arc::new(StringArray::from(sm)));

    RecordBatch::try_new(schema.clone(), columns).map_err(io::Error::other)
}

/// The one place both writers get their Parquet properties, so the streaming
/// and the batch path cannot drift into different files.
///
/// Statistics are per column on purpose: `timestamp_ns` keeps page-level
/// min/max because time is the axis the page index can actually prune — rows
/// are piecewise time-ordered inside a group. Every string column stays at
/// chunk level: a page's min/max over random hex like `trace_id` spans the
/// alphabet and prunes nothing, and the page index would carry (and load)
/// those dead bounds for every page of every wide column.
fn part_writer_properties(row_group_size: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_column_statistics_enabled(ColumnPath::from("timestamp_ns"), EnabledStatistics::Page)
        // Without a row bound a whole 8192-row group's timestamp chunk fits
        // one default-sized page, and one page per chunk makes the page index
        // exactly as coarse as the group bounds it exists to refine — measured
        // as a two-second window costing what the whole range costs. At 1024
        // rows a page, a stream's run inside a group carries usefully tight
        // per-page time bounds; the cost is more pages everywhere, which the
        // write bench and the disk axis price.
        .set_data_page_row_count_limit(1024)
        .build()
}

#[allow(clippy::too_many_arguments)]
fn write_parquet(
    path: &Path,
    rows: &[Row],
    parsed_rows: &[Option<BTreeMap<String, String>>],
    stream_labels: &[String],
    metadata_columns: &[(String, u64)],
    parsed_columns: &[(String, u64)],
    row_group_size: usize,
) -> io::Result<()> {
    let metadata_keys: Vec<String> = metadata_columns
        .iter()
        .map(|(key, _)| key.clone())
        .collect();
    let parsed_keys: Vec<String> = parsed_columns.iter().map(|(key, _)| key.clone()).collect();
    let schema = part_schema(stream_labels, &metadata_keys, &parsed_keys);
    let bounds = row_group_bounds(rows, row_group_size);

    let file = fs::File::create(path)?;
    let props = part_writer_properties(row_group_size);
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(io::Error::other)?;
    // The sidecars address row groups by ordinal, so the Parquet row groups
    // must be exactly `bounds`. Flushing after each batch pins the boundary
    // instead of letting the writer choose one that straddles a tenant.
    for (start, end) in &bounds {
        let batch = row_group_batch(
            &schema,
            &rows[*start..*end],
            &parsed_rows[*start..*end],
            stream_labels,
            &metadata_keys,
            &parsed_keys,
        )?;
        writer.write(&batch).map_err(io::Error::other)?;
        writer.flush().map_err(io::Error::other)?;
    }
    writer.close().map_err(io::Error::other)?;
    sync_file(path)?;
    Ok(())
}

fn encode_blooms(rows: &[Row], row_group_size: usize) -> io::Result<Vec<u8>> {
    let bounds = row_group_bounds(rows, row_group_size);
    let mut buf = Vec::new();
    buf.extend_from_slice(BLOOM_MAGIC);
    buf.extend_from_slice(&(bounds.len() as u32).to_le_bytes());
    for (start, end) in &bounds {
        buf.extend_from_slice(&encode_group_blooms(&rows[*start..*end])?);
    }
    Ok(buf)
}

/// One row group's two filters, length-prefixed, exactly as they sit in the
/// `BTF4` section.
///
/// Split out of [`encode_blooms`] so a writer that never holds the whole part
/// can produce the same bytes a row group at a time. The batch path above still
/// calls it, so the existing part-format tests are what prove the two agree.
fn encode_group_blooms(rows: &[Row]) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut unique_trigrams: BTreeSet<[u8; 3]> = BTreeSet::new();
    // One pass. Sizing the filter needs the token count and filling it needs
    // the tokens, and this used to be two passes that each ran the JSON and
    // logfmt parsers over every line — the whole parse, twice, for a count.
    // The tokens are collected once instead; the scratch lives for one row
    // group and its capacity is exactly the count the two-pass version
    // computed, so the encoded filter is byte-identical.
    let mut exact_tokens: Vec<Vec<u8>> = Vec::new();
    for row in rows {
        for tri in crate::bloom::trigrams(&row.line) {
            unique_trigrams.insert(tri);
        }
        for (name, value) in &row.structured_metadata {
            for value in crate::logql::canonical_index_values(value) {
                exact_tokens.push(encode_exact_field_token(name, &value)?);
            }
        }
        for (name, values) in crate::logql::indexed_parser_fields(&row.line) {
            for value in values {
                for value in crate::logql::canonical_index_values(&value) {
                    exact_tokens.push(encode_exact_field_token(&name, &value)?);
                }
            }
        }
    }
    // No token to index means no filter. A filter built for zero items
    // still costs the `optimal_bits` floor, and an all-zero filter and an
    // absent one prune identically — the absent one just does it without
    // occupying 140 bytes in every row group of every part.
    let mut exact_fields =
        (!exact_tokens.is_empty()).then(|| BloomFilter::with_capacity(exact_tokens.len(), 0.01));
    if let Some(exact_fields) = &mut exact_fields {
        for token in &exact_tokens {
            exact_fields.insert(token);
        }
    }
    let estimated_items = unique_trigrams.len().max(1);
    let mut bloom = BloomFilter::with_capacity(estimated_items, 0.01);
    for tri in &unique_trigrams {
        bloom.insert(tri);
    }
    let bytes = bloom.encode();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bytes);
    let bytes = exact_fields
        .map(|exact_fields| exact_fields.encode())
        .unwrap_or_default();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bytes);
    Ok(buf)
}
