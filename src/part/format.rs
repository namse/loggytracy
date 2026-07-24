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

pub fn rows_from_snapshot(snapshot: &HashMap<Labels, Vec<LogEntry>>) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::new();
    for (labels, entries) in snapshot {
        for e in entries {
            rows.push(Row::from_entry(labels, e));
        }
    }
    rows.sort_by_key(|r| r.timestamp_ns);
    rows
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
        part_rows.sort_by_key(|r| r.timestamp_ns);
        let part_id = gen_part_id(part_rows[0].timestamp_ns);

        let tmp_dir = tmp_root.join(&part_id);
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }
        if let Err(e) = fs::create_dir_all(&tmp_dir) {
            rollback_committed(&committed_dirs);
            return Err(e);
        }

        let stream_labels = collect_stream_labels(&part_rows);
        if let Err(e) = write_part_files(
            &tmp_dir,
            &part_id,
            &partition,
            &part_rows,
            &stream_labels,
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
        // rename의 내구성을 보장하기 위해 부모(파티션) 디렉터리와 parts_root를 fsync.
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

fn write_part_files(
    dir: &Path,
    id: &str,
    partition: &str,
    rows: &[Row],
    stream_labels: &[String],
    row_group_size: usize,
) -> io::Result<()> {
    write_parquet(&dir.join(DATA_FILE), rows, stream_labels, row_group_size)?;
    write_bloom(&dir.join(BLOOM_FILE), rows, row_group_size)?;
    write_stream_index(
        &dir.join(STREAM_INDEX_FILE),
        rows,
        row_group_size,
        stream_labels,
    )?;
    write_meta(
        &dir.join(META_FILE),
        id,
        partition,
        rows,
        row_group_size,
        stream_labels,
    )?;
    Ok(())
}

fn row_group_bounds(n: usize, row_group_size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + row_group_size).min(n);
        out.push((start, end));
        start = end;
    }
    out
}

fn write_parquet(
    path: &Path,
    rows: &[Row],
    stream_labels: &[String],
    row_group_size: usize,
) -> io::Result<()> {
    let mut fields = vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("_msg", DataType::Utf8, false),
    ];
    for label in stream_labels {
        fields.push(Field::new(label, DataType::Utf8, true));
    }
    fields.push(Field::new("structured_metadata", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(fields));

    let ts: Vec<i64> = rows.iter().map(|r| r.timestamp_ns).collect();
    let msg: Vec<&str> = rows.iter().map(|r| r.line.as_str()).collect();
    let sm: Vec<Option<String>> = rows
        .iter()
        .map(|r| {
            if r.structured_metadata.is_empty() {
                None
            } else {
                serde_json::to_string(&r.structured_metadata).ok()
            }
        })
        .collect();

    let mut columns: Vec<ArrayRef> = vec![
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
    columns.push(Arc::new(StringArray::from(sm)));

    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(io::Error::other)?;

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    writer.close().map_err(io::Error::other)?;
    sync_file(path)?;
    Ok(())
}

fn write_bloom(path: &Path, rows: &[Row], row_group_size: usize) -> io::Result<()> {
    let bounds = row_group_bounds(rows.len(), row_group_size);
    let mut buf = Vec::new();
    buf.extend_from_slice(BLOOM_MAGIC_V3);
    buf.extend_from_slice(&(bounds.len() as u32).to_le_bytes());
    for (start, end) in &bounds {
        let mut unique_trigrams: BTreeSet<[u8; 3]> = BTreeSet::new();
        // Count the actual indexed tokens instead of estimating from rows.
        // The second pass keeps the existing bounded-memory insertion path,
        // while sizing the filter for wide structured rows as well as sparse
        // rows.
        let mut exact_capacity = 0usize;
        for row in &rows[*start..*end] {
            for (_name, value) in &row.structured_metadata {
                exact_capacity = exact_capacity
                    .saturating_add(crate::logql::canonical_index_values(value).len());
            }
            for (_name, values) in crate::logql::indexed_parser_fields(&row.line) {
                for value in values {
                    exact_capacity = exact_capacity
                        .saturating_add(crate::logql::canonical_index_values(&value).len());
                }
            }
        }
        let exact_capacity = exact_capacity.max(1);
        let mut exact_fields = BloomFilter::with_capacity(exact_capacity, 0.01);
        for row in &rows[*start..*end] {
            for tri in crate::bloom::trigrams(&row.line) {
                unique_trigrams.insert(tri);
            }
            for (name, value) in &row.structured_metadata {
                for value in crate::logql::canonical_index_values(value) {
                    exact_fields.insert(&encode_exact_field_token(name, &value)?);
                }
            }
            for (name, values) in crate::logql::indexed_parser_fields(&row.line) {
                for value in values {
                    for value in crate::logql::canonical_index_values(&value) {
                        exact_fields.insert(&encode_exact_field_token(&name, &value)?);
                    }
                }
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
        let bytes = exact_fields.encode();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
}

