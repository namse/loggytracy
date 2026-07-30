fn encode_exact_field_token(name: &str, value: &str) -> io::Result<Vec<u8>> {
    let name_len = u32::try_from(name.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field name is too large"))?;
    let value_len = u32::try_from(value.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field value is too large"))?;
    let capacity = EXACT_FIELD_TOKEN_MAGIC
        .len()
        .checked_add(1 + 4 + name.len() + 4 + value.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "field token is too large"))?;
    let mut token = Vec::with_capacity(capacity);
    token.extend_from_slice(EXACT_FIELD_TOKEN_MAGIC);
    token.push(EXACT_FIELD_SCALAR_SCOPE);
    token.extend_from_slice(&name_len.to_le_bytes());
    token.extend_from_slice(name.as_bytes());
    token.extend_from_slice(&value_len.to_le_bytes());
    token.extend_from_slice(value.as_bytes());
    Ok(token)
}

/// Writes the blooms and the stream index as two length-prefixed sections
/// behind one header, so a reader takes one file open and one checksum.
fn write_index(
    path: &Path,
    rows: &[Row],
    row_group_size: usize,
    stream_labels: &[String],
) -> io::Result<()> {
    let bloom = encode_blooms(rows, row_group_size)?;
    let streams = encode_stream_index(rows, row_group_size, stream_labels)?;
    write_index_sections(path, &bloom, &streams)
}

/// The container both writers emit: magic, then each section length-prefixed.
///
/// Shared so a streaming writer cannot lay the file out differently from the
/// batch one.
fn write_index_sections(path: &Path, bloom: &[u8], streams: &[u8]) -> io::Result<()> {
    let mut buf = Vec::with_capacity(INDEX_MAGIC.len() + 8 + bloom.len() + streams.len());
    buf.extend_from_slice(INDEX_MAGIC);
    for section in [bloom, streams] {
        let length = u32::try_from(section.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "index section is too large")
        })?;
        buf.extend_from_slice(&length.to_le_bytes());
        buf.extend_from_slice(section);
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
}

/// Splits a container back into `(blooms, stream index)`.
fn split_index(buf: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if !buf.starts_with(INDEX_MAGIC) {
        return Err("part index file has an unrecognized header".to_string());
    }
    let mut rest = &buf[INDEX_MAGIC.len()..];
    let mut sections: [&[u8]; 2] = [&[], &[]];
    for section in sections.iter_mut() {
        if rest.len() < 4 {
            return Err("part index file is truncated".to_string());
        }
        let length = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        rest = &rest[4..];
        if rest.len() < length {
            return Err("part index file is truncated".to_string());
        }
        *section = &rest[..length];
        rest = &rest[length..];
    }
    Ok((sections[0], sections[1]))
}

fn encode_stream_index(
    rows: &[Row],
    row_group_size: usize,
    stream_labels: &[String],
) -> io::Result<Vec<u8>> {
    let bounds = row_group_bounds(rows, row_group_size);
    // Keyed by borrows of the rows rather than by copies of them.
    // `BTreeMap::entry` needs an owned key, so this used to clone every label
    // name and every label value once **per row per label** to look up a
    // posting list that already existed.
    let mut index: BTreeMap<&str, BTreeMap<&str, RoaringBitmap>> = BTreeMap::new();
    for (rg, (start, end)) in bounds.iter().enumerate() {
        for row in &rows[*start..*end] {
            for label in stream_labels {
                if let Some(v) = row.labels.get(label) {
                    index
                        .entry(label.as_str())
                        .or_default()
                        .entry(v.as_str())
                        .or_default()
                        .insert(rg as u32);
                }
            }
        }
    }
    let entry_count: usize = index.values().map(|m| m.len()).sum();
    let entries = index.iter().flat_map(|(name, values)| {
        values
            .iter()
            .map(move |(value, bitmap)| (*name, *value, bitmap))
    });
    serialize_stream_index(entry_count, entries)
}

/// `SIX1` from `(label, value, postings)` in the order the reader expects.
///
/// Takes an iterator rather than the map so the streaming writer, whose keys
/// must be owned because its rows are gone, produces the same bytes as the
/// batch path, whose keys borrow the rows.
fn serialize_stream_index<'a>(
    entry_count: usize,
    entries: impl IntoIterator<Item = (&'a str, &'a str, &'a RoaringBitmap)>,
) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.extend_from_slice(STREAM_MAGIC);
    buf.extend_from_slice(&(entry_count as u32).to_le_bytes());
    for (name, value, bitmap) in entries {
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        let value_bytes = value.as_bytes();
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(value_bytes);
        let mut bm_bytes = Vec::new();
        bitmap
            .serialize_into(&mut bm_bytes)
            .map_err(io::Error::other)?;
        buf.extend_from_slice(&(bm_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bm_bytes);
    }
    Ok(buf)
}

