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

fn write_stream_index(
    path: &Path,
    rows: &[Row],
    row_group_size: usize,
    stream_labels: &[String],
) -> io::Result<()> {
    let bounds = row_group_bounds(rows, row_group_size);
    let mut index: StreamMap = BTreeMap::new();
    for (rg, (start, end)) in bounds.iter().enumerate() {
        for row in &rows[*start..*end] {
            for label in stream_labels {
                if let Some(v) = row.labels.get(label) {
                    index
                        .entry(label.clone())
                        .or_default()
                        .entry(v.clone())
                        .or_default()
                        .insert(rg as u32);
                }
            }
        }
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(STREAM_MAGIC);
    let entry_count: usize = index.values().map(|m| m.len()).sum();
    buf.extend_from_slice(&(entry_count as u32).to_le_bytes());
    for (name, values) in &index {
        for (value, bitmap) in values {
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
    }
    fs::write(path, &buf)?;
    sync_file(path)?;
    Ok(())
}

