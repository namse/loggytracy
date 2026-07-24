#[allow(dead_code)]
pub fn replay(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
) -> Result<(u64, u64), String> {
    let traces = TraceMemTable::new();
    replay_with_traces(wal_path, ckpt_path, memtable, &traces)
}

pub fn replay_with_traces(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
    trace_memtable: &TraceMemTable,
) -> Result<(u64, u64), String> {
    recover_unfinished_compaction(wal_path, ckpt_path).map_err(|e| e.to_string())?;
    let checkpoint = read_checkpoint(ckpt_path).map_err(|e| e.to_string())?;
    replay_from(wal_path, checkpoint, memtable, trace_memtable).map(|end| (checkpoint, end))
}

fn recover_unfinished_compaction(wal_path: &Path, ckpt_path: &Path) -> Result<(), IoError> {
    let state_path = wal_path.with_file_name(COMPACTION_STATE_FILE);
    let Some(state) = read_compaction_state(&state_path)? else {
        return Ok(());
    };
    if state.phase != 1 {
        return Ok(());
    }
    let tmp_path = wal_path.with_extension("wal.compact.tmp");
    if !tmp_path.exists() {
        // The replacement WAL is already in place; replay its suffix from
        // checkpoint zero.
        return Ok(());
    }

    // Rename never committed. Restore the old checkpoint before replay so a
    // crash between checkpoint=0 and rename cannot replay flushed records.
    write_checkpoint(ckpt_path, state.offset)?;
    std::fs::remove_file(&tmp_path)?;
    std::fs::remove_file(&state_path)?;
    if let Some(parent) = wal_path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn replay_from(
    wal_path: &Path,
    checkpoint: u64,
    memtable: &MemTable,
    trace_memtable: &TraceMemTable,
) -> Result<u64, String> {
    if !wal_path.exists() {
        if checkpoint == 0 {
            return Ok(0);
        }
        return Err(format!(
            "journal checkpoint {checkpoint} exists but WAL {} is missing",
            wal_path.display()
        ));
    }
    let mut file = std::fs::File::open(wal_path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    if checkpoint > file_len {
        return Err(format!(
            "journal checkpoint {checkpoint} is beyond WAL length {file_len}"
        ));
    }
    if checkpoint == file_len {
        return Ok(checkpoint);
    }
    file.seek(SeekFrom::Start(checkpoint))
        .map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(file);
    let mut offset = checkpoint;
    let mut replayed = 0u64;
    loop {
        let mut header = [0u8; RECORD_HEADER_SIZE];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.to_string()),
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let expected_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let record_end = offset
            .checked_add(RECORD_HEADER_SIZE as u64)
            .and_then(|end| end.checked_add(len as u64))
            .ok_or_else(|| format!("journal record length overflows at offset {offset}"))?;
        if record_end > file_len {
            tracing::warn!(
                offset,
                len,
                "journal partial record at tail, stopping replay"
            );
            break;
        }
        if len > MAX_RECORD_BYTES {
            return Err(format!(
                "journal record at offset {offset} is too large: {len} bytes (maximum {MAX_RECORD_BYTES})"
            ));
        }
        let mut data = vec![0u8; len];
        match reader.read_exact(&mut data) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                tracing::warn!(offset, "journal partial record at tail, stopping replay");
                break;
            }
            Err(e) => return Err(e.to_string()),
        }
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != expected_crc {
            if record_end == file_len {
                tracing::warn!(offset, "journal crc mismatch at tail, stopping replay");
                break;
            }
            return Err(format!("journal record crc mismatch at offset {offset}"));
        }
        if let Some(trace_data) = decode_trace_record(&data)? {
            let request = ExportTraceServiceRequest::decode(trace_data)
                .map_err(|e| format!("trace protobuf decode failed at offset {offset}: {e}"))?;
            let spans = normalize_request(request)
                .map_err(|e| format!("trace record invalid at offset {offset}: {e}"))?;
            trace_memtable.insert(spans);
        } else {
            match PushRequest::decode(data.as_slice()) {
                Ok(req) => {
                    for stream in &req.streams {
                        let labels = match proto::parse_labels(&stream.labels) {
                            Ok(l) => l,
                            Err(e) => {
                                return Err(format!(
                                    "journal record has invalid labels at offset {offset}: {e}"
                                ));
                            }
                        };
                        let entries: Vec<LogEntry> = stream
                        .entries
                        .iter()
                        .map(|e| {
                            let timestamp_ns = e.timestamp_ns().map_err(|error| {
                                format!(
                                    "journal record has invalid timestamp at offset {offset}: {error}"
                                )
                            })?;
                            Ok(LogEntry {
                                timestamp_ns,
                                line: e.line.clone(),
                                structured_metadata: e
                                    .structured_metadata
                                    .iter()
                                    .map(|lp| (lp.name.clone(), lp.value.clone()))
                                    .collect(),
                            })
                        })
                        .collect::<Result<_, String>>()?;
                        memtable.insert(labels, entries);
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "journal protobuf decode failed at offset {offset}: {e}"
                    ));
                }
            }
        }
        offset += (RECORD_HEADER_SIZE + len) as u64;
        replayed += 1;
    }
    if replayed > 0 {
        tracing::info!(replayed, offset, "journal replay complete");
    }
    Ok(offset)
}

fn decode_trace_record(data: &[u8]) -> Result<Option<&[u8]>, String> {
    if !data.starts_with(TRACE_RECORD_MAGIC) {
        return Ok(None);
    }
    if data.len() <= TRACE_RECORD_MAGIC.len() {
        return Err("trace journal record is missing its version".to_string());
    }
    if data[TRACE_RECORD_MAGIC.len()] != TRACE_RECORD_VERSION {
        return Err(format!(
            "unsupported trace journal record version {}",
            data[TRACE_RECORD_MAGIC.len()]
        ));
    }
    Ok(Some(&data[TRACE_RECORD_MAGIC.len() + 1..]))
}
