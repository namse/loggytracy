#[allow(dead_code)]
pub fn replay(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
    default_tenant: &TenantId,
) -> Result<(u64, u64), String> {
    let traces = TraceMemTable::new();
    replay_with_traces(wal_path, ckpt_path, memtable, &traces, default_tenant)
}

/// What a replay put back, so a restart can be told from a clean start.
///
/// Delivery is at-least-once by design: the checkpoint advances after a flush,
/// so a crash in between leaves records in the WAL that are already durable in
/// parts, and replay writes them a second time. The copies are collapsed the
/// first time the two parts are merged, but until then they are two log lines,
/// and this is the only thing that says how many there could be — an operator
/// could not otherwise tell a restart that duplicated nothing from one that
/// duplicated a minute of logs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayReport {
    pub checkpoint: u64,
    pub end_offset: u64,
    pub records: u64,
    pub entries: u64,
}

pub fn replay_with_traces(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
    trace_memtable: &TraceMemTable,
    default_tenant: &TenantId,
) -> Result<(u64, u64), String> {
    replay_reporting(wal_path, ckpt_path, memtable, trace_memtable, default_tenant)
        .map(|report| (report.checkpoint, report.end_offset))
}

pub fn replay_reporting(
    wal_path: &Path,
    ckpt_path: &Path,
    memtable: &MemTable,
    trace_memtable: &TraceMemTable,
    default_tenant: &TenantId,
) -> Result<ReplayReport, String> {
    recover_unfinished_compaction(wal_path, ckpt_path).map_err(|e| e.to_string())?;
    let checkpoint = read_checkpoint(ckpt_path).map_err(|e| e.to_string())?;
    let mut report = ReplayReport {
        checkpoint,
        ..ReplayReport::default()
    };
    let end = replay_from(
        wal_path,
        checkpoint,
        memtable,
        trace_memtable,
        default_tenant,
        &mut report,
    )?;
    report.end_offset = end;
    Ok(report)
}

fn recover_unfinished_compaction(wal_path: &Path, ckpt_path: &Path) -> Result<(), IoError> {
    let state_path = wal_path.with_file_name(COMPACTION_STATE_FILE);
    let Some(state) = read_compaction_state(&state_path)? else {
        return Ok(());
    };
    let tmp_path = wal_path.with_extension("wal.compact.tmp");
    if !tmp_path.exists() {
        // The replacement WAL is already in place; replay its suffix from
        // checkpoint zero. The intent record still has to go, for the same
        // reason as above.
        remove_compaction_state(&state_path, wal_path)?;
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
    default_tenant: &TenantId,
    report: &mut ReplayReport,
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
        if let Some((tenant, kind, payload)) = decode_tenant_record(&data)
            .map_err(|error| format!("journal record invalid at offset {offset}: {error}"))?
        {
            match kind {
                TENANT_RECORD_KIND_LOGS => {
                    report.entries += replay_log_record(&tenant, payload, offset, memtable)?;
                }
                TENANT_RECORD_KIND_TRACES => {
                    replay_trace_record(&tenant, payload, offset, trace_memtable)?;
                }
                TENANT_RECORD_KIND_OTLP_LOGS => {
                    report.entries += replay_otlp_log_record(&tenant, payload, offset, memtable)?;
                }
                other => {
                    return Err(format!(
                        "unsupported tenant journal record kind {other} at offset {offset}"
                    ));
                }
            }
        } else {
            report.entries += replay_log_record(default_tenant, &data, offset, memtable)?;
        }
        offset += (RECORD_HEADER_SIZE + len) as u64;
        replayed += 1;
        report.records += 1;
    }
    if replayed > 0 {
        tracing::info!(replayed, offset, "journal replay complete");
    }
    Ok(offset)
}

/// Returns the entries put back, which is what an operator needs to size the
/// duplication a restart may have caused.
fn replay_log_record(
    tenant: &TenantId,
    payload: &[u8],
    offset: u64,
    memtable: &MemTable,
) -> Result<u64, String> {
    let request = PushRequest::decode(payload)
        .map_err(|e| format!("journal protobuf decode failed at offset {offset}: {e}"))?;
    let mut replayed_entries = 0u64;
    for stream in &request.streams {
        let labels = proto::parse_labels(&stream.labels)
            .map_err(|e| format!("journal record has invalid labels at offset {offset}: {e}"))?;
        let entries: Vec<LogEntry> = stream
            .entries
            .iter()
            .map(|e| {
                let timestamp_ns = e.timestamp_ns().map_err(|error| {
                    format!("journal record has invalid timestamp at offset {offset}: {error}")
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
            .collect::<Result<Vec<LogEntry>, String>>()?;
        replayed_entries += entries.len() as u64;
        memtable.insert(tenant.clone(), labels, entries);
    }
    Ok(replayed_entries)
}

/// The OTLP counterpart of `replay_log_record`: the payload is the export as
/// it arrived, normalized here exactly as ingest normalized it before the
/// crash. An `EmptyRequest` is skipped rather than fatal — a record was only
/// appended after normalizing non-empty, so hitting one here is a bug to log,
/// not a reason to refuse startup — while any other normalization failure is
/// the same hard error a corrupt kind-0 record is.
fn replay_otlp_log_record(
    tenant: &TenantId,
    payload: &[u8],
    offset: u64,
    memtable: &MemTable,
) -> Result<u64, String> {
    let request = ExportLogsServiceRequest::decode(payload)
        .map_err(|e| format!("OTLP log protobuf decode failed at offset {offset}: {e}"))?;
    let streams = match crate::otlp_log::normalize_request(&request) {
        Ok(streams) => streams,
        Err(crate::otlp_log::OtlpLogError::EmptyRequest) => {
            tracing::warn!(offset, "empty OTLP log record in journal, skipping");
            return Ok(0);
        }
        Err(e) => return Err(format!("OTLP log record invalid at offset {offset}: {e}")),
    };
    let mut replayed_entries = 0u64;
    for (labels, entries) in streams {
        replayed_entries += entries.len() as u64;
        memtable.insert(tenant.clone(), labels, entries);
    }
    Ok(replayed_entries)
}

fn replay_trace_record(
    tenant: &TenantId,
    payload: &[u8],
    offset: u64,
    trace_memtable: &TraceMemTable,
) -> Result<(), String> {
    let request = ExportTraceServiceRequest::decode(payload)
        .map_err(|e| format!("trace protobuf decode failed at offset {offset}: {e}"))?;
    let spans = normalize_request(tenant, request)
        .map_err(|e| format!("trace record invalid at offset {offset}: {e}"))?;
    trace_memtable.insert(spans);
    Ok(())
}

