impl Journal {
    pub fn spawn(config: &Config, memtable: Arc<MemTable>) -> Result<Self, IoError> {
        Self::spawn_with_traces(config, memtable, Arc::new(TraceMemTable::new()))
    }

    pub fn spawn_with_traces(
        config: &Config,
        memtable: Arc<MemTable>,
        trace_memtable: Arc<TraceMemTable>,
    ) -> Result<Self, IoError> {
        let dir = &config.data_dir;
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join(WAL_FILE);
        let ckpt_path = dir.join(CKPT_FILE);

        // Initialize the WAL synchronously so startup/readiness cannot race a
        // failed open in the background writer. Sync both the empty file and
        // its parent directory: fsyncing a newly-created file alone does not
        // make its directory entry crash-durable on POSIX filesystems.
        let wal_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        wal_file.sync_all()?;
        std::fs::File::open(dir)?.sync_all()?;
        drop(wal_file);

        let (tx, rx) = mpsc::channel::<JournalCmd>(4096);

        let max_batch_bytes = config.max_batch_bytes;
        let max_batch_ms = config.max_batch_ms;
        let healthy = Arc::new(AtomicBool::new(true));
        let backlog = Arc::new(WalBacklog::default());
        backlog.set_wal_bytes(std::fs::metadata(&wal_path)?.len());
        backlog.set_checkpoint_bytes(read_checkpoint(&ckpt_path)?);

        let wal_path_clone = wal_path.clone();
        let ckpt_path_clone = ckpt_path.clone();
        let writer_health = healthy.clone();
        let trace_memtable_clone = trace_memtable.clone();
        let writer_backlog = backlog.clone();
        let writer_memtable = memtable.clone();
        tokio::spawn(async move {
            let result = writer_loop(
                rx,
                &wal_path_clone,
                &ckpt_path_clone,
                writer_memtable,
                trace_memtable_clone,
                max_batch_bytes,
                max_batch_ms,
                &writer_backlog,
            )
            .await;
            writer_health.store(false, Ordering::Release);
            if let Err(e) = result {
                tracing::error!(error = %e, "journal writer terminated");
            }
        });

        Ok(Self {
            tx,
            wal_path,
            ckpt_path,
            healthy,
            memtable,
            trace_memtable,
            backlog,
        })
    }

    pub fn wal_backlog_bytes(&self) -> u64 {
        self.backlog.bytes()
    }

    /// The memtable this journal feeds. Held so the ingest gate can size both
    /// buffers from the journal alone, rather than every caller having to
    /// thread the log memtable in beside it.
    pub fn log_memtable(&self) -> &Arc<MemTable> {
        &self.memtable
    }

    /// An OTLP log export: `data` is the encoded `ExportLogsServiceRequest`
    /// itself — for the HTTP protobuf transport the received body verbatim —
    /// and `streams` is what `otlp_log::normalize_request` already produced
    /// for the memtable, so nothing is normalized twice and nothing is
    /// re-encoded into a second message for the WAL's sake.
    pub async fn append_otlp_logs(
        &self,
        tenant: TenantId,
        data: Vec<u8>,
        streams: Vec<(Labels, Vec<LogEntry>)>,
    ) -> Result<(), IoError> {
        let framed = {
            let _arena = crate::memprof::enter(crate::memprof::Arena::Wal);
            frame_tenant_record(&tenant, TENANT_RECORD_KIND_OTLP_LOGS, &data)
        };
        self.send_append(framed, tenant, streams, Vec::new()).await
    }

    pub async fn append_trace(
        &self,
        tenant: TenantId,
        data: Vec<u8>,
        spans: Vec<TraceSpan>,
    ) -> Result<(), IoError> {
        let framed = {
            let _arena = crate::memprof::enter(crate::memprof::Arena::Wal);
            frame_tenant_record(&tenant, TENANT_RECORD_KIND_TRACES, &data)
        };
        self.send_append(framed, tenant, Vec::new(), spans).await
    }

    async fn send_append(
        &self,
        data: Vec<u8>,
        tenant: TenantId,
        streams: Vec<(Labels, Vec<LogEntry>)>,
        traces: Vec<TraceSpan>,
    ) -> Result<(), IoError> {
        if data.len() > MAX_RECORD_BYTES {
            return Err(IoError::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "journal record is too large: {} bytes (maximum {})",
                    data.len(),
                    MAX_RECORD_BYTES
                ),
            ));
        }
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Append {
                data,
                tenant,
                streams,
                traces,
                done: done_tx,
            })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped",
            )),
        }
    }

    pub fn trace_memtable(&self) -> Arc<TraceMemTable> {
        self.trace_memtable.clone()
    }

    pub async fn checkpoint(&self) -> Result<CheckpointSnapshot, IoError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Checkpoint { done: done_tx })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped",
            )),
        }
    }

    pub fn set_checkpoint(&self, offset: u64) -> Result<(), IoError> {
        write_checkpoint(&self.ckpt_path, offset)?;
        self.backlog.set_checkpoint_bytes(offset);
        Ok(())
    }

    /// Drops the durable WAL prefix through `offset`. This command runs in
    /// the writer task, so appends that arrived after the flush snapshot are
    /// copied into the replacement WAL before new appends can proceed.
    pub async fn compact_checkpoint(&self, offset: u64) -> Result<(), IoError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(JournalCmd::Compact {
                offset,
                done: done_tx,
            })
            .await
            .map_err(|_| IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed"))?;
        done_rx.await.map_err(|_| {
            IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped during compaction",
            )
        })?
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn ckpt_path(&self) -> &Path {
        &self.ckpt_path
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
}


#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    mut rx: mpsc::Receiver<JournalCmd>,
    path: &Path,
    ckpt_path: &Path,
    memtable: Arc<MemTable>,
    trace_memtable: Arc<TraceMemTable>,
    max_batch_bytes: usize,
    max_batch_ms: u64,
    backlog: &WalBacklog,
) -> Result<(), IoError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let mut good_len = file.metadata().await?.len();
    backlog.set_wal_bytes(good_len);

    loop {
        let first = match rx.recv().await {
            Some(c) => c,
            None => break,
        };

        let mut pending_checkpoint: Option<oneshot::Sender<Result<CheckpointSnapshot, IoError>>> =
            None;
        let mut pending_compact: Option<(u64, oneshot::Sender<Result<(), IoError>>)> = None;

        let mut batch: Vec<AppendBatchItem> = Vec::new();
        let mut batch_bytes = 0usize;
        let mut closed = false;

        match first {
            JournalCmd::Append {
                data,
                tenant,
                streams,
                traces,
                done,
            } => {
                batch_bytes += data.len();
                batch.push((data, tenant, streams, traces, done));
                // Take what has already arrived and write it. Waiting for more
                // charged every push the full linger even when the channel was
                // empty, which fixed single-connection throughput at
                // 1000/max_batch_ms pushes per second regardless of load.
                //
                // Batching still happens, and happens where it should: this
                // task is busy writing and fsyncing, so everything that arrives
                // during that window is already queued when the next iteration
                // looks. Group commit forms behind the write rather than in
                // front of it.
                let deadline = tokio::time::Instant::now() + Duration::from_millis(max_batch_ms);
                while batch_bytes < max_batch_bytes {
                    let next = if max_batch_ms == 0 {
                        match rx.try_recv() {
                            Ok(command) => Ok(Some(command)),
                            Err(mpsc::error::TryRecvError::Empty) => Err(()),
                            Err(mpsc::error::TryRecvError::Disconnected) => Ok(None),
                        }
                    } else {
                        // A deliberate linger: trades latency for fewer fsyncs
                        // on a disk where an fsync costs more than the wait.
                        tokio::time::timeout_at(deadline, rx.recv())
                            .await
                            .map_err(|_| ())
                    };
                    match next {
                        Ok(Some(JournalCmd::Append {
                            data,
                            tenant,
                            streams,
                            traces,
                            done,
                        })) => {
                            batch_bytes += data.len();
                            batch.push((data, tenant, streams, traces, done));
                        }
                        Ok(Some(JournalCmd::Checkpoint { done })) => {
                            pending_checkpoint = Some(done);
                            break;
                        }
                        Ok(Some(JournalCmd::Compact { offset, done })) => {
                            pending_compact = Some((offset, done));
                            break;
                        }
                        Ok(None) => {
                            closed = true;
                            break;
                        }
                        Err(()) => break,
                    }
                }
            }
            JournalCmd::Checkpoint { done } => {
                pending_checkpoint = Some(done);
            }
            JournalCmd::Compact { offset, done } => {
                pending_compact = Some((offset, done));
            }
        }

        if !batch.is_empty() {
            let wal_arena = crate::memprof::enter(crate::memprof::Arena::Wal);
            let mut buf = Vec::with_capacity(batch_bytes + batch.len() * RECORD_HEADER_SIZE);
            for (data, _, _, _, _) in &batch {
                let len = u32::try_from(data.len()).map_err(|_| {
                    IoError::new(
                        std::io::ErrorKind::InvalidInput,
                        "journal record exceeds u32",
                    )
                })?;
                let crc = crc32fast::hash(data);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&crc.to_le_bytes());
                buf.extend_from_slice(data);
            }

            drop(wal_arena);
            let write_result = async {
                file.write_all(&buf).await?;
                file.flush().await?;
                file.sync_all().await
            }
            .await;

            match write_result {
                Ok(()) => {
                    good_len += buf.len() as u64;
                    // Published before the acks below: a client that sees its
                    // 204 must already be counted against the next request's
                    // backpressure gate.
                    backlog.set_wal_bytes(good_len);
                    // The entries were allocated under the ingest label and
                    // are moved, not copied, so the memtable's own nodes are
                    // charged to the same arena its contents already are.
                    let _arena = crate::memprof::enter(crate::memprof::Arena::Ingest);
                    for (_, tenant, streams, traces, done) in batch.drain(..) {
                        for (labels, entries) in streams {
                            memtable.insert(tenant.clone(), labels, entries);
                        }
                        trace_memtable.insert(traces);
                        let _ = done.send(Ok(()));
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "journal write failed, truncating partial record");
                    for (_, _, _, _, done) in batch.drain(..) {
                        let _ = done.send(Err(IoError::new(e.kind(), e.to_string())));
                    }
                    let recovered = async {
                        file.set_len(good_len).await?;
                        file.sync_all().await
                    }
                    .await;
                    if let Err(te) = recovered {
                        tracing::error!(error = %te, "journal truncate failed, fencing writer");
                        return Err(te);
                    }
                    backlog.set_wal_bytes(good_len);
                }
            }
        }

        if let Some(done) = pending_checkpoint {
            if let Err(e) = file.sync_all().await {
                let _ = done.send(Err(IoError::new(e.kind(), e.to_string())));
                return Err(e);
            }
            let offset = good_len;
            let snapshot = memtable.begin_flush();
            let trace_snapshot = trace_memtable.begin_flush();
            let _ = done.send(Ok(CheckpointSnapshot {
                offset,
                snapshot,
                trace_snapshot,
            }));
        }

        if let Some((offset, done)) = pending_compact {
            let result = compact_wal(&mut file, path, ckpt_path, offset, &mut good_len).await;
            match result {
                Ok(()) => {
                    // Compaction resets the checkpoint to zero and leaves only
                    // the retained suffix, so both halves of the backlog move.
                    backlog.set_checkpoint_bytes(0);
                    backlog.set_wal_bytes(good_len);
                    let _ = done.send(Ok(()));
                }
                Err(error) => {
                    let error_for_caller = IoError::new(error.kind(), error.to_string());
                    let _ = done.send(Err(error_for_caller));
                    // Compaction can fail before or after replacing the WAL
                    // (for example, a directory fsync can fail after rename).
                    // Reopen the path and continue serving appends so the
                    // caller can retry the same checkpoint instead of
                    // permanently fencing the journal writer.
                    let reopened = async {
                        let reopened = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(path)
                            .await?;
                        let length = reopened.metadata().await?.len();
                        Ok::<_, IoError>((reopened, length))
                    }
                    .await;
                    match reopened {
                        Ok((reopened, length)) => {
                            file = reopened;
                            good_len = length;
                            backlog.set_checkpoint_bytes(read_checkpoint(ckpt_path).unwrap_or(0));
                            backlog.set_wal_bytes(good_len);
                        }
                        Err(reopen_error) => {
                            tracing::error!(
                                error = %reopen_error,
                                "journal reopen after compaction failure failed; fencing writer"
                            );
                            return Err(reopen_error);
                        }
                    }
                }
            }
        }

        if closed {
            break;
        }
    }

    Ok(())
}


