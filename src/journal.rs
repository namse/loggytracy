use std::io::Error as IoError;
use std::path::Path;
use std::time::Duration;

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;

const RECORD_HEADER_SIZE: usize = 8;

struct PendingWrite {
    data: Vec<u8>,
    done: oneshot::Sender<Result<(), IoError>>,
}

pub struct Journal {
    tx: mpsc::Sender<PendingWrite>,
}

impl Journal {
    pub fn spawn(config: &Config) -> Result<Self, IoError> {
        let dir = &config.data_dir;
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join("journal.wal");

        let (tx, rx) = mpsc::channel::<PendingWrite>(4096);

        let max_batch_bytes = config.max_batch_bytes;
        let max_batch_ms = config.max_batch_ms;

        tokio::spawn(async move {
            if let Err(e) = writer_loop(rx, &wal_path, max_batch_bytes, max_batch_ms).await {
                tracing::error!(error = %e, "journal writer terminated");
            }
        });

        Ok(Self { tx })
    }

    pub async fn append(&self, data: Vec<u8>) -> Result<(), IoError> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(PendingWrite { data, done: done_tx })
            .await
            .map_err(|_| {
                IoError::new(std::io::ErrorKind::BrokenPipe, "journal writer closed")
            })?;
        match done_rx.await {
            Ok(result) => result,
            Err(_) => Err(IoError::new(
                std::io::ErrorKind::BrokenPipe,
                "journal writer dropped",
            )),
        }
    }
}

async fn writer_loop(
    mut rx: mpsc::Receiver<PendingWrite>,
    path: &Path,
    max_batch_bytes: usize,
    max_batch_ms: u64,
) -> Result<(), IoError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let mut good_len = file.metadata().await?.len();

    let mut batch: Vec<PendingWrite> = Vec::new();
    let mut batch_bytes = 0usize;

    loop {
        let first = match rx.recv().await {
            Some(w) => w,
            None => break,
        };
        batch_bytes += first.data.len();
        batch.push(first);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(max_batch_ms);
        let mut closed = false;
        while batch_bytes < max_batch_bytes {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(w)) => {
                    batch_bytes += w.data.len();
                    batch.push(w);
                }
                Ok(None) => {
                    closed = true;
                    break;
                }
                Err(_) => break,
            }
        }

        let mut buf = Vec::with_capacity(batch_bytes + batch.len() * RECORD_HEADER_SIZE);
        for w in &batch {
            let len = w.data.len() as u32;
            let crc = crc32fast::hash(&w.data);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&crc.to_le_bytes());
            buf.extend_from_slice(&w.data);
        }

        let write_result = async {
            file.write_all(&buf).await?;
            file.flush().await?;
            file.sync_all().await
        }
        .await;

        let ack: Result<(), &IoError> = write_result.as_ref().map(|_| ());
        for w in batch.drain(..) {
            let _ = w
                .done
                .send(ack.map_err(|e| IoError::new(e.kind(), e.to_string())));
        }
        batch_bytes = 0;

        match write_result {
            Ok(()) => {
                good_len += buf.len() as u64;
            }
            Err(e) => {
                tracing::error!(error = %e, "journal write failed, truncating partial record");
                let recovered = async {
                    file.set_len(good_len).await?;
                    file.sync_all().await
                }
                .await;
                if let Err(te) = recovered {
                    tracing::error!(error = %te, "journal truncate failed, fencing writer");
                    return Err(te);
                }
            }
        }

        if closed {
            break;
        }
    }

    Ok(())
}
