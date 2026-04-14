pub mod codec;
pub mod record;

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
    task::JoinHandle,
};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Encoder},
    sync::CancellationToken,
};

use crate::{
    config::FsyncMode,
    store::{
        persistence::{codec::RecordCodec, record::Record},
        traits::Store,
        types::{Entry, Expiry},
    },
    utils::pkg::parse_version,
};

const MAGIC: &[u8] = env!("CARGO_PKG_NAME").as_bytes();
const VERSION: [u8; 3] = parse_version(env!("CARGO_PKG_VERSION"));
const HEADER_LEN: usize = MAGIC.len() + VERSION.len();

pub struct AofEngine {
    fsync_mode: FsyncMode,
    writer: Arc<Mutex<File>>,
    dirty: Arc<AtomicBool>,
}

impl AofEngine {
    pub async fn open(
        path: PathBuf,
        fsync_mode: FsyncMode,
        cancel: CancellationToken,
    ) -> Result<(Self, Option<JoinHandle<()>>)> {
        ensure_parent_dir(&path).await?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;

        let metadata = file.metadata().await?;
        if metadata.len() == 0 {
            file.write_all(MAGIC).await?;
            file.write_all(&VERSION).await?;
            if matches!(fsync_mode, FsyncMode::Always) {
                file.sync_data().await?;
            }
        }

        let writer = Arc::new(Mutex::new(file.try_clone().await?));
        let dirty = Arc::new(AtomicBool::new(false));

        let fsync_handle = if matches!(fsync_mode, FsyncMode::EverySec) {
            let writer = writer.clone();
            let dirty = dirty.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            if dirty.swap(false, Ordering::Relaxed) {
                                let file = writer.lock().await;
                                if let Err(err) = file.sync_data().await {
                                    eprintln!("AOF final fsync error: {err:?}");
                                }
                            }
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            if dirty.swap(false, Ordering::Relaxed) {
                                let file = writer.lock().await;
                                if let Err(err) = file.sync_data().await {
                                    eprintln!("AOF fsync error: {err:?}");
                                    dirty.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            }))
        } else {
            None
        };

        Ok((
            Self {
                fsync_mode,
                writer,
                dirty,
            },
            fsync_handle,
        ))
    }

    pub async fn append(&self, record: Record) -> Result<()> {
        let mut codec = RecordCodec;
        let mut frame = BytesMut::new();
        codec.encode(record, &mut frame)?;

        let mut file = self.writer.lock().await;
        file.write_all(&frame).await?;

        if matches!(self.fsync_mode, FsyncMode::Always) {
            file.sync_data().await?;
        }

        if matches!(self.fsync_mode, FsyncMode::EverySec) {
            self.dirty.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    pub async fn replay_into(&self, store: &impl Store) -> Result<()> {
        let mut file = self.writer.lock().await;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw).await?;
        file.seek(std::io::SeekFrom::End(0)).await?;

        if raw.is_empty() {
            return Ok(());
        }

        validate_header(&raw)?;

        let mut codec = RecordCodec;
        let mut buf = BytesMut::from(&raw[HEADER_LEN..]);

        while let Some(record) = codec.decode(&mut buf)? {
            apply_record(store, record).await?;
        }

        if !buf.is_empty() {
            eprintln!(
                "warning: truncated AOF tail detected ({} bytes ignored)",
                buf.len()
            );
        }

        Ok(())
    }
}

async fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

fn validate_header(raw: &[u8]) -> Result<()> {
    if raw.len() < HEADER_LEN {
        return Err(anyhow!("invalid AOF: file too small for header"));
    }
    if &raw[..MAGIC.len()] != MAGIC {
        return Err(anyhow!("invalid AOF: bad magic"));
    }
    if raw[MAGIC.len()..HEADER_LEN] != VERSION {
        return Err(anyhow!("unsupported AOF version: {}", raw[MAGIC.len()]));
    }
    Ok(())
}

async fn apply_record(store: &impl Store, record: Record) -> Result<()> {
    match record {
        Record::Set { key, value, exp_ms } => {
            let exp = match exp_ms {
                Some(at) => Expiry::At(at),
                None => Expiry::None,
            };
            store.set(key, Entry { value, exp }).await;
        }
        Record::Del { keys } => {
            store.del(&keys).await;
        }
        Record::MSet { items } => {
            store.mset(&items).await;
        }
        Record::FlushDb => {
            store.clear().await;
        }
    }
    Ok(())
}
