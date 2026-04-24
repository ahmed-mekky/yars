pub mod aof;
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
    fsync_mode: std::sync::Mutex<FsyncMode>,
    writer: Arc<Mutex<File>>,
    dirty: Arc<AtomicBool>,
    fsync_handle: std::sync::Mutex<Option<JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl AofEngine {
    pub async fn open(path: PathBuf, fsync_mode: FsyncMode) -> Result<Self> {
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
        let cancel = CancellationToken::new();

        let fsync_handle = if matches!(fsync_mode, FsyncMode::EverySec) {
            let writer = writer.clone();
            let dirty = dirty.clone();
            let cancel_clone = cancel.clone();
            Some(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_clone.cancelled() => {
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

        Ok(Self {
            fsync_mode: std::sync::Mutex::new(fsync_mode),
            writer,
            dirty,
            fsync_handle: std::sync::Mutex::new(fsync_handle),
            cancel,
        })
    }

    pub async fn append(&self, record: Record) -> Result<()> {
        let mut codec = RecordCodec;
        let mut frame = BytesMut::new();
        codec.encode(record, &mut frame)?;

        let mut file = self.writer.lock().await;
        file.write_all(&frame).await?;

        let fsync_mode = *self.fsync_mode.lock().unwrap();
        if matches!(fsync_mode, FsyncMode::Always) {
            file.sync_data().await?;
        }

        if matches!(fsync_mode, FsyncMode::EverySec) {
            self.dirty.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn set_fsync_mode(&self, mode: FsyncMode) {
        *self.fsync_mode.lock().unwrap() = mode;
    }

    pub async fn replay_into(&self, store: &dyn Store) -> Result<()> {
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

    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let handle = self.fsync_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.await.ok();
        }
    }
}

#[async_trait::async_trait]
impl aof::Aof for AofEngine {
    async fn append(&self, record: Record) -> Result<()> {
        self.append(record).await
    }

    async fn replay_into(&self, store: &dyn Store) -> Result<()> {
        self.replay_into(store).await
    }

    fn set_fsync_mode(&self, mode: FsyncMode) {
        self.set_fsync_mode(mode);
    }

    async fn shutdown(&self) {
        self.shutdown().await;
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

async fn apply_record(store: &dyn Store, record: Record) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;
    use tokio_util::bytes::Bytes;

    #[tokio::test]
    async fn open_creates_header_on_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let engine = AofEngine::open(path.clone(), FsyncMode::No).await.unwrap();

        drop(engine);

        let raw = std::fs::read(&path).unwrap();
        assert!(raw.len() >= HEADER_LEN);
        assert_eq!(&raw[..MAGIC.len()], MAGIC);
        assert_eq!(&raw[MAGIC.len()..HEADER_LEN], VERSION);
    }

    #[tokio::test]
    async fn append_and_replay_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let engine = AofEngine::open(path.clone(), FsyncMode::No).await.unwrap();

        engine
            .append(Record::Set {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
                exp_ms: None,
            })
            .await
            .unwrap();

        engine
            .append(Record::Del {
                keys: vec![Bytes::from_static(b"k1")],
            })
            .await
            .unwrap();

        let store = MemoryStore::new();
        engine.replay_into(&store).await.unwrap();
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn replay_applies_set_and_mset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let engine = AofEngine::open(path.clone(), FsyncMode::No).await.unwrap();

        engine
            .append(Record::Set {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"1"),
                exp_ms: None,
            })
            .await
            .unwrap();

        engine
            .append(Record::MSet {
                items: vec![
                    (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
                    (Bytes::from_static(b"c"), Bytes::from_static(b"3")),
                ],
            })
            .await
            .unwrap();

        let store = MemoryStore::new();
        engine.replay_into(&store).await.unwrap();
        assert_eq!(store.len().await, 3);
    }

    #[tokio::test]
    async fn replay_applies_flushdb() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.aof");
        let engine = AofEngine::open(path.clone(), FsyncMode::No).await.unwrap();

        engine
            .append(Record::Set {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"1"),
                exp_ms: None,
            })
            .await
            .unwrap();
        engine.append(Record::FlushDb).await.unwrap();

        let store = MemoryStore::new();
        engine.replay_into(&store).await.unwrap();
        assert!(store.is_empty().await);
    }

    #[test]
    fn validate_header_too_short() {
        assert!(validate_header(b"short").is_err());
    }

    #[test]
    fn validate_header_bad_magic() {
        let mut raw = vec![0u8; HEADER_LEN];
        raw[MAGIC.len()..HEADER_LEN].copy_from_slice(&VERSION);
        assert!(validate_header(&raw).is_err());
    }

    #[test]
    fn validate_header_bad_version() {
        let mut raw = Vec::from(MAGIC);
        raw.extend_from_slice(&[99, 99, 99]);
        assert!(validate_header(&raw).is_err());
    }

    #[test]
    fn validate_header_ok() {
        let mut raw = Vec::from(MAGIC);
        raw.extend_from_slice(&VERSION);
        assert!(validate_header(&raw).is_ok());
    }
}
