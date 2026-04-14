pub mod codec;
pub mod record;

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};
use tokio_util::{bytes::BytesMut, codec::Decoder, codec::Encoder};

use crate::{
    config::FsyncMode,
    store::{
        persistence::{codec::RecordCodec, record::Record},
        traits::Store,
        types::{Entry, Expiry},
    },
};

const MAGIC: &[u8; 4] = b"YARS";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 5;

pub struct AofEngine {
    fsync_mode: FsyncMode,
    writer: Mutex<File>,
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
            file.write_all(&[VERSION]).await?;
            if matches!(fsync_mode, FsyncMode::Always) {
                file.sync_data().await?;
            }
        }

        Ok(Self {
            fsync_mode,
            writer: Mutex::new(file),
        })
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
    if raw[MAGIC.len()] != VERSION {
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
