use crate::{config::FsyncMode, protocol::command::Command, store::traits::Store};
use anyhow::Result;
use std::path::PathBuf;
use tokio::{
    fs::{File, OpenOptions},
    sync::Mutex,
};
pub struct AofEngine {
    path: PathBuf,
    fsync_mode: FsyncMode,
    writer: Mutex<File>,
}
impl AofEngine {
    pub async fn open(path: PathBuf, fsync_mode: FsyncMode) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            fsync_mode,
            writer: Mutex::new(file),
        })
    }
    pub async fn append(&self, command: &Command) -> Result<()> {
        Ok(())
    }
    pub async fn replay_into(&self, store: &impl Store) -> Result<()> {
        Ok(())
    }
}

