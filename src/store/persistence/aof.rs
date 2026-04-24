use anyhow::Result;
use async_trait::async_trait;

use crate::{
    config::FsyncMode,
    store::{persistence::record::Record, traits::Store},
};

#[async_trait]
pub trait Aof: Send + Sync + 'static {
    async fn append(&self, record: Record) -> Result<()>;
    async fn replay_into(&self, store: &dyn Store) -> Result<()>;
    fn set_fsync_mode(&self, _mode: FsyncMode) {}
    async fn shutdown(&self) {}
}

pub struct NoopAof;

#[async_trait]
impl Aof for NoopAof {
    async fn append(&self, _record: Record) -> Result<()> {
        Ok(())
    }

    async fn replay_into(&self, _store: &dyn Store) -> Result<()> {
        Ok(())
    }
}
