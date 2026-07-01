use crate::{
    config::{AppConfig, FsyncMode},
    store::{
        memory::MemoryStore,
        persistence::{AofEngine, record::Record},
    },
};
use anyhow::Result;

pub enum Aof {
    Real(AofEngine),
    Noop,
}

impl Aof {
    pub async fn new(config: &AppConfig) -> Result<Self> {
        Ok(match config.append_only {
            true => Self::Real(AofEngine::open(config.aof_path.clone(), config.fsync_mode).await?),
            false => Self::Noop,
        })
    }

    pub async fn append(&self, data: Record) -> Result<()> {
        match self {
            Aof::Real(engine) => Ok(engine.append(data).await?),
            Aof::Noop => Ok(()),
        }
    }

    pub fn set_fsync_mode(&self, mode: FsyncMode) {
        match self {
            Aof::Real(engine) => engine.set_fsync_mode(mode),
            Aof::Noop => {}
        };
    }

    pub async fn shutdown(&self) {
        match self {
            Aof::Real(engine) => engine.shutdown().await,
            Aof::Noop => {}
        }
    }

    pub async fn replay(&self, store: &mut MemoryStore) -> Result<()> {
        match self {
            Aof::Real(engine) => engine.replay_into(store).await,
            Aof::Noop => Ok(()),
        }
    }
}
