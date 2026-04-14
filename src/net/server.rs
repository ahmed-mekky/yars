use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{
    config::AppConfig,
    net::session::Session,
    store::{memory::MemoryStore, persistence::AofEngine},
};

pub struct Server {
    listener: TcpListener,
    store: Arc<MemoryStore>,
    config: Arc<AppConfig>,
    aof: Option<Arc<AofEngine>>,
}

impl Server {
    pub async fn bind(addr: &str, config: AppConfig) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let store = Arc::new(MemoryStore::default());

        let aof = if config.append_only {
            Some(Arc::new(
                AofEngine::open(config.aof_path.clone(), config.fsync_mode).await?,
            ))
        } else {
            None
        };

        Ok(Self {
            listener,
            store,
            aof,
            config: Arc::new(config),
        })
    }

    pub async fn run(self) -> Result<()> {
        if let Some(aof) = &self.aof {
            aof.replay_into(self.store.as_ref()).await?;
        }
        loop {
            let (socket, _) = self.listener.accept().await?;
            let store = Arc::clone(&self.store);
            let config = Arc::clone(&self.config);
            let aof = self.aof.clone();

            tokio::spawn(async move {
                let session = Session::new(socket, store, config, aof);
                if let Err(err) = session.handle().await {
                    eprintln!("Connection error: {err:?}");
                }
            });
        }
    }
}
