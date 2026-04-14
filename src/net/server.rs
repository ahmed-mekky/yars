use std::sync::Arc;

use anyhow::Result;
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

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
    cancel: CancellationToken,
    fsync_handle: Option<JoinHandle<()>>,
}

impl Server {
    pub async fn bind(addr: &str, config: AppConfig) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let store = Arc::new(MemoryStore::default());
        let cancel = CancellationToken::new();

        let (aof, fsync_handle) = if config.append_only {
            let (engine, handle) =
                AofEngine::open(config.aof_path.clone(), config.fsync_mode, cancel.clone()).await?;
            (Some(Arc::new(engine)), handle)
        } else {
            (None, None)
        };

        Ok(Self {
            listener,
            store,
            aof,
            config: Arc::new(config),
            cancel,
            fsync_handle,
        })
    }

    pub async fn run(self) -> Result<()> {
        if let Some(aof) = &self.aof {
            aof.replay_into(self.store.as_ref()).await?;
        }

        let result = tokio::select! {
            res = self.accept_loop() => res,
            _ = self.cancel.cancelled() => {
                println!("Shutting down...");
                Ok(())
            }
        };

        if let Some(handle) = self.fsync_handle {
            handle.await.ok();
        }

        result
    }

    async fn accept_loop(&self) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let store = Arc::clone(&self.store);
            let config = Arc::clone(&self.config);
            let aof = self.aof.clone();
            let cancel = self.cancel.clone();

            tokio::spawn(async move {
                let session = Session::new(socket, store, config, aof, cancel);
                if let Err(err) = session.handle().await {
                    eprintln!("Connection error: {err:?}");
                }
            });
        }
    }
}
