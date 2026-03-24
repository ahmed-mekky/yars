use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{net::session::Session, store::memory::MemoryStore};

pub struct Server {
    listener: TcpListener,
    store: Arc<MemoryStore>,
}

impl Server {
    pub async fn bind(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let store = Arc::new(MemoryStore::default());
        Ok(Self { listener, store })
    }

    pub async fn run(self) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let store = Arc::clone(&self.store);
            tokio::spawn(async move {
                let session = Session::new(socket, store);
                if let Err(err) = session.handle().await {
                    eprintln!("Connection error: {err:?}");
                }
            });
        }
    }
}
