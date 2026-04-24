use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{config::AppConfig, net::session::Session, service::context::ServerContext};

pub struct Server {
    listener: TcpListener,
    ctx: Arc<ServerContext>,
}

impl Server {
    pub async fn bind(addr: &str, config: AppConfig) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let ctx = ServerContext::new(config).await?;

        Ok(Self { listener, ctx })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub async fn run(self) -> Result<()> {
        self.ctx.aof.replay_into(&self.ctx.store).await?;

        let result = tokio::select! {
            res = self.accept_loop() => res,
            _ = self.ctx.cancel.cancelled() => {
                println!("Shutting down...");
                Ok(())
            }
        };

        self.ctx.aof.shutdown().await;
        result
    }

    async fn accept_loop(&self) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let ctx = Arc::clone(&self.ctx);

            tokio::spawn(async move {
                let session = Session::new(socket, ctx);
                if let Err(err) = session.handle().await {
                    eprintln!("Connection error: {err:?}");
                }
            });
        }
    }
}
