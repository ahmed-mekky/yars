use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{connection::Connection, db::Db};

pub struct Server {
    listener: TcpListener,
    db: Arc<Db>,
}

impl Server {
    pub async fn bind(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let db = Arc::new(Db::new());
        Ok(Self { listener, db })
    }

    pub async fn run(self) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let db = Arc::clone(&self.db);
            tokio::spawn(async move {
                let connection = Connection::new(socket, db);
                if let Err(err) = connection.handle().await {
                    println!("Connection error: {:?}", err);
                }
            });
        }
    }
}
