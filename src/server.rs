use anyhow::Result;
use tokio::net::TcpListener;

use crate::connection::Connection;

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    pub async fn run(self) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            tokio::spawn(async move {
                let connection = Connection::new(socket);
                if let Err(err) = connection.handle().await {
                    println!("Connection error: {:?}", err);
                }
            });
        }
    }
}
