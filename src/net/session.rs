use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

use crate::{
    protocol::{command::Command, resp::RespCodec},
    service::dispatcher,
    store::memory::MemoryStore,
};

pub struct Session {
    framed: Framed<TcpStream, RespCodec>,
    store: Arc<MemoryStore>,
}

impl Session {
    pub fn new(socket: TcpStream, store: Arc<MemoryStore>) -> Self {
        Self {
            framed: RespCodec.framed(socket),
            store,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        while let Some(frame) = self.framed.next().await {
            let frame = frame?;
            let result = match Command::try_from(frame) {
                Ok(cmd) => dispatcher::execute(&self.store, cmd).await,
                Err(err_frame) => err_frame,
            };
            self.framed.send(result).await?;
        }
        Ok(())
    }
}
