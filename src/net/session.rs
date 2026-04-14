use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::{
    codec::{Decoder, Framed},
    sync::CancellationToken,
};

use crate::{
    config::AppConfig,
    protocol::{command::Command, resp::RespCodec},
    service::dispatcher,
    store::{memory::MemoryStore, persistence::AofEngine},
};

pub struct Session {
    framed: Framed<TcpStream, RespCodec>,
    store: Arc<MemoryStore>,
    config: Arc<AppConfig>,
    aof: Option<Arc<AofEngine>>,
    cancel: CancellationToken,
}

impl Session {
    pub fn new(
        socket: TcpStream,
        store: Arc<MemoryStore>,
        config: Arc<AppConfig>,
        aof: Option<Arc<AofEngine>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            framed: RespCodec.framed(socket),
            store,
            config,
            aof,
            cancel,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        loop {
            tokio::select! {
                frame = self.framed.next() => {
                    let frame = match frame {
                        Some(frame) => frame?,
                        None => break,
                    };
                    let result = match Command::try_from(frame) {
                        Ok(Command::SHUTDOWN) => {
                            self.framed
                                .send(crate::protocol::resp::Frame::SimpleString("OK".into()))
                                .await?;
                            self.cancel.cancel();
                            break;
                        }
                        Ok(cmd) => dispatcher::execute(&self.store, &self.config, &self.aof, cmd).await,
                        Err(err_frame) => err_frame,
                    };
                    self.framed.send(result).await?;
                }
                _ = self.cancel.cancelled() => {
                    break;
                }
            }
        }
        Ok(())
    }
}
