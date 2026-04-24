use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

use crate::{
    protocol::{command::Command, resp::RespCodec},
    service::context::ServerContext,
};

pub struct Session {
    framed: Framed<TcpStream, RespCodec>,
    ctx: Arc<ServerContext>,
}

impl Session {
    pub fn new(socket: TcpStream, ctx: Arc<ServerContext>) -> Self {
        Self {
            framed: RespCodec.framed(socket),
            ctx,
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
                            self.ctx.cancel.cancel();
                            break;
                        }
                        Ok(cmd) => self.ctx.execute(cmd).await,
                        Err(err_frame) => err_frame,
                    };
                    self.framed.send(result).await?;
                }
                _ = self.ctx.cancel.cancelled() => {
                    break;
                }
            }
        }
        Ok(())
    }
}
