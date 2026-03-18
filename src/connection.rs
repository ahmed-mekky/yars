use crate::db::Db;
use crate::resp::{Command, Frame, RespCodec};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};

pub struct Connection {
    framed: Framed<TcpStream, RespCodec>,
    db: Arc<Db>,
}

impl Connection {
    pub fn new(socket: TcpStream, db: Arc<Db>) -> Self {
        Self {
            framed: RespCodec.framed(socket),
            db,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        while let Some(frame) = self.framed.next().await {
            let frame = frame?;

            let result = match Command::try_from(frame) {
                Ok(cmd) => self.execute(cmd).await,
                Err(err_frame) => err_frame,
            };

            self.framed.send(result).await?;
        }
        Ok(())
    }

    async fn execute(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Ping => Frame::SimpleString("PONG".into()),

            Command::Get { key } => match self.db.get(&key).await {
                Some(entry) => Frame::BulkString(entry.value),
                None => Frame::NullBulkString,
            },

            Command::Set { key, entry } => {
                self.db.set(key, entry).await;
                Frame::SimpleString("OK".into())
            }

            Command::Del { keys } => Frame::Integer(self.db.del(&keys).await),

            Command::Exists { keys } => Frame::Integer(self.db.exists(&keys).await),

            Command::MGet { keys } => Frame::Array(
                self.db
                    .mget(&keys)
                    .await
                    .iter()
                    .cloned()
                    .map(|e| match e {
                        Some(entry) => Frame::BulkString(entry.value),
                        None => Frame::NullBulkString,
                    })
                    .collect(),
            ),
        }
    }
}
