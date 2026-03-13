use crate::db::Db;
use crate::resp::{Command, Frame, RespCodec};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::Instant;
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
            println!("Received: {}", frame);
            let result = match Command::try_from(frame) {
                Ok(cmd) => self.execute(cmd).await,
                Err(err_frame) => err_frame,
            };
            println!("Result: {}", result);
            self.framed.send(result).await?;
        }
        Ok(())
    }

    async fn execute(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Ping => Frame::SimpleString("PONG".into()),
            Command::Get { key } => match self.db.get(&key).await {
                Ok(Some(entry)) => {
                    dbg!(entry.clone());
                    if entry.exp.is_none() || entry.exp.unwrap() > Instant::now() {
                        Frame::BulkString(entry.value)
                    } else {
                        self.db.forget(&key).await;
                        Frame::NullBulkString
                    }
                }
                Ok(None) => Frame::NullBulkString,
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::Set { key, entry } => match self.db.set(key, entry).await {
                Ok(_) => Frame::SimpleString("OK".into()),
                Err(e) => Frame::Error(e.to_string()),
            },
            Command::Del { keys } => match self.db.del(keys).await {
                Ok(count) => Frame::Integer(count),
                Err(e) => Frame::Error(e.to_string()),
            },
        }
    }
}
