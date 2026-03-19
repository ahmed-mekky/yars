use crate::db::Db;
use crate::resp::{Command, Expiry, Frame, RespCodec};
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
            Command::PING => Frame::SimpleString("PONG".into()),

            Command::GET { key } => match self.db.get(&key).await {
                Some(entry) => Frame::BulkString(entry.value),
                None => Frame::NullBulkString,
            },

            Command::SET { key, entry } => {
                self.db.set(key, entry).await;
                Frame::SimpleString("OK".into())
            }

            Command::DEL { keys } => Frame::Integer(self.db.del(&keys).await),

            Command::EXISTS { keys } => Frame::Integer(self.db.exists(&keys).await),

            Command::MGET { keys } => Frame::Array(
                self.db
                    .mget(&keys)
                    .await
                    .iter()
                    .map(|e| match e {
                        Some(entry) => Frame::BulkString(entry.value.clone()),
                        None => Frame::NullBulkString,
                    })
                    .collect(),
            ),

            Command::MSET { items } => {
                self.db.mset(&items).await;
                Frame::SimpleString("OK".into())
            }

            Command::TTL { key } => {
                let now = crate::utils::get_current_millis();
                match self.db.get(&key).await {
                    None => Frame::Integer(-2),
                    Some(entry) => match entry.exp {
                        Expiry::At(exp) => {
                            let ttl = exp.saturating_sub(now);
                            Frame::Integer((ttl / 1000) as i64)
                        }
                        Expiry::None | Expiry::Keep => Frame::Integer(-1),
                    },
                }
            }

            Command::PTTL { key } => {
                let now = crate::utils::get_current_millis();
                match self.db.get(&key).await {
                    None => Frame::Integer(-2),
                    Some(entry) => match entry.exp {
                        Expiry::At(exp) => {
                            let ttl = exp.saturating_sub(now);
                            Frame::Integer(ttl as i64)
                        }
                        Expiry::None | Expiry::Keep => Frame::Integer(-1),
                    },
                }
            }

            Command::PERSIST { key } => {
                if let Some(mut entry) = self.db.get(&key).await
                    && let Expiry::At(_) = entry.exp
                {
                    entry.exp = Expiry::None;
                    self.db.set(key, entry).await;
                    return Frame::Integer(1);
                }
                Frame::Integer(0)
            }

            Command::EXPIRE { key, ttl } => {
                let now = crate::utils::get_current_millis();
                if let Some(mut entry) = self.db.get(&key).await {
                    entry.exp = Expiry::At(now.saturating_add(ttl));
                    self.db.set(key, entry).await;
                    return Frame::Integer(1);
                }
                Frame::Integer(0)
            }

            Command::PEXPIRE { key, ttl } => {
                let now = crate::utils::get_current_millis();
                if let Some(mut entry) = self.db.get(&key).await {
                    entry.exp = Expiry::At(now.saturating_add(ttl));
                    self.db.set(key, entry).await;
                    return Frame::Integer(1);
                }
                Frame::Integer(0)
            }

            Command::ECHO { msg } => Frame::BulkString(msg),

            Command::DBSIZE => Frame::Integer(self.db.len().await as i64),

            Command::FLUSHDB => {
                self.db.clear().await;
                Frame::Integer(1)
            }
        }
    }
}
