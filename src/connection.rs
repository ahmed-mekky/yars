use crate::db::Db;
use crate::resp::{Frame, RespCodec};
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
            println!("Received: {}", frame);
            let result = self.handle_command(frame).await;
            println!("Result: {}", result);
            self.framed.send(result).await?;
        }
        Ok(())
    }

    async fn handle_command(&self, frame: Frame) -> Frame {
        match frame {
            Frame::Array(parts) => self.dispatch(parts).await,
            _ => Frame::Error("ERR unknown command".into()),
        }
    }

    async fn dispatch(&self, parts: Vec<Frame>) -> Frame {
        let Some(Frame::BulkString(cmd)) = parts.first() else {
            return Frame::Error("ERR missing command".into());
        };
        match cmd.as_slice() {
            b"PING" => Frame::SimpleString("PONG".into()),
            b"SET" => self.cmd_set(&parts).await,
            b"GET" => self.cmd_get(&parts).await,
            b"DEL" => self.cmd_del(&parts).await,
            _ => Frame::Error("ERR unknown command".into()),
        }
    }

    async fn cmd_set(&self, parts: &[Frame]) -> Frame {
        let (Some(Frame::BulkString(key)), Some(Frame::BulkString(value))) =
            (parts.get(1), parts.get(2))
        else {
            return Frame::Error("ERR wrong number of arguments for SET".into());
        };

        match self
            .db
            .set(String::from_utf8_lossy(key).into_owned(), value.clone())
            .await
        {
            Ok(_) => Frame::SimpleString("OK".into()),
            Err(e) => Frame::Error(e.to_string()),
        }
    }
    async fn cmd_get(&self, parts: &[Frame]) -> Frame {
        let Some(Frame::BulkString(key)) = parts.get(1) else {
            return Frame::Error("ERR wrong number of arguments for GET".into());
        };

        match self.db.get(String::from_utf8_lossy(key).into_owned()).await {
            Ok(Some(value)) => Frame::BulkString(value),
            Ok(None) => Frame::NullBulkString,
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    async fn cmd_del(&self, parts: &[Frame]) -> Frame {
        let Some(Frame::BulkString(key)) = parts.get(1) else {
            return Frame::Error("ERR wrong number of arguments for DEL".into());
        };

        match self.db.del(String::from_utf8_lossy(key).into_owned()).await {
            Ok(_) => Frame::Integer(1),
            Err(e) => Frame::Error(e.to_string()),
        }
    }
}
