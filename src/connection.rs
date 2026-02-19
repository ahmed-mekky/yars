use std::sync::Arc;

use anyhow::Result;
use nom::AsBytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::db::Db;
use crate::resp::{Frame, Parser, Writer};

pub struct Connection {
    socket: TcpStream,
    buffer: [u8; 1024],
    db: Arc<Db>,
}

impl Connection {
    pub fn new(socket: TcpStream, db: Arc<Db>) -> Self {
        Self {
            socket,
            buffer: [0; 1024],
            db,
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        println!("Accepted connection from {:?}", self.socket.peer_addr());

        loop {
            match self.socket.read(&mut self.buffer).await {
                Ok(0) => return Ok(()),
                Ok(n) => {
                    let result = Parser::parse(&self.buffer[0..n]);

                    let frame = result.unwrap_or_else(|e| Frame::Error(e.to_string()));
                    println!("Received: {}", frame);

                    let response = self.handle_command(frame).await;

                    let buffer = Writer::write(&response);
                    let n = buffer.len();
                    if self
                        .socket
                        .write_all(&buffer.as_bytes()[0..n])
                        .await
                        .is_err()
                    {
                        println!("Failed to write data to socket");
                        return Ok(());
                    }
                }
                Err(e) => {
                    println!("Error reading from socket: {:?}", e);
                    return Ok(());
                }
            }
        }
    }

    async fn handle_command(&self, frame: Frame) -> Frame {
        println!("Handling command: {}", &frame);
        match frame {
            Frame::Array(frame) => {
                if let Some(command) = frame.first() {
                    match command {
                        Frame::BulkString(Some(cmd)) => match cmd.as_str() {
                            "PING" => Frame::SimpleString("PONG".into()),
                            "SET" => {
                                if let Some(key) = frame.get(1)
                                    && let Some(value) = frame.get(2)
                                {
                                    let _ =
                                        self.db.set(key.to_string(), value.to_vec().unwrap()).await;
                                    return Frame::Status("OK".into());
                                }
                                Frame::Error("Invalid arguments for SET command".into())
                            }
                            "GET" => {
                                if let Some(key) = frame.get(1) {
                                    if let Ok(value_option) = self.db.get(key.to_string()).await
                                        && let Some(value) = value_option
                                    {
                                        return Frame::BulkString(Some(
                                            String::from_utf8(value).unwrap(),
                                        ));
                                    }
                                    return Frame::BulkString(None);
                                }
                                Frame::Error("Invalid arguments for GET command".into())
                            }
                            _ => Frame::Error("Unknown command".into()),
                        },
                        _ => {
                            println!("Invalid command format");
                            Frame::Error("Invalid command format".into())
                        }
                    }
                } else {
                    Frame::Error("Command is missing".into())
                }
            }
            _ => Frame::Error("ERR Not Supported".to_string()),
        }
    }
}
