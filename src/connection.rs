use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::resp::Parser;

pub struct Connection {
    socket: TcpStream,
    buffer: [u8; 1024],
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            buffer: [0; 1024],
        }
    }

    pub async fn handle(mut self) -> Result<()> {
        println!("Accepted connection from {:?}", self.socket.peer_addr());

        if self.socket.write_all(b"PONG\r\n").await.is_err() {
            println!("Failed to write data to socket");
            return Ok(());
        }

        loop {
            match self.socket.read(&mut self.buffer).await {
                Ok(0) => return Ok(()),
                Ok(n) => {
                    let resp_type = Parser::parse(&self.buffer[0..n]);
                    println!("Received: {:?}", resp_type);

                    let buffer = format!("{:?}\r\n", resp_type);
                    let n = buffer.len();
                    if self.socket.write_all(&buffer.as_bytes()[0..n]).await.is_err() {
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
}
