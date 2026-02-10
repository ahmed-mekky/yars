use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Server is running on port 6379");

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buffer = [0; 1024];

            println!("Accepted connection from {:?}", socket.peer_addr());

            if socket.write_all(b"PONG\r\n").await.is_err() {
                println!("Failed to write data to socket");
                return;
            }

            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) => return,
                    Ok(n) => {
                        let resp_type = RespTypes::new(&buffer[0..n]);
                        println!("Received: {:?}", resp_type);

                        let buffer = format!("{:?}\r\n", resp_type);
                        let n = buffer.len();
                        if socket.write_all(&buffer.as_bytes()[0..n]).await.is_err() {
                            println!("Failed to write data to socket");
                            return;
                        }
                    }
                    Err(e) => {
                        println!("Error reading from socket: {:?}", e);
                        return;
                    }
                }
            }
        });
    }
}
enum RespTypes {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespTypes>),
}

impl std::fmt::Debug for RespTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString(s) => write!(f, "SimpleString({})", s),
            Self::Error(e) => write!(f, "Error({})", e),
            Self::Integer(i) => write!(f, "Integer({})", i),
            Self::BulkString(s) => write!(f, "BulkString({})", s),
            Self::Array(a) => {
                f.write_str("Array(")?;
                for item in a.iter() {
                    write!(f, "{:?}", item)?;
                }
                f.write_str(")")
            }
        }
    }
}

impl RespTypes {
    pub fn new(buf: &[u8]) -> Result<RespTypes, &'static str> {
        if buf.len() < 2 {
            return Err("Buffer too short");
        }

        let cr_pos = buf.iter().position(|&x| x == b'\r');
        if cr_pos.is_none()
            || (cr_pos.unwrap() + 1 >= buf.len())
            || buf[cr_pos.unwrap() + 1] != b'\n'
        {
            return Err("Invalid CRLF sequence");
        }

        let cr_pos = cr_pos.unwrap();

        match buf[0] {
            b'+' => Ok(RespTypes::SimpleString(
                str::from_utf8(&buf[1..cr_pos])
                    .map_err(|_| "Invalid UTF-8 encoding")?
                    .to_string(),
            )),
            b':' => Ok(RespTypes::Integer(
                str::from_utf8(&buf[1..cr_pos])
                    .map_err(|_| "Invalid UTF-8 encoding")?
                    .parse()
                    .map_err(|_| "Invalid integer format")?,
            )),
            b'-' => Ok(RespTypes::Error(
                str::from_utf8(&buf[1..cr_pos])
                    .map_err(|_| "Invalid UTF-8 encoding")?
                    .to_string(),
            )),
            b'$' => {
                let len: i32 = str::from_utf8(&buf[1..cr_pos])
                    .map_err(|_| "Invalid UTF-8 encoding")?
                    .parse()
                    .map_err(|_| "Invalid integer format")?;
                if len >= 1 {
                    Ok(RespTypes::BulkString(
                        str::from_utf8(&buf[cr_pos + 2..len as usize + cr_pos + 2])
                            .map_err(|_| "Invalid UTF-8 encoding")?
                            .to_string(),
                    ))
                } else {
                    Ok(RespTypes::BulkString("".to_string()))
                }
            }
            _ => Err("Unknown RESP type"),
        }
    }
}
