use anyhow::{Result, anyhow};
use nom::{
    IResult, Parser,
    bytes::{tag, take_until},
    character::char,
    error::Error as NomError,
    sequence::{preceded, terminated},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<()> {
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
    BulkString(Option<String>),
    Array(Vec<RespTypes>),
}

type ParseResult<'a> = IResult<&'a [u8], &'a [u8], NomError<&'a [u8]>>;
impl RespTypes {
    pub fn new(buf: &[u8]) -> Result<RespTypes> {
        if buf.len() < 2 {
            return Err(anyhow!("Buffer too short"));
        }

        match buf[0] {
            b'+' => Ok(Self::parse_simple_string(buf)?),
            b':' => Ok(Self::parse_integer(buf)?),
            b'-' => Ok(Self::parse_error(buf)?),
            b'$' => {
                let (remaining, string_len) = Self::parse_raw_buffer(buf, '$')?;
                let len: i32 = str::from_utf8(string_len)?.parse()?;

                if len == -1 {
                    return Ok(RespTypes::BulkString(None));
                }

                let len = len as usize;
                if remaining.len() < len + 2 {
                    return Err(anyhow!("Buffer too short for bulk string"));
                }

                Ok(RespTypes::BulkString(Some(
                    str::from_utf8(&remaining[..len])?.to_string(),
                )))
            }
            _ => Err(anyhow!("Unknown RESP type")),
        }
    }

    fn parse_raw_buffer(buf: &[u8], prefix: char) -> Result<(&[u8], &[u8])> {
        let result: ParseResult =
            preceded(char(prefix), terminated(take_until("\r\n"), tag("\r\n"))).parse(buf);

        result.map_err(|e| anyhow!("Parse error: {}", e))
    }

    fn parse_simple_string(buf: &[u8]) -> Result<Self> {
        let string = str::from_utf8(Self::parse_raw_buffer(buf, '+')?.1)
            .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
            .to_string();
        Ok(Self::SimpleString(string))
    }

    fn parse_integer(buf: &[u8]) -> Result<Self> {
        let integer = str::from_utf8(Self::parse_raw_buffer(buf, ':')?.1)
            .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
            .parse()
            .map_err(|e| anyhow!("Invalid integer format: {}", e))?;
        Ok(Self::Integer(integer))
    }

    fn parse_error(buf: &[u8]) -> Result<Self> {
        let error = str::from_utf8(Self::parse_raw_buffer(buf, '-')?.1)
            .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
            .to_string();
        Ok(Self::Error(error))
    }
}

impl std::fmt::Debug for RespTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SimpleString(s) => write!(f, "SimpleString({})", s),
            Self::Error(e) => write!(f, "Error({})", e),
            Self::Integer(i) => write!(f, "Integer({})", i),
            Self::BulkString(s) => write!(f, "BulkString({:?})", s),
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
