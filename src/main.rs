use anyhow::{Result, anyhow};
use btoi::btoi;
use nom::{
    Parser,
    bytes::{complete::tag, take_until},
    character::{
        char,
        complete::{crlf, line_ending},
    },
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

impl RespTypes {
    pub fn new(buf: &[u8]) -> Result<RespTypes> {
        if buf.len() < 2 {
            return Err(anyhow!("Buffer too short"));
        }

        match buf[0] {
            b'+' => Ok(Self::parse_simple_string(buf)?.1),
            b':' => Ok(Self::parse_integer(buf)?.1),
            b'-' => Ok(Self::parse_error(buf)?.1),
            b'$' => Ok(Self::parse_bulk_string(buf)?.1),
            b'*' => Ok(Self::parse_array(buf)?.1),
            _ => Err(anyhow!("Unknown RESP type")),
        }
    }

    fn parse_raw_buffer<'a>(buf: &'a [u8], prefix: &'a str) -> Result<(&'a [u8], &'a [u8])> {
        let result: nom::IResult<&[u8], &[u8]> =
            preceded(tag(prefix), terminated(take_until("\r\n"), line_ending)).parse(buf);

        result.map_err(|e| anyhow!("Parse error: {}", e))
    }

    fn parse_simple_string(buf: &[u8]) -> Result<(&[u8], Self)> {
        let (remaining, string) = Self::parse_raw_buffer(buf, "+")?;
        Ok((
            remaining,
            Self::SimpleString(
                str::from_utf8(string)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            ),
        ))
    }

    fn parse_integer(buf: &[u8]) -> Result<(&[u8], Self)> {
        let (remaining, result) = Self::parse_raw_buffer(buf, ":")?;
        Ok((remaining, Self::Integer(btoi(result)?)))
    }

    fn parse_error(buf: &[u8]) -> Result<(&[u8], Self)> {
        let (remaining, result) = Self::parse_raw_buffer(buf, "-")?;

        Ok((
            remaining,
            Self::Error(
                str::from_utf8(result)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            ),
        ))
    }

    fn parse_bulk_string(buf: &[u8]) -> Result<(&[u8], Self)> {
        let parse_result: nom::IResult<&[u8], &[u8]> =
            preceded(char('$'), take_until("\r\n")).parse(buf);
        let (remaining, len_u8) = parse_result.map_err(|e| anyhow!("Parse error: {}", e))?;
        let len: i32 = btoi(len_u8)?;

        if len == -1 {
            return Ok((remaining, RespTypes::BulkString(None)));
        }

        let (remaining, result) = Self::parse_raw_buffer(remaining, "\r\n")?;
        if result.len() != len as usize {
            return Err(anyhow!("Buffer len doesn't match prefixed len"));
        }

        Ok((
            remaining,
            RespTypes::BulkString(Some(
                str::from_utf8(result)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            )),
        ))
    }

    fn parse_array(buf: &[u8]) -> Result<(&[u8], Self)> {
        let parse_result: nom::IResult<&[u8], &[u8]> =
            preceded(char('*'), take_until("\r\n")).parse(buf);
        let (remaining, len_u8) = parse_result.map_err(|e| anyhow!("Parse error: {}", e))?;
        let len: i32 = btoi(len_u8)?;

        let mut array = vec![];

        let parse_result: nom::IResult<&[u8], &[u8]> = crlf(remaining);
        let (mut remaining, _) = parse_result.map_err(|e| anyhow!("Parse error: {}", e))?;
        while !remaining.is_empty() {
            let (rem, result) = match remaining[0] {
                b'+' => Self::parse_simple_string(remaining)?,
                b':' => Self::parse_integer(remaining)?,
                b'-' => Self::parse_error(remaining)?,
                b'$' => Self::parse_bulk_string(remaining)?,
                b'*' => Self::parse_array(remaining)?,
                _ => return Err(anyhow!("Unknown RESP type")),
            };
            array.push(result);
            remaining = rem
        }
        if len != array.len() as i32 {
            return Err(anyhow!("Buffer len doesn't match prefixed len"));
        }

        Ok((remaining, RespTypes::Array(array)))
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
                f.write_str("Array(\n")?;
                for item in a.iter() {
                    writeln!(f, "{:?},", item)?;
                }
                f.write_str(")")
            }
        }
    }
}
