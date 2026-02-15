use anyhow::{Result, anyhow};
use btoi::btoi;
use nom::{
    Parser as NomParser,
    bytes::{complete::tag, take_until},
    character::{
        char,
        complete::{crlf, line_ending},
    },
    sequence::{preceded, terminated},
};

#[derive(Clone)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<Frame>),
}

pub struct Parser;

impl Parser {
    pub fn parse(buf: &[u8]) -> Result<Frame> {
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

    fn parse_simple_string(buf: &[u8]) -> Result<(&[u8], Frame)> {
        let (remaining, string) = Self::parse_raw_buffer(buf, "+")?;
        Ok((
            remaining,
            Frame::SimpleString(
                str::from_utf8(string)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            ),
        ))
    }

    fn parse_integer(buf: &[u8]) -> Result<(&[u8], Frame)> {
        let (remaining, result) = Self::parse_raw_buffer(buf, ":")?;
        Ok((remaining, Frame::Integer(btoi(result)?)))
    }

    fn parse_error(buf: &[u8]) -> Result<(&[u8], Frame)> {
        let (remaining, result) = Self::parse_raw_buffer(buf, "-")?;

        Ok((
            remaining,
            Frame::Error(
                str::from_utf8(result)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            ),
        ))
    }

    fn parse_bulk_string(buf: &[u8]) -> Result<(&[u8], Frame)> {
        let parse_result: nom::IResult<&[u8], &[u8]> =
            preceded(char('$'), take_until("\r\n")).parse(buf);
        let (remaining, len_u8) = parse_result.map_err(|e| anyhow!("Parse error: {}", e))?;
        let len: i32 = btoi(len_u8)?;

        if len == -1 {
            return Ok((remaining, Frame::BulkString(None)));
        }

        let (remaining, result) = Self::parse_raw_buffer(remaining, "\r\n")?;
        if result.len() != len as usize {
            return Err(anyhow!("Buffer len doesn't match prefixed len"));
        }

        Ok((
            remaining,
            Frame::BulkString(Some(
                str::from_utf8(result)
                    .map_err(|e| anyhow!("Invalid UTF-8 encoding: {}", e))?
                    .to_string(),
            )),
        ))
    }

    fn parse_array(buf: &[u8]) -> Result<(&[u8], Frame)> {
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

        Ok((remaining, Frame::Array(array)))
    }
}

impl std::fmt::Debug for Frame {
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

pub struct Writer;

impl Writer {
    pub fn write(frame: &Frame) -> Vec<u8> {
        match frame {
            Frame::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::Error(e) => format!("-{}\r\n", e).into_bytes(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::BulkString(s) => match s {
                Some(s) => {
                    let mut bytes = format!("${}\r\n", s.len()).into_bytes();

                    bytes.extend_from_slice(s.as_bytes());
                    bytes.extend_from_slice(b"\r\n");
                    bytes
                }
                None => b"$-1\r\n".to_vec(),
            },
            Frame::Array(a) => {
                let mut bytes = vec![b'*'];
                bytes.extend(format!("{}", a.len()).into_bytes());
                bytes.extend(b"\r\n");
                for item in a.iter() {
                    bytes.extend(Self::write(item));
                }
                bytes
            }
        }
    }
}
