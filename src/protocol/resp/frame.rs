use anyhow::{Result, anyhow};
use btoi::btoi;
use nom::{
    Parser as NomParser,
    bytes::streaming::{tag, take, take_until},
    character::streaming::{crlf, line_ending},
    combinator::map_res,
    sequence::{preceded, terminated},
};
use tokio_util::bytes::Bytes;

#[derive(Clone, Debug)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<Frame>),
    NullArray,
}

impl Frame {
    pub fn parse(buf: &[u8]) -> Result<Option<(Frame, usize)>> {
        if buf.is_empty() {
            return Ok(None);
        }
        let result = match buf[0] {
            b'+' => Self::parse_simple_string(buf),
            b':' => Self::parse_integer(buf),
            b'-' => Self::parse_error(buf),
            b'$' => Self::parse_bulk_string(buf),
            b'*' => Self::parse_array(buf),
            _ => return Err(anyhow!("Unknown RESP type: {}", buf[0])),
        };

        match result {
            Ok((remaining, frame)) => {
                let consumed = buf.len() - remaining.len();
                Ok(Some((frame, consumed)))
            }
            Err(nom::Err::Incomplete(_)) => Ok(None),
            Err(e) => Err(anyhow!("Parse error: {}", e)),
        }
    }

    fn parse_raw_buffer<'a>(buf: &'a [u8], prefix: &'a str) -> nom::IResult<&'a [u8], &'a [u8]> {
        preceded(tag(prefix), terminated(take_until("\r\n"), line_ending)).parse(buf)
    }

    fn parse_simple_string(buf: &[u8]) -> nom::IResult<&[u8], Frame> {
        map_res(
            |b| Self::parse_raw_buffer(b, "+"),
            |bytes| std::str::from_utf8(bytes).map(|s| Frame::SimpleString(s.to_string())),
        )
        .parse(buf)
    }

    fn parse_integer(buf: &[u8]) -> nom::IResult<&[u8], Frame> {
        map_res(
            |b| Self::parse_raw_buffer(b, ":"),
            |bytes| btoi::<i64>(bytes).map(Frame::Integer),
        )
        .parse(buf)
    }

    fn parse_error(buf: &[u8]) -> nom::IResult<&[u8], Frame> {
        map_res(
            |b| Self::parse_raw_buffer(b, "-"),
            |bytes| std::str::from_utf8(bytes).map(|s| Frame::Error(s.to_string())),
        )
        .parse(buf)
    }

    fn parse_bulk_string(buf: &[u8]) -> nom::IResult<&[u8], Frame> {
        let (remaining, len_bytes) = preceded(tag("$"), take_until("\r\n")).parse(buf)?;
        let len: i32 = btoi(len_bytes).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(
                remaining,
                nom::error::ErrorKind::Digit,
            ))
        })?;

        if len == -1 {
            let (remaining, _) = crlf(remaining)?;
            return Ok((remaining, Frame::NullBulkString));
        }

        let len = len as usize;

        let (remaining, _) = crlf(remaining)?;
        let (remaining, data) = take(len)(remaining)?;
        let (remaining, _) = crlf(remaining)?;

        Ok((remaining, Frame::BulkString(Bytes::copy_from_slice(data))))
    }

    fn parse_array(buf: &[u8]) -> nom::IResult<&[u8], Frame> {
        let (mut remaining, len_bytes) = preceded(tag("*"), take_until("\r\n")).parse(buf)?;
        let len: i32 = btoi(len_bytes).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(
                remaining,
                nom::error::ErrorKind::Digit,
            ))
        })?;

        if len == -1 {
            let (remaining, _) = crlf(remaining)?;
            return Ok((remaining, Frame::NullArray));
        }

        let (rem, _) = crlf(remaining)?;
        remaining = rem;

        let mut array = Vec::with_capacity(len as usize);

        while array.len() < len as usize {
            let (rem, frame) = match remaining.first() {
                Some(b'+') => Self::parse_simple_string(remaining)?,
                Some(b':') => Self::parse_integer(remaining)?,
                Some(b'-') => Self::parse_error(remaining)?,
                Some(b'$') => Self::parse_bulk_string(remaining)?,
                Some(b'*') => Self::parse_array(remaining)?,
                Some(_) => {
                    return Err(nom::Err::Failure(nom::error::Error::new(
                        remaining,
                        nom::error::ErrorKind::Tag,
                    )));
                }
                None => return Err(nom::Err::Incomplete(nom::Needed::Unknown)),
            };

            array.push(frame);
            remaining = rem;
        }

        Ok((remaining, Frame::Array(array)))
    }
}

impl From<&Frame> for Bytes {
    fn from(value: &Frame) -> Self {
        match value {
            Frame::SimpleString(s) => Bytes::from(format!("+{}\r\n", s)),
            Frame::Error(e) => Bytes::from(format!("-{}\r\n", e)),
            Frame::Integer(i) => Bytes::from(format!(":{}\r\n", i)),
            Frame::BulkString(s) => {
                let mut data = format!("${}\r\n", s.len()).into_bytes();
                data.extend_from_slice(s);
                data.extend_from_slice(b"\r\n");
                Bytes::from(data)
            }
            Frame::NullBulkString => Bytes::from_static(b"$-1\r\n"),
            Frame::Array(a) => {
                let mut bytes = format!("*{}\r\n", a.len()).into_bytes();
                for item in a {
                    bytes.extend_from_slice(&Bytes::from(item));
                }
                Bytes::from(bytes)
            }
            Frame::NullArray => Bytes::from_static(b"*-1\r\n"),
        }
    }
}
