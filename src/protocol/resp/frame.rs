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

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: &Frame) {
        let bytes: Bytes = Bytes::from(frame);
        let (parsed, consumed) = Frame::parse(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        assert_frames_equal(frame, &parsed);
    }

    fn assert_frames_equal(a: &Frame, b: &Frame) {
        match (a, b) {
            (Frame::SimpleString(a), Frame::SimpleString(b)) => assert_eq!(a, b),
            (Frame::Error(a), Frame::Error(b)) => assert_eq!(a, b),
            (Frame::Integer(a), Frame::Integer(b)) => assert_eq!(a, b),
            (Frame::BulkString(a), Frame::BulkString(b)) => assert_eq!(a, b),
            (Frame::NullBulkString, Frame::NullBulkString) => {}
            (Frame::Array(a), Frame::Array(b)) => {
                assert_eq!(a.len(), b.len());
                for (expected, actual) in a.iter().zip(b.iter()) {
                    assert_frames_equal(expected, actual);
                }
            }
            (Frame::NullArray, Frame::NullArray) => {}
            _ => panic!("frame type mismatch"),
        }
    }

    #[test]
    fn parse_simple_string() {
        let (frame, consumed) = Frame::parse(b"+OK\r\n").unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert!(matches!(frame, Frame::SimpleString(s) if s == "OK"));
    }

    #[test]
    fn parse_error() {
        let (frame, consumed) = Frame::parse(b"-ERR unknown\r\n").unwrap().unwrap();
        assert_eq!(consumed, 14);
        assert!(matches!(frame, Frame::Error(s) if s == "ERR unknown"));
    }

    #[test]
    fn parse_integer() {
        let (frame, consumed) = Frame::parse(b":42\r\n").unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert!(matches!(frame, Frame::Integer(i) if i == 42));
    }

    #[test]
    fn parse_negative_integer() {
        let (frame, _) = Frame::parse(b":-3\r\n").unwrap().unwrap();
        assert!(matches!(frame, Frame::Integer(i) if i == -3));
    }

    #[test]
    fn parse_bulk_string() {
        let input = b"$5\r\nhello\r\n";
        let (frame, consumed) = Frame::parse(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert!(matches!(frame, Frame::BulkString(b) if b.as_ref() == b"hello"));
    }

    #[test]
    fn parse_null_bulk_string() {
        let (frame, consumed) = Frame::parse(b"$-1\r\n").unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert!(matches!(frame, Frame::NullBulkString));
    }

    #[test]
    fn parse_array() {
        let input = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (frame, consumed) = Frame::parse(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn parse_null_array() {
        let (frame, consumed) = Frame::parse(b"*-1\r\n").unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert!(matches!(frame, Frame::NullArray));
    }

    #[test]
    fn parse_nested_array() {
        let input = b"*1\r\n*2\r\n+hello\r\n+world\r\n";
        let (frame, consumed) = Frame::parse(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        let Frame::Array(outer) = frame else {
            panic!("expected array")
        };
        assert_eq!(outer.len(), 1);
        assert!(matches!(&outer[0], Frame::Array(inner) if inner.len() == 2));
    }

    #[test]
    fn parse_empty_array() {
        let input = b"*0\r\n";
        let (frame, consumed) = Frame::parse(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.is_empty());
    }

    #[test]
    fn incomplete_input_returns_none() {
        assert!(Frame::parse(b"+OK").unwrap().is_none());
        assert!(Frame::parse(b"$5\r\nhel").unwrap().is_none());
        assert!(Frame::parse(b"*2\r\n").unwrap().is_none());
    }

    #[test]
    fn empty_input_returns_none() {
        assert!(Frame::parse(b"").unwrap().is_none());
    }

    #[test]
    fn unknown_prefix_returns_error() {
        assert!(Frame::parse(b"%bad\r\n").is_err());
    }

    #[test]
    fn round_trip_simple_string() {
        round_trip(&Frame::SimpleString("PONG".into()));
    }

    #[test]
    fn round_trip_error() {
        round_trip(&Frame::Error("ERR something".into()));
    }

    #[test]
    fn round_trip_integer() {
        round_trip(&Frame::Integer(99));
        round_trip(&Frame::Integer(-1));
    }

    #[test]
    fn round_trip_bulk_string() {
        round_trip(&Frame::BulkString(Bytes::from_static(b"data")));
    }

    #[test]
    fn round_trip_null_bulk_string() {
        round_trip(&Frame::NullBulkString);
    }

    #[test]
    fn round_trip_array() {
        round_trip(&Frame::Array(vec![
            Frame::SimpleString("hi".into()),
            Frame::Integer(1),
        ]));
    }

    #[test]
    fn round_trip_null_array() {
        round_trip(&Frame::NullArray);
    }
}
