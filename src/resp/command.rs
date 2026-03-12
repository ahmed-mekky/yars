use crate::resp::Frame;
use anyhow::Result;
use tokio_util::bytes::Bytes;

pub enum Command {
    Ping,
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { keys: Vec<Bytes> },
}

impl TryFrom<Frame> for Command {
    type Error = Frame;
    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let Frame::Array(parts) = frame else {
            return Err(Frame::Error("ERR expected array".into()));
        };

        let Some(Frame::BulkString(cmd)) = parts.first() else {
            return Err(Frame::Error("ERR missing command".into()));
        };
        match cmd.to_ascii_uppercase().as_slice() {
            b"PING" => Self::parse_ping(),
            b"GET" => Self::parse_get(parts),
            b"SET" => Self::parse_set(parts),
            b"DEL" => Self::parse_del(parts),
            _ => Err(Frame::Error("ERR unknown command {}".into())),
        }
    }
}

impl Command {
    fn parse_ping() -> Result<Command, Frame> {
        Ok(Command::Ping)
    }
    fn parse_get(input: Vec<Frame>) -> Result<Command, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Command::Get {
            key: Bytes::copy_from_slice(key),
        })
    }

    fn parse_set(input: Vec<Frame>) -> Result<Command, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("ERR missing key".into()));
        };
        let Some(Frame::BulkString(value)) = input.get(2) else {
            return Err(Frame::Error("ERR missing value".into()));
        };
        Ok(Command::Set {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        })
    }

    fn parse_del(input: Vec<Frame>) -> Result<Command, Frame> {
        let keys = input
            .get(1..)
            .ok_or(Frame::Error("ERR missing key".into()))?
            .iter()
            .filter_map(|key| match key {
                Frame::BulkString(key) => Some(Bytes::copy_from_slice(key)),
                _ => None,
            })
            .collect();
        Ok(Command::Del { keys })
    }
}
