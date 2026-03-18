use crate::{resp::Frame, utils::get_current_millis};
use anyhow::Result;
use tokio_util::bytes::Bytes;

pub enum Command {
    Ping,
    Get { key: Bytes },
    Set { key: Bytes, entry: Entry },
    Del { keys: Vec<Bytes> },
    Exists { keys: Vec<Bytes> },
    MGet { keys: Vec<Bytes> },
    MSet { items: Vec<(Bytes, Bytes)> },
    Ttl { key: Bytes },
    Pttl { key: Bytes },
    Persist { key: Bytes },
    Expire { key: Bytes, ttl: u64 },
    PExpire { key: Bytes, ttl: u64 },
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
            b"EXISTS" => Self::parse_exists(parts),
            b"MGET" => Self::parse_mget(parts),
            b"MSET" => Self::parse_mset(parts),
            b"TTL" => Self::parse_ttl(parts),
            b"PTTL" => Self::parse_pttl(parts),
            b"PERSIST" => Self::parse_persist(parts),
            b"EXPIRE" => Self::parse_expire(parts),
            b"PEXPIRE" => Self::parse_pexpire(parts),
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
        let key = match input.get(1) {
            Some(Frame::BulkString(b)) => b.clone(),
            _ => return Err(Frame::Error("ERR missing key".into())),
        };

        let value = match input.get(2) {
            Some(Frame::BulkString(b)) => b.clone(),
            _ => return Err(Frame::Error("ERR missing value".into())),
        };

        let exp = Self::parse_exp(input)?;
        Ok(Command::Set {
            key,
            entry: Entry { value, exp },
        })
    }

    fn parse_exp(input: Vec<Frame>) -> Result<Expiry, Frame> {
        let Some(Frame::BulkString(sub_command)) = input.get(3) else {
            return Ok(Expiry::None);
        };

        match sub_command.to_ascii_uppercase().as_slice() {
            b"EX" => {
                let Some(Frame::BulkString(bytes)) = input.get(4) else {
                    return Err(Frame::Error("ERR syntax error".into()));
                };

                let secs = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Frame::Error("ERR value is not an integer or out of range".into())
                    })?;

                Ok(Expiry::At(get_current_millis() + secs * 1000))
            }
            b"PX" => {
                let Some(Frame::BulkString(bytes)) = input.get(4) else {
                    return Err(Frame::Error("ERR syntax error".into()));
                };

                let msecs = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Frame::Error("ERR value is not an integer or out of range".into())
                    })?;

                Ok(Expiry::At(get_current_millis() + msecs))
            }
            b"EXAT" => {
                let Some(Frame::BulkString(bytes)) = input.get(4) else {
                    return Err(Frame::Error("ERR syntax error".into()));
                };

                let secs = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Frame::Error("ERR value is not an integer or out of range".into())
                    })?;

                if secs * 1000 < get_current_millis() {
                    return Err(Frame::Error("ERR invalid timestamp".into()));
                }

                Ok(Expiry::At(secs * 1000))
            }
            b"PXAT" => {
                let Some(Frame::BulkString(bytes)) = input.get(4) else {
                    return Err(Frame::Error("ERR syntax error".into()));
                };

                let msecs = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Frame::Error("ERR value is not an integer or out of range".into())
                    })?;

                if msecs < get_current_millis() {
                    return Err(Frame::Error("ERR invalid timestamp".into()));
                }

                Ok(Expiry::At(msecs))
            }
            b"KEEPTTL" => Ok(Expiry::Keep),
            _ => Err(Frame::Error("ERR syntax error".into())),
        }
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

    fn parse_exists(input: Vec<Frame>) -> Result<Command, Frame> {
        let keys = input
            .get(1..)
            .ok_or(Frame::Error("ERR missing key".into()))?
            .iter()
            .filter_map(|key| match key {
                Frame::BulkString(key) => Some(Bytes::copy_from_slice(key)),
                _ => None,
            })
            .collect();
        Ok(Command::Exists { keys })
    }

    fn parse_mget(input: Vec<Frame>) -> Result<Command, Frame> {
        let keys = input
            .get(1..)
            .ok_or(Frame::Error("ERR missing key".into()))?
            .iter()
            .filter_map(|key| match key {
                Frame::BulkString(key) => Some(Bytes::copy_from_slice(key)),
                _ => None,
            })
            .collect();
        Ok(Command::MGet { keys })
    }

    fn parse_mset(input: Vec<Frame>) -> Result<Command, Frame> {
        if input.len() < 3 || input.len().is_multiple_of(2) {
            return Err(Frame::Error("ERR wrong number of arguments".into()));
        }
        let mut items = Vec::with_capacity((input.len() - 1) / 2);
        for chunk in input[1..].chunks_exact(2) {
            let key = match &chunk[0] {
                Frame::BulkString(b) => Bytes::copy_from_slice(b),
                _ => return Err(Frame::Error("ERR syntax error".into())),
            };
            let value = match &chunk[1] {
                Frame::BulkString(b) => b.clone(),
                _ => return Err(Frame::Error("ERR syntax error".into())),
            };
            items.push((key, value));
        }
        Ok(Command::MSet { items })
    }

    fn parse_ttl(input: Vec<Frame>) -> Result<Command, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Command::Ttl {
            key: Bytes::copy_from_slice(key),
        })
    }

    fn parse_pttl(input: Vec<Frame>) -> Result<Command, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Command::Pttl {
            key: Bytes::copy_from_slice(key),
        })
    }

    fn parse_persist(input: Vec<Frame>) -> Result<Command, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Command::Persist {
            key: Bytes::copy_from_slice(key),
        })
    }

    fn parse_expire(parts: Vec<Frame>) -> std::result::Result<Command, Frame> {
        if parts.len() < 2 {
            return Err(Frame::Error("ERR wrong number of arguments".into()));
        }
        let key = match parts.get(1) {
            Some(Frame::BulkString(b)) => Bytes::copy_from_slice(b),
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };
        let bytes = match parts.get(2) {
            Some(Frame::BulkString(t)) => t,
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        let ttl = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Frame::Error("ERR value is not an integer or out of range".into()))?
            * 1000;

        Ok(Command::Expire { key, ttl })
    }

    fn parse_pexpire(parts: Vec<Frame>) -> std::result::Result<Command, Frame> {
        if parts.len() < 2 {
            return Err(Frame::Error("ERR wrong number of arguments".into()));
        }
        let key = match parts.get(1) {
            Some(Frame::BulkString(b)) => Bytes::copy_from_slice(b),
            _ => return Err(Frame::Error("ERR syntaxerror".into())),
        };
        let bytes = match parts.get(2) {
            Some(Frame::BulkString(t)) => t,
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        let ttl = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Frame::Error("ERR value is not an integer or out of range".into()))?;

        Ok(Command::PExpire { key, ttl })
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Bytes,
    pub exp: Expiry,
}

#[derive(Clone, Debug)]
pub enum Expiry {
    Keep,
    At(u64),
    None,
}

impl Entry {
    pub fn is_expired(&self, now: u64) -> bool {
        match self.exp {
            Expiry::At(exp) => now > exp,
            _ => false,
        }
    }
}
