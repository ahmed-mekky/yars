use crate::{resp::Frame, utils::get_current_millis};
use tokio_util::bytes::Bytes;

#[allow(clippy::upper_case_acronyms)]
pub enum Command {
    PING,
    GET { key: Bytes },
    SET { key: Bytes, entry: Entry },
    DEL { keys: Vec<Bytes> },
    EXISTS { keys: Vec<Bytes> },
    MGET { keys: Vec<Bytes> },
    MSET { items: Vec<(Bytes, Bytes)> },
    TTL { key: Bytes },
    PTTL { key: Bytes },
    PERSIST { key: Bytes },
    EXPIRE { key: Bytes, ttl: u64 },
    PEXPIRE { key: Bytes, ttl: u64 },
    ECHO { msg: Bytes },
    DBSIZE,
    FLUSHDB,
    GETDEL { key: Bytes },
    GETSET { key: Bytes, entry: Entry },
    SETNX { key: Bytes, entry: Entry },
}

impl TryFrom<Frame> for Command {
    type Error = Frame;
    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let Frame::Array(input) = frame else {
            return Err(Frame::Error("ERR expected array".into()));
        };

        let Some(Frame::BulkString(cmd)) = input.first() else {
            return Err(Frame::Error("ERR missing command".into()));
        };
        match cmd.to_ascii_uppercase().as_slice() {
            b"PING" => Ok(Command::PING),
            b"DBSIZE" => Ok(Command::DBSIZE),
            b"FLUSHDB" => Ok(Command::FLUSHDB),
            b"GET" => Ok(Command::GET {
                key: Self::parse_key(&input)?,
            }),
            b"SET" => Ok(Command::SET {
                key: Self::parse_key(&input)?,
                entry: Self::parse_entry(&input)?,
            }),
            b"DEL" => Ok(Command::DEL {
                keys: Self::parse_keys(&input)?,
            }),
            b"EXISTS" => Ok(Command::EXISTS {
                keys: Self::parse_keys(&input)?,
            }),
            b"MGET" => Ok(Command::MGET {
                keys: Self::parse_keys(&input)?,
            }),
            b"MSET" => Ok(Command::MSET {
                items: Self::parse_items(&input)?,
            }),
            b"TTL" => Ok(Command::TTL {
                key: Self::parse_key(&input)?,
            }),
            b"PTTL" => Ok(Command::PTTL {
                key: Self::parse_key(&input)?,
            }),
            b"PERSIST" => Ok(Command::PERSIST {
                key: Self::parse_key(&input)?,
            }),
            b"EXPIRE" => Ok(Command::EXPIRE {
                key: Self::parse_key(&input)?,
                ttl: Self::parse_ttl(&input)? * 1000,
            }),
            b"PEXPIRE" => Ok(Command::PEXPIRE {
                key: Self::parse_key(&input)?,
                ttl: Self::parse_ttl(&input)?,
            }),
            b"ECHO" => Ok(Command::ECHO {
                msg: Self::parse_msg(&input)?,
            }),
            b"GETSET" => Ok(Command::GETSET {
                key: Self::parse_key(&input)?,
                entry: Self::parse_entry(&input)?,
            }),
            b"GETDEL" => Ok(Command::GETDEL {
                key: Self::parse_key(&input)?,
            }),
            b"SETNX" => Ok(Command::SETNX {
                key: Self::parse_key(&input)?,
                entry: Self::parse_entry(&input)?,
            }),
            _ => Err(Frame::Error("ERR unknown command".into())),
        }
    }
}

impl Command {
    fn parse_key(input: &[Frame]) -> Result<Bytes, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Bytes::copy_from_slice(key))
    }

    fn parse_keys(input: &[Frame]) -> Result<Vec<Bytes>, Frame> {
        Ok(input
            .get(1..)
            .ok_or(Frame::Error("ERR missing key".into()))?
            .iter()
            .filter_map(|key| match key {
                Frame::BulkString(key) => Some(Bytes::copy_from_slice(key)),
                _ => None,
            })
            .collect())
    }

    fn parse_entry(input: &[Frame]) -> Result<Entry, Frame> {
        let value = match input.get(2) {
            Some(Frame::BulkString(b)) => b.clone(),
            _ => return Err(Frame::Error("ERR missing value".into())),
        };

        let exp = Self::parse_exp(input)?;
        Ok(Entry { value, exp })
    }

    fn parse_exp(input: &[Frame]) -> Result<Expiry, Frame> {
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

    fn parse_items(input: &[Frame]) -> Result<Vec<(Bytes, Bytes)>, Frame> {
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
        Ok(items)
    }

    fn parse_ttl(input: &[Frame]) -> Result<u64, Frame> {
        let bytes = match input.get(2) {
            Some(Frame::BulkString(t)) => t,
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        let ttl = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Frame::Error("ERR value is not an integer or out of range".into()))?;
        Ok(ttl)
    }

    fn parse_msg(input: &[Frame]) -> Result<Bytes, Frame> {
        let msg = match input.get(1) {
            Some(Frame::BulkString(msg)) => Bytes::copy_from_slice(msg),
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };
        Ok(msg)
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
