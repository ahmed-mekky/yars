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
                key: Self::parse_key(input)?,
            }),
            b"SET" => Self::parse_set(input),
            b"DEL" => Ok(Command::DEL {
                keys: Self::parse_keys(input)?,
            }),
            b"EXISTS" => Ok(Command::EXISTS {
                keys: Self::parse_keys(input)?,
            }),
            b"MGET" => Ok(Command::MGET {
                keys: Self::parse_keys(input)?,
            }),
            b"MSET" => Self::parse_mset(input),
            b"TTL" => Ok(Command::TTL {
                key: Self::parse_key(input)?,
            }),
            b"PTTL" => Ok(Command::PTTL {
                key: Self::parse_key(input)?,
            }),
            b"PERSIST" => Ok(Command::PERSIST {
                key: Self::parse_key(input)?,
            }),
            b"EXPIRE" => Self::parse_expire(input),
            b"PEXPIRE" => Self::parse_pexpire(input),
            b"ECHO" => Self::parse_echo(input),
            _ => Err(Frame::Error("ERR unknown command".into())),
        }
    }
}

impl Command {
    fn parse_key(input: Vec<Frame>) -> Result<Bytes, Frame> {
        let Some(Frame::BulkString(key)) = input.get(1) else {
            return Err(Frame::Error("Err missing key".into()));
        };
        Ok(Bytes::copy_from_slice(key))
    }

    fn parse_keys(input: Vec<Frame>) -> Result<Vec<Bytes>, Frame> {
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
        Ok(Command::SET {
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
        Ok(Command::MSET { items })
    }

    fn parse_expire(input: Vec<Frame>) -> std::result::Result<Command, Frame> {
        if input.len() < 2 {
            return Err(Frame::Error("ERR wrong number of arguments".into()));
        }
        let key = match input.get(1) {
            Some(Frame::BulkString(b)) => Bytes::copy_from_slice(b),
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };
        let bytes = match input.get(2) {
            Some(Frame::BulkString(t)) => t,
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        let ttl = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Frame::Error("ERR value is not an integer or out of range".into()))?
            * 1000;

        Ok(Command::EXPIRE { key, ttl })
    }

    fn parse_pexpire(input: Vec<Frame>) -> std::result::Result<Command, Frame> {
        if input.len() < 2 {
            return Err(Frame::Error("ERR wrong number of arguments".into()));
        }
        let key = match input.get(1) {
            Some(Frame::BulkString(b)) => Bytes::copy_from_slice(b),
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };
        let bytes = match input.get(2) {
            Some(Frame::BulkString(t)) => t,
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        let ttl = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Frame::Error("ERR value is not an integer or out of range".into()))?;

        Ok(Command::PEXPIRE { key, ttl })
    }

    fn parse_echo(input: Vec<Frame>) -> Result<Command, Frame> {
        let msg = match input.get(1) {
            Some(Frame::BulkString(msg)) => Bytes::copy_from_slice(msg),
            _ => return Err(Frame::Error("ERR syntax error".into())),
        };

        Ok(Command::ECHO { msg })
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
