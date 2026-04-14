use crate::{
    protocol::{command::Command, resp::Frame},
    store::types::{Entry, Expiry},
    utils::time::get_current_millis,
};
use tokio_util::bytes::Bytes;

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
            b"CONFIG" => parse_config(&input),
            b"DBSIZE" => Ok(Command::DBSIZE),
            b"FLUSHDB" => Ok(Command::FLUSHDB),
            b"INFO" => Ok(Command::INFO),
            b"GET" => Ok(Command::GET {
                key: parse_key(&input)?,
            }),
            b"SET" => Ok(Command::SET {
                key: parse_key(&input)?,
                entry: parse_entry(&input)?,
            }),
            b"DEL" => Ok(Command::DEL {
                keys: parse_keys(&input)?,
            }),
            b"EXISTS" => Ok(Command::EXISTS {
                keys: parse_keys(&input)?,
            }),
            b"MGET" => Ok(Command::MGET {
                keys: parse_keys(&input)?,
            }),
            b"MSET" => Ok(Command::MSET {
                items: parse_items(&input)?,
            }),
            b"TTL" => Ok(Command::TTL {
                key: parse_key(&input)?,
            }),
            b"PTTL" => Ok(Command::PTTL {
                key: parse_key(&input)?,
            }),
            b"PERSIST" => Ok(Command::PERSIST {
                key: parse_key(&input)?,
            }),
            b"EXPIRE" => Ok(Command::EXPIRE {
                key: parse_key(&input)?,
                ttl: parse_ttl(&input)? * 1000,
            }),
            b"PEXPIRE" => Ok(Command::PEXPIRE {
                key: parse_key(&input)?,
                ttl: parse_ttl(&input)?,
            }),
            b"ECHO" => Ok(Command::ECHO {
                msg: parse_msg(&input)?,
            }),
            b"GETSET" => Ok(Command::GETSET {
                key: parse_key(&input)?,
                entry: parse_entry(&input)?,
            }),
            b"GETDEL" => Ok(Command::GETDEL {
                key: parse_key(&input)?,
            }),
            b"SETNX" => Ok(Command::SETNX {
                key: parse_key(&input)?,
                entry: parse_entry(&input)?,
            }),
            b"INCR" => Ok(Command::INCR {
                key: parse_key(&input)?,
            }),
            b"DECR" => Ok(Command::DECR {
                key: parse_key(&input)?,
            }),
            b"STRLEN" => Ok(Command::STRLEN {
                key: parse_key(&input)?,
            }),
            b"APPEND" => Ok(Command::APPEND {
                key: parse_key(&input)?,
                value: parse_value(&input)?,
            }),
            b"SHUTDOWN" => Ok(Command::SHUTDOWN),
            _ => Err(Frame::Error("ERR unknown command".into())),
        }
    }
}

fn parse_key(input: &[Frame]) -> Result<Bytes, Frame> {
    let Some(Frame::BulkString(key)) = input.get(1) else {
        return Err(Frame::Error("ERR missing key".into()));
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

    let exp = parse_exp(input)?;
    Ok(Entry { value, exp })
}

fn parse_value(input: &[Frame]) -> Result<Bytes, Frame> {
    let value = match input.get(2) {
        Some(Frame::BulkString(b)) => b.clone(),
        _ => return Err(Frame::Error("ERR missing value".into())),
    };

    Ok(value)
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

fn parse_config(input: &[Frame]) -> Result<Command, Frame> {
    if input.len() != 3 {
        return Err(Frame::Error(
            "ERR wrong number of arguments for 'config|get' command".into(),
        ));
    }

    let Some(Frame::BulkString(sub)) = input.get(1) else {
        return Err(Frame::Error("ERR syntax error".into()));
    };

    if !sub.eq_ignore_ascii_case(b"get") {
        return Err(Frame::Error("ERR only CONFIG GET is supported".into()));
    }

    let Some(Frame::BulkString(pattern)) = input.get(2) else {
        return Err(Frame::Error("ERR syntax error".into()));
    };

    Ok(Command::CONFIG {
        pattern: pattern.clone(),
    })
}
