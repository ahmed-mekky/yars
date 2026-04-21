use crate::{
    protocol::{command::Command, resp::Frame},
    store::types::{Entry, Expiry},
    utils::time::get_current_millis,
};
use nom::AsBytes;
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
    let Some(Frame::BulkString(sub)) = input.get(1) else {
        return Err(Frame::Error("ERR syntax error".into()));
    };

    match sub.to_ascii_uppercase().as_bytes() {
        b"GET" => {
            if input.len() != 3 {
                return Err(Frame::Error(
                    "ERR wrong number of arguments for 'config|get' command".into(),
                ));
            }

            let Some(Frame::BulkString(pattern)) = input.get(2) else {
                return Err(Frame::Error("ERR syntax error".into()));
            };

            Ok(Command::CONFIG_GET {
                pattern: pattern.clone(),
            })
        }
        b"SET" => {
            if input.len() != 4 {
                return Err(Frame::Error(
                    "ERR wrong number of arguments for 'config|set' command".into(),
                ));
            }

            let Some(Frame::BulkString(key)) = input.get(2) else {
                return Err(Frame::Error("ERR syntax error".into()));
            };

            let Some(Frame::BulkString(value)) = input.get(3) else {
                return Err(Frame::Error("ERR syntax error".into()));
            };
            Ok(Command::CONFIG_SET {
                key: key.clone(),
                value: value.clone(),
            })
        }
        b"REWRITE" => {
            if input.len() != 2 {
                return Err(Frame::Error(
                    "ERR wrong number of arguments for 'config|rewrite' command".into(),
                ));
            }
            Ok(Command::CONFIG_REWRITE)
        }
        _ => Err(Frame::Error("ERR unknown command for 'CONFIG' ".into())),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protocol::{command::Command, resp::Frame},
        store::types::Expiry,
    };
    use tokio_util::bytes::Bytes;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.as_bytes().to_vec()))
    }

    fn bulk_bytes(b: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(b))
    }

    fn cmd_frame(args: &[Frame]) -> Frame {
        Frame::Array(args.to_vec())
    }

    #[test]
    fn non_array_returns_error() {
        let result = Command::try_from(Frame::SimpleString("PING".into()));
        assert!(result.is_err());
    }

    #[test]
    fn missing_command_returns_error() {
        let result = Command::try_from(Frame::Array(vec![]));
        assert!(result.is_err());
    }

    #[test]
    fn unknown_command_returns_error() {
        let frame = cmd_frame(&[bulk("UNKNOWNCMD")]);
        let err = Command::try_from(frame).unwrap_err();
        assert!(matches!(err, Frame::Error(s) if s.contains("unknown command")));
    }

    #[test]
    fn parse_ping() {
        let frame = cmd_frame(&[bulk("PING")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::PING)));
    }

    #[test]
    fn parse_dbsize() {
        let frame = cmd_frame(&[bulk("DBSIZE")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::DBSIZE)));
    }

    #[test]
    fn parse_flushdb() {
        let frame = cmd_frame(&[bulk("FLUSHDB")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::FLUSHDB)));
    }

    #[test]
    fn parse_info() {
        let frame = cmd_frame(&[bulk("INFO")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::INFO)));
    }

    #[test]
    fn parse_shutdown() {
        let frame = cmd_frame(&[bulk("SHUTDOWN")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::SHUTDOWN)));
    }

    #[test]
    fn parse_echo() {
        let frame = cmd_frame(&[bulk("ECHO"), bulk("hello")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::ECHO { msg } if msg.as_ref() == b"hello"));
    }

    #[test]
    fn parse_echo_missing_msg() {
        let frame = cmd_frame(&[bulk("ECHO")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_get() {
        let frame = cmd_frame(&[bulk("GET"), bulk("mykey")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::GET { key } if key.as_ref() == b"mykey"));
    }

    #[test]
    fn parse_get_missing_key() {
        let frame = cmd_frame(&[bulk("GET")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_set_basic() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v")]);
        let cmd = Command::try_from(frame).unwrap();
        match cmd {
            Command::SET { key, entry } => {
                assert_eq!(key.as_ref(), b"k");
                assert_eq!(entry.value.as_ref(), b"v");
                assert!(matches!(entry.exp, Expiry::None));
            }
            _ => panic!("expected SET"),
        }
    }

    #[test]
    fn parse_set_with_ex() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v"), bulk("EX"), bulk("60")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::SET { entry, .. } if matches!(entry.exp, Expiry::At(_))));
    }

    #[test]
    fn parse_set_with_px() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v"), bulk("PX"), bulk("5000")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::SET { entry, .. } if matches!(entry.exp, Expiry::At(_))));
    }

    #[test]
    fn parse_set_with_keepttl() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v"), bulk("KEEPTTL")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::SET { entry, .. } if matches!(entry.exp, Expiry::Keep)));
    }

    #[test]
    fn parse_set_with_invalid_modifier() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v"), bulk("BOGUS")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_set_missing_value() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_del() {
        let frame = cmd_frame(&[bulk("DEL"), bulk("a"), bulk("b")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::DEL { keys } if keys.len() == 2));
    }

    #[test]
    fn parse_exists() {
        let frame = cmd_frame(&[bulk("EXISTS"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(
            matches!(cmd, Command::EXISTS { keys } if keys.len() == 1 && keys[0].as_ref() == b"k")
        );
    }

    #[test]
    fn parse_mget() {
        let frame = cmd_frame(&[bulk("MGET"), bulk("a"), bulk("b"), bulk("c")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::MGET { keys } if keys.len() == 3));
    }

    #[test]
    fn parse_mset() {
        let frame = cmd_frame(&[bulk("MSET"), bulk("k1"), bulk("v1"), bulk("k2"), bulk("v2")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::MSET { items } if items.len() == 2));
    }

    #[test]
    fn parse_mset_odd_args_errors() {
        let frame = cmd_frame(&[bulk("MSET"), bulk("k1"), bulk("v1"), bulk("k2")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_ttl() {
        let frame = cmd_frame(&[bulk("TTL"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::TTL { key } if key.as_ref() == b"k"));
    }

    #[test]
    fn parse_pttl() {
        let frame = cmd_frame(&[bulk("PTTL"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::PTTL { key } if key.as_ref() == b"k"));
    }

    #[test]
    fn parse_persist() {
        let frame = cmd_frame(&[bulk("PERSIST"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::PERSIST { key } if key.as_ref() == b"k"));
    }

    #[test]
    fn parse_expire() {
        let frame = cmd_frame(&[bulk("EXPIRE"), bulk("k"), bulk("10")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::EXPIRE { ttl, .. } if ttl == 10000));
    }

    #[test]
    fn parse_pexpire() {
        let frame = cmd_frame(&[bulk("PEXPIRE"), bulk("k"), bulk("500")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::PEXPIRE { ttl, .. } if ttl == 500));
    }

    #[test]
    fn parse_expire_missing_ttl() {
        let frame = cmd_frame(&[bulk("EXPIRE"), bulk("k")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_expire_non_numeric_ttl() {
        let frame = cmd_frame(&[bulk("EXPIRE"), bulk("k"), bulk("abc")]);
        assert!(Command::try_from(frame).is_err());
    }

    #[test]
    fn parse_getdel() {
        let frame = cmd_frame(&[bulk("GETDEL"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::GETDEL { key } if key.as_ref() == b"k"));
    }

    #[test]
    fn parse_getset() {
        let frame = cmd_frame(&[bulk("GETSET"), bulk("k"), bulk("v")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(
            matches!(cmd, Command::GETSET { key, entry } if key.as_ref() == b"k" && entry.value.as_ref() == b"v")
        );
    }

    #[test]
    fn parse_setnx() {
        let frame = cmd_frame(&[bulk("SETNX"), bulk("k"), bulk("v")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(
            matches!(cmd, Command::SETNX { key, entry } if key.as_ref() == b"k" && entry.value.as_ref() == b"v")
        );
    }

    #[test]
    fn parse_incr() {
        let frame = cmd_frame(&[bulk("INCR"), bulk("counter")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::INCR { key } if key.as_ref() == b"counter"));
    }

    #[test]
    fn parse_decr() {
        let frame = cmd_frame(&[bulk("DECR"), bulk("counter")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::DECR { key } if key.as_ref() == b"counter"));
    }

    #[test]
    fn parse_strlen() {
        let frame = cmd_frame(&[bulk("STRLEN"), bulk("k")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::STRLEN { .. }));
    }

    #[test]
    fn parse_append() {
        let frame = cmd_frame(&[bulk("APPEND"), bulk("k"), bulk("v")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::APPEND { .. }));
    }

    #[test]
    fn parse_config_get() {
        let frame = cmd_frame(&[bulk("CONFIG"), bulk("GET"), bulk("*")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::CONFIG_GET { pattern } if pattern.as_ref() == b"*"));
    }

    #[test]
    fn parse_config_set() {
        let frame = cmd_frame(&[bulk("CONFIG"), bulk("SET"), bulk("appendonly"), bulk("no")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(
            matches!(cmd, Command::CONFIG_SET { key, value } if (key.as_ref() == b"appendonly" && value.as_ref() == b"no"))
        );
    }

    #[test]
    fn parse_config_rewrite() {
        let frame = cmd_frame(&[bulk("CONFIG"), bulk("REWRITE")]);
        let cmd = Command::try_from(frame).unwrap();
        assert!(matches!(cmd, Command::CONFIG_REWRITE));
    }

    #[test]
    fn parse_config_wrong_arg_count() {
        let frame = cmd_frame(&[bulk("CONFIG"), bulk("GET")]);
        let err = Command::try_from(frame).unwrap_err();
        assert!(matches!(err, Frame::Error(s) if s.contains("wrong number")));
    }

    #[test]
    fn parse_case_insensitive() {
        let frame = cmd_frame(&[bulk_bytes(b"ping")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::PING)));

        let frame = cmd_frame(&[bulk_bytes(b"PiNg")]);
        assert!(matches!(Command::try_from(frame), Ok(Command::PING)));
    }

    #[test]
    fn parse_set_ex_with_non_numeric() {
        let frame = cmd_frame(&[bulk("SET"), bulk("k"), bulk("v"), bulk("EX"), bulk("abc")]);
        assert!(Command::try_from(frame).is_err());
    }
}
