use tokio_util::bytes::Bytes;

use crate::protocol::command::Command;

pub enum KeyTopology {
    NoKey,
    Single(Bytes),
    Multi(Vec<Bytes>),
}

impl Command {
    pub fn key_topology(&self) -> KeyTopology {
        match self {
            Command::PING | Command::DBSIZE | Command::FLUSHDB => KeyTopology::NoKey,
            Command::GET { key }
            | Command::SET { key, .. }
            | Command::TTL { key }
            | Command::PTTL { key }
            | Command::PERSIST { key }
            | Command::EXPIRE { key, .. }
            | Command::PEXPIRE { key, .. }
            | Command::GETDEL { key }
            | Command::GETSET { key, .. }
            | Command::SETNX { key, .. }
            | Command::INCR { key }
            | Command::DECR { key }
            | Command::STRLEN { key }
            | Command::APPEND { key, .. } => KeyTopology::Single(key.clone()),
            Command::DEL { keys } | Command::EXISTS { keys } | Command::MGET { keys } => {
                KeyTopology::Multi(keys.clone())
            }
            Command::MSET { items } => {
                KeyTopology::Multi(items.iter().map(|(key, _)| key.clone()).collect())
            }
            Command::ECHO { .. } => KeyTopology::NoKey,
        }
    }
}
