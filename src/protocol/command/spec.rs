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
            Command::PING
            | Command::CONFIG_GET { .. }
            | Command::CONFIG_SET { .. }
            | Command::CONFIG_REWRITE
            | Command::DBSIZE
            | Command::FLUSHDB
            | Command::INFO
            | Command::SHUTDOWN => KeyTopology::NoKey,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::types::{Entry, Expiry};

    fn key(cmd: Command) -> Option<Bytes> {
        match cmd.key_topology() {
            KeyTopology::Single(k) => Some(k),
            _ => None,
        }
    }

    fn keys(cmd: Command) -> Option<Vec<Bytes>> {
        match cmd.key_topology() {
            KeyTopology::Multi(ks) => Some(ks),
            _ => None,
        }
    }

    fn no_key(cmd: Command) -> bool {
        matches!(cmd.key_topology(), KeyTopology::NoKey)
    }

    #[test]
    fn ping_is_no_key() {
        assert!(no_key(Command::PING));
    }

    #[test]
    fn get_is_single() {
        let k = Bytes::from_static(b"k");
        assert_eq!(key(Command::GET { key: k.clone() }), Some(k));
    }

    #[test]
    fn set_is_single() {
        let k = Bytes::from_static(b"k");
        assert_eq!(
            key(Command::SET {
                key: k.clone(),
                entry: Entry {
                    value: Bytes::from_static(b"v"),
                    exp: Expiry::None
                },
            }),
            Some(k)
        );
    }

    #[test]
    fn del_is_multi() {
        let ks = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        assert_eq!(keys(Command::DEL { keys: ks.clone() }), Some(ks));
    }

    #[test]
    fn exists_is_multi() {
        let ks = vec![Bytes::from_static(b"a")];
        assert_eq!(keys(Command::EXISTS { keys: ks.clone() }), Some(ks));
    }

    #[test]
    fn mget_is_multi() {
        let ks = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
        assert_eq!(keys(Command::MGET { keys: ks.clone() }), Some(ks));
    }

    #[test]
    fn mset_is_multi() {
        let items = vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))];
        assert_eq!(
            keys(Command::MSET {
                items: items.clone()
            }),
            Some(vec![Bytes::from_static(b"a")])
        );
    }

    #[test]
    fn echo_is_no_key() {
        assert!(no_key(Command::ECHO {
            msg: Bytes::from_static(b"hi")
        }));
    }

    #[test]
    fn config_commands_are_no_key() {
        assert!(no_key(Command::CONFIG_GET {
            pattern: Bytes::from_static(b"*")
        }));
        assert!(no_key(Command::CONFIG_SET {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v")
        }));
        assert!(no_key(Command::CONFIG_REWRITE));
    }

    #[test]
    fn dbsize_flushdb_info_shutdown_are_no_key() {
        assert!(no_key(Command::DBSIZE));
        assert!(no_key(Command::FLUSHDB));
        assert!(no_key(Command::INFO));
        assert!(no_key(Command::SHUTDOWN));
    }

    #[test]
    fn single_key_commands_match() {
        let k = Bytes::from_static(b"k");
        assert_eq!(key(Command::TTL { key: k.clone() }), Some(k.clone()));
        assert_eq!(key(Command::PTTL { key: k.clone() }), Some(k.clone()));
        assert_eq!(key(Command::PERSIST { key: k.clone() }), Some(k.clone()));
        assert_eq!(
            key(Command::EXPIRE {
                key: k.clone(),
                ttl: 10
            }),
            Some(k.clone())
        );
        assert_eq!(
            key(Command::PEXPIRE {
                key: k.clone(),
                ttl: 10
            }),
            Some(k.clone())
        );
        assert_eq!(key(Command::GETDEL { key: k.clone() }), Some(k.clone()));
        assert_eq!(
            key(Command::GETSET {
                key: k.clone(),
                entry: Entry {
                    value: Bytes::from_static(b"v"),
                    exp: Expiry::None
                },
            }),
            Some(k.clone())
        );
        assert_eq!(
            key(Command::SETNX {
                key: k.clone(),
                entry: Entry {
                    value: Bytes::from_static(b"v"),
                    exp: Expiry::None
                },
            }),
            Some(k.clone())
        );
        assert_eq!(key(Command::INCR { key: k.clone() }), Some(k.clone()));
        assert_eq!(key(Command::DECR { key: k.clone() }), Some(k.clone()));
        assert_eq!(key(Command::STRLEN { key: k.clone() }), Some(k.clone()));
        assert_eq!(
            key(Command::APPEND {
                key: k.clone(),
                value: Bytes::from_static(b"v")
            }),
            Some(k)
        );
    }
}
