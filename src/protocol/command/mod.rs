mod parse;
pub mod spec;

use tokio_util::bytes::Bytes;

use crate::store::types::Entry;

#[allow(clippy::upper_case_acronyms)]
pub enum Command {
    PING,
    CONFIG { pattern: Bytes },
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
    INFO,
    GETDEL { key: Bytes },
    GETSET { key: Bytes, entry: Entry },
    SETNX { key: Bytes, entry: Entry },
    INCR { key: Bytes },
    DECR { key: Bytes },
    STRLEN { key: Bytes },
    APPEND { key: Bytes, value: Bytes },
}
