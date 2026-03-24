use crate::{
    protocol::{command::Command, resp::Frame},
    service::handlers::{
        multikey::{del, exists, mget, mset},
        nokey::{dbsize, echo, flushdb, ping},
        singlekey::{
            append, decr, expire, get, getdel, getset, incr, persist, pttl, set, setnx, strlen, ttl,
        },
    },
    store::{memory::MemoryStore, traits::Store},
    utils::time::get_current_millis,
};

pub async fn execute(store: &MemoryStore, cmd: Command) -> Frame {
    execute_with_store(store, cmd).await
}

pub async fn execute_with_store(store: &impl Store, cmd: Command) -> Frame {
    match cmd {
        Command::PING => ping().await,
        Command::ECHO { msg } => echo(msg).await,
        Command::DBSIZE => dbsize(store).await,
        Command::FLUSHDB => flushdb(store).await,
        Command::GET { key } => get(store, key).await,
        Command::SET { key, entry } => set(store, key, entry).await,
        Command::GETDEL { key } => getdel(store, key).await,
        Command::GETSET { key, entry } => getset(store, key, entry).await,
        Command::SETNX { key, entry } => setnx(store, key, entry).await,
        Command::INCR { key } => incr(store, key).await,
        Command::DECR { key } => decr(store, key).await,
        Command::STRLEN { key } => strlen(store, key).await,
        Command::APPEND { key, value } => append(store, key, value).await,
        Command::TTL { key } => ttl(store, key, get_current_millis()).await,
        Command::PTTL { key } => pttl(store, key, get_current_millis()).await,
        Command::PERSIST { key } => persist(store, key).await,
        Command::EXPIRE { key, ttl } => expire(store, key, ttl, get_current_millis()).await,
        Command::PEXPIRE { key, ttl } => expire(store, key, ttl, get_current_millis()).await,
        Command::DEL { keys } => del(store, keys).await,
        Command::EXISTS { keys } => exists(store, keys).await,
        Command::MGET { keys } => mget(store, keys).await,
        Command::MSET { items } => mset(store, items).await,
    }
}
