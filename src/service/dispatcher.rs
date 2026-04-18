use crate::{
    config::AppConfig,
    protocol::{command::Command, resp::Frame},
    service::handlers::{
        SetMutation,
        multikey::{del, exists, mget, mset},
        nokey::{config_get, config_rewrite, config_set, dbsize, echo, flushdb, info, ping},
        singlekey::{
            append, decr, expire, get, getdel, getset, incr, persist, pttl, set, setnx, strlen, ttl,
        },
    },
    store::{
        memory::MemoryStore,
        persistence::{AofEngine, record::Record},
        types::Expiry,
    },
    utils::time::get_current_millis,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn execute(
    store: &MemoryStore,
    config: &Arc<RwLock<AppConfig>>,
    aof: &Option<Arc<AofEngine>>,
    cmd: Command,
) -> Frame {
    let (frame, set_mutation) = dispatch(store, config, aof, &cmd).await;

    if !matches!(frame, Frame::Error(_)) {
        store.increment_commands();
        if let Some(aof) = aof
            && let Some(record) = aof_record(&cmd, &frame, set_mutation)
            && let Err(err) = aof.append(record).await
        {
            eprintln!("AOF write error: {err:?}");
        }
    }

    frame
}

async fn dispatch(
    store: &MemoryStore,
    config: &Arc<RwLock<AppConfig>>,
    aof: &Option<Arc<AofEngine>>,
    cmd: &Command,
) -> (Frame, Option<SetMutation>) {
    match cmd {
        Command::PING => ping().await,
        Command::CONFIG_GET { pattern } => config_get(config, pattern.clone()).await,
        Command::CONFIG_SET { key, value } => {
            config_set(config, aof, key.clone(), value.clone()).await
        }
        Command::CONFIG_REWRITE => config_rewrite(config).await,
        Command::ECHO { msg } => echo(msg.clone()).await,
        Command::DBSIZE => dbsize(store).await,
        Command::FLUSHDB => flushdb(store).await,
        Command::INFO => info(store).await,
        Command::GET { key } => get(store, key.clone()).await,
        Command::SET { key, entry } => set(store, key.clone(), entry.clone()).await,
        Command::GETDEL { key } => getdel(store, key.clone()).await,
        Command::GETSET { key, entry } => getset(store, key.clone(), entry.clone()).await,
        Command::SETNX { key, entry } => setnx(store, key.clone(), entry.clone()).await,
        Command::INCR { key } => incr(store, key.clone()).await,
        Command::DECR { key } => decr(store, key.clone()).await,
        Command::STRLEN { key } => strlen(store, key.clone()).await,
        Command::APPEND { key, value } => append(store, key.clone(), value.clone()).await,
        Command::TTL { key } => ttl(store, key.clone(), get_current_millis()).await,
        Command::PTTL { key } => pttl(store, key.clone(), get_current_millis()).await,
        Command::PERSIST { key } => persist(store, key.clone()).await,
        Command::EXPIRE { key, ttl } => {
            expire(store, key.clone(), *ttl, get_current_millis()).await
        }
        Command::PEXPIRE { key, ttl } => {
            expire(store, key.clone(), *ttl, get_current_millis()).await
        }
        Command::DEL { keys } => del(store, keys.clone()).await,
        Command::EXISTS { keys } => exists(store, keys.clone()).await,
        Command::MGET { keys } => mget(store, keys.clone()).await,
        Command::MSET { items } => mset(store, items.clone()).await,
        #[allow(unreachable_patterns)]
        Command::SHUTDOWN => unreachable!(),
    }
}

fn aof_record(cmd: &Command, frame: &Frame, set_mutation: Option<SetMutation>) -> Option<Record> {
    match cmd {
        Command::SET { .. }
        | Command::SETNX { .. }
        | Command::GETSET { .. }
        | Command::INCR { .. }
        | Command::DECR { .. }
        | Command::APPEND { .. }
        | Command::PERSIST { .. }
        | Command::EXPIRE { .. }
        | Command::PEXPIRE { .. } => {
            let (key, entry) = set_mutation?;
            let exp_ms = match entry.exp {
                Expiry::At(ms) => Some(ms),
                Expiry::None => None,
                Expiry::Keep => None,
            };
            Some(Record::Set {
                key,
                value: entry.value,
                exp_ms,
            })
        }
        Command::DEL { keys } => Some(Record::Del { keys: keys.clone() }),
        Command::GETDEL { key } => {
            if matches!(frame, Frame::BulkString(_)) {
                Some(Record::Del {
                    keys: vec![key.clone()],
                })
            } else {
                None
            }
        }
        Command::MSET { items } => Some(Record::MSet {
            items: items.clone(),
        }),
        Command::FLUSHDB => Some(Record::FlushDb),
        _ => None,
    }
}
