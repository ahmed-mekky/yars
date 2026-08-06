use crate::{
    config::AppConfig,
    protocol::{command::Command, resp::Frame},
    service::handlers::{
        multikey::{del, exists, mget, mset},
        nokey::{config_get, config_rewrite, config_set, dbsize, echo, flushdb, info, ping},
        singlekey::{
            append, decr, expire, get, getdel, getset, incr, persist, pexpire, pttl, set, setnx,
            strlen, ttl,
        },
    },
    store::{actor::StoreActor, handle::StoreHandle, memory::MemoryStore, persistence::aof::Aof},
    utils::time::get_current_millis,
};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub struct ServerContext {
    pub store: StoreHandle,
    pub config: Arc<RwLock<AppConfig>>,
    pub cancel: CancellationToken,
}

impl ServerContext {
    pub async fn new(config: AppConfig) -> anyhow::Result<Arc<Self>> {
        let num_shards = config.num_shards;
        let mut senders = Vec::with_capacity(num_shards.into());

        for _ in 0..num_shards {
            let store = MemoryStore::new();
            let aof = Aof::new(&config).await?;
            let (tx, _) = StoreActor::spawn(store, aof, 1024);
            senders.push(tx);
        }

        let store_handle = StoreHandle::new(senders);

        Ok(Arc::new(Self {
            store: store_handle,
            config: Arc::new(RwLock::new(config)),
            cancel: CancellationToken::new(),
        }))
    }

    pub async fn execute(&self, cmd: &Command) -> Result<Frame> {
        let store = &self.store;
        match cmd {
            Command::PING => ping(),
            Command::CONFIG_GET { pattern } => config_get(&self.config, pattern.clone()).await,
            Command::CONFIG_SET { key, value } => {
                config_set(&self.config, store, key.clone(), value.clone()).await
            }
            Command::CONFIG_REWRITE => config_rewrite(&self.config).await,
            Command::ECHO { msg } => echo(msg.clone()),
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
                pexpire(store, key.clone(), *ttl, get_current_millis()).await
            }
            Command::DEL { keys } => del(store, keys.clone()).await,
            Command::EXISTS { keys } => exists(store, keys.clone()).await,
            Command::MGET { keys } => mget(store, keys.clone()).await,
            Command::MSET { items } => mset(store, items.clone()).await,
            #[allow(unreachable_patterns)]
            Command::SHUTDOWN => unreachable!(),
        }
    }
}
