use crate::{
    config::AppConfig,
    protocol::{command::Command, resp::Frame},
    service::handlers::{
        CommandEffect,
        multikey::{del, exists, mget, mset},
        nokey::{config_get, config_rewrite, config_set, dbsize, echo, flushdb, info, ping},
        singlekey::{
            append, decr, expire, get, getdel, getset, incr, persist, pttl, set, setnx, strlen, ttl,
        },
    },
    store::{
        memory::MemoryStore,
        persistence::{
            AofEngine,
            aof::{Aof, NoopAof},
        },
    },
    utils::time::get_current_millis,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub struct ServerContext {
    pub store: MemoryStore,
    pub config: Arc<RwLock<AppConfig>>,
    pub aof: Arc<dyn Aof>,
    pub cancel: CancellationToken,
}

impl ServerContext {
    pub async fn new(config: AppConfig) -> anyhow::Result<Arc<Self>> {
        let store = MemoryStore::new();
        let aof: Arc<dyn Aof> = if config.append_only {
            let engine = AofEngine::open(config.aof_path.clone(), config.fsync_mode).await?;
            Arc::new(engine)
        } else {
            Arc::new(NoopAof)
        };
        Ok(Arc::new(Self {
            store,
            config: Arc::new(RwLock::new(config)),
            aof,
            cancel: CancellationToken::new(),
        }))
    }

    pub async fn execute(&self, cmd: Command) -> Frame {
        let effect = self.dispatch(&cmd).await;
        match effect {
            CommandEffect::Read(frame) => {
                if !matches!(frame, Frame::Error(_)) {
                    self.store.increment_commands();
                }
                frame
            }
            CommandEffect::Write(frame, record) => {
                if !matches!(frame, Frame::Error(_)) {
                    self.store.increment_commands();
                    if let Err(err) = self.aof.append(record).await {
                        eprintln!("AOF write error: {err:?}");
                    }
                }
                frame
            }
        }
    }

    async fn dispatch(&self, cmd: &Command) -> CommandEffect {
        let store = &self.store;
        let config = &self.config;
        let aof = &self.aof;
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
}
