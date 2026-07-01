use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio_util::bytes::Bytes;

use crate::{
    config::FsyncMode,
    store::{
        handle::StoreHandle,
        memory::MemoryStore,
        persistence::{aof::Aof, record::Record},
        types::{Entry, Expiry},
    },
};

pub enum StoreRequest {
    Get(Bytes),
    Exists(Vec<Bytes>),
    MGet(Vec<Bytes>),
    Len,
    IsEmpty,
    Set(Bytes, Entry),
    Del(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
    Clear,
    GetDel(Bytes),
    GetSet(Bytes, Entry),
    SetNx(Bytes, Entry),
    Incr(Bytes),
    Decr(Bytes),
    Strlen(Bytes),
    Append(Bytes, Bytes),
    Persist(Bytes),
    Expire(Bytes, u64, u64),
    PExpire(Bytes, u64, u64),
    Ttl(Bytes, u64),
    Pttl(Bytes, u64),
    Info,
    SetFsyncMode(FsyncMode),
    Shutdown,
    Replay,
}
pub enum StoreResponse {
    Get(Option<Entry>),
    Exists(i64),
    MGet(Vec<Option<Entry>>),
    Len(i64),
    IsEmpty(bool),
    Set,
    Del(i64),
    MSet,
    Clear,
    GetDel(Option<Entry>),
    GetSet(Option<Entry>),
    SetNx(bool),
    Incr(i64),
    Decr(i64),
    Strlen(i64),
    Append(i64),
    Persist(bool),
    Expire(bool),
    PExpire(bool),
    Error(anyhow::Error),
    Ttl(i64),
    Pttl(i64),
    Info(String),
    SetFsyncMode,
    Shutdown,
    Replay,
}

pub struct StoreMessage {
    pub request: StoreRequest,
    pub respond_to: oneshot::Sender<Result<StoreResponse>>,
}
pub struct StoreActor {
    store: MemoryStore,
    aof: Aof,
    rx: mpsc::Receiver<StoreMessage>,
}

pub enum ActorResult {
    Read(StoreResponse),
    Write(StoreResponse, Record),
    Error(anyhow::Error),
}

impl StoreActor {
    pub fn new(store: MemoryStore, aof: Aof, rx: mpsc::Receiver<StoreMessage>) -> Self {
        Self { store, aof, rx }
    }

    pub fn spawn(
        store: MemoryStore,
        aof: Aof,
        buffer: usize,
    ) -> (StoreHandle, tokio::task::JoinHandle<()>) {
        let (tx, rx) = mpsc::channel(buffer);
        let actor = StoreActor::new(store, aof, rx);
        let handle = tokio::spawn(actor.run());
        (StoreHandle::new(tx), handle)
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            self.process(msg).await;
        }
    }

    async fn handle(&mut self, req: StoreRequest) -> ActorResult {
        match req {
            StoreRequest::Get(key) => ActorResult::Read(StoreResponse::Get(self.store.get(&key))),
            StoreRequest::Set(key, entry) => {
                let resolved = self.store.set(key.clone(), entry.clone());
                ActorResult::Write(StoreResponse::Set, to_set_record(key, resolved))
            }
            StoreRequest::Del(keys) => ActorResult::Write(
                StoreResponse::Del(keys.iter().map(|k| self.store.del(k)).sum()),
                Record::Del { keys },
            ),
            StoreRequest::Len => ActorResult::Read(StoreResponse::Len(self.store.len() as i64)),
            StoreRequest::IsEmpty => {
                ActorResult::Read(StoreResponse::IsEmpty(self.store.is_empty()))
            }
            StoreRequest::Exists(keys) => {
                ActorResult::Read(StoreResponse::Exists(self.store.exists(&keys)))
            }
            StoreRequest::MGet(keys) => ActorResult::Read(StoreResponse::MGet(
                keys.iter().map(|k| self.store.get(k)).collect(),
            )),
            StoreRequest::MSet(kvs) => {
                self.store.mset(&kvs);
                ActorResult::Write(StoreResponse::MSet, Record::MSet { items: kvs })
            }
            StoreRequest::Clear => {
                self.store.clear();
                ActorResult::Write(StoreResponse::Clear, Record::FlushDb)
            }
            StoreRequest::GetDel(key) => match self.store.getdel(&key) {
                Some(entry) => ActorResult::Write(
                    StoreResponse::GetDel(Some(entry)),
                    Record::Del { keys: vec![key] },
                ),
                None => ActorResult::Read(StoreResponse::GetDel(None)),
            },
            StoreRequest::GetSet(key, entry) => {
                let (old, resolved) = self.store.getset(key.clone(), entry.clone());
                ActorResult::Write(StoreResponse::GetSet(old), to_set_record(key, resolved))
            }
            StoreRequest::SetNx(key, entry) => match self.store.setnx(key.clone(), entry.clone()) {
                true => ActorResult::Write(StoreResponse::SetNx(true), to_set_record(key, entry)),
                false => ActorResult::Read(StoreResponse::SetNx(false)),
            },
            StoreRequest::Incr(key) => {
                let (entry, sum) = match self.store.incr(key.clone()) {
                    Ok(result) => result,
                    Err(e) => return ActorResult::Error(e),
                };
                ActorResult::Write(StoreResponse::Incr(sum), to_set_record(key, entry))
            }
            StoreRequest::Decr(key) => {
                let (entry, sub) = match self.store.decr(key.clone()) {
                    Ok(result) => result,
                    Err(e) => return ActorResult::Error(e),
                };

                ActorResult::Write(StoreResponse::Decr(sub), to_set_record(key, entry))
            }
            StoreRequest::Strlen(key) => {
                let result = self.store.strlen(key.clone());
                ActorResult::Read(StoreResponse::Strlen(result))
            }
            StoreRequest::Append(key, value) => {
                let entry = self.store.append(key.clone(), value);
                ActorResult::Write(
                    StoreResponse::Append(entry.value.len() as i64),
                    to_set_record(key, entry),
                )
            }
            StoreRequest::Persist(key) => match self.store.persist(key.clone()) {
                Some(entry) => {
                    ActorResult::Write(StoreResponse::Persist(true), to_set_record(key, entry))
                }
                None => ActorResult::Read(StoreResponse::Persist(false)),
            },
            StoreRequest::Expire(key, expiry, now) => {
                match self.store.pexpire(key.clone(), expiry, now) {
                    Some(entry) => {
                        ActorResult::Write(StoreResponse::Expire(true), to_set_record(key, entry))
                    }
                    None => ActorResult::Read(StoreResponse::Expire(false)),
                }
            }
            StoreRequest::PExpire(key, expiry, now) => {
                match self.store.pexpire(key.clone(), expiry, now) {
                    Some(entry) => {
                        ActorResult::Write(StoreResponse::PExpire(true), to_set_record(key, entry))
                    }
                    None => ActorResult::Read(StoreResponse::PExpire(false)),
                }
            }
            StoreRequest::Ttl(key, now) => {
                ActorResult::Read(StoreResponse::Ttl(self.store.ttl(key, now)))
            }
            StoreRequest::Pttl(key, now) => {
                ActorResult::Read(StoreResponse::Pttl(self.store.pttl(key, now)))
            }
            StoreRequest::Info => ActorResult::Read(StoreResponse::Info(self.store.info())),
            StoreRequest::SetFsyncMode(mode) => {
                self.aof.set_fsync_mode(mode);
                ActorResult::Read(StoreResponse::SetFsyncMode)
            }
            StoreRequest::Shutdown => {
                self.aof.shutdown().await;
                ActorResult::Read(StoreResponse::Shutdown)
            }
            StoreRequest::Replay => {
                if let Err(e) = self.aof.replay(&mut self.store).await {
                    ActorResult::Error(e)
                } else {
                    ActorResult::Read(StoreResponse::Replay)
                }
            }
        }
    }

    async fn process(&mut self, msg: StoreMessage) {
        if !matches!(
            msg.request,
            StoreRequest::Replay | StoreRequest::Shutdown | StoreRequest::SetFsyncMode(_)
        ) {
            self.store.increment_commands();
        }
        match self.handle(msg.request).await {
            ActorResult::Read(res) => {
                let _ = msg.respond_to.send(Ok(res));
            }
            ActorResult::Write(res, record) => {
                if let Err(e) = self.aof.append(record).await {
                    let _ = msg.respond_to.send(Err(e));
                } else {
                    let _ = msg.respond_to.send(Ok(res));
                }
            }
            ActorResult::Error(e) => {
                let _ = msg.respond_to.send(Err(e));
            }
        }
    }
}

fn to_set_record(key: Bytes, entry: Entry) -> Record {
    let exp_ms = match entry.exp {
        Expiry::At(ms) => Some(ms),
        Expiry::None | Expiry::Keep => None,
    };
    Record::Set {
        key,
        value: entry.value,
        exp_ms,
    }
}
