use crate::store::{
    actor::{StoreMessage, StoreRequest, StoreResponse},
    types::Entry,
};
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio_util::bytes::Bytes;

#[derive(Clone)]
pub struct StoreHandle {
    tx: mpsc::Sender<StoreMessage>,
}

impl StoreHandle {
    pub fn new(tx: mpsc::Sender<StoreMessage>) -> Self {
        Self { tx }
    }

    pub async fn get(&self, key: Bytes) -> Result<Option<Entry>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Get(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Get(entry))) => Ok(entry),
            Some(Ok(_)) => Err(anyhow::anyhow!("Unexpected response")),
            Some(Err(e)) => Err(e),
            None => Err(anyhow::anyhow!("store actor unavailable")),
        }
    }
    pub async fn set(&self, key: Bytes, entry: Entry) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Set(key, entry),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Set)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn getdel(&self, key: Bytes) -> Result<Option<Entry>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::GetDel(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::GetDel(entry))) => Ok(entry),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn getset(&self, key: Bytes, entry: Entry) -> Result<Option<Entry>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::GetSet(key, entry),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::GetSet(entry))) => Ok(entry),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn setnx(&self, key: Bytes, entry: Entry) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::SetNx(key, entry),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::SetNx(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn incr(&self, key: Bytes) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Incr(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Incr(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn decr(&self, key: Bytes) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Decr(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Decr(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn strlen(&self, key: Bytes) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Strlen(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Strlen(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn append(&self, key: Bytes, value: Bytes) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Append(key, value),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Append(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn persist(&self, key: Bytes) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Persist(key),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Persist(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn expire(&self, key: Bytes, expiry: u64, now: u64) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Expire(key, expiry, now),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Expire(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn pexpire(&self, key: Bytes, expiry: u64, now: u64) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::PExpire(key, expiry, now),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::PExpire(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn ttl(&self, key: Bytes, now: u64) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Ttl(key, now),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Ttl(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn pttl(&self, key: Bytes, now: u64) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Pttl(key, now),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Pttl(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn len(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Len,
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Len(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn clear(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Clear,
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Clear)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub(crate) async fn info(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Info,
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Info(info))) => Ok(info),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn exists(&self, keys: Vec<Bytes>) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Exists(keys),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Exists(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn mset(&self, items: Vec<(Bytes, Bytes)>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::MSet(items),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::MSet)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn mget(&self, keys: Vec<Bytes>) -> Result<Vec<Option<Entry>>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::MGet(keys),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::MGet(entry))) => Ok(entry),
            Some(Ok(_)) => Err(anyhow::anyhow!("Unexpected response")),
            Some(Err(e)) => Err(e),
            None => Err(anyhow::anyhow!("store actor unavailable")),
        }
    }

    pub async fn del(&self, keys: Vec<Bytes>) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Del(keys),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Del(result))) => Ok(result),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn set_fsync_mode(&self, fsync_mode: crate::config::FsyncMode) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::SetFsyncMode(fsync_mode),
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::SetFsyncMode)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Shutdown,
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Shutdown)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }

    pub async fn replay(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(StoreMessage {
                request: StoreRequest::Replay,
                respond_to: tx,
            })
            .await
            .ok();
        match rx.await.ok() {
            Some(Ok(StoreResponse::Replay)) => Ok(()),
            Some(Err(e)) => Err(e),
            _ => Err(anyhow::anyhow!("Unexpected error")),
        }
    }
}
