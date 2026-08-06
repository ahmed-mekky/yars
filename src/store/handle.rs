use std::collections::HashMap;

use crate::store::{
    actor::{StoreMessage, StoreRequest, StoreResponse},
    hash::shard_index,
    memory::ServerInfo,
    types::Entry,
};
use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
use tokio_util::bytes::Bytes;

#[derive(Clone)]
pub struct StoreHandle {
    shards: Vec<mpsc::Sender<StoreMessage>>,
    num_shards: u8,
}

impl StoreHandle {
    pub fn new(shards: Vec<mpsc::Sender<StoreMessage>>) -> Self {
        let num_shards = shards.len() as u8;
        Self { shards, num_shards }
    }

    fn route(&self, key: &[u8]) -> &mpsc::Sender<StoreMessage> {
        let idx = crate::store::hash::shard_index(key, self.num_shards);
        &self.shards[idx]
    }

    pub async fn get(&self, key: Bytes) -> Result<Option<Entry>> {
        let (tx, rx) = oneshot::channel();
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        self.route(&key)
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
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::Len,
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        let mut total = 0i64;
        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Len(n))) => total += n,
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(total)
    }

    pub async fn clear(&self) -> Result<()> {
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::Clear,
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Clear)) => (),
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(())
    }

    pub(crate) async fn info(&self) -> Result<String> {
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::Info,
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        let mut info = ServerInfo::default();
        for fut in futs {
            info += match fut.await? {
                Ok(Ok(StoreResponse::Info(info))) => info,
                _ => return Err(anyhow::anyhow!("shard error")),
            };
        }
        Ok(format!(
            "yars_version:{}\r\ndb_keys:{}\r\nused_memory:{}\r\nuptime_seconds:{}\r\ntotal_commands:{}\r\n",
            env!("CARGO_PKG_VERSION"),
            info.key_count,
            info.used_memory,
            info.uptime_seconds,
            info.total_commands
        ))
    }

    pub async fn exists(&self, keys: Vec<Bytes>) -> Result<i64> {
        let mut groups: HashMap<usize, Vec<Bytes>> = HashMap::new();
        for key in &keys {
            let idx = shard_index(key, self.num_shards);
            groups.entry(idx).or_default().push(key.clone());
        }

        let mut futs = Vec::new();
        for (shard_idx, group_keys) in groups {
            let tx = self.shards[shard_idx].clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let fut = tokio::spawn(async move {
                let _ = tx
                    .send(StoreMessage {
                        request: StoreRequest::Exists(group_keys),
                        respond_to: reply_tx,
                    })
                    .await;
                reply_rx.await
            });
            futs.push(fut);
        }

        let mut total = 0i64;
        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Exists(count))) => total += count,
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(total)
    }

    pub async fn mset(&self, items: Vec<(Bytes, Bytes)>) -> Result<()> {
        let mut groups: HashMap<usize, Vec<(Bytes, Bytes)>> = HashMap::new();

        for (key, value) in &items {
            let idx = shard_index(key, self.num_shards);
            groups
                .entry(idx)
                .or_default()
                .push((key.clone(), value.clone()));
        }

        let mut futs = Vec::new();
        for (shard_idx, group_keys) in groups {
            let tx = self.shards[shard_idx].clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let fut = tokio::spawn(async move {
                let _ = tx
                    .send(StoreMessage {
                        request: StoreRequest::MSet(group_keys),
                        respond_to: reply_tx,
                    })
                    .await;
                reply_rx.await
            });
            futs.push(fut);
        }

        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::MSet)) => (),
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(())
    }

    pub async fn mget(&self, keys: Vec<Bytes>) -> Result<Vec<Option<Entry>>> {
        let mut groups: HashMap<usize, Vec<(usize, Bytes)>> = HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            let shard = shard_index(key, self.num_shards);
            groups.entry(shard).or_default().push((i, key.clone()));
        }

        let mut futs = Vec::new();
        for (shard_idx, group) in &groups {
            let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
            let shard_keys: Vec<Bytes> = group.iter().map(|(_, k)| k.clone()).collect();
            let tx = self.shards[*shard_idx].clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let fut = tokio::spawn(async move {
                let _ = tx
                    .send(StoreMessage {
                        request: StoreRequest::MGet(shard_keys),
                        respond_to: reply_tx,
                    })
                    .await;
                reply_rx.await
            });
            futs.push((indices, fut));
        }

        let mut results = vec![None; keys.len()];
        for (indices, fut) in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::MGet(entries))) => {
                    for (local_idx, original_idx) in indices.into_iter().enumerate() {
                        results[original_idx] = entries[local_idx].clone();
                    }
                }
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(results)
    }

    pub async fn del(&self, keys: Vec<Bytes>) -> Result<i64> {
        let mut groups: HashMap<usize, Vec<Bytes>> = HashMap::new();
        for key in &keys {
            let idx = shard_index(key, self.num_shards);
            groups.entry(idx).or_default().push(key.clone());
        }

        let mut futs = Vec::new();
        for (shard_idx, group_keys) in groups {
            let tx = self.shards[shard_idx].clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let fut = tokio::spawn(async move {
                let _ = tx
                    .send(StoreMessage {
                        request: StoreRequest::Del(group_keys),
                        respond_to: reply_tx,
                    })
                    .await;
                reply_rx.await
            });
            futs.push(fut);
        }

        let mut total = 0i64;
        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Del(count))) => total += count,
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(total)
    }

    pub async fn set_fsync_mode(&self, fsync_mode: crate::config::FsyncMode) -> Result<()> {
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::SetFsyncMode(fsync_mode),
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::SetFsyncMode)) => (),
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::Shutdown,
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Shutdown)) => (),
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(())
    }

    pub async fn replay(&self) -> Result<()> {
        let futs: Vec<_> = self
            .shards
            .iter()
            .map(|tx| {
                let tx = tx.clone();
                let (reply_tx, reply_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _ = tx
                        .send(StoreMessage {
                            request: StoreRequest::Replay,
                            respond_to: reply_tx,
                        })
                        .await;
                    reply_rx.await
                })
            })
            .collect();

        for fut in futs {
            match fut.await? {
                Ok(Ok(StoreResponse::Replay)) => (),
                _ => return Err(anyhow::anyhow!("shard error")),
            }
        }
        Ok(())
    }
}
