use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

use crate::{
    resp::{Entry, Expiry, Frame},
    utils::get_current_millis,
};

pub struct Db(RwLock<HashMap<Bytes, Entry>>);

impl Db {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    pub async fn set(&self, key: Bytes, mut entry: Entry) {
        let mut map = self.0.write().await;

        if let Expiry::Keep = entry.exp {
            entry.exp = match map.get(&key) {
                Some(existing) if !existing.is_expired(get_current_millis()) => {
                    existing.exp.clone()
                }
                _ => Expiry::None,
            };
        }

        map.insert(key, entry);
    }

    pub async fn get(&self, key: &Bytes) -> Option<Entry> {
        let now = get_current_millis();

        {
            let map = self.0.read().await;
            match map.get(key) {
                Some(entry) if !entry.is_expired(now) => return Some(entry.clone()),
                None => return None,
                _ => {}
            }
        }

        self.0.write().await.remove(key);
        None
    }

    pub async fn del(&self, keys: &[Bytes]) -> i64 {
        let mut map = self.0.write().await;
        keys.iter().filter(|k| map.remove(*k).is_some()).count() as i64
    }

    pub async fn exists(&self, keys: &[Bytes]) -> i64 {
        self.0
            .read()
            .await
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .filter(|(_, v)| !v.is_expired(get_current_millis()))
            .count() as i64
    }

    pub async fn mget(&self, keys: &[Bytes]) -> Vec<Option<Entry>> {
        let now = get_current_millis();

        let map = self.0.read().await;
        keys.iter()
            .map(|k| map.get(k).filter(|v| !v.is_expired(now)).cloned())
            .collect()
    }

    pub async fn mset(&self, items: &[(Bytes, Bytes)]) {
        let mut map = self.0.write().await;
        for (key, value) in items {
            map.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    exp: Expiry::None,
                },
            );
        }
    }

    pub async fn len(&self) -> usize {
        self.0.read().await.len()
    }
    pub async fn clear(&self) {
        self.0.write().await.clear();
    }

    pub async fn incr(&self, key: Bytes) -> Frame {
        self.step(key, 1).await
    }

    pub async fn decr(&self, key: Bytes) -> Frame {
        self.step(key, -1).await
    }

    async fn step(&self, key: Bytes, delta: i64) -> Frame {
        let mut entry = match self.get(&key).await {
            Some(entry) => entry,
            None => Entry {
                value: b"0".to_vec().into(),
                exp: Expiry::None,
            },
        };
        let current = match std::str::from_utf8(&entry.value)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(v) => v,
            None => return Frame::Error("ERR value is not an integer or out of range".into()),
        };
        let next = match current.checked_add(delta) {
            Some(v) => v,
            None => return Frame::Error("ERR value is not an integer or out of range".into()),
        };
        entry.value = next.to_string().into();
        self.set(key, entry).await;
        Frame::Integer(next)
    }
}
