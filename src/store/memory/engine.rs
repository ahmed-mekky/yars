use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

use crate::{
    store::{
        traits::Store,
        types::{Entry, Expiry},
    },
    utils::time::get_current_millis,
};

#[derive(Default)]
pub struct MemoryStore(RwLock<HashMap<Bytes, Entry>>);

#[async_trait]
impl Store for MemoryStore {
    async fn set(&self, key: Bytes, mut entry: Entry) {
        let mut map = self.0.write().await;
        let existing_exp = map
            .get(&key)
            .filter(|current| !current.is_expired(get_current_millis()))
            .map(|current| &current.exp);

        entry.exp = match &entry.exp {
            Expiry::Keep => existing_exp.cloned().unwrap_or(Expiry::None),
            _ => entry.exp.clone(),
        };
        map.insert(key, entry);
    }

    async fn get(&self, key: &Bytes) -> Option<Entry> {
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

    async fn del(&self, keys: &[Bytes]) -> i64 {
        let mut map = self.0.write().await;
        keys.iter().filter(|k| map.remove(*k).is_some()).count() as i64
    }

    async fn exists(&self, keys: &[Bytes]) -> i64 {
        self.0
            .read()
            .await
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .filter(|(_, v)| !v.is_expired(get_current_millis()))
            .count() as i64
    }

    async fn mget(&self, keys: &[Bytes]) -> Vec<Option<Entry>> {
        let now = get_current_millis();
        let map = self.0.read().await;
        keys.iter()
            .map(|k| map.get(k).filter(|v| !v.is_expired(now)).cloned())
            .collect()
    }

    async fn mset(&self, items: &[(Bytes, Bytes)]) {
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

    async fn len(&self) -> usize {
        self.0.read().await.len()
    }

    async fn clear(&self) {
        self.0.write().await.clear();
    }

    async fn is_empty(&self) -> bool {
        self.0.read().await.is_empty()
    }
}
