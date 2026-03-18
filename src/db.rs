use std::collections::HashMap;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

use crate::{
    resp::{Entry, Expiry},
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
}
