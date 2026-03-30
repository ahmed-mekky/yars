use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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

pub struct MemoryStore {
    map: RwLock<HashMap<Bytes, Entry>>,
    start_time: Instant,
    commands_processed: AtomicU64,
    total_memory: AtomicU64,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
            commands_processed: AtomicU64::new(0),
            total_memory: AtomicU64::new(0),
        }
    }

    pub fn increment_commands(&self) {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_commands(&self) -> u64 {
        self.commands_processed.load(Ordering::Relaxed)
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub async fn used_memory(&self) -> usize {
        self.total_memory.load(Ordering::Relaxed) as usize
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn set(&self, key: Bytes, mut entry: Entry) {
        let mut map = self.map.write().await;
        let old_memory = map
            .get(&key)
            .filter(|current| !current.is_expired(get_current_millis()))
            .map(|e| key.len() + e.value.len())
            .unwrap_or(0);

        let new_memory = key.len() + entry.value.len();
        self.total_memory.fetch_add(
            (new_memory as i64 - old_memory as i64) as u64,
            Ordering::Relaxed,
        );

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
            let map = self.map.read().await;
            match map.get(key) {
                Some(entry) if !entry.is_expired(now) => return Some(entry.clone()),
                None => return None,
                _ => {}
            }
        }

        self.map.write().await.remove(key);
        None
    }

    async fn del(&self, keys: &[Bytes]) -> i64 {
        let mut map = self.map.write().await;
        let mut freed_memory: usize = 0;
        let mut deleted_count = 0;
        for key in keys {
            if let Some(entry) = map.remove(key) {
                freed_memory += key.len() + entry.value.len();
                deleted_count += 1;
            }
        }
        self.total_memory
            .fetch_sub(freed_memory as u64, Ordering::Relaxed);
        deleted_count
    }

    async fn exists(&self, keys: &[Bytes]) -> i64 {
        self.map
            .read()
            .await
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .filter(|(_, v)| !v.is_expired(get_current_millis()))
            .count() as i64
    }

    async fn mget(&self, keys: &[Bytes]) -> Vec<Option<Entry>> {
        let now = get_current_millis();
        let map = self.map.read().await;
        keys.iter()
            .map(|k| map.get(k).filter(|v| !v.is_expired(now)).cloned())
            .collect()
    }

    async fn mset(&self, items: &[(Bytes, Bytes)]) {
        let mut map = self.map.write().await;
        let mut added_memory: usize = 0;
        for (key, value) in items {
            let old_memory = map.get(key).map(|e| key.len() + e.value.len()).unwrap_or(0);
            let new_memory = key.len() + value.len();
            added_memory += new_memory - old_memory;

            map.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    exp: Expiry::None,
                },
            );
        }
        self.total_memory
            .fetch_add(added_memory as u64, Ordering::Relaxed);
    }

    async fn len(&self) -> usize {
        self.map.read().await.len()
    }

    async fn clear(&self) {
        self.map.write().await.clear();
        self.total_memory.store(0, Ordering::Relaxed);
    }

    async fn is_empty(&self) -> bool {
        self.map.read().await.is_empty()
    }
}
