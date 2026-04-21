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
    async fn set(&self, key: Bytes, mut entry: Entry) -> Entry {
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
        map.insert(key, entry.clone());
        entry
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(value: &[u8], exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from(value.to_vec()),
            exp,
        }
    }

    #[tokio::test]
    async fn set_and_get_round_trip() {
        let store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        let val = entry(b"v", Expiry::None);
        store.set(key.clone(), val.clone()).await;
        let got = store.get(&key).await.unwrap();
        assert_eq!(got.value, val.value);
        assert!(matches!(got.exp, Expiry::None));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let store = MemoryStore::new();
        assert!(store.get(&Bytes::from_static(b"missing")).await.is_none());
    }

    #[tokio::test]
    async fn get_expired_returns_none_and_cleans_up() {
        let store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        store.set(key.clone(), entry(b"v", Expiry::At(0))).await;
        assert!(store.get(&key).await.is_none());
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn get_non_expired_returns_some() {
        let store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        let far_future = get_current_millis() + 1_000_000;
        store
            .set(key.clone(), entry(b"v", Expiry::At(far_future)))
            .await;
        let got = store.get(&key).await.unwrap();
        assert_eq!(got.value, entry(b"v", Expiry::None).value);
    }

    #[tokio::test]
    async fn del_removes_keys_and_returns_count() {
        let store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        let k3 = Bytes::from_static(b"c");
        store.set(k1.clone(), entry(b"1", Expiry::None)).await;
        store.set(k2.clone(), entry(b"2", Expiry::None)).await;
        assert_eq!(store.del(&[k1.clone(), k2.clone(), k3]).await, 2);
        assert!(store.get(&k1).await.is_none());
        assert!(store.get(&k2).await.is_none());
    }

    #[tokio::test]
    async fn exists_counts_non_expired_keys() {
        let store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        let k3 = Bytes::from_static(b"c");
        store.set(k1.clone(), entry(b"1", Expiry::None)).await;
        store.set(k2.clone(), entry(b"2", Expiry::At(0))).await;
        assert_eq!(store.exists(&[k1.clone(), k2, k3]).await, 1);
    }

    #[tokio::test]
    async fn mget_returns_entries_in_order() {
        let store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        let k3 = Bytes::from_static(b"c");
        store.set(k1.clone(), entry(b"1", Expiry::None)).await;
        store.set(k3.clone(), entry(b"3", Expiry::None)).await;
        let results = store.mget(&[k1.clone(), k2, k3.clone()]).await;
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].as_ref().unwrap().value,
            entry(b"1", Expiry::None).value
        );
        assert!(results[1].is_none());
        assert_eq!(
            results[2].as_ref().unwrap().value,
            entry(b"3", Expiry::None).value
        );
    }

    #[tokio::test]
    async fn mget_skips_expired_entries() {
        let store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        store.set(k1.clone(), entry(b"1", Expiry::At(0))).await;
        let results = store.mget(std::slice::from_ref(&k1)).await;
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn mset_sets_multiple_keys() {
        let store = MemoryStore::new();
        let items = vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
        ];
        store.mset(&items).await;
        assert_eq!(
            store.get(&Bytes::from_static(b"a")).await.unwrap().value,
            Bytes::from_static(b"1")
        );
        assert_eq!(
            store.get(&Bytes::from_static(b"b")).await.unwrap().value,
            Bytes::from_static(b"2")
        );
    }

    #[tokio::test]
    async fn len_and_is_empty_and_clear() {
        let store = MemoryStore::new();
        assert!(store.is_empty().await);
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        assert_eq!(store.len().await, 1);
        assert!(!store.is_empty().await);
        store.clear().await;
        assert_eq!(store.len().await, 0);
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn clear_resets_memory() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"vv", Expiry::None))
            .await;
        assert!(store.used_memory().await > 0);
        store.clear().await;
        assert_eq!(store.used_memory().await, 0);
    }

    #[tokio::test]
    async fn memory_tracks_updates() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None)).await;
        let mem_after_first = store.used_memory().await;
        store
            .set(k.clone(), entry(b"much_longer_value", Expiry::None))
            .await;
        let mem_after_second = store.used_memory().await;
        assert!(mem_after_second > mem_after_first);
        store.del(&[k]).await;
        assert_eq!(store.used_memory().await, 0);
    }

    #[tokio::test]
    async fn total_commands_starts_at_zero() {
        let store = MemoryStore::new();
        assert_eq!(store.total_commands(), 0);
    }

    #[tokio::test]
    async fn increment_commands() {
        let store = MemoryStore::new();
        store.increment_commands();
        store.increment_commands();
        assert_eq!(store.total_commands(), 2);
    }

    #[tokio::test]
    async fn uptime_is_nonzero_after_sleep() {
        let store = MemoryStore::new();
        assert_eq!(store.uptime_seconds(), 0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = store.uptime_seconds();
    }

    #[tokio::test]
    async fn set_with_keep_preserves_existing_expiry() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let far_future = get_current_millis() + 1_000_000;
        store
            .set(k.clone(), entry(b"v1", Expiry::At(far_future)))
            .await;
        let resolved = store.set(k.clone(), entry(b"v2", Expiry::Keep)).await;
        assert!(matches!(resolved.exp, Expiry::At(t) if t == far_future));
        let got = store.get(&k).await.unwrap();
        assert!(matches!(got.exp, Expiry::At(t) if t == far_future));
    }

    #[tokio::test]
    async fn set_with_keep_on_missing_uses_none() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = store.set(k.clone(), entry(b"v", Expiry::Keep)).await;
        assert!(matches!(resolved.exp, Expiry::None));
    }
}
