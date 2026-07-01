use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio_util::bytes::Bytes;

use crate::{
    store::types::{Entry, Expiry},
    utils::time::get_current_millis,
};

pub struct MemoryStore {
    map: HashMap<Bytes, Entry>,
    start_time: Instant,
    commands_processed: u64,
    total_memory: usize,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            start_time: Instant::now(),
            commands_processed: u64::default(),
            total_memory: usize::default(),
        }
    }

    pub fn increment_commands(&mut self) {
        self.commands_processed += 1;
    }

    pub fn total_commands(&self) -> u64 {
        self.commands_processed
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn add_memory(&mut self, new_memory: usize) {
        self.total_memory = self.total_memory.saturating_add(new_memory);
    }

    pub fn update_memory(&mut self, new: usize, old: usize) {
        if new >= old {
            self.total_memory = self.total_memory.saturating_add(new - old);
        } else {
            self.total_memory = self.total_memory.saturating_sub(old - new);
        }
    }

    pub fn free_memory(&mut self, freed_memory: usize) {
        self.total_memory = self.total_memory.saturating_sub(freed_memory);
    }

    pub fn clear_memory(&mut self) {
        self.total_memory = 0;
    }

    pub fn used_memory(&self) -> usize {
        self.total_memory
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn info(&self) -> String {
        let key_count = self.len() as i64;
        let used_memory = self.used_memory();
        let uptime_seconds = self.uptime_seconds();
        let total_commands = self.total_commands();

        format!(
            "yars_version:{}\r\ndb_keys:{}\r\nused_memory:{}\r\nuptime_seconds:{}\r\ntotal_commands:{}\r\n",
            env!("CARGO_PKG_VERSION"),
            key_count,
            used_memory,
            uptime_seconds,
            total_commands
        )
    }
}

impl MemoryStore {
    pub fn set(&mut self, key: Bytes, mut entry: Entry) -> Entry {
        let old_memory = self
            .map
            .get(&key)
            .map(|e| key.len() + e.value.len())
            .unwrap_or(0);

        let new_memory = key.len() + entry.value.len();
        self.update_memory(new_memory, old_memory);

        let existing_exp = self
            .map
            .get(&key)
            .filter(|current| !current.is_expired(get_current_millis()))
            .map(|current| &current.exp);

        entry.exp = match &entry.exp {
            Expiry::Keep => existing_exp.cloned().unwrap_or(Expiry::None),
            _ => entry.exp.clone(),
        };
        self.map.insert(key, entry.clone());
        entry
    }

    pub fn get(&mut self, key: &Bytes) -> Option<Entry> {
        let now = get_current_millis();
        {
            match self.map.get(key) {
                Some(entry) if !entry.is_expired(now) => return Some(entry.clone()),
                None => return None,
                _ => {}
            }
        }
        let freed_memory = self
            .map
            .remove(key)
            .map(|e| key.len() + e.value.len())
            .unwrap_or(0);
        self.free_memory(freed_memory);
        None
    }

    pub fn del(&mut self, key: &Bytes) -> i64 {
        if let Some(entry) = self.map.remove(key) {
            self.free_memory(key.len() + entry.value.len());
            return 1;
        }
        0
    }

    pub fn exists(&self, keys: &[Bytes]) -> i64 {
        self.map
            .iter()
            .filter(|(k, _)| keys.contains(k))
            .filter(|(_, v)| !v.is_expired(get_current_millis()))
            .count() as i64
    }

    pub fn mget(&self, keys: &[Bytes]) -> Vec<Option<Entry>> {
        let now = get_current_millis();
        keys.iter()
            .map(|k| self.map.get(k).filter(|v| !v.is_expired(now)).cloned())
            .collect()
    }

    pub fn mset(&mut self, items: &[(Bytes, Bytes)]) {
        for (key, value) in items {
            let old_memory = self
                .map
                .get(key)
                .map(|e| key.len() + e.value.len())
                .unwrap_or(0);
            let new_memory = key.len() + value.len();
            self.update_memory(new_memory, old_memory);

            self.map.insert(
                key.clone(),
                Entry {
                    value: value.clone(),
                    exp: Expiry::None,
                },
            );
        }
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.clear_memory();
    }

    pub fn incr(&mut self, key: Bytes) -> Result<(Entry, i64)> {
        self.incr_by(key, 1)
    }

    pub fn decr(&mut self, key: Bytes) -> Result<(Entry, i64)> {
        self.incr_by(key, -1)
    }

    fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<(Entry, i64)> {
        let mut entry = match self.get(&key) {
            Some(entry) => entry,
            None => Entry {
                value: b"0".to_vec().into(),
                exp: Expiry::None,
            },
        };

        let current = std::str::from_utf8(&entry.value)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .context("ERR value is not an integer")?;
        let result = current
            .checked_add(delta)
            .context("ERR value is out of range")?;
        entry.value = result.to_string().into();
        let entry = self.set(key, entry);
        Ok((entry, result))
    }

    pub fn strlen(&mut self, key: Bytes) -> i64 {
        match self.get(&key) {
            Some(entry) => entry.value.len() as i64,
            None => 0,
        }
    }

    pub fn append(&mut self, key: Bytes, value: Bytes) -> Entry {
        if let Some(mut entry) = self.get(&key) {
            let combined = [entry.value, value].concat();
            entry.value = Bytes::copy_from_slice(&combined);
            return self.set(key, entry);
        }

        let entry = Entry {
            value,
            exp: Expiry::None,
        };
        self.set(key, entry)
    }

    pub fn getdel(&mut self, key: &Bytes) -> Option<Entry> {
        let entry = self.get(key);
        if entry.is_some() {
            self.del(key);
        }
        entry
    }

    pub fn getset(&mut self, key: Bytes, entry: Entry) -> (Option<Entry>, Entry) {
        let existing = self.get(&key);
        let resolved = self.set(key, entry);
        (existing, resolved)
    }

    pub fn setnx(&mut self, key: Bytes, entry: Entry) -> bool {
        if self.get(&key).is_none() {
            self.set(key, entry);
            return true;
        }
        false
    }

    pub fn persist(&mut self, key: Bytes) -> Option<Entry> {
        let mut entry = self.get(&key)?;
        if matches!(entry.exp, Expiry::None) {
            return None;
        }
        entry.exp = Expiry::None;
        Some(self.set(key, entry))
    }

    pub fn pexpire(&mut self, key: Bytes, ttl: u64, now: u64) -> Option<Entry> {
        if let Some(mut entry) = self.get(&key) {
            entry.exp = Expiry::At(now.saturating_add(ttl));
            let resolved = self.set(key, entry);
            return Some(resolved);
        }
        None
    }

    pub fn ttl(&mut self, key: Bytes, now: u64) -> i64 {
        match self.get(&key) {
            None => -2,
            Some(entry) => match entry.exp {
                Expiry::At(exp) => (exp.saturating_sub(now) / 1000) as i64,
                Expiry::None | Expiry::Keep => -1,
            },
        }
    }

    pub fn pttl(&mut self, key: Bytes, now: u64) -> i64 {
        match self.get(&key) {
            None => -2,
            Some(entry) => match entry.exp {
                Expiry::At(exp) => exp.saturating_sub(now) as i64,
                Expiry::None | Expiry::Keep => -1,
            },
        }
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
        let mut store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        let val = entry(b"v", Expiry::None);
        store.set(key.clone(), val.clone());
        let got = store.get(&key).unwrap();
        assert_eq!(got.value, val.value);
        assert!(matches!(got.exp, Expiry::None));
    }

    #[tokio::test]
    async fn get_missing_returns_none() {
        let mut store = MemoryStore::new();
        assert!(store.get(&Bytes::from_static(b"missing")).is_none());
    }

    #[tokio::test]
    async fn get_expired_returns_none_and_cleans_up() {
        let mut store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        store.set(key.clone(), entry(b"v", Expiry::At(0)));
        assert!(store.get(&key).is_none());
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn get_non_expired_returns_some() {
        let mut store = MemoryStore::new();
        let key = Bytes::from_static(b"k");
        let far_future = get_current_millis() + 1_000_000;
        store.set(key.clone(), entry(b"v", Expiry::At(far_future)));
        let got = store.get(&key).unwrap();
        assert_eq!(got.value, entry(b"v", Expiry::None).value);
    }

    #[tokio::test]
    async fn del_removes_keys_and_returns_count() {
        let mut store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        store.set(k1.clone(), entry(b"1", Expiry::None));
        store.set(k2.clone(), entry(b"2", Expiry::None));
        store.del(&k2);
        assert!(store.get(&k2).is_none());
        store.del(&k1);
        assert!(store.get(&k1).is_none());
    }

    #[tokio::test]
    async fn exists_counts_non_expired_keys() {
        let mut store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        let k3 = Bytes::from_static(b"c");
        store.set(k1.clone(), entry(b"1", Expiry::None));
        store.set(k2.clone(), entry(b"2", Expiry::At(0)));
        assert_eq!(store.exists(&[k1.clone(), k2, k3]), 1);
    }

    #[tokio::test]
    async fn mget_returns_entries_in_order() {
        let mut store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        let k2 = Bytes::from_static(b"b");
        let k3 = Bytes::from_static(b"c");
        store.set(k1.clone(), entry(b"1", Expiry::None));
        store.set(k3.clone(), entry(b"3", Expiry::None));
        let results = store.mget(&[k1.clone(), k2, k3.clone()]);
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
        let mut store = MemoryStore::new();
        let k1 = Bytes::from_static(b"a");
        store.set(k1.clone(), entry(b"1", Expiry::At(0)));
        let results = store.mget(std::slice::from_ref(&k1));
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn mset_sets_multiple_keys() {
        let mut store = MemoryStore::new();
        let items = vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
        ];
        store.mset(&items);
        assert_eq!(
            store.get(&Bytes::from_static(b"a")).unwrap().value,
            Bytes::from_static(b"1")
        );
        assert_eq!(
            store.get(&Bytes::from_static(b"b")).unwrap().value,
            Bytes::from_static(b"2")
        );
    }

    #[tokio::test]
    async fn len_and_is_empty_and_clear() {
        let mut store = MemoryStore::new();
        assert!(store.is_empty());
        store.set(Bytes::from_static(b"k"), entry(b"v", Expiry::None));
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn clear_resets_memory() {
        let mut store = MemoryStore::new();
        store.set(Bytes::from_static(b"k"), entry(b"vv", Expiry::None));
        assert!(store.used_memory() > 0);
        store.clear();
        assert_eq!(store.used_memory(), 0);
    }

    #[tokio::test]
    async fn memory_tracks_updates() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None));
        let mem_after_first = store.used_memory();
        store.set(k.clone(), entry(b"much_longer_value", Expiry::None));
        let mem_after_second = store.used_memory();
        assert!(mem_after_second > mem_after_first);
        store.del(&k);
        assert_eq!(store.used_memory(), 0);
    }

    #[tokio::test]
    async fn total_commands_starts_at_zero() {
        let store = MemoryStore::new();
        assert_eq!(store.total_commands(), 0);
    }

    #[tokio::test]
    async fn increment_commands() {
        let mut store = MemoryStore::new();
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
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let far_future = get_current_millis() + 1_000_000;
        store.set(k.clone(), entry(b"v1", Expiry::At(far_future)));
        let resolved = store.set(k.clone(), entry(b"v2", Expiry::Keep));
        assert!(matches!(resolved.exp, Expiry::At(t) if t == far_future));
        let got = store.get(&k).unwrap();
        assert!(matches!(got.exp, Expiry::At(t) if t == far_future));
    }

    #[tokio::test]
    async fn set_with_keep_on_missing_uses_none() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = store.set(k.clone(), entry(b"v", Expiry::Keep));
        assert!(matches!(resolved.exp, Expiry::None));
    }

    #[test]
    fn incr_on_new_key_starts_at_zero() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        let resolved = store.incr(k).unwrap();
        assert_eq!(resolved.0.value, Bytes::from_static(b"1"));
    }

    #[test]
    fn decr_on_new_key_starts_at_zero() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        let resolved = store.decr(k).unwrap();
        assert_eq!(resolved.0.value, Bytes::from_static(b"-1"));
    }

    #[test]
    fn incr_increments_existing_value() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store.set(k.clone(), entry(b"5", Expiry::None));
        let resolved = store.incr(k).unwrap();
        assert_eq!(resolved.0.value, Bytes::from_static(b"6"));
    }

    #[test]
    fn decr_decrements_existing_value() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store.set(k.clone(), entry(b"5", Expiry::None));
        let resolved = store.decr(k).unwrap();
        assert_eq!(resolved.0.value, Bytes::from_static(b"4"));
    }

    #[test]
    fn incr_non_integer_errors() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"not_a_number", Expiry::None));
        let err = store.incr(k).unwrap_err();
        assert!(err.to_string().contains("integer"));
    }

    #[test]
    fn strlen_existing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"hello", Expiry::None));
        assert_eq!(store.strlen(k), 5);
    }

    #[test]
    fn strlen_missing() {
        let mut store = MemoryStore::new();
        assert_eq!(store.strlen(Bytes::from_static(b"missing")), 0);
    }

    #[test]
    fn append_new_key() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = store.append(k.clone(), Bytes::from_static(b"abc"));
        assert_eq!(resolved.value, Bytes::from_static(b"abc"));
    }

    #[test]
    fn append_existing_key() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"hello", Expiry::None));
        let resolved = store.append(k, Bytes::from_static(b" world"));
        assert_eq!(resolved.value, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn getdel_existing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None));
        let got = store.getdel(&k).unwrap();
        assert_eq!(got.value, Bytes::from_static(b"v"));
        assert!(store.get(&k).is_none());
    }

    #[test]
    fn getdel_missing() {
        let mut store = MemoryStore::new();
        assert!(store.getdel(&Bytes::from_static(b"missing")).is_none());
    }

    #[test]
    fn getset_existing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None));
        let (existing, resolved) = store.getset(k.clone(), entry(b"new", Expiry::None));
        assert_eq!(existing.unwrap().value, Bytes::from_static(b"old"));
        assert_eq!(resolved.value, Bytes::from_static(b"new"));
    }

    #[test]
    fn getset_missing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let (existing, resolved) = store.getset(k.clone(), entry(b"new", Expiry::None));
        assert!(existing.is_none());
        assert_eq!(resolved.value, Bytes::from_static(b"new"));
    }

    #[test]
    fn setnx_on_missing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = store.setnx(k.clone(), entry(b"v", Expiry::None));
        assert!(resolved);
    }

    #[tokio::test]
    async fn setnx_on_existing() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None));
        let resolved = store.setnx(k, entry(b"new", Expiry::None));
        assert!(!resolved);
    }

    #[test]
    fn persist_removes_expiry() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let far_future = crate::utils::time::get_current_millis() + 1_000_000;
        store.set(k.clone(), entry(b"v", Expiry::At(far_future)));
        let resolved = store.persist(k.clone()).unwrap();
        assert!(matches!(resolved.exp, Expiry::None));
        let got = store.get(&k).unwrap();
        assert!(matches!(got.exp, Expiry::None));
    }

    #[test]
    fn persist_on_non_expiring_returns_none() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None));
        assert!(store.persist(k).is_none());
    }

    #[test]
    fn pexpire_sets_expiry() {
        let mut store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None));
        let now = crate::utils::time::get_current_millis();
        let resolved = store.pexpire(k.clone(), 5000, now).unwrap();
        assert!(matches!(resolved.exp, Expiry::At(t) if t == now + 5000));
    }

    #[test]
    fn pexpire_on_missing_returns_none() {
        let mut store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        assert!(
            store
                .pexpire(Bytes::from_static(b"missing"), 5000, now)
                .is_none()
        );
    }
}
