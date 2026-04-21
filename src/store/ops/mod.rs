use tokio_util::bytes::Bytes;

use crate::store::{
    traits::Store,
    types::{Entry, Expiry},
};

pub async fn incr(store: &impl Store, key: Bytes) -> Result<Entry, &'static str> {
    incr_by(store, key, 1).await
}

pub async fn decr(store: &impl Store, key: Bytes) -> Result<Entry, &'static str> {
    incr_by(store, key, -1).await
}

pub async fn incr_by(store: &impl Store, key: Bytes, delta: i64) -> Result<Entry, &'static str> {
    let mut entry = match store.get(&key).await {
        Some(entry) => entry,
        None => Entry {
            value: b"0".to_vec().into(),
            exp: Expiry::None,
        },
    };

    let current = std::str::from_utf8(&entry.value)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or("ERR value is not an integer or out of range")?;
    current
        .checked_add(delta)
        .ok_or("ERR value is not an integer or out of range")?;
    entry.value = current.checked_add(delta).unwrap().to_string().into();
    let resolved = store.set(key, entry).await;
    Ok(resolved)
}

pub async fn strlen(store: &impl Store, key: Bytes) -> i64 {
    match store.get(&key).await {
        Some(entry) => entry.value.len() as i64,
        None => 0,
    }
}

pub async fn append(store: &impl Store, key: Bytes, value: Bytes) -> Entry {
    if let Some(mut entry) = store.get(&key).await {
        let combined = [entry.value, value].concat();
        entry.value = Bytes::copy_from_slice(&combined);
        return store.set(key, entry).await;
    }

    let entry = Entry {
        value,
        exp: Expiry::None,
    };
    store.set(key, entry).await
}

pub async fn getdel(store: &impl Store, key: Bytes) -> Option<Entry> {
    let entry = store.get(&key).await;
    if entry.is_some() {
        store.del(&[key]).await;
    }
    entry
}

pub async fn getset(store: &impl Store, key: Bytes, entry: Entry) -> (Option<Entry>, Entry) {
    let existing = store.get(&key).await;
    let resolved = store.set(key, entry).await;
    (existing, resolved)
}

pub async fn setnx(store: &impl Store, key: Bytes, entry: Entry) -> Option<Entry> {
    if store.get(&key).await.is_none() {
        let resolved = store.set(key, entry).await;
        return Some(resolved);
    }
    None
}

pub async fn persist(store: &impl Store, key: Bytes) -> Option<Entry> {
    if let Some(mut entry) = store.get(&key).await
        && let Expiry::At(_) = entry.exp
    {
        entry.exp = Expiry::None;
        let resolved = store.set(key, entry).await;
        return Some(resolved);
    }
    None
}

pub async fn pexpire(store: &impl Store, key: Bytes, ttl: u64, now: u64) -> Option<Entry> {
    if let Some(mut entry) = store.get(&key).await {
        entry.exp = Expiry::At(now.saturating_add(ttl));
        let resolved = store.set(key, entry).await;
        return Some(resolved);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::MemoryStore;

    fn entry(value: &[u8], exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from(value.to_vec()),
            exp,
        }
    }

    #[tokio::test]
    async fn incr_on_new_key_starts_at_zero() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        let resolved = incr(&store, k).await.unwrap();
        assert_eq!(resolved.value, Bytes::from_static(b"1"));
    }

    #[tokio::test]
    async fn decr_on_new_key_starts_at_zero() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        let resolved = decr(&store, k).await.unwrap();
        assert_eq!(resolved.value, Bytes::from_static(b"-1"));
    }

    #[tokio::test]
    async fn incr_increments_existing_value() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store.set(k.clone(), entry(b"5", Expiry::None)).await;
        let resolved = incr(&store, k).await.unwrap();
        assert_eq!(resolved.value, Bytes::from_static(b"6"));
    }

    #[tokio::test]
    async fn decr_decrements_existing_value() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store.set(k.clone(), entry(b"5", Expiry::None)).await;
        let resolved = decr(&store, k).await.unwrap();
        assert_eq!(resolved.value, Bytes::from_static(b"4"));
    }

    #[tokio::test]
    async fn incr_by_with_delta() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store.set(k.clone(), entry(b"10", Expiry::None)).await;
        let resolved = incr_by(&store, k, -3).await.unwrap();
        assert_eq!(resolved.value, Bytes::from_static(b"7"));
    }

    #[tokio::test]
    async fn incr_by_overflow_errors() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"counter");
        store
            .set(k.clone(), entry(b"9223372036854775807", Expiry::None))
            .await;
        let err = incr_by(&store, k, 1).await.unwrap_err();
        assert!(err.contains("integer"));
    }

    #[tokio::test]
    async fn incr_non_integer_errors() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store
            .set(k.clone(), entry(b"not_a_number", Expiry::None))
            .await;
        let err = incr(&store, k).await.unwrap_err();
        assert!(err.contains("integer"));
    }

    #[tokio::test]
    async fn strlen_existing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"hello", Expiry::None)).await;
        assert_eq!(strlen(&store, k).await, 5);
    }

    #[tokio::test]
    async fn strlen_missing() {
        let store = MemoryStore::new();
        assert_eq!(strlen(&store, Bytes::from_static(b"missing")).await, 0);
    }

    #[tokio::test]
    async fn append_new_key() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = append(&store, k.clone(), Bytes::from_static(b"abc")).await;
        assert_eq!(resolved.value, Bytes::from_static(b"abc"));
    }

    #[tokio::test]
    async fn append_existing_key() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"hello", Expiry::None)).await;
        let resolved = append(&store, k, Bytes::from_static(b" world")).await;
        assert_eq!(resolved.value, Bytes::from_static(b"hello world"));
    }

    #[tokio::test]
    async fn getdel_existing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None)).await;
        let got = getdel(&store, k.clone()).await.unwrap();
        assert_eq!(got.value, Bytes::from_static(b"v"));
        assert!(store.get(&k).await.is_none());
    }

    #[tokio::test]
    async fn getdel_missing() {
        let store = MemoryStore::new();
        assert!(
            getdel(&store, Bytes::from_static(b"missing"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn getset_existing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None)).await;
        let (existing, resolved) = getset(&store, k.clone(), entry(b"new", Expiry::None)).await;
        assert_eq!(existing.unwrap().value, Bytes::from_static(b"old"));
        assert_eq!(resolved.value, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn getset_missing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let (existing, resolved) = getset(&store, k.clone(), entry(b"new", Expiry::None)).await;
        assert!(existing.is_none());
        assert_eq!(resolved.value, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn setnx_on_missing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let resolved = setnx(&store, k.clone(), entry(b"v", Expiry::None)).await;
        assert!(resolved.is_some());
        assert_eq!(resolved.unwrap().value, Bytes::from_static(b"v"));
    }

    #[tokio::test]
    async fn setnx_on_existing() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"old", Expiry::None)).await;
        let resolved = setnx(&store, k, entry(b"new", Expiry::None)).await;
        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn persist_removes_expiry() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        let far_future = crate::utils::time::get_current_millis() + 1_000_000;
        store
            .set(k.clone(), entry(b"v", Expiry::At(far_future)))
            .await;
        let resolved = persist(&store, k.clone()).await.unwrap();
        assert!(matches!(resolved.exp, Expiry::None));
        let got = store.get(&k).await.unwrap();
        assert!(matches!(got.exp, Expiry::None));
    }

    #[tokio::test]
    async fn persist_on_non_expiring_returns_none() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None)).await;
        assert!(persist(&store, k).await.is_none());
    }

    #[tokio::test]
    async fn pexpire_sets_expiry() {
        let store = MemoryStore::new();
        let k = Bytes::from_static(b"k");
        store.set(k.clone(), entry(b"v", Expiry::None)).await;
        let now = crate::utils::time::get_current_millis();
        let resolved = pexpire(&store, k.clone(), 5000, now).await.unwrap();
        assert!(matches!(resolved.exp, Expiry::At(t) if t == now + 5000));
    }

    #[tokio::test]
    async fn pexpire_on_missing_returns_none() {
        let store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        assert!(
            pexpire(&store, Bytes::from_static(b"missing"), 5000, now)
                .await
                .is_none()
        );
    }
}
