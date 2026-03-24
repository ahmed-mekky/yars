use tokio_util::bytes::Bytes;

use crate::store::{
    traits::Store,
    types::{Entry, Expiry},
};

pub async fn incr(store: &impl Store, key: Bytes) -> Result<i64, &'static str> {
    step(store, key, 1).await
}

pub async fn decr(store: &impl Store, key: Bytes) -> Result<i64, &'static str> {
    step(store, key, -1).await
}

async fn step(store: &impl Store, key: Bytes, delta: i64) -> Result<i64, &'static str> {
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
    let next = current
        .checked_add(delta)
        .ok_or("ERR value is not an integer or out of range")?;
    entry.value = next.to_string().into();
    store.set(key, entry).await;
    Ok(next)
}

pub async fn strlen(store: &impl Store, key: Bytes) -> i64 {
    match store.get(&key).await {
        Some(entry) => entry.value.len() as i64,
        None => 0,
    }
}

pub async fn append(store: &impl Store, key: Bytes, value: Bytes) -> i64 {
    if let Some(mut entry) = store.get(&key).await {
        let combined = [entry.value, value].concat();
        let len = combined.len() as i64;
        entry.value = Bytes::copy_from_slice(&combined);
        store.set(key, entry).await;
        return len;
    }

    let len = value.len() as i64;
    let entry = Entry {
        value,
        exp: Expiry::None,
    };
    store.set(key, entry).await;
    len
}

pub async fn getdel(store: &impl Store, key: Bytes) -> Option<Entry> {
    let entry = store.get(&key).await;
    if entry.is_some() {
        store.del(&[key]).await;
    }
    entry
}

pub async fn getset(store: &impl Store, key: Bytes, entry: Entry) -> Option<Entry> {
    let existing = store.get(&key).await;
    store.set(key, entry).await;
    existing
}

pub async fn setnx(store: &impl Store, key: Bytes, entry: Entry) -> i64 {
    if store.get(&key).await.is_none() {
        store.set(key, entry).await;
        return 1;
    }
    0
}

pub async fn persist(store: &impl Store, key: Bytes) -> i64 {
    if let Some(mut entry) = store.get(&key).await
        && let Expiry::At(_) = entry.exp
    {
        entry.exp = Expiry::None;
        store.set(key, entry).await;
        return 1;
    }
    0
}

pub async fn pexpire(store: &impl Store, key: Bytes, ttl: u64, now: u64) -> i64 {
    if let Some(mut entry) = store.get(&key).await {
        entry.exp = Expiry::At(now.saturating_add(ttl));
        store.set(key, entry).await;
        return 1;
    }
    0
}
