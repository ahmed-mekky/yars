use tokio_util::bytes::Bytes;

use crate::{
    protocol::resp::Frame,
    store::{
        ops,
        traits::Store,
        types::{Entry, Expiry},
    },
};

pub async fn get(store: &impl Store, key: Bytes) -> Frame {
    match store.get(&key).await {
        Some(entry) => Frame::BulkString(entry.value),
        None => Frame::NullBulkString,
    }
}

pub async fn set(store: &impl Store, key: Bytes, entry: Entry) -> Frame {
    store.set(key, entry).await;
    Frame::SimpleString("OK".into())
}

pub async fn getdel(store: &impl Store, key: Bytes) -> Frame {
    match ops::getdel(store, key).await {
        Some(entry) => Frame::BulkString(entry.value),
        None => Frame::NullBulkString,
    }
}

pub async fn getset(store: &impl Store, key: Bytes, entry: Entry) -> Frame {
    match ops::getset(store, key, entry).await {
        Some(existing) => Frame::BulkString(existing.value),
        None => Frame::NullBulkString,
    }
}

pub async fn setnx(store: &impl Store, key: Bytes, entry: Entry) -> Frame {
    Frame::Integer(ops::setnx(store, key, entry).await)
}

pub async fn incr(store: &impl Store, key: Bytes) -> Frame {
    match ops::incr(store, key).await {
        Ok(next) => Frame::Integer(next),
        Err(msg) => Frame::Error(msg.into()),
    }
}

pub async fn decr(store: &impl Store, key: Bytes) -> Frame {
    match ops::decr(store, key).await {
        Ok(next) => Frame::Integer(next),
        Err(msg) => Frame::Error(msg.into()),
    }
}

pub async fn strlen(store: &impl Store, key: Bytes) -> Frame {
    Frame::Integer(ops::strlen(store, key).await)
}

pub async fn append(store: &impl Store, key: Bytes, value: Bytes) -> Frame {
    Frame::Integer(ops::append(store, key, value).await)
}

pub async fn ttl(store: &impl Store, key: Bytes, now: u64) -> Frame {
    match store.get(&key).await {
        None => Frame::Integer(-2),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => Frame::Integer((exp.saturating_sub(now) / 1000) as i64),
            Expiry::None | Expiry::Keep => Frame::Integer(-1),
        },
    }
}

pub async fn pttl(store: &impl Store, key: Bytes, now: u64) -> Frame {
    match store.get(&key).await {
        None => Frame::Integer(-2),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => Frame::Integer(exp.saturating_sub(now) as i64),
            Expiry::None | Expiry::Keep => Frame::Integer(-1),
        },
    }
}

pub async fn persist(store: &impl Store, key: Bytes) -> Frame {
    Frame::Integer(ops::persist(store, key).await)
}

pub async fn expire(store: &impl Store, key: Bytes, ttl_ms: u64, now: u64) -> Frame {
    Frame::Integer(ops::pexpire(store, key, ttl_ms, now).await)
}
