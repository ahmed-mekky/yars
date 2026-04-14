use crate::{
    protocol::resp::Frame,
    service::handlers::SetMutation,
    store::{
        ops,
        traits::Store,
        types::{Entry, Expiry},
    },
};
use tokio_util::bytes::Bytes;

pub async fn get(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    match store.get(&key).await {
        Some(entry) => (Frame::BulkString(entry.value), None),
        None => (Frame::NullBulkString, None),
    }
}

pub async fn set(store: &impl Store, key: Bytes, entry: Entry) -> (Frame, Option<SetMutation>) {
    let resolved = store.set(key.clone(), entry).await;
    (Frame::SimpleString("OK".into()), Some((key, resolved)))
}

pub async fn getdel(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    match ops::getdel(store, key).await {
        Some(entry) => (Frame::BulkString(entry.value), None),
        None => (Frame::NullBulkString, None),
    }
}

pub async fn getset(store: &impl Store, key: Bytes, entry: Entry) -> (Frame, Option<SetMutation>) {
    let (existing, resolved) = ops::getset(store, key.clone(), entry).await;
    let frame = match existing {
        Some(e) => Frame::BulkString(e.value),
        None => Frame::NullBulkString,
    };
    (frame, Some((key, resolved)))
}

pub async fn setnx(store: &impl Store, key: Bytes, entry: Entry) -> (Frame, Option<SetMutation>) {
    match ops::setnx(store, key.clone(), entry).await {
        Some(resolved) => (Frame::Integer(1), Some((key, resolved))),
        None => (Frame::Integer(0), None),
    }
}

pub async fn incr(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    match ops::incr(store, key.clone()).await {
        Ok(resolved) => {
            let value = std::str::from_utf8(&resolved.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap();
            (Frame::Integer(value), Some((key, resolved)))
        }
        Err(msg) => (Frame::Error(msg.into()), None),
    }
}

pub async fn decr(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    match ops::decr(store, key.clone()).await {
        Ok(resolved) => {
            let value = std::str::from_utf8(&resolved.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap();
            (Frame::Integer(value), Some((key, resolved)))
        }
        Err(msg) => (Frame::Error(msg.into()), None),
    }
}

pub async fn strlen(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    (Frame::Integer(ops::strlen(store, key).await), None)
}

pub async fn append(store: &impl Store, key: Bytes, value: Bytes) -> (Frame, Option<SetMutation>) {
    let resolved = ops::append(store, key.clone(), value).await;
    (
        Frame::Integer(resolved.value.len() as i64),
        Some((key, resolved)),
    )
}

pub async fn ttl(store: &impl Store, key: Bytes, now: u64) -> (Frame, Option<SetMutation>) {
    match store.get(&key).await {
        None => (Frame::Integer(-2), None),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => (
                Frame::Integer((exp.saturating_sub(now) / 1000) as i64),
                None,
            ),
            Expiry::None | Expiry::Keep => (Frame::Integer(-1), None),
        },
    }
}

pub async fn pttl(store: &impl Store, key: Bytes, now: u64) -> (Frame, Option<SetMutation>) {
    match store.get(&key).await {
        None => (Frame::Integer(-2), None),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => (Frame::Integer(exp.saturating_sub(now) as i64), None),
            Expiry::None | Expiry::Keep => (Frame::Integer(-1), None),
        },
    }
}

pub async fn persist(store: &impl Store, key: Bytes) -> (Frame, Option<SetMutation>) {
    match ops::persist(store, key.clone()).await {
        Some(resolved) => (Frame::Integer(1), Some((key, resolved))),
        None => (Frame::Integer(0), None),
    }
}

pub async fn expire(
    store: &impl Store,
    key: Bytes,
    ttl_ms: u64,
    now: u64,
) -> (Frame, Option<SetMutation>) {
    match ops::pexpire(store, key.clone(), ttl_ms, now).await {
        Some(resolved) => (Frame::Integer(1), Some((key, resolved))),
        None => (Frame::Integer(0), None),
    }
}
