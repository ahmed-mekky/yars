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
    async fn get_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let (frame, mutation) = get(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::BulkString(b) if b.as_ref() == b"v"));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn get_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) = get(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::NullBulkString));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn set_returns_ok_and_mutation() {
        let store = MemoryStore::new();
        let (frame, mutation) =
            set(&store, Bytes::from_static(b"k"), entry(b"v", Expiry::None)).await;
        assert!(matches!(frame, Frame::SimpleString(s) if s == "OK"));
        let (k, e) = mutation.unwrap();
        assert_eq!(k, Bytes::from_static(b"k"));
        assert_eq!(e.value, Bytes::from_static(b"v"));
    }

    #[tokio::test]
    async fn getdel_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let (frame, mutation) = getdel(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::BulkString(b) if b.as_ref() == b"v"));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn getdel_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) = getdel(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::NullBulkString));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn getset_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"old", Expiry::None))
            .await;
        let (frame, mutation) = getset(
            &store,
            Bytes::from_static(b"k"),
            entry(b"new", Expiry::None),
        )
        .await;
        assert!(matches!(frame, Frame::BulkString(b) if b.as_ref() == b"old"));
        let (k, e) = mutation.unwrap();
        assert_eq!(k, Bytes::from_static(b"k"));
        assert_eq!(e.value, Bytes::from_static(b"new"));
    }

    #[tokio::test]
    async fn getset_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) = getset(
            &store,
            Bytes::from_static(b"k"),
            entry(b"new", Expiry::None),
        )
        .await;
        assert!(matches!(frame, Frame::NullBulkString));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn setnx_on_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) =
            setnx(&store, Bytes::from_static(b"k"), entry(b"v", Expiry::None)).await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn setnx_on_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"old", Expiry::None))
            .await;
        let (frame, mutation) = setnx(
            &store,
            Bytes::from_static(b"k"),
            entry(b"new", Expiry::None),
        )
        .await;
        assert!(matches!(frame, Frame::Integer(0)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn incr_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"5", Expiry::None))
            .await;
        let (frame, mutation) = incr(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(6)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn incr_non_integer_errors() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"abc", Expiry::None))
            .await;
        let (frame, mutation) = incr(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Error(_)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn decr_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"5", Expiry::None))
            .await;
        let (frame, mutation) = decr(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(4)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn strlen_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"hello", Expiry::None))
            .await;
        let (frame, mutation) = strlen(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(5)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn strlen_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) = strlen(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(0)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn append_new_key() {
        let store = MemoryStore::new();
        let (frame, mutation) =
            append(&store, Bytes::from_static(b"k"), Bytes::from_static(b"abc")).await;
        assert!(matches!(frame, Frame::Integer(3)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn append_existing_key() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"hello", Expiry::None))
            .await;
        let (frame, mutation) = append(
            &store,
            Bytes::from_static(b"k"),
            Bytes::from_static(b" world"),
        )
        .await;
        assert!(matches!(frame, Frame::Integer(11)));
        let (_, e) = mutation.unwrap();
        assert_eq!(e.value, Bytes::from_static(b"hello world"));
    }

    #[tokio::test]
    async fn ttl_missing() {
        let store = MemoryStore::new();
        let (frame, _) = ttl(&store, Bytes::from_static(b"k"), 0).await;
        assert!(matches!(frame, Frame::Integer(-2)));
    }

    #[tokio::test]
    async fn ttl_no_expiry() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let (frame, _) = ttl(&store, Bytes::from_static(b"k"), 0).await;
        assert!(matches!(frame, Frame::Integer(-1)));
    }

    #[tokio::test]
    async fn ttl_with_expiry() {
        let store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        store
            .set(
                Bytes::from_static(b"k"),
                entry(b"v", Expiry::At(now + 10_000)),
            )
            .await;
        let (frame, _) = ttl(&store, Bytes::from_static(b"k"), now).await;
        assert!(matches!(frame, Frame::Integer(10)));
    }

    #[tokio::test]
    async fn pttl_missing() {
        let store = MemoryStore::new();
        let (frame, _) = pttl(&store, Bytes::from_static(b"k"), 0).await;
        assert!(matches!(frame, Frame::Integer(-2)));
    }

    #[tokio::test]
    async fn pttl_no_expiry() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let (frame, _) = pttl(&store, Bytes::from_static(b"k"), 0).await;
        assert!(matches!(frame, Frame::Integer(-1)));
    }

    #[tokio::test]
    async fn pttl_with_expiry() {
        let store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        store
            .set(
                Bytes::from_static(b"k"),
                entry(b"v", Expiry::At(now + 10_000)),
            )
            .await;
        let (frame, _) = pttl(&store, Bytes::from_static(b"k"), now).await;
        assert!(matches!(frame, Frame::Integer(10_000)));
    }

    #[tokio::test]
    async fn persist_existing_with_expiry() {
        let store = MemoryStore::new();
        let far_future = crate::utils::time::get_current_millis() + 1_000_000;
        store
            .set(
                Bytes::from_static(b"k"),
                entry(b"v", Expiry::At(far_future)),
            )
            .await;
        let (frame, mutation) = persist(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn persist_missing() {
        let store = MemoryStore::new();
        let (frame, mutation) = persist(&store, Bytes::from_static(b"k")).await;
        assert!(matches!(frame, Frame::Integer(0)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn expire_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let now = crate::utils::time::get_current_millis();
        let (frame, mutation) = expire(&store, Bytes::from_static(b"k"), 5000, now).await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_some());
    }

    #[tokio::test]
    async fn expire_missing() {
        let store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        let (frame, mutation) = expire(&store, Bytes::from_static(b"k"), 5000, now).await;
        assert!(matches!(frame, Frame::Integer(0)));
        assert!(mutation.is_none());
    }
}
