use crate::{
    protocol::resp::Frame,
    service::handlers::CommandEffect,
    store::{
        ops,
        traits::Store,
        types::{Entry, Expiry},
    },
};
use tokio_util::bytes::Bytes;

pub async fn get(store: &impl Store, key: Bytes) -> CommandEffect {
    match store.get(&key).await {
        Some(entry) => CommandEffect::Read(Frame::BulkString(entry.value)),
        None => CommandEffect::Read(Frame::NullBulkString),
    }
}

pub async fn set(store: &impl Store, key: Bytes, entry: Entry) -> CommandEffect {
    let resolved = store.set(key.clone(), entry).await;
    CommandEffect::from_set(Frame::SimpleString("OK".into()), key, resolved)
}

pub async fn getdel(store: &impl Store, key: Bytes) -> CommandEffect {
    match ops::getdel(store, key.clone()).await {
        Some(entry) => CommandEffect::Write(
            Frame::BulkString(entry.value),
            crate::store::persistence::record::Record::Del { keys: vec![key] },
        ),
        None => CommandEffect::Read(Frame::NullBulkString),
    }
}

pub async fn getset(store: &impl Store, key: Bytes, entry: Entry) -> CommandEffect {
    let (existing, resolved) = ops::getset(store, key.clone(), entry).await;
    let frame = match existing {
        Some(e) => Frame::BulkString(e.value),
        None => Frame::NullBulkString,
    };
    CommandEffect::from_set(frame, key, resolved)
}

pub async fn setnx(store: &impl Store, key: Bytes, entry: Entry) -> CommandEffect {
    match ops::setnx(store, key.clone(), entry).await {
        Some(resolved) => CommandEffect::from_set(Frame::Integer(1), key, resolved),
        None => CommandEffect::Read(Frame::Integer(0)),
    }
}

pub async fn incr(store: &impl Store, key: Bytes) -> CommandEffect {
    match ops::incr(store, key.clone()).await {
        Ok(resolved) => {
            let value = std::str::from_utf8(&resolved.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap();
            CommandEffect::from_set(Frame::Integer(value), key, resolved)
        }
        Err(msg) => CommandEffect::Read(Frame::Error(msg.into())),
    }
}

pub async fn decr(store: &impl Store, key: Bytes) -> CommandEffect {
    match ops::decr(store, key.clone()).await {
        Ok(resolved) => {
            let value = std::str::from_utf8(&resolved.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap();
            CommandEffect::from_set(Frame::Integer(value), key, resolved)
        }
        Err(msg) => CommandEffect::Read(Frame::Error(msg.into())),
    }
}

pub async fn strlen(store: &impl Store, key: Bytes) -> CommandEffect {
    CommandEffect::Read(Frame::Integer(ops::strlen(store, key).await))
}

pub async fn append(store: &impl Store, key: Bytes, value: Bytes) -> CommandEffect {
    let resolved = ops::append(store, key.clone(), value).await;
    CommandEffect::from_set(Frame::Integer(resolved.value.len() as i64), key, resolved)
}

pub async fn ttl(store: &impl Store, key: Bytes, now: u64) -> CommandEffect {
    match store.get(&key).await {
        None => CommandEffect::Read(Frame::Integer(-2)),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => {
                CommandEffect::Read(Frame::Integer((exp.saturating_sub(now) / 1000) as i64))
            }
            Expiry::None | Expiry::Keep => CommandEffect::Read(Frame::Integer(-1)),
        },
    }
}

pub async fn pttl(store: &impl Store, key: Bytes, now: u64) -> CommandEffect {
    match store.get(&key).await {
        None => CommandEffect::Read(Frame::Integer(-2)),
        Some(entry) => match entry.exp {
            Expiry::At(exp) => CommandEffect::Read(Frame::Integer(exp.saturating_sub(now) as i64)),
            Expiry::None | Expiry::Keep => CommandEffect::Read(Frame::Integer(-1)),
        },
    }
}

pub async fn persist(store: &impl Store, key: Bytes) -> CommandEffect {
    match ops::persist(store, key.clone()).await {
        Some(resolved) => CommandEffect::from_set(Frame::Integer(1), key, resolved),
        None => CommandEffect::Read(Frame::Integer(0)),
    }
}

pub async fn expire(store: &impl Store, key: Bytes, ttl_ms: u64, now: u64) -> CommandEffect {
    match ops::pexpire(store, key.clone(), ttl_ms, now).await {
        Some(resolved) => CommandEffect::from_set(Frame::Integer(1), key, resolved),
        None => CommandEffect::Read(Frame::Integer(0)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::handlers::tests::{entry, read_frame, write_frame};
    use crate::store::memory::MemoryStore;
    use crate::store::persistence::record::Record;

    #[tokio::test]
    async fn get_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let frame = read_frame(get(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::BulkString("v".into()));
    }

    #[tokio::test]
    async fn get_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(get(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::NullBulkString);
    }

    #[tokio::test]
    async fn set_returns_ok_and_mutation() {
        let store = MemoryStore::new();
        let (frame, record) =
            write_frame(set(&store, Bytes::from_static(b"k"), entry(b"v", Expiry::None)).await);
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        assert!(
            matches!(record, Record::Set { key, value, .. } if key == Bytes::from_static(b"k") && value == Bytes::from_static(b"v"))
        );
    }

    #[tokio::test]
    async fn getdel_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let (frame, record) = write_frame(getdel(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::BulkString("v".into()));
        assert!(matches!(record, Record::Del { keys } if keys == vec![Bytes::from_static(b"k")]));
        assert!(store.get(&Bytes::from_static(b"k")).await.is_none());
    }

    #[tokio::test]
    async fn getdel_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(getdel(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::NullBulkString);
    }

    #[tokio::test]
    async fn getset_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"old", Expiry::None))
            .await;
        let (frame, record) = write_frame(
            getset(
                &store,
                Bytes::from_static(b"k"),
                entry(b"new", Expiry::None),
            )
            .await,
        );
        assert_eq!(frame, Frame::BulkString("old".into()));
        assert!(
            matches!(record, Record::Set { key, value, .. } if key == Bytes::from_static(b"k") && value == Bytes::from_static(b"new"))
        );
    }

    #[tokio::test]
    async fn getset_missing() {
        let store = MemoryStore::new();
        let (frame, record) = write_frame(
            getset(
                &store,
                Bytes::from_static(b"k"),
                entry(b"new", Expiry::None),
            )
            .await,
        );
        assert_eq!(frame, Frame::NullBulkString);
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn setnx_on_missing() {
        let store = MemoryStore::new();
        let (frame, record) =
            write_frame(setnx(&store, Bytes::from_static(b"k"), entry(b"v", Expiry::None)).await);
        assert_eq!(frame, Frame::Integer(1));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn setnx_on_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"old", Expiry::None))
            .await;
        let frame = read_frame(
            setnx(
                &store,
                Bytes::from_static(b"k"),
                entry(b"new", Expiry::None),
            )
            .await,
        );
        assert_eq!(frame, Frame::Integer(0));
    }

    #[tokio::test]
    async fn incr_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"5", Expiry::None))
            .await;
        let (frame, record) = write_frame(incr(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(6));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn incr_non_integer_errors() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"abc", Expiry::None))
            .await;
        let frame = read_frame(incr(&store, Bytes::from_static(b"k")).await);
        assert!(matches!(frame, Frame::Error(_)));
    }

    #[tokio::test]
    async fn decr_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"5", Expiry::None))
            .await;
        let (frame, record) = write_frame(decr(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(4));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn strlen_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"hello", Expiry::None))
            .await;
        let frame = read_frame(strlen(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(5));
    }

    #[tokio::test]
    async fn strlen_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(strlen(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(0));
    }

    #[tokio::test]
    async fn append_new_key() {
        let store = MemoryStore::new();
        let (frame, record) =
            write_frame(append(&store, Bytes::from_static(b"k"), Bytes::from_static(b"abc")).await);
        assert_eq!(frame, Frame::Integer(3));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn append_existing_key() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"hello", Expiry::None))
            .await;
        let (frame, record) = write_frame(
            append(
                &store,
                Bytes::from_static(b"k"),
                Bytes::from_static(b" world"),
            )
            .await,
        );
        assert_eq!(frame, Frame::Integer(11));
        assert!(
            matches!(record, Record::Set { key, value, .. } if key == Bytes::from_static(b"k") && value == Bytes::from_static(b"hello world"))
        );
    }

    #[tokio::test]
    async fn ttl_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(ttl(&store, Bytes::from_static(b"k"), 0).await);
        assert_eq!(frame, Frame::Integer(-2));
    }

    #[tokio::test]
    async fn ttl_no_expiry() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let frame = read_frame(ttl(&store, Bytes::from_static(b"k"), 0).await);
        assert_eq!(frame, Frame::Integer(-1));
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
        let frame = read_frame(ttl(&store, Bytes::from_static(b"k"), now).await);
        assert!(matches!(frame, Frame::Integer(10)));
    }

    #[tokio::test]
    async fn pttl_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(pttl(&store, Bytes::from_static(b"k"), 0).await);
        assert_eq!(frame, Frame::Integer(-2));
    }

    #[tokio::test]
    async fn pttl_no_expiry() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let frame = read_frame(pttl(&store, Bytes::from_static(b"k"), 0).await);
        assert_eq!(frame, Frame::Integer(-1));
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
        let frame = read_frame(pttl(&store, Bytes::from_static(b"k"), now).await);
        assert_eq!(frame, Frame::Integer(10_000));
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
        let (frame, record) = write_frame(persist(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(1));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn persist_missing() {
        let store = MemoryStore::new();
        let frame = read_frame(persist(&store, Bytes::from_static(b"k")).await);
        assert_eq!(frame, Frame::Integer(0));
    }

    #[tokio::test]
    async fn expire_existing() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await;
        let now = crate::utils::time::get_current_millis();
        let (frame, record) =
            write_frame(expire(&store, Bytes::from_static(b"k"), 5000, now).await);
        assert_eq!(frame, Frame::Integer(1));
        assert!(matches!(record, Record::Set { key, .. } if key == Bytes::from_static(b"k")));
    }

    #[tokio::test]
    async fn expire_missing() {
        let store = MemoryStore::new();
        let now = crate::utils::time::get_current_millis();
        let frame = read_frame(expire(&store, Bytes::from_static(b"k"), 5000, now).await);
        assert_eq!(frame, Frame::Integer(0));
    }
}
