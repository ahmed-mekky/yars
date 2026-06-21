use crate::{
    protocol::resp::Frame,
    store::{handle::StoreHandle, types::Entry},
};
use anyhow::Result;
use tokio_util::bytes::Bytes;

pub async fn get(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(store
        .get(key)
        .await?
        .map_or(Frame::NullBulkString, |e| Frame::BulkString(e.value)))
}

pub async fn set(store: &StoreHandle, key: Bytes, entry: Entry) -> Result<Frame> {
    store.set(key, entry).await?;
    Ok(Frame::SimpleString("OK".into()))
}

pub async fn getdel(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(store
        .getdel(key)
        .await?
        .map_or(Frame::NullBulkString, |e| Frame::BulkString(e.value)))
}

pub async fn getset(store: &StoreHandle, key: Bytes, entry: Entry) -> Result<Frame> {
    Ok(store
        .getset(key, entry)
        .await?
        .map_or(Frame::NullBulkString, |e| Frame::BulkString(e.value)))
}

pub async fn setnx(store: &StoreHandle, key: Bytes, entry: Entry) -> Result<Frame> {
    Ok(Frame::Integer(store.setnx(key, entry).await? as i64))
}
pub async fn incr(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(Frame::Integer(store.incr(key).await?))
}

pub async fn decr(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(Frame::Integer(store.decr(key).await?))
}

pub async fn strlen(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(Frame::Integer(store.strlen(key).await?))
}

pub async fn append(store: &StoreHandle, key: Bytes, value: Bytes) -> Result<Frame> {
    Ok(Frame::Integer(store.append(key, value).await?))
}

pub async fn ttl(store: &StoreHandle, key: Bytes, now: u64) -> Result<Frame> {
    Ok(Frame::Integer(store.ttl(key, now).await?))
}

pub async fn pttl(store: &StoreHandle, key: Bytes, now: u64) -> Result<Frame> {
    Ok(Frame::Integer(store.pttl(key, now).await?))
}

pub async fn persist(store: &StoreHandle, key: Bytes) -> Result<Frame> {
    Ok(Frame::Integer(store.persist(key).await? as i64))
}

pub async fn expire(store: &StoreHandle, key: Bytes, expiry: u64, now: u64) -> Result<Frame> {
    Ok(Frame::Integer(store.expire(key, expiry, now).await? as i64))
}

pub async fn pexpire(store: &StoreHandle, key: Bytes, expiry: u64, now: u64) -> Result<Frame> {
    Ok(Frame::Integer(
        store.pexpire(key, expiry, now).await? as i64,
    ))
}

#[cfg(any())]
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
