use crate::{protocol::resp::Frame, service::handlers::SetMutation, store::traits::Store};
use tokio_util::bytes::Bytes;

pub async fn del(store: &impl Store, keys: Vec<Bytes>) -> (Frame, Option<SetMutation>) {
    (Frame::Integer(store.del(&keys).await), None)
}

pub async fn exists(store: &impl Store, keys: Vec<Bytes>) -> (Frame, Option<SetMutation>) {
    (Frame::Integer(store.exists(&keys).await), None)
}

pub async fn mget(store: &impl Store, keys: Vec<Bytes>) -> (Frame, Option<SetMutation>) {
    let values = store
        .mget(&keys)
        .await
        .iter()
        .map(|e| match e {
            Some(entry) => Frame::BulkString(entry.value.clone()),
            None => Frame::NullBulkString,
        })
        .collect();
    (Frame::Array(values), None)
}

pub async fn mset(store: &impl Store, items: Vec<(Bytes, Bytes)>) -> (Frame, Option<SetMutation>) {
    store.mset(&items).await;
    (Frame::SimpleString("OK".into()), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        memory::MemoryStore,
        types::{Entry, Expiry},
    };

    fn entry(value: &[u8], exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from(value.to_vec()),
            exp,
        }
    }

    #[tokio::test]
    async fn del_returns_count() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        store
            .set(Bytes::from_static(b"b"), entry(b"2", Expiry::None))
            .await;
        let (frame, mutation) = del(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")],
        )
        .await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn exists_returns_count() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        let (frame, mutation) = exists(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn mget_returns_array() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        let (frame, mutation) = mget(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await;
        assert!(mutation.is_none());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], Frame::BulkString(b) if b.as_ref() == b"1"));
        assert!(matches!(&items[1], Frame::NullBulkString));
    }

    #[tokio::test]
    async fn mset_returns_ok() {
        let store = MemoryStore::new();
        let items = vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
        ];
        let (frame, mutation) = mset(&store, items).await;
        assert!(matches!(frame, Frame::SimpleString(s) if s == "OK"));
        assert!(mutation.is_none());
        assert_eq!(
            store.get(&Bytes::from_static(b"a")).await.unwrap().value,
            Bytes::from_static(b"1")
        );
        assert_eq!(
            store.get(&Bytes::from_static(b"b")).await.unwrap().value,
            Bytes::from_static(b"2")
        );
    }
}
