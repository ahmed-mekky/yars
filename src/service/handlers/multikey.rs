use crate::{protocol::resp::Frame, service::handlers::CommandEffect, store::traits::Store};
use tokio_util::bytes::Bytes;

pub async fn del(store: &impl Store, keys: Vec<Bytes>) -> CommandEffect {
    let count = store.del(&keys).await;
    CommandEffect::Write(
        Frame::Integer(count),
        crate::store::persistence::record::Record::Del { keys },
    )
}

pub async fn exists(store: &impl Store, keys: Vec<Bytes>) -> CommandEffect {
    CommandEffect::Read(Frame::Integer(store.exists(&keys).await))
}

pub async fn mget(store: &impl Store, keys: Vec<Bytes>) -> CommandEffect {
    let values = store
        .mget(&keys)
        .await
        .iter()
        .map(|e| match e {
            Some(entry) => Frame::BulkString(entry.value.clone()),
            None => Frame::NullBulkString,
        })
        .collect();
    CommandEffect::Read(Frame::Array(values))
}

pub async fn mset(store: &impl Store, items: Vec<(Bytes, Bytes)>) -> CommandEffect {
    store.mset(&items).await;
    CommandEffect::Write(
        Frame::SimpleString("OK".into()),
        crate::store::persistence::record::Record::MSet { items },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::handlers::tests::{entry, read_frame, write_frame};
    use crate::store::persistence::record::Record;
    use crate::store::{memory::MemoryStore, types::Expiry};

    #[tokio::test]
    async fn del_returns_count() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        store
            .set(Bytes::from_static(b"b"), entry(b"2", Expiry::None))
            .await;
        let (frame, record) = write_frame(
            del(
                &store,
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")],
            )
            .await,
        );
        assert_eq!(frame, Frame::Integer(1));
        assert!(
            matches!(record, Record::Del { keys } if keys == vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")])
        );
    }

    #[tokio::test]
    async fn exists_returns_count() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        let frame = read_frame(
            exists(
                &store,
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )
            .await,
        );
        assert_eq!(frame, Frame::Integer(1));
    }

    #[tokio::test]
    async fn mget_returns_array() {
        let store = MemoryStore::new();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await;
        let frame = read_frame(
            mget(
                &store,
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )
            .await,
        );
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Frame::BulkString("1".into()));
        assert_eq!(items[1], Frame::NullBulkString);
    }

    #[tokio::test]
    async fn mset_returns_ok() {
        let store = MemoryStore::new();
        let items = vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
        ];
        let (frame, record) = write_frame(mset(&store, items.clone()).await);
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        assert!(matches!(record, Record::MSet { items: ri } if ri == items));
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
