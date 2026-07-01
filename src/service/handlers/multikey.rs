use crate::{protocol::resp::Frame, store::handle::StoreHandle};
use anyhow::Result;
use tokio_util::bytes::Bytes;

pub async fn del(store: &StoreHandle, keys: Vec<Bytes>) -> Result<Frame> {
    Ok(Frame::Integer(store.del(keys).await?))
}

pub async fn exists(store: &StoreHandle, keys: Vec<Bytes>) -> Result<Frame> {
    Ok(Frame::Integer(store.exists(keys).await?))
}

pub async fn mget(store: &StoreHandle, keys: Vec<Bytes>) -> Result<Frame> {
    Ok(Frame::Array(
        store
            .mget(keys)
            .await?
            .iter()
            .map(|e| match e {
                Some(entry) => Frame::BulkString(entry.value.clone()),
                None => Frame::NullBulkString,
            })
            .collect(),
    ))
}

pub async fn mset(store: &StoreHandle, items: Vec<(Bytes, Bytes)>) -> Result<Frame> {
    store.mset(items).await?;
    Ok(Frame::SimpleString("OK".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::handlers::tests::{entry, spawn_test_store};
    use crate::store::types::Expiry;

    #[tokio::test]
    async fn del_returns_count() {
        let store = spawn_test_store();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await
            .unwrap();
        store
            .set(Bytes::from_static(b"b"), entry(b"2", Expiry::None))
            .await
            .unwrap();
        let frame = del(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")],
        )
        .await
        .unwrap();
        assert_eq!(frame, Frame::Integer(1));
    }

    #[tokio::test]
    async fn exists_returns_count() {
        let store = spawn_test_store();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await
            .unwrap();
        let frame = exists(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await
        .unwrap();
        assert_eq!(frame, Frame::Integer(1));
    }

    #[tokio::test]
    async fn mget_returns_array() {
        let store = spawn_test_store();
        store
            .set(Bytes::from_static(b"a"), entry(b"1", Expiry::None))
            .await
            .unwrap();
        let frame = mget(
            &store,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await
        .unwrap();
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Frame::BulkString("1".into()));
        assert_eq!(items[1], Frame::NullBulkString);
    }

    #[tokio::test]
    async fn mset_returns_ok() {
        let store = spawn_test_store();
        let items = vec![
            (Bytes::from_static(b"a"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"b"), Bytes::from_static(b"2")),
        ];
        let frame = mset(&store, items).await.unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        assert_eq!(
            store
                .get(Bytes::from_static(b"a"))
                .await
                .unwrap()
                .unwrap()
                .value,
            Bytes::from_static(b"1")
        );
        assert_eq!(
            store
                .get(Bytes::from_static(b"b"))
                .await
                .unwrap()
                .unwrap()
                .value,
            Bytes::from_static(b"2")
        );
    }
}
