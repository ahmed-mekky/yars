use tokio_util::bytes::Bytes;

use crate::{protocol::resp::Frame, store::traits::Store};

pub async fn del(store: &impl Store, keys: Vec<Bytes>) -> Frame {
    Frame::Integer(store.del(&keys).await)
}

pub async fn exists(store: &impl Store, keys: Vec<Bytes>) -> Frame {
    Frame::Integer(store.exists(&keys).await)
}

pub async fn mget(store: &impl Store, keys: Vec<Bytes>) -> Frame {
    let values = store
        .mget(&keys)
        .await
        .iter()
        .map(|e| match e {
            Some(entry) => Frame::BulkString(entry.value.clone()),
            None => Frame::NullBulkString,
        })
        .collect();
    Frame::Array(values)
}

pub async fn mset(store: &impl Store, items: Vec<(Bytes, Bytes)>) -> Frame {
    store.mset(&items).await;
    Frame::SimpleString("OK".into())
}
