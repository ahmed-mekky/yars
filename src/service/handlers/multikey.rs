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
