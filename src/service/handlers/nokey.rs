use crate::{protocol::resp::Frame, store::traits::Store};

pub async fn ping() -> Frame {
    Frame::SimpleString("PONG".into())
}

pub async fn echo(msg: tokio_util::bytes::Bytes) -> Frame {
    Frame::BulkString(msg)
}

pub async fn dbsize(store: &impl Store) -> Frame {
    Frame::Integer(store.len().await as i64)
}

pub async fn flushdb(store: &impl Store) -> Frame {
    store.clear().await;
    Frame::Integer(1)
}
