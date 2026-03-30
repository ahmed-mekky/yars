use crate::{
    protocol::resp::Frame,
    store::{memory::MemoryStore, traits::Store},
};

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

pub async fn info(store: &MemoryStore) -> Frame {
    let key_count = store.len().await as i64;
    let used_memory = store.used_memory().await;
    let uptime_seconds = store.uptime_seconds();
    let total_commands = store.total_commands();

    let info = format!(
        "yars_version:{}\r\ndb_keys:{}\r\nused_memory:{}\r\nuptime_seconds:{}\r\ntotal_commands:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        key_count,
        used_memory,
        uptime_seconds,
        total_commands
    );
    Frame::BulkString(info.into())
}
