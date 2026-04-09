use crate::{
    config::AppConfig,
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

pub async fn config_get(config: &AppConfig, pattern: tokio_util::bytes::Bytes) -> Frame {
    let Some(pattern) = std::str::from_utf8(&pattern)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return Frame::Error("ERR pattern is not valid UTF-8".into());
    };

    let mut values = Vec::new();

    if pattern == "*" || pattern == "appendonly" {
        values.push(Frame::BulkString("appendonly".into()));
        values.push(Frame::BulkString(config.append_only.to_string().into()));
    }
    if pattern == "*" || pattern == "appendfilename" {
        values.push(Frame::BulkString("appendfilename".into()));
        values.push(Frame::BulkString(
            config.aof_path.to_string_lossy().into_owned().into(),
        ));
    }
    if pattern == "*" || pattern == "appendfsync" {
        values.push(Frame::BulkString("appendfsync".into()));
        values.push(Frame::BulkString(config.fsync_mode.as_str().into()));
    }

    Frame::Array(values)
}
