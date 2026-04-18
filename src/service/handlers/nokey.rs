use crate::{
    config::AppConfig,
    protocol::resp::Frame,
    service::handlers::SetMutation,
    store::{memory::MemoryStore, persistence::AofEngine, traits::Store},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

pub async fn ping() -> (Frame, Option<SetMutation>) {
    (Frame::SimpleString("PONG".into()), None)
}

pub async fn echo(msg: Bytes) -> (Frame, Option<SetMutation>) {
    (Frame::BulkString(msg), None)
}

pub async fn dbsize(store: &impl Store) -> (Frame, Option<SetMutation>) {
    (Frame::Integer(store.len().await as i64), None)
}

pub async fn flushdb(store: &impl Store) -> (Frame, Option<SetMutation>) {
    store.clear().await;
    (Frame::Integer(1), None)
}

pub async fn info(store: &MemoryStore) -> (Frame, Option<SetMutation>) {
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
    (Frame::BulkString(info.into()), None)
}

pub async fn config_get(
    config: &Arc<RwLock<AppConfig>>,
    pattern: Bytes,
) -> (Frame, Option<SetMutation>) {
    let Some(pattern) = std::str::from_utf8(&pattern)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return (Frame::Error("ERR pattern is not valid UTF-8".into()), None);
    };

    let config = config.read().await;
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

    (Frame::Array(values), None)
}

pub async fn config_set(
    config: &Arc<RwLock<AppConfig>>,
    aof: &Option<Arc<AofEngine>>,
    key: Bytes,
    value: Bytes,
) -> (Frame, Option<SetMutation>) {
    let Some(key) = std::str::from_utf8(&key)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return (Frame::Error("ERR key is not valid UTF-8".into()), None);
    };

    let Some(value) = std::str::from_utf8(&value).ok() else {
        return (Frame::Error("ERR value is not a valid string".into()), None);
    };

    match &key[..] {
        "appendfsync" => {
            let mut config = config.write().await;
            match config.set_fsync_mode(value) {
                Ok(()) => {
                    if let Some(aof) = aof {
                        aof.set_fsync_mode(config.fsync_mode);
                    }
                    (Frame::SimpleString("OK".to_string()), None)
                }
                Err(e) => (Frame::Error(format!("ERR {e}")), None),
            }
        }
        "appendonly" => {
            let mut config = config.write().await;
            match value.parse::<bool>() {
                Ok(v) => {
                    config.append_only = v;
                    (Frame::SimpleString("OK".into()), None)
                }
                Err(e) => (Frame::Error(format!("ERR {e}")), None),
            }
        }
        "appendfilename" => {
            let mut config = config.write().await;
            if value.is_empty() {
                (Frame::Error("ERR empty filename".into()), None)
            } else {
                config.aof_path = config.data_dir.join(value);
                (Frame::SimpleString("OK".into()), None)
            }
        }
        _ => (
            Frame::Error("ERR unknown configuration option".into()),
            None,
        ),
    }
}

pub async fn config_rewrite(config: &Arc<RwLock<AppConfig>>) -> (Frame, Option<SetMutation>) {
    let config = config.read().await;
    match config.write_to_file() {
        Ok(()) => (Frame::SimpleString("OK".into()), None),
        Err(e) => (Frame::Error(format!("ERR {e}")), None),
    }
}
