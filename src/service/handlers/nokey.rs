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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{
        memory::MemoryStore,
        types::{Entry, Expiry},
    };

    fn make_config() -> Arc<RwLock<AppConfig>> {
        Arc::new(RwLock::new(AppConfig {
            append_only: true,
            aof_path: std::path::PathBuf::from("/tmp/test.aof"),
            fsync_mode: crate::config::FsyncMode::EverySec,
            config_path: std::path::PathBuf::from("/tmp/test.toml"),
            data_dir: std::path::PathBuf::from("/tmp"),
        }))
    }

    #[tokio::test]
    async fn ping_returns_pong() {
        let (frame, mutation) = ping().await;
        assert!(matches!(frame, Frame::SimpleString(s) if s == "PONG"));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn echo_returns_bulk_string() {
        let (frame, mutation) = echo(Bytes::from_static(b"hello")).await;
        assert!(matches!(frame, Frame::BulkString(b) if b.as_ref() == b"hello"));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn dbsize_returns_count() {
        let store = MemoryStore::new();
        store
            .set(
                Bytes::from_static(b"k"),
                Entry {
                    value: Bytes::from_static(b"v"),
                    exp: Expiry::None,
                },
            )
            .await;
        let (frame, mutation) = dbsize(&store).await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_none());
    }

    #[tokio::test]
    async fn flushdb_returns_one() {
        let store = MemoryStore::new();
        store
            .set(
                Bytes::from_static(b"k"),
                Entry {
                    value: Bytes::from_static(b"v"),
                    exp: Expiry::None,
                },
            )
            .await;
        let (frame, mutation) = flushdb(&store).await;
        assert!(matches!(frame, Frame::Integer(1)));
        assert!(mutation.is_none());
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn info_contains_expected_fields() {
        let store = MemoryStore::new();
        let (frame, mutation) = info(&store).await;
        assert!(mutation.is_none());
        let Frame::BulkString(data) = frame else {
            panic!("expected bulk string")
        };
        let info_str = std::str::from_utf8(&data).unwrap();
        assert!(info_str.contains("yars_version"));
        assert!(info_str.contains("db_keys"));
        assert!(info_str.contains("used_memory"));
        assert!(info_str.contains("uptime_seconds"));
        assert!(info_str.contains("total_commands"));
    }

    #[tokio::test]
    async fn config_get_star_returns_all() {
        let config = make_config();
        let (frame, mutation) = config_get(&config, Bytes::from_static(b"*")).await;
        assert!(mutation.is_none());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.len() >= 6);
    }

    #[tokio::test]
    async fn config_get_specific_key() {
        let config = make_config();
        let (frame, mutation) = config_get(&config, Bytes::from_static(b"appendonly")).await;
        assert!(mutation.is_none());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], Frame::BulkString(b) if b.as_ref() == b"appendonly"));
        assert!(matches!(&items[1], Frame::BulkString(b) if b.as_ref() == b"true"));
    }

    #[tokio::test]
    async fn config_get_unknown_returns_empty() {
        let config = make_config();
        let (frame, mutation) = config_get(&config, Bytes::from_static(b"unknown")).await;
        assert!(mutation.is_none());
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn config_set_fsync_mode_ok() {
        let config = make_config();
        let (frame, mutation) = config_set(
            &config,
            &None,
            Bytes::from_static(b"appendfsync"),
            Bytes::from_static(b"no"),
        )
        .await;
        assert!(matches!(frame, Frame::SimpleString(s) if s == "OK"));
        assert!(mutation.is_none());
        let cfg = config.read().await;
        assert!(matches!(cfg.fsync_mode, crate::config::FsyncMode::No));
    }

    #[tokio::test]
    async fn config_set_appendonly_ok() {
        let config = make_config();
        let (frame, _mutation) = config_set(
            &config,
            &None,
            Bytes::from_static(b"appendonly"),
            Bytes::from_static(b"false"),
        )
        .await;
        assert!(matches!(frame, Frame::SimpleString(s) if s == "OK"));
        assert!(!config.read().await.append_only);
    }

    #[tokio::test]
    async fn config_set_unknown_returns_error() {
        let config = make_config();
        let (frame, mutation) = config_set(
            &config,
            &None,
            Bytes::from_static(b"unknown"),
            Bytes::from_static(b"v"),
        )
        .await;
        assert!(matches!(frame, Frame::Error(_)));
        assert!(mutation.is_none());
    }
}
