use crate::{
    config::AppConfig,
    protocol::resp::Frame,
    service::handlers::CommandEffect,
    store::{memory::MemoryStore, persistence::aof::Aof, traits::Store},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

pub async fn ping() -> CommandEffect {
    CommandEffect::Read(Frame::SimpleString("PONG".into()))
}

pub async fn echo(msg: Bytes) -> CommandEffect {
    CommandEffect::Read(Frame::BulkString(msg))
}

pub async fn dbsize(store: &impl Store) -> CommandEffect {
    CommandEffect::Read(Frame::Integer(store.len().await as i64))
}

pub async fn flushdb(store: &impl Store) -> CommandEffect {
    store.clear().await;
    CommandEffect::Write(
        Frame::Integer(1),
        crate::store::persistence::record::Record::FlushDb,
    )
}

pub async fn info(store: &MemoryStore) -> CommandEffect {
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
    CommandEffect::Read(Frame::BulkString(info.into()))
}

pub async fn config_get(config: &Arc<RwLock<AppConfig>>, pattern: Bytes) -> CommandEffect {
    let Some(pattern) = std::str::from_utf8(&pattern)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return CommandEffect::Read(Frame::Error("ERR pattern is not valid UTF-8".into()));
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

    CommandEffect::Read(Frame::Array(values))
}

pub async fn config_set(
    config: &Arc<RwLock<AppConfig>>,
    aof: &Arc<dyn Aof>,
    key: Bytes,
    value: Bytes,
) -> CommandEffect {
    let Some(key) = std::str::from_utf8(&key)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return CommandEffect::Read(Frame::Error("ERR key is not valid UTF-8".into()));
    };

    let Some(value) = std::str::from_utf8(&value).ok() else {
        return CommandEffect::Read(Frame::Error("ERR value is not a valid string".into()));
    };

    match &key[..] {
        "appendfsync" => {
            let mut config = config.write().await;
            match config.set_fsync_mode(value) {
                Ok(()) => {
                    aof.set_fsync_mode(config.fsync_mode);
                    CommandEffect::Read(Frame::SimpleString("OK".to_string()))
                }
                Err(e) => CommandEffect::Read(Frame::Error(format!("ERR {e}"))),
            }
        }
        "appendonly" => {
            let mut config = config.write().await;
            match value.parse::<bool>() {
                Ok(v) => {
                    config.append_only = v;
                    CommandEffect::Read(Frame::SimpleString("OK".into()))
                }
                Err(e) => CommandEffect::Read(Frame::Error(format!("ERR {e}"))),
            }
        }
        "appendfilename" => {
            let mut config = config.write().await;
            if value.is_empty() {
                CommandEffect::Read(Frame::Error("ERR empty filename".into()))
            } else {
                config.aof_path = config.data_dir.join(value);
                CommandEffect::Read(Frame::SimpleString("OK".into()))
            }
        }
        _ => CommandEffect::Read(Frame::Error("ERR unknown configuration option".into())),
    }
}

pub async fn config_rewrite(config: &Arc<RwLock<AppConfig>>) -> CommandEffect {
    let config = config.read().await;
    match config.write_to_file() {
        Ok(()) => CommandEffect::Read(Frame::SimpleString("OK".into())),
        Err(e) => CommandEffect::Read(Frame::Error(format!("ERR {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::handlers::tests::{read_frame, write_frame};
    use crate::store::persistence::record::Record;
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
        let frame = read_frame(ping().await);
        assert_eq!(frame, Frame::SimpleString("PONG".into()));
    }

    #[tokio::test]
    async fn echo_returns_bulk_string() {
        let frame = read_frame(echo(Bytes::from_static(b"hello")).await);
        assert_eq!(frame, Frame::BulkString("hello".into()));
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
        let frame = read_frame(dbsize(&store).await);
        assert_eq!(frame, Frame::Integer(1));
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
        let (frame, record) = write_frame(flushdb(&store).await);
        assert_eq!(frame, Frame::Integer(1));
        assert!(matches!(record, Record::FlushDb));
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn info_contains_expected_fields() {
        let store = MemoryStore::new();
        let frame = read_frame(info(&store).await);
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
        let frame = read_frame(config_get(&config, Bytes::from_static(b"*")).await);
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.len() >= 6);
    }

    #[tokio::test]
    async fn config_get_specific_key() {
        let config = make_config();
        let frame = read_frame(config_get(&config, Bytes::from_static(b"appendonly")).await);
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Frame::BulkString("appendonly".into()));
        assert_eq!(items[1], Frame::BulkString("true".into()));
    }

    #[tokio::test]
    async fn config_get_unknown_returns_empty() {
        let config = make_config();
        let frame = read_frame(config_get(&config, Bytes::from_static(b"unknown")).await);
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn config_set_fsync_mode_ok() {
        let config = make_config();
        let aof: Arc<dyn crate::store::persistence::aof::Aof> =
            Arc::new(crate::store::persistence::aof::NoopAof);
        let frame = read_frame(
            config_set(
                &config,
                &aof,
                Bytes::from_static(b"appendfsync"),
                Bytes::from_static(b"no"),
            )
            .await,
        );
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        let cfg = config.read().await;
        assert!(matches!(cfg.fsync_mode, crate::config::FsyncMode::No));
    }

    #[tokio::test]
    async fn config_set_appendonly_ok() {
        let config = make_config();
        let aof: Arc<dyn crate::store::persistence::aof::Aof> =
            Arc::new(crate::store::persistence::aof::NoopAof);
        let frame = read_frame(
            config_set(
                &config,
                &aof,
                Bytes::from_static(b"appendonly"),
                Bytes::from_static(b"false"),
            )
            .await,
        );
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        assert!(!config.read().await.append_only);
    }

    #[tokio::test]
    async fn config_set_unknown_returns_error() {
        let config = make_config();
        let aof: Arc<dyn crate::store::persistence::aof::Aof> =
            Arc::new(crate::store::persistence::aof::NoopAof);
        let frame = read_frame(
            config_set(
                &config,
                &aof,
                Bytes::from_static(b"unknown"),
                Bytes::from_static(b"v"),
            )
            .await,
        );
        assert!(matches!(frame, Frame::Error(_)));
    }
}
