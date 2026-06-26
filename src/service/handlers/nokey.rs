use crate::{config::AppConfig, protocol::resp::Frame, store::handle::StoreHandle};
use anyhow::Result;
use anyhow::anyhow;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

pub fn ping() -> Result<Frame> {
    Ok(Frame::SimpleString("PONG".into()))
}

pub fn echo(msg: Bytes) -> Result<Frame> {
    Ok(Frame::BulkString(msg))
}

pub async fn dbsize(store: &StoreHandle) -> Result<Frame> {
    Ok(Frame::Integer(store.len().await?))
}

pub async fn flushdb(store: &StoreHandle) -> Result<Frame> {
    store.clear().await?;
    Ok(Frame::Integer(1))
}

pub async fn info(store: &StoreHandle) -> Result<Frame> {
    Ok(Frame::BulkString(store.info().await?.into()))
}

pub async fn config_get(config: &Arc<RwLock<AppConfig>>, pattern: Bytes) -> Result<Frame> {
    let Some(pattern) = std::str::from_utf8(&pattern)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return Ok(Frame::Error("ERR pattern is not valid UTF-8".into()));
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

    Ok(Frame::Array(values))
}

pub async fn config_set(
    config: &Arc<RwLock<AppConfig>>,
    store: &StoreHandle,
    key: Bytes,
    value: Bytes,
) -> Result<Frame> {
    let Some(key) = std::str::from_utf8(&key)
        .ok()
        .map(|s| s.to_ascii_lowercase())
    else {
        return Err(anyhow!("ERR key is not valid UTF-8"));
    };

    let Some(value) = std::str::from_utf8(&value).ok() else {
        return Err(anyhow!("ERR value is not a valid string"));
    };

    match &key[..] {
        "appendfsync" => {
            let mut config = config.write().await;
            match config.set_fsync_mode(value) {
                Ok(()) => {
                    store.set_fsync_mode(config.fsync_mode).await?;
                    Ok(Frame::SimpleString("OK".to_string()))
                }
                Err(e) => Err(anyhow!(format!("ERR {e}"))),
            }
        }
        "appendonly" => {
            let mut config = config.write().await;
            match value.parse::<bool>() {
                Ok(v) => {
                    config.append_only = v;
                    Ok(Frame::SimpleString("OK".into()))
                }
                Err(e) => Err(anyhow!(format!("ERR {e}"))),
            }
        }
        "appendfilename" => {
            let mut config = config.write().await;
            if value.is_empty() {
                Err(anyhow!("ERR empty filename"))
            } else {
                config.aof_path = config.data_dir.join(value);
                Ok(Frame::SimpleString("OK".into()))
            }
        }
        _ => Err(anyhow!("ERR unknown configuration option")),
    }
}

pub async fn config_rewrite(config: &Arc<RwLock<AppConfig>>) -> Result<Frame> {
    let config = config.read().await;
    match config.write_to_file() {
        Ok(()) => Ok(Frame::SimpleString("OK".into())),
        Err(e) => Err(anyhow!(format!("ERR {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::handlers::tests::{entry, spawn_test_store};
    use crate::store::types::Expiry;

    fn make_config() -> Arc<RwLock<AppConfig>> {
        Arc::new(RwLock::new(AppConfig {
            append_only: true,
            aof_path: std::path::PathBuf::from("/tmp/test.aof"),
            fsync_mode: crate::config::FsyncMode::EverySec,
            config_path: std::path::PathBuf::from("/tmp/test.toml"),
            data_dir: std::path::PathBuf::from("/tmp"),
        }))
    }

    #[test]
    fn ping_returns_pong() {
        let frame = ping().unwrap();
        assert_eq!(frame, Frame::SimpleString("PONG".into()));
    }

    #[test]
    fn echo_returns_bulk_string() {
        let frame = echo(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(frame, Frame::BulkString("hello".into()));
    }

    #[tokio::test]
    async fn dbsize_returns_count() {
        let store = spawn_test_store();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await
            .unwrap();
        let frame = dbsize(&store).await.unwrap();
        assert_eq!(frame, Frame::Integer(1));
    }

    #[tokio::test]
    async fn flushdb_returns_one() {
        let store = spawn_test_store();
        store
            .set(Bytes::from_static(b"k"), entry(b"v", Expiry::None))
            .await
            .unwrap();
        let frame = flushdb(&store).await.unwrap();
        assert_eq!(frame, Frame::Integer(1));
        assert_eq!(store.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn info_contains_expected_fields() {
        let store = spawn_test_store();
        let frame = info(&store).await.unwrap();
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
        let frame = config_get(&config, Bytes::from_static(b"*")).await.unwrap();
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.len() >= 6);
    }

    #[tokio::test]
    async fn config_get_specific_key() {
        let config = make_config();
        let frame = config_get(&config, Bytes::from_static(b"appendonly"))
            .await
            .unwrap();
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
        let frame = config_get(&config, Bytes::from_static(b"unknown"))
            .await
            .unwrap();
        let Frame::Array(items) = frame else {
            panic!("expected array")
        };
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn config_set_fsync_mode_ok() {
        let config = make_config();
        let store = spawn_test_store();
        let frame = config_set(
            &config,
            &store,
            Bytes::from_static(b"appendfsync"),
            Bytes::from_static(b"no"),
        )
        .await
        .unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        let cfg = config.read().await;
        assert!(matches!(cfg.fsync_mode, crate::config::FsyncMode::No));
    }

    #[tokio::test]
    async fn config_set_appendonly_ok() {
        let config = make_config();
        let store = spawn_test_store();
        let frame = config_set(
            &config,
            &store,
            Bytes::from_static(b"appendonly"),
            Bytes::from_static(b"false"),
        )
        .await
        .unwrap();
        assert_eq!(frame, Frame::SimpleString("OK".into()));
        assert!(!config.read().await.append_only);
    }

    #[tokio::test]
    async fn config_set_unknown_returns_error() {
        let config = make_config();
        let store = spawn_test_store();
        let result = config_set(
            &config,
            &store,
            Bytes::from_static(b"unknown"),
            Bytes::from_static(b"v"),
        )
        .await;
        assert!(result.is_err());
    }
}
