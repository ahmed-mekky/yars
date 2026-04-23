mod common;

use common::{connect, send_cmd, shutdown_server, spawn_server};
use yars::protocol::resp::Frame;

#[tokio::test]
async fn ping_returns_pong() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();
    let response = send_cmd(&mut framed, &["PING"]).await.unwrap();
    assert_eq!(response, Frame::SimpleString("PONG".into()));
    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn echo_returns_bulk_string() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();
    let response = send_cmd(&mut framed, &["ECHO", "hello world"])
        .await
        .unwrap();
    assert_eq!(response, Frame::BulkString("hello world".into()));
    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn dbsize_and_flushdb() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["DBSIZE"]).await.unwrap();
    assert_eq!(response, Frame::Integer(0));

    let response = send_cmd(&mut framed, &["SET", "k", "v"]).await.unwrap();
    assert_eq!(response, Frame::SimpleString("OK".into()));

    let response = send_cmd(&mut framed, &["DBSIZE"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["FLUSHDB"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["DBSIZE"]).await.unwrap();
    assert_eq!(response, Frame::Integer(0));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn info_contains_fields() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();
    let response = send_cmd(&mut framed, &["INFO"]).await.unwrap();
    let Frame::BulkString(data) = response else {
        panic!("expected bulk string");
    };
    let info = std::str::from_utf8(&data).unwrap();
    assert!(info.contains("yars_version"));
    assert!(info.contains("db_keys"));
    assert!(info.contains("used_memory"));
    assert!(info.contains("uptime_seconds"));
    assert!(info.contains("total_commands"));
    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn shutdown_returns_ok_and_stops_server() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();
    let response = send_cmd(&mut framed, &["SHUTDOWN"]).await.unwrap();
    assert_eq!(response, Frame::SimpleString("OK".into()));

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    assert!(connect(port).await.is_err());

    let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), handle)
        .await
        .unwrap();
    assert!(result.is_ok());
}
