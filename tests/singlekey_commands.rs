mod common;

use common::{connect, send_cmd, shutdown_server, spawn_server};
use yars::protocol::resp::Frame;

#[tokio::test]
async fn set_and_get_round_trip() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["SET", "mykey", "myvalue"])
        .await
        .unwrap();
    assert_eq!(response, Frame::SimpleString("OK".into()));

    let response = send_cmd(&mut framed, &["GET", "mykey"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("myvalue".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn get_missing_returns_null() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["GET", "nosuchkey"]).await.unwrap();
    assert_eq!(response, Frame::NullBulkString);

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn getdel_returns_value_and_removes_key() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "v"]).await.unwrap();
    let response = send_cmd(&mut framed, &["GETDEL", "k"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("v".into()));

    let response = send_cmd(&mut framed, &["GET", "k"]).await.unwrap();
    assert_eq!(response, Frame::NullBulkString);

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn getset_returns_old_value() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "old"]).await.unwrap();
    let response = send_cmd(&mut framed, &["GETSET", "k", "new"])
        .await
        .unwrap();
    assert_eq!(response, Frame::BulkString("old".into()));

    let response = send_cmd(&mut framed, &["GET", "k"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("new".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn setnx_sets_only_when_missing() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["SETNX", "k", "first"])
        .await
        .unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["SETNX", "k", "second"])
        .await
        .unwrap();
    assert_eq!(response, Frame::Integer(0));

    let response = send_cmd(&mut framed, &["GET", "k"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("first".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn incr_starts_at_one() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["INCR", "counter"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["INCR", "counter"]).await.unwrap();
    assert_eq!(response, Frame::Integer(2));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn decr_starts_at_minus_one() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["DECR", "counter"]).await.unwrap();
    assert_eq!(response, Frame::Integer(-1));

    let response = send_cmd(&mut framed, &["DECR", "counter"]).await.unwrap();
    assert_eq!(response, Frame::Integer(-2));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn incr_non_integer_returns_error() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "abc"]).await.unwrap();
    let response = send_cmd(&mut framed, &["INCR", "k"]).await.unwrap();
    assert!(
        matches!(response, Frame::Error(ref s) if s.contains("integer")),
        "expected integer error, got {response:?}"
    );

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn append_and_strlen() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["APPEND", "k", "hello"])
        .await
        .unwrap();
    assert_eq!(response, Frame::Integer(5));

    let response = send_cmd(&mut framed, &["APPEND", "k", " world"])
        .await
        .unwrap();
    assert_eq!(response, Frame::Integer(11));

    let response = send_cmd(&mut framed, &["STRLEN", "k"]).await.unwrap();
    assert_eq!(response, Frame::Integer(11));

    let response = send_cmd(&mut framed, &["GET", "k"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("hello world".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn ttl_and_expire() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "v"]).await.unwrap();
    let response = send_cmd(&mut framed, &["TTL", "k"]).await.unwrap();
    assert_eq!(response, Frame::Integer(-1)); // no expiry

    let response = send_cmd(&mut framed, &["EXPIRE", "k", "10"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["TTL", "k"]).await.unwrap();
    assert!(matches!(response, Frame::Integer(t) if t > 0 && t <= 10));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn pttl_matches_ttl() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "v", "PX", "5000"])
        .await
        .unwrap();
    let response = send_cmd(&mut framed, &["PTTL", "k"]).await.unwrap();
    assert!(matches!(response, Frame::Integer(t) if t > 0 && t <= 5000));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn persist_removes_expiry() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "k", "v"]).await.unwrap();
    send_cmd(&mut framed, &["EXPIRE", "k", "10"]).await.unwrap();

    let response = send_cmd(&mut framed, &["PERSIST", "k"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["TTL", "k"]).await.unwrap();
    assert_eq!(response, Frame::Integer(-1));

    shutdown_server(port, handle).await.unwrap();
}
