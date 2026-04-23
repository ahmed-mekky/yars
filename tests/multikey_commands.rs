mod common;

use common::{connect, send_cmd, shutdown_server, spawn_server};
use yars::protocol::resp::Frame;

#[tokio::test]
async fn del_returns_deleted_count() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "a", "1"]).await.unwrap();
    send_cmd(&mut framed, &["SET", "b", "2"]).await.unwrap();

    let response = send_cmd(&mut framed, &["DEL", "a", "c"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    let response = send_cmd(&mut framed, &["GET", "a"]).await.unwrap();
    assert_eq!(response, Frame::NullBulkString);

    let response = send_cmd(&mut framed, &["GET", "b"]).await.unwrap();
    assert_eq!(response, Frame::BulkString("2".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn exists_returns_count() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    send_cmd(&mut framed, &["SET", "a", "1"]).await.unwrap();

    let response = send_cmd(&mut framed, &["EXISTS", "a", "b"]).await.unwrap();
    assert_eq!(response, Frame::Integer(1));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn mset_and_mget() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["MSET", "k1", "v1", "k2", "v2"])
        .await
        .unwrap();
    assert_eq!(response, Frame::SimpleString("OK".into()));

    let response = send_cmd(&mut framed, &["MGET", "k1", "missing", "k2"])
        .await
        .unwrap();
    let Frame::Array(items) = response else {
        panic!("expected array");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], Frame::BulkString("v1".into()));
    assert_eq!(items[1], Frame::NullBulkString);
    assert_eq!(items[2], Frame::BulkString("v2".into()));

    shutdown_server(port, handle).await.unwrap();
}

#[tokio::test]
async fn mget_returns_empty_array_when_no_keys() {
    let (port, handle) = spawn_server().await.unwrap();
    let mut framed = connect(port).await.unwrap();

    let response = send_cmd(&mut framed, &["MGET"]).await.unwrap();
    let Frame::Array(items) = response else {
        panic!("expected array");
    };
    assert!(items.is_empty());

    shutdown_server(port, handle).await.unwrap();
}
