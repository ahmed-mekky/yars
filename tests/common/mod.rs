use anyhow::Result;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{net::TcpStream, time::timeout};
use tokio_util::{bytes::Bytes, codec::Framed};
use yars::{
    config::{AppConfig, FsyncMode},
    net::server::Server,
    protocol::resp::{Frame, RespCodec},
};

pub async fn spawn_server() -> Result<(u16, tokio::task::JoinHandle<Result<()>>)> {
    let tmp = tempfile::tempdir()?;
    let config = AppConfig {
        append_only: false,
        aof_path: tmp.path().join("data.aof"),
        fsync_mode: FsyncMode::No,
        config_path: tmp.path().join("yars.toml"),
        data_dir: tmp.path().to_path_buf(),
    };

    let server = Server::bind("127.0.0.1:0", config).await?;
    let port = server.local_addr()?.port();

    let handle = tokio::spawn(async move { server.run().await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok((port, handle))
}

pub async fn connect(port: u16) -> Result<Framed<TcpStream, RespCodec>> {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).await?;
    Ok(Framed::new(stream, RespCodec))
}

pub async fn send_cmd(framed: &mut Framed<TcpStream, RespCodec>, parts: &[&str]) -> Result<Frame> {
    let array = Frame::Array(
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::from(p.to_string().into_bytes())))
            .collect(),
    );
    framed.send(array).await?;
    let response = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connection closed unexpectedly"))??;
    Ok(response)
}

pub async fn shutdown_server(port: u16, handle: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
    let mut framed = connect(port).await?;
    let response = send_cmd(&mut framed, &["SHUTDOWN"]).await?;
    assert_eq!(response, Frame::SimpleString("OK".into()));
    let _ = timeout(Duration::from_secs(5), handle).await?;
    Ok(())
}
