mod connection;
mod resp;
mod server;

use anyhow::Result;
use server::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let server = Server::bind("127.0.0.1:6379").await?;
    println!("Server is running on port 6379");
    server.run().await?;
    Ok(())
}
