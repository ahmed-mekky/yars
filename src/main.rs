use anyhow::Result;
use yars::{config::AppConfig, net::server::Server};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cfg = AppConfig::from_env()?;
    println!("Application configuration loaded:\n {:?}", &cfg);
    let server = Server::bind("127.0.0.1:6379", cfg).await?;
    println!("Server is running on port 6379");
    server.run().await?;
    Ok(())
}
