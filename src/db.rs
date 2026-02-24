use std::{collections::HashMap, error::Error, sync::Arc};

use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

pub struct Db(Arc<RwLock<HashMap<String, Bytes>>>);

impl Db {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn set(&self, key: String, value: Bytes) -> Result<(), Box<dyn Error>> {
        self.0.write().await.insert(key, value);
        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<Option<Bytes>, Box<dyn Error>> {
        let map = self.0.read().await;
        Ok(map.get(&key).cloned())
    }

    pub async fn del(&self, key: String) -> Result<(), Box<dyn Error>> {
        self.0.write().await.remove(&key);
        Ok(())
    }
}
