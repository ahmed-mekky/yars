use std::{collections::HashMap, error::Error, sync::Arc};

use tokio::sync::RwLock;

pub struct Db(Arc<RwLock<HashMap<String, Vec<u8>>>>);

impl Db {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn set(&self, key: String, value: Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.0.write().await.insert(key, value);
        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
        let map = self.0.read().await;
        Ok(map.get(&key).cloned())
    }
}
