use std::{collections::HashMap, error::Error};

use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

pub struct Db(RwLock<HashMap<String, Bytes>>);

impl Db {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    pub async fn set(&self, key: String, value: Bytes) -> Result<(), Box<dyn Error>> {
        self.0.write().await.insert(key, value);
        Ok(())
    }

    pub async fn get(&self, key: String) -> Result<Option<Bytes>, Box<dyn Error>> {
        let map = self.0.read().await;
        Ok(map.get(&key).cloned())
    }

    pub async fn del(&self, keys: Vec<String>) -> Result<i64, Box<dyn Error>> {
        let mut map = self.0.write().await;
        let mut count = 0;
        for key in keys {
            if map.remove(&key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }
}
