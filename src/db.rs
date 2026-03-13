use std::{collections::HashMap, error::Error};

use tokio::sync::RwLock;
use tokio_util::bytes::Bytes;

use crate::resp::{Entry, Expiry};

pub struct Db(RwLock<HashMap<Bytes, Entry>>);

impl Db {
    pub fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    pub async fn set(&self, key: Bytes, mut entry: Entry) -> Result<(), Box<dyn Error>> {
        if let Expiry::Keep = entry.exp {
            //TODO remove unwrap
            entry.exp = self.get(&key).await?.unwrap().exp;
        };
        self.0.write().await.insert(key, entry);
        Ok(())
    }

    pub async fn get(&self, key: &Bytes) -> Result<Option<Entry>, Box<dyn Error>> {
        let map = self.0.read().await;
        Ok(map.get(key).cloned())
    }

    pub async fn del(&self, keys: Vec<Bytes>) -> Result<i64, Box<dyn Error>> {
        let mut map = self.0.write().await;
        let mut count = 0;
        for key in keys {
            if map.remove(&key).is_some() {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn forget(&self, key: &Bytes) {
        if let Ok(mut map) = self.0.try_write() {
            map.remove(key);
        }
    }
}
