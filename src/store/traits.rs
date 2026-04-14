use async_trait::async_trait;
use tokio_util::bytes::Bytes;

use crate::store::types::Entry;

#[async_trait]
pub trait Store: Send + Sync {
    async fn set(&self, key: Bytes, entry: Entry) -> Entry;
    async fn get(&self, key: &Bytes) -> Option<Entry>;
    async fn del(&self, keys: &[Bytes]) -> i64;
    async fn exists(&self, keys: &[Bytes]) -> i64;
    async fn mget(&self, keys: &[Bytes]) -> Vec<Option<Entry>>;
    async fn mset(&self, items: &[(Bytes, Bytes)]);
    async fn len(&self) -> usize;
    async fn clear(&self);
    async fn is_empty(&self) -> bool;
}
