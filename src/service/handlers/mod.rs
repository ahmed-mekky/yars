pub mod multikey;
pub mod nokey;
pub mod singlekey;

#[cfg(test)]
mod tests {
    use crate::store::{
        actor::StoreActor,
        handle::StoreHandle,
        memory::MemoryStore,
        persistence::aof::Aof,
        types::{Entry, Expiry},
    };
    use tokio_util::bytes::Bytes;

    pub fn entry(value: &[u8], exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from(value.to_vec()),
            exp,
        }
    }

    pub fn spawn_test_store() -> StoreHandle {
        let (handle, _) = StoreActor::spawn(MemoryStore::new(), Aof::Noop, 100);
        handle
    }
}
