pub fn shard_index(key: &[u8], num_shards: u8) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in key {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % num_shards as u64) as usize
}
