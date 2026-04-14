use crate::store::types::Entry;
use tokio_util::bytes::Bytes;

pub mod multikey;
pub mod nokey;
pub mod singlekey;

pub type SetMutation = (Bytes, Entry);
