use crate::{
    protocol::resp::Frame,
    store::{
        persistence::record::Record,
        types::{Entry, Expiry},
    },
};
use tokio_util::bytes::Bytes;

pub mod multikey;
pub mod nokey;
pub mod singlekey;

pub enum CommandEffect {
    Read(Frame),
    Write(Frame, Record),
}

impl CommandEffect {
    pub fn from_set(frame: Frame, key: Bytes, entry: Entry) -> Self {
        let exp_ms = match entry.exp {
            Expiry::At(ms) => Some(ms),
            Expiry::None | Expiry::Keep => None,
        };
        Self::Write(
            frame,
            Record::Set {
                key,
                value: entry.value,
                exp_ms,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::bytes::Bytes;

    use crate::{
        protocol::resp::Frame,
        service::handlers::CommandEffect,
        store::{
            persistence::record::Record,
            types::{Entry, Expiry},
        },
    };

    pub fn entry(value: &[u8], exp: Expiry) -> Entry {
        Entry {
            value: Bytes::from(value.to_vec()),
            exp,
        }
    }

    pub fn read_frame(effect: CommandEffect) -> Frame {
        match effect {
            CommandEffect::Read(frame) => frame,
            CommandEffect::Write(frame, _) => panic!("expected read, got write: {frame:?}"),
        }
    }

    pub fn write_frame(effect: CommandEffect) -> (Frame, Record) {
        match effect {
            CommandEffect::Write(frame, record) => (frame, record),
            CommandEffect::Read(frame) => panic!("expected write, got read: {frame:?}"),
        }
    }
}
