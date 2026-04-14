use anyhow::anyhow;
use tokio_util::bytes::Bytes;

#[derive(Clone, Debug)]
pub enum Record {
    Set {
        key: Bytes,
        value: Bytes,
        exp_ms: Option<u64>,
    },
    Del {
        keys: Vec<Bytes>,
    },
    MSet {
        items: Vec<(Bytes, Bytes)>,
    },
    FlushDb,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum RecordTag {
    Set = 0,
    Del = 1,
    MSet = 2,
    FlushDb = 3,
}

impl TryFrom<u8> for RecordTag {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Set),
            2 => Ok(Self::Del),
            3 => Ok(Self::MSet),
            6 => Ok(Self::FlushDb),
            _ => Err(anyhow!("unknown record tag")),
        }
    }
}

impl From<&Record> for RecordTag {
    fn from(value: &Record) -> Self {
        match value {
            Record::Set { .. } => Self::Set,
            Record::Del { .. } => Self::Del,
            Record::MSet { .. } => Self::MSet,
            Record::FlushDb => Self::FlushDb,
        }
    }
}
