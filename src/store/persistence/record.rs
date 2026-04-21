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
            0 => Ok(Self::Set),
            1 => Ok(Self::Del),
            2 => Ok(Self::MSet),
            3 => Ok(Self::FlushDb),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_valid_tags() {
        assert!(matches!(RecordTag::try_from(0u8), Ok(RecordTag::Set)));
        assert!(matches!(RecordTag::try_from(1u8), Ok(RecordTag::Del)));
        assert!(matches!(RecordTag::try_from(2u8), Ok(RecordTag::MSet)));
        assert!(matches!(RecordTag::try_from(3u8), Ok(RecordTag::FlushDb)));
    }

    #[test]
    fn try_from_invalid_tag() {
        assert!(RecordTag::try_from(4u8).is_err());
        assert!(RecordTag::try_from(255u8).is_err());
    }

    #[test]
    fn from_record_set() {
        let record = Record::Set {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            exp_ms: None,
        };
        assert_eq!(RecordTag::from(&record) as u8, 0);
    }

    #[test]
    fn from_record_del() {
        let record = Record::Del {
            keys: vec![Bytes::from_static(b"k")],
        };
        assert_eq!(RecordTag::from(&record) as u8, 1);
    }

    #[test]
    fn from_record_mset() {
        let record = Record::MSet {
            items: vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v"))],
        };
        assert_eq!(RecordTag::from(&record) as u8, 2);
    }

    #[test]
    fn from_record_flushdb() {
        assert_eq!(RecordTag::from(&Record::FlushDb) as u8, 3);
    }
}
