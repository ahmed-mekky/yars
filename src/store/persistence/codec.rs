use anyhow::{Result, anyhow};
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::{Decoder, Encoder},
};

use crate::store::persistence::record::{Record, RecordTag};

pub struct RecordCodec;

impl Encoder<Record> for RecordCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Record, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut payload = BytesMut::new();
        encode_payload(item, &mut payload)?;
        dst.put_u32(payload.len() as u32);
        dst.extend_from_slice(&payload);
        Ok(())
    }
}

impl Decoder for RecordCodec {
    type Item = Record;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut len_buf = &src[..4];
        let frame_len = len_buf.get_u32() as usize;

        if src.len() < 4 + frame_len {
            return Ok(None);
        }

        src.advance(4);
        let payload = src.split_to(frame_len);
        decode_payload(&payload)
    }
}

fn encode_payload(record: Record, out: &mut BytesMut) -> Result<()> {
    out.put_u8(RecordTag::from(&record) as u8);

    match record {
        Record::Set { key, value, exp_ms } => {
            put_bytes(out, &key);
            put_bytes(out, &value);
            put_opt_u64(out, exp_ms);
        }
        Record::Del { keys } => {
            out.put_u32(keys.len() as u32);
            for key in keys {
                put_bytes(out, &key);
            }
        }
        Record::MSet { items } => {
            out.put_u32(items.len() as u32);
            for (key, value) in items {
                put_bytes(out, &key);
                put_bytes(out, &value);
            }
        }
        Record::FlushDb => {}
    }
    Ok(())
}

fn decode_payload(mut input: &[u8]) -> Result<Option<Record>> {
    if !input.has_remaining() {
        return Err(anyhow!("empty record payload"));
    }
    let tag = RecordTag::try_from(input.get_u8())?;

    let record = match tag {
        RecordTag::Set => {
            let key = get_bytes(&mut input)?;
            let value = get_bytes(&mut input)?;
            let exp_ms = get_opt_u64(&mut input)?;
            Record::Set { key, value, exp_ms }
        }
        RecordTag::Del => {
            let count = input.get_u32() as usize;
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(get_bytes(&mut input)?);
            }
            Record::Del { keys }
        }
        RecordTag::MSet => {
            let count = input.get_u32() as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let key = get_bytes(&mut input)?;
                let value = get_bytes(&mut input)?;
                items.push((key, value));
            }
            Record::MSet { items }
        }
        RecordTag::FlushDb => Record::FlushDb,
    };

    if input.has_remaining() {
        return Err(anyhow!("trailing bytes in record payload"));
    }

    Ok(Some(record))
}

fn put_bytes(out: &mut BytesMut, bytes: &Bytes) {
    out.put_u32(bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn get_bytes(input: &mut &[u8]) -> Result<Bytes> {
    if input.remaining() < 4 {
        return Err(anyhow!("truncated bytes length"));
    }
    let len = input.get_u32() as usize;
    if input.remaining() < len {
        return Err(anyhow!("truncated bytes data"));
    }
    Ok(Bytes::copy_from_slice(&input.copy_to_bytes(len)))
}

fn put_opt_u64(out: &mut BytesMut, value: Option<u64>) {
    match value {
        Some(v) => {
            out.put_u8(1);
            out.put_u64(v);
        }
        None => out.put_u8(0),
    }
}

fn get_opt_u64(input: &mut &[u8]) -> Result<Option<u64>> {
    if input.remaining() < 1 {
        return Err(anyhow!("truncated optional flag"));
    }
    match input.get_u8() {
        0 => Ok(None),
        1 => {
            if input.remaining() < 8 {
                return Err(anyhow!("truncated optional u64"));
            }
            Ok(Some(input.get_u64()))
        }
        _ => Err(anyhow!("invalid optional flag")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::persistence::record::Record;

    fn round_trip(record: Record) {
        let mut codec = RecordCodec;
        let mut buf = BytesMut::new();
        codec.encode(record.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        match (&record, &decoded) {
            (
                Record::Set { key, value, exp_ms },
                Record::Set {
                    key: dk,
                    value: dv,
                    exp_ms: de,
                },
            ) => {
                assert_eq!(key, dk);
                assert_eq!(value, dv);
                assert_eq!(exp_ms, de);
            }
            (Record::Del { keys }, Record::Del { keys: dk }) => {
                assert_eq!(keys, dk);
            }
            (Record::MSet { items }, Record::MSet { items: di }) => {
                assert_eq!(items, di);
            }
            (Record::FlushDb, Record::FlushDb) => {}
            _ => panic!("record type mismatch"),
        }
    }

    #[test]
    fn round_trip_set_no_expiry() {
        round_trip(Record::Set {
            key: Bytes::from_static(b"mykey"),
            value: Bytes::from_static(b"myval"),
            exp_ms: None,
        });
    }

    #[test]
    fn round_trip_set_with_expiry() {
        round_trip(Record::Set {
            key: Bytes::from_static(b"mykey"),
            value: Bytes::from_static(b"myval"),
            exp_ms: Some(123456789),
        });
    }

    #[test]
    fn round_trip_del_single_key() {
        round_trip(Record::Del {
            keys: vec![Bytes::from_static(b"k1")],
        });
    }

    #[test]
    fn round_trip_del_multiple_keys() {
        round_trip(Record::Del {
            keys: vec![
                Bytes::from_static(b"k1"),
                Bytes::from_static(b"k2"),
                Bytes::from_static(b"k3"),
            ],
        });
    }

    #[test]
    fn round_trip_mset() {
        round_trip(Record::MSet {
            items: vec![
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            ],
        });
    }

    #[test]
    fn round_trip_flushdb() {
        round_trip(Record::FlushDb);
    }

    #[test]
    fn decode_incomplete_length_prefix() {
        let mut codec = RecordCodec;
        let mut buf = BytesMut::from(&[0u8, 0][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_incomplete_payload() {
        let mut codec = RecordCodec;
        let mut buf = BytesMut::from(&[0u8, 0, 0, 10, 0][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_empty_payload_errors() {
        let result = decode_payload(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_invalid_tag_errors() {
        let result = decode_payload(&[99]);
        assert!(result.is_err());
    }

    #[test]
    fn encode_decode_multiple_sequential() {
        let mut codec = RecordCodec;
        let mut buf = BytesMut::new();

        let records = vec![
            Record::Set {
                key: Bytes::from_static(b"a"),
                value: Bytes::from_static(b"1"),
                exp_ms: None,
            },
            Record::Del {
                keys: vec![Bytes::from_static(b"b")],
            },
            Record::FlushDb,
        ];

        for rec in &records {
            codec.encode(rec.clone(), &mut buf).unwrap();
        }

        for expected in &records {
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            match (expected, &decoded) {
                (
                    Record::Set { key, value, exp_ms },
                    Record::Set {
                        key: dk,
                        value: dv,
                        exp_ms: de,
                    },
                ) => {
                    assert_eq!(key, dk);
                    assert_eq!(value, dv);
                    assert_eq!(exp_ms, de);
                }
                (Record::Del { keys }, Record::Del { keys: dk }) => {
                    assert_eq!(keys, dk);
                }
                (Record::FlushDb, Record::FlushDb) => {}
                _ => panic!("mismatch"),
            }
        }

        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
