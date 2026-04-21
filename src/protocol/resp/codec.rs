use crate::protocol::resp::Frame;
use tokio_util::{
    bytes::{Buf, Bytes, BytesMut},
    codec::{Decoder, Encoder},
};

pub struct RespCodec;

impl Encoder<Frame> for RespCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, frame: Frame, buf: &mut BytesMut) -> Result<(), Self::Error> {
        buf.extend_from_slice(&Bytes::from(&frame));
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = anyhow::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Frame::parse(buf) {
            Ok(Some((frame, consumed))) => {
                buf.advance(consumed);
                Ok(Some(frame))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::bytes::Bytes;

    #[test]
    fn round_trip_simple_string() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::new();
        let frame = Frame::SimpleString("PONG".into());
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn round_trip_array() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::new();
        let frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"hello")),
            Frame::Integer(42),
        ]);
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn decode_incomplete_returns_none() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from(&b"$5\r\nhel"[..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
