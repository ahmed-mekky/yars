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
