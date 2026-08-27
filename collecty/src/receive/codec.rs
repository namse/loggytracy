use bytes::{Buf, Bytes};
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

#[derive(Default, Clone, Copy)]
pub struct PassthroughCodec;

impl Codec for PassthroughCodec {
    type Encode = ();
    type Decode = Bytes;
    type Encoder = EmptyEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> EmptyEncoder {
        EmptyEncoder
    }

    fn decoder(&mut self) -> RawDecoder {
        RawDecoder
    }
}

pub struct EmptyEncoder;

impl Encoder for EmptyEncoder {
    type Item = ();
    type Error = Status;

    fn encode(&mut self, _item: (), _dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        Ok(())
    }
}

pub struct RawDecoder;

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}
