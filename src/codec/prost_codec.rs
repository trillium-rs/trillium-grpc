use crate::{Codec, Status};
use bytes::{Bytes, BytesMut};
use prost::Message;

/// The default protobuf codec, backed by [`prost`].
pub struct Prost;

impl<T> Codec<T> for Prost
where
    T: Message + Default + 'static,
{
    fn content_type_suffix() -> &'static str {
        "proto"
    }

    fn encode(value: &T) -> Result<Bytes, Status> {
        let mut buf = BytesMut::with_capacity(value.encoded_len());
        value
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("prost encode failed: {e}")))?;
        Ok(buf.freeze())
    }

    fn decode(bytes: &[u8]) -> Result<T, Status> {
        T::decode(bytes).map_err(|e| Status::invalid_argument(format!("prost decode failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use a built-in prost Message impl (Vec<u8>) to avoid needing prost-build.
    // For the protobuf wire format, a bytes field at tag 1 encodes to:
    //   tag (1<<3 | 2 = 0x0A) + varint(len) + payload
    // For an empty Vec<u8>, prost omits the field entirely.

    #[test]
    fn prost_codec_suffix() {
        assert_eq!(<Prost as Codec<Vec<u8>>>::content_type_suffix(), "proto");
    }

    #[test]
    fn prost_codec_encode_empty() {
        let value: Vec<u8> = Vec::new();
        let encoded = <Prost as Codec<Vec<u8>>>::encode(&value).unwrap();
        assert!(encoded.is_empty());
    }

    #[test]
    fn prost_codec_decode_empty() {
        let decoded: Vec<u8> = <Prost as Codec<Vec<u8>>>::decode(&[]).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn prost_codec_decode_garbage_is_invalid_argument() {
        // 0xFF is not a valid prost tag — high continuation bit, never terminates.
        let err = <Prost as Codec<Vec<u8>>>::decode(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap_err();
        assert_eq!(err.code, crate::Code::InvalidArgument);
    }
}
