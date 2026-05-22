//! Encode a message into a gRPC frame. See [`encode_frame`].

use std::borrow::Cow;

use crate::{Codec, Encoding, Status};

/// gRPC wire framing: 5-byte prefix (1 byte compressed-flag, 4 bytes
/// big-endian length) followed by payload.
const PREFIX_LEN: usize = 5;

/// Encode one message as a framed gRPC wire-format buffer:
/// `[compressed=flag][len: u32 BE][payload]`.
///
/// `encoding == Identity` writes a flag-0 frame with the bare codec output.
/// Anything else compresses the codec output with the given codec and
/// writes a flag-1 frame.
pub fn encode_frame<C, T>(value: &T, encoding: Encoding) -> Result<Vec<u8>, Status>
where
    C: Codec<T>,
{
    encode_payload(&C::encode(value)?, encoding)
}

/// Codec-agnostic variant: wrap already-encoded bytes in a gRPC frame,
/// compressing if `encoding` is non-Identity. Used by ResponseSink / Channel
/// after the codec encode fn pointer has produced the payload.
pub fn encode_payload(payload: &[u8], encoding: Encoding) -> Result<Vec<u8>, Status> {
    let (flag, payload): (u8, Cow<'_, [u8]>) = match encoding {
        Encoding::Identity => (0, Cow::Borrowed(payload)),
        #[cfg(any(feature = "gzip", feature = "deflate", feature = "zstd"))]
        _ => (1, Cow::Owned(encoding.compress(payload)?)),
    };
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.push(flag);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::Prost;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
        out.push(0);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn encode_single_frame_identity() {
        let buf = encode_frame::<Prost, Vec<u8>>(&b"hi".to_vec(), Encoding::Identity).unwrap();
        // Vec<u8> as a top-level prost Message is bytes-tagged: tag 0x0A, len 2, "hi"
        assert_eq!(buf, frame(&[0x0A, 0x02, b'h', b'i']));
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn encode_single_frame_gzip_sets_compressed_flag() {
        let buf = encode_frame::<Prost, Vec<u8>>(&b"hi".to_vec(), Encoding::Gzip).unwrap();
        assert_eq!(buf[0], 1, "compressed flag");
        // Round-trip the payload to confirm it's actually gzip-compressed.
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        let payload = &buf[PREFIX_LEN..PREFIX_LEN + len];
        let decoded = Encoding::Gzip.decompress(payload, 1024).unwrap();
        assert_eq!(decoded, [0x0A, 0x02, b'h', b'i']);
    }
}
