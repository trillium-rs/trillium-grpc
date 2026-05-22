//! Decode a gRPC body into a stream of messages. See [`MessageStream`].

use crate::{Encoding, Status};
use futures_lite::{AsyncRead, Stream};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub use crate::encoding::DEFAULT_MAX_MESSAGE_SIZE;

/// gRPC wire framing: 5-byte prefix (1 byte compressed-flag, 4 bytes
/// big-endian length) followed by payload.
const PREFIX_LEN: usize = 5;

/// Stream of decoded messages over a length-prefixed gRPC body.
///
/// Wraps any `AsyncRead` (request body or response body) and yields decoded
/// messages until the underlying reader signals EOF cleanly between frames.
/// EOF mid-frame produces an error item and ends the stream.
///
/// When the per-message Compressed-Flag is set, the payload is run through
/// the [`Encoding`] configured via [`with_encoding`](Self::with_encoding).
/// `Identity` (the default) rejects compressed frames with `Internal` —
/// the peer claimed compression after we advertised none.
pub struct MessageStream<T, R> {
    reader: R,
    state: ReadState,
    max_message_size: usize,
    encoding: Encoding,
    decode: fn(&[u8]) -> Result<T, Status>,
}

pub(crate) enum ReadState {
    ReadingPrefix {
        buf: [u8; PREFIX_LEN],
        filled: usize,
    },
    ReadingPayload {
        compressed: bool,
        payload: Vec<u8>,
        filled: usize,
    },
    Done,
}

impl ReadState {
    pub(crate) fn new() -> Self {
        Self::ReadingPrefix {
            buf: [0u8; PREFIX_LEN],
            filled: 0,
        }
    }
}

impl<T, R> MessageStream<T, R> {
    /// Wrap `reader` (a gRPC body), decoding each frame's payload with
    /// `decode`. Defaults to `Identity` encoding and the
    /// [`DEFAULT_MAX_MESSAGE_SIZE`] cap; adjust with
    /// [`with_encoding`](Self::with_encoding) and
    /// [`with_max_message_size`](Self::with_max_message_size).
    pub fn new(reader: R, decode: fn(&[u8]) -> Result<T, Status>) -> Self {
        Self {
            reader,
            state: ReadState::new(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            encoding: Encoding::Identity,
            decode,
        }
    }

    /// Reject any single message whose framed length (or decompressed size)
    /// exceeds `max` bytes, with `ResourceExhausted`.
    pub fn with_max_message_size(mut self, max: usize) -> Self {
        self.max_message_size = max;
        self
    }

    /// Decompress payloads with the per-message Compressed-Flag set using
    /// this encoding. Compressed frames received without an encoding
    /// configured (the default `Identity`) are rejected.
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }
}

impl<T, R> Stream for MessageStream<T, R>
where
    R: AsyncRead + Unpin,
    T: 'static,
{
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_read_message(
            Pin::new(&mut this.reader),
            &mut this.state,
            cx,
            this.decode,
            this.encoding,
            this.max_message_size,
        )
    }
}

/// Drive one step of the message-read state machine on an `AsyncRead`.
///
/// Returns `Poll::Ready(None)` for a clean EOF between frames, `Poll::Ready(Some(Err))`
/// for a per-message error (state transitions to `Done` afterwards), and
/// `Poll::Pending` when the reader has no more bytes ready.
///
/// The same `state` value must be passed in across polls so partial prefix
/// and partial payload reads can resume. Once the state reaches `Done`,
/// further calls return `Poll::Ready(None)`.
pub(crate) fn poll_read_message<T, R>(
    mut reader: Pin<&mut R>,
    state: &mut ReadState,
    cx: &mut Context<'_>,
    decode: fn(&[u8]) -> Result<T, Status>,
    encoding: Encoding,
    max_message_size: usize,
) -> Poll<Option<Result<T, Status>>>
where
    R: AsyncRead + ?Sized,
{
    loop {
        match state {
            ReadState::Done => return Poll::Ready(None),

            ReadState::ReadingPrefix { buf, filled } => {
                while *filled < PREFIX_LEN {
                    let dst = &mut buf[*filled..];
                    match reader.as_mut().poll_read(cx, dst) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => {
                            *state = ReadState::Done;
                            return Poll::Ready(Some(Err(Status::unavailable(format!(
                                "read error: {e}"
                            )))));
                        }
                        Poll::Ready(Ok(0)) => {
                            if *filled == 0 {
                                *state = ReadState::Done;
                                return Poll::Ready(None);
                            } else {
                                *state = ReadState::Done;
                                return Poll::Ready(Some(Err(Status::internal(
                                    "unexpected EOF in frame prefix",
                                ))));
                            }
                        }
                        Poll::Ready(Ok(n)) => *filled += n,
                    }
                }

                let compressed = buf[0] != 0;
                let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

                if len > max_message_size {
                    *state = ReadState::Done;
                    return Poll::Ready(Some(Err(Status::resource_exhausted(format!(
                        "received message of {len} bytes exceeds limit of {max_message_size}"
                    )))));
                }

                *state = ReadState::ReadingPayload {
                    compressed,
                    payload: vec![0u8; len],
                    filled: 0,
                };
            }

            ReadState::ReadingPayload {
                compressed,
                payload,
                filled,
            } => {
                while *filled < payload.len() {
                    let dst = &mut payload[*filled..];
                    match reader.as_mut().poll_read(cx, dst) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => {
                            *state = ReadState::Done;
                            return Poll::Ready(Some(Err(Status::unavailable(format!(
                                "read error: {e}"
                            )))));
                        }
                        Poll::Ready(Ok(0)) => {
                            *state = ReadState::Done;
                            return Poll::Ready(Some(Err(Status::internal(
                                "unexpected EOF in frame payload",
                            ))));
                        }
                        Poll::Ready(Ok(n)) => *filled += n,
                    }
                }

                let compressed = *compressed;
                let payload = std::mem::take(payload);
                *state = ReadState::new();

                let bytes = if compressed {
                    if matches!(encoding, Encoding::Identity) {
                        return Poll::Ready(Some(Err(Status::internal(
                            "received compressed message but no encoding negotiated",
                        ))));
                    }
                    match encoding.decompress(&payload, max_message_size) {
                        Ok(b) => b,
                        Err(status) => return Poll::Ready(Some(Err(status))),
                    }
                } else {
                    payload
                };

                return Poll::Ready(Some(decode(&bytes)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Code, Codec, codec::Prost};
    use futures_lite::{StreamExt, future::block_on};

    /// Helper: build a single framed message: [compressed=0, len BE u32, payload].
    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
        out.push(0); // compressed flag
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn vec_decode(bytes: &[u8]) -> Result<Vec<u8>, Status> {
        <Prost as Codec<Vec<u8>>>::decode(bytes)
    }

    type Stream<'a> = MessageStream<Vec<u8>, &'a [u8]>;

    fn new_stream(bytes: &[u8]) -> Stream<'_> {
        MessageStream::new(bytes, vec_decode)
    }

    #[test]
    fn empty_input_yields_none() {
        let bytes: &[u8] = &[];
        let mut s = new_stream(bytes);
        assert!(block_on(s.next()).is_none());
    }

    #[test]
    fn single_empty_message() {
        let body = frame(&[]);
        let mut s = new_stream(&body[..]);
        // Vec<u8> as a prost Message decodes from empty bytes to empty Vec
        let msg = block_on(s.next()).unwrap().unwrap();
        assert!(msg.is_empty());
        assert!(block_on(s.next()).is_none());
    }

    #[test]
    fn multiple_messages() {
        // Vec<u8> as a prost top-level Message: a `bytes` field at tag 1.
        // Encoding: tag byte 0x0A, then varint-len, then payload.
        // For payload b"hi": [0x0A, 0x02, b'h', b'i']
        let mut body = Vec::new();
        body.extend_from_slice(&frame(&[0x0A, 0x02, b'h', b'i']));
        body.extend_from_slice(&frame(&[0x0A, 0x03, b'b', b'y', b'e']));

        let mut s = new_stream(&body[..]);
        let m1 = block_on(s.next()).unwrap().unwrap();
        let m2 = block_on(s.next()).unwrap().unwrap();
        assert_eq!(m1, b"hi");
        assert_eq!(m2, b"bye");
        assert!(block_on(s.next()).is_none());
    }

    #[test]
    fn partial_prefix_at_eof_is_error() {
        let body = [0u8, 0u8, 0u8]; // 3 of 5 prefix bytes
        let mut s = new_stream(&body[..]);
        let err = block_on(s.next()).unwrap().unwrap_err();
        assert_eq!(err.code, Code::Internal);
        assert!(block_on(s.next()).is_none());
    }

    #[test]
    fn partial_payload_at_eof_is_error() {
        let mut body = Vec::new();
        body.push(0); // compressed
        body.extend_from_slice(&10u32.to_be_bytes()); // claim 10 bytes
        body.extend_from_slice(&[1, 2, 3]); // only deliver 3
        let mut s = new_stream(&body[..]);
        let err = block_on(s.next()).unwrap().unwrap_err();
        assert_eq!(err.code, Code::Internal);
    }

    #[test]
    fn oversized_message_is_resource_exhausted() {
        let mut body = Vec::new();
        body.push(0);
        body.extend_from_slice(&100u32.to_be_bytes());
        let mut s = new_stream(&body[..]).with_max_message_size(50);
        let err = block_on(s.next()).unwrap().unwrap_err();
        assert_eq!(err.code, Code::ResourceExhausted);
    }

    #[test]
    fn compressed_flag_with_identity_encoding_is_internal() {
        // Peer set the Compressed-Flag but we negotiated no encoding —
        // protocol error.
        let mut body = Vec::new();
        body.push(1); // compressed
        body.extend_from_slice(&0u32.to_be_bytes());
        let mut s = new_stream(&body[..]);
        let err = block_on(s.next()).unwrap().unwrap_err();
        assert_eq!(err.code, Code::Internal);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn compressed_frame_decompressed_with_gzip() {
        // Frame body: gzip-compressed prost-encoded `Vec<u8>` of b"hi".
        let inner = [0x0Au8, 0x02, b'h', b'i'];
        let compressed = Encoding::Gzip.compress(&inner).unwrap();

        let mut body = Vec::new();
        body.push(1); // compressed flag
        body.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        body.extend_from_slice(&compressed);

        let mut s = new_stream(&body[..]).with_encoding(Encoding::Gzip);
        let msg = block_on(s.next()).unwrap().unwrap();
        assert_eq!(msg, b"hi");
    }

    #[test]
    fn codec_decode_failure_propagates_invalid_argument() {
        // 0xFF is an invalid prost tag (non-terminating varint).
        let body = frame(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let mut s = new_stream(&body[..]);
        let err = block_on(s.next()).unwrap().unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
    }
}
