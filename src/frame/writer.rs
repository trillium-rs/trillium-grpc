use crate::{Codec, Encoding, Status};
use futures_lite::{AsyncRead, Stream};
use pin_project_lite::pin_project;
use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use trillium::{BodySource, Headers};

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
    let payload = C::encode(value)?;
    let (flag, payload) = match encoding {
        Encoding::Identity => (0u8, payload.to_vec()),
        #[cfg(any(feature = "gzip", feature = "deflate", feature = "zstd"))]
        _ => (1u8, encoding.compress(&payload)?),
    };
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.push(flag);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pin_project! {
    /// `BodySource` adapter that turns a `Stream<Item = Result<T, Status>>`
    /// into a framed gRPC response body. On stream completion (or first error)
    /// emits `grpc-status` trailers via [`BodySource::trailers`].
    ///
    /// - Stream yields `None` cleanly → trailers carry `grpc-status: 0`
    /// - Stream yields `Some(Err(status))` → trailers carry that status; the
    ///   stream is dropped without further polling
    /// - Codec encode failure → trailers carry that error status
    ///
    /// Each yielded message is framed with the configured [`Encoding`]
    /// (default `Identity`); use [`with_encoding`](Self::with_encoding) to
    /// compress.
    pub struct StreamBody<C, T, S> {
        #[pin]
        stream: S,
        state: WriteState,
        trailers: Option<Headers>,
        encoding: Encoding,
        _marker: PhantomData<fn() -> (C, T)>,
    }
}

enum WriteState {
    Polling,
    WritingFrame { buf: Vec<u8>, written: usize },
    Done,
}

impl<C, T, S> StreamBody<C, T, S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            state: WriteState::Polling,
            trailers: None,
            encoding: Encoding::Identity,
            _marker: PhantomData,
        }
    }

    /// Compress every framed message with this encoding. The peer reads
    /// the codec from `grpc-encoding` on the response (server side) or
    /// request (client side) — this writer assumes that header has already
    /// been set on the conn.
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }
}

impl<C, T, S> AsyncRead for StreamBody<C, T, S>
where
    C: Codec<T>,
    T: Send + 'static,
    S: Stream<Item = Result<T, Status>>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        loop {
            match this.state {
                WriteState::Done => return Poll::Ready(Ok(0)),

                WriteState::WritingFrame { buf, written } => {
                    if *written < buf.len() {
                        let n = std::cmp::min(dst.len(), buf.len() - *written);
                        if n == 0 {
                            // dst was zero-length; can't make progress without spinning
                            return Poll::Ready(Ok(0));
                        }
                        dst[..n].copy_from_slice(&buf[*written..*written + n]);
                        *written += n;
                        return Poll::Ready(Ok(n));
                    }
                    *this.state = WriteState::Polling;
                }

                WriteState::Polling => match this.stream.as_mut().poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        *this.trailers = Some(Status::ok().into_trailers());
                        *this.state = WriteState::Done;
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Some(Err(status))) => {
                        *this.trailers = Some(status.into_trailers());
                        *this.state = WriteState::Done;
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Some(Ok(value))) => {
                        match encode_frame::<C, T>(&value, *this.encoding) {
                            Ok(buf) => {
                                *this.state = WriteState::WritingFrame { buf, written: 0 };
                            }
                            Err(status) => {
                                *this.trailers = Some(status.into_trailers());
                                *this.state = WriteState::Done;
                                return Poll::Ready(Ok(0));
                            }
                        }
                    }
                },
            }
        }
    }
}

impl<C, T, S> BodySource for StreamBody<C, T, S>
where
    C: Codec<T>,
    T: Send + 'static,
    S: Stream<Item = Result<T, Status>> + Send + 'static,
{
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        self.project().trailers.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Code, codec::Prost};
    use futures_lite::{AsyncReadExt, future::block_on, stream};

    type Body<S> = StreamBody<Prost, Vec<u8>, S>;

    /// Drive the BodySource to completion: read all bytes, then collect trailers.
    fn drain<S>(body: Body<S>) -> (Vec<u8>, Headers)
    where
        S: Stream<Item = Result<Vec<u8>, Status>> + Send + Unpin + 'static,
    {
        block_on(async move {
            let mut body = Box::pin(body);
            let mut bytes = Vec::new();
            body.read_to_end(&mut bytes).await.unwrap();
            let trailers = body.as_mut().trailers().unwrap_or_default();
            (bytes, trailers)
        })
    }

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

    #[test]
    fn empty_stream_emits_only_ok_trailers() {
        let s = stream::iter(Vec::<Result<Vec<u8>, Status>>::new());
        let (bytes, trailers) = drain(StreamBody::new(s));
        assert!(bytes.is_empty());
        assert_eq!(trailers.get_str("grpc-status"), Some("0"));
    }

    #[test]
    fn single_message_then_ok_trailers() {
        let s = stream::iter(vec![Ok::<_, Status>(b"hi".to_vec())]);
        let (bytes, trailers) = drain(StreamBody::new(s));
        assert_eq!(bytes, frame(&[0x0A, 0x02, b'h', b'i']));
        assert_eq!(trailers.get_str("grpc-status"), Some("0"));
    }

    #[test]
    fn multi_message_then_ok_trailers() {
        let s = stream::iter(vec![
            Ok::<_, Status>(b"a".to_vec()),
            Ok::<_, Status>(b"bc".to_vec()),
        ]);
        let (bytes, trailers) = drain(StreamBody::new(s));
        let mut expected = Vec::new();
        expected.extend_from_slice(&frame(&[0x0A, 0x01, b'a']));
        expected.extend_from_slice(&frame(&[0x0A, 0x02, b'b', b'c']));
        assert_eq!(bytes, expected);
        assert_eq!(trailers.get_str("grpc-status"), Some("0"));
    }

    #[test]
    fn error_mid_stream_yields_error_trailers() {
        let s = stream::iter(vec![
            Ok::<_, Status>(b"a".to_vec()),
            Err(Status::not_found("gone")),
            Ok(b"never written".to_vec()),
        ]);
        let (bytes, trailers) = drain(StreamBody::new(s));
        // First message was framed; the error stops further messages.
        assert_eq!(bytes, frame(&[0x0A, 0x01, b'a']));
        assert_eq!(
            trailers.get_str("grpc-status"),
            Some(&*(Code::NotFound as u8).to_string())
        );
        assert_eq!(trailers.get_str("grpc-message"), Some("gone"));
    }

    #[test]
    fn error_only_stream_yields_only_error_trailers() {
        let s = stream::iter(vec![Err::<Vec<u8>, _>(Status::permission_denied(
            "no",
        ))]);
        let (bytes, trailers) = drain(StreamBody::new(s));
        assert!(bytes.is_empty());
        assert_eq!(
            trailers.get_str("grpc-status"),
            Some(&*(Code::PermissionDenied as u8).to_string())
        );
        assert_eq!(trailers.get_str("grpc-message"), Some("no"));
    }
}
