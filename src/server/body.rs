//! Outbound [`BodySource`] implementations for the half-duplex response shapes.
//!
//! These turn a handler's return value into a normal trillium response body
//! that carries `grpc-status` (plus any trailing metadata) in HTTP trailers —
//! no `Upgrade` involved. [`OneShotBody`] is the whole response known up front
//! (unary, client-streaming); [`StreamBody`] pulls a user stream lazily
//! (server-streaming).

use crate::{Encoding, Status, frame::writer::encode_payload};
use bytes::Bytes;
use futures_lite::{AsyncRead, Stream};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use trillium::Headers;
use trillium_http::BodySource;

/// A cancellation signal handed to [`StreamBody`]: a future that resolves to a
/// terminal [`Status`] (e.g. `cancelled` on shutdown, `deadline_exceeded` on a
/// `grpc-timeout`) and is otherwise `Pending` forever.
pub(crate) type CancelSignal = Pin<Box<dyn Future<Output = Status> + Send>>;

/// A fully-buffered response body plus its trailers. Used where the entire
/// response is known when the handler returns: zero or one framed message
/// (unary / client-streaming), followed by the prepared `grpc-status` trailers.
pub(crate) struct OneShotBody {
    bytes: Vec<u8>,
    pos: usize,
    trailers: Option<Headers>,
}

impl OneShotBody {
    /// `bytes` is the already-framed body (one length-prefixed message, or
    /// empty for an error/no-message response); `trailers` is the complete
    /// trailer block including `grpc-status`.
    pub(crate) fn new(bytes: Vec<u8>, trailers: Headers) -> Self {
        Self {
            bytes,
            pos: 0,
            trailers: Some(trailers),
        }
    }
}

impl AsyncRead for OneShotBody {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let remaining = &this.bytes[this.pos..];
        let n = remaining.len().min(buf.len());
        buf[..n].copy_from_slice(&remaining[..n]);
        this.pos += n;
        Poll::Ready(Ok(n))
    }
}

impl BodySource for OneShotBody {
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        self.get_mut().trailers.take()
    }
}

pin_project! {
    /// A lazily-pulled server-streaming response body: each item from the
    /// user's stream is encoded into a gRPC frame on demand. Trailers are
    /// derived from how the stream ended — clean end → `grpc-status: 0`; an
    /// `Err(Status)` item or an encode failure → that status — merged onto the
    /// trailing metadata the handler set before returning the stream.
    pub(crate) struct StreamBody<Resp, S> {
        #[pin]
        stream: S,
        encode: fn(&Resp) -> Result<Bytes, Status>,
        encoding: Encoding,
        base_trailers: Headers,
        cancel: Option<CancelSignal>,
        pending: Vec<u8>,
        pos: usize,
        status: Option<Status>,
        finished: bool,
    }
}

impl<Resp, S> StreamBody<Resp, S> {
    pub(crate) fn new(
        stream: S,
        encode: fn(&Resp) -> Result<Bytes, Status>,
        encoding: Encoding,
        base_trailers: Headers,
        cancel: Option<CancelSignal>,
    ) -> Self {
        Self {
            stream,
            encode,
            encoding,
            base_trailers,
            cancel,
            pending: Vec::new(),
            pos: 0,
            status: None,
            finished: false,
        }
    }
}

impl<Resp, S> AsyncRead for StreamBody<Resp, S>
where
    S: Stream<Item = Result<Resp, Status>>,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        loop {
            if *this.pos < this.pending.len() {
                let remaining = &this.pending[*this.pos..];
                let n = remaining.len().min(buf.len());
                buf[..n].copy_from_slice(&remaining[..n]);
                *this.pos += n;
                return Poll::Ready(Ok(n));
            }
            if *this.finished {
                return Poll::Ready(Ok(0));
            }
            // Cancellation (shutdown / deadline) cuts the stream between frames.
            if let Some(cancel) = this.cancel.as_mut()
                && let Poll::Ready(status) = cancel.as_mut().poll(cx)
            {
                *this.finished = true;
                *this.status = Some(status);
                return Poll::Ready(Ok(0));
            }
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    *this.finished = true;
                    if this.status.is_none() {
                        *this.status = Some(Status::ok());
                    }
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Some(Ok(resp))) => {
                    match (*this.encode)(&resp)
                        .and_then(|payload| encode_payload(&payload, *this.encoding))
                    {
                        Ok(frame) => {
                            *this.pending = frame;
                            *this.pos = 0;
                        }
                        Err(status) => {
                            *this.finished = true;
                            *this.status = Some(status);
                            return Poll::Ready(Ok(0));
                        }
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    *this.finished = true;
                    *this.status = Some(status);
                    return Poll::Ready(Ok(0));
                }
            }
        }
    }
}

impl<Resp, S> BodySource for StreamBody<Resp, S>
where
    S: Stream<Item = Result<Resp, Status>> + Send + 'static,
    Resp: Send + 'static,
{
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        let this = self.project();
        let mut trailers = std::mem::take(this.base_trailers);
        this.status
            .take()
            .unwrap_or_else(Status::ok)
            .write_into(&mut trailers);
        Some(trailers)
    }
}
