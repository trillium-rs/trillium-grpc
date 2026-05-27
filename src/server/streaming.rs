//! Borrowed streaming primitives handed to service methods.
//!
//! [`RequestStream`] decodes inbound request messages from a boxed reader — the
//! request body during `run()` (via [`GrpcServerConn::requests`]), and continues
//! against the same retained body after a bidi upgrade. [`Channel`] is the
//! bidirectional read+write surface a bidi responder drives over the upgraded
//! transport.
//!
//! Codec is type-erased into `fn` pointers so these user-facing types carry no
//! codec parameter.
//!
//! [`GrpcServerConn::requests`]: crate::server::GrpcServerConn::requests

use crate::{
    Encoding, Status,
    encoding::DEFAULT_MAX_MESSAGE_SIZE,
    frame::{
        reader::{ReadState, poll_read_message},
        writer::encode_payload,
    },
};
use bytes::Bytes;
use futures_lite::{AsyncRead, AsyncWriteExt, Stream};
use std::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};
use trillium::{Headers, Upgrade};

/// A stream of decoded request messages.
///
/// Produced by [`GrpcServerConn::requests`](crate::server::GrpcServerConn::requests). Read
/// it with [`recv`](Self::recv) (or as a [`Stream`]); `recv` yields `Ok(None)`
/// on clean end-of-stream and `Err` on a decode or transport error.
pub struct RequestStream<'a, T> {
    reader: Pin<Box<dyn AsyncRead + Send + 'a>>,
    state: ReadState,
    decode: fn(&[u8]) -> Result<T, Status>,
    encoding: Encoding,
    max_message_size: usize,
}

impl<'a, T> RequestStream<'a, T> {
    pub(crate) fn new(
        reader: Pin<Box<dyn AsyncRead + Send + 'a>>,
        decode: fn(&[u8]) -> Result<T, Status>,
        encoding: Encoding,
    ) -> Self {
        Self {
            reader,
            state: ReadState::new(),
            decode,
            encoding,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    fn poll_message(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<T, Status>>> {
        poll_read_message(
            self.reader.as_mut(),
            &mut self.state,
            cx,
            self.decode,
            self.encoding,
            self.max_message_size,
        )
    }

    /// The next decoded request message: `Ok(None)` on clean end-of-stream,
    /// `Err` on a per-message decode error or transport failure.
    pub async fn recv(&mut self) -> Result<Option<T>, Status> {
        poll_fn(|cx| match self.poll_message(cx) {
            Poll::Ready(Some(Ok(t))) => Poll::Ready(Ok(Some(t))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        })
        .await
    }
}

impl<T: 'static> Stream for RequestStream<'_, T> {
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_message(cx)
    }
}

/// Bidirectional channel: read decoded requests, write framed responses, over
/// the upgraded transport.
///
/// Handed to a [`BidiResponder`](crate::BidiResponder). Turn-taking by
/// construction — both [`recv`](Self::recv) and [`send`](Self::send) take
/// `&mut self`, so the underlying `&mut Upgrade` is never aliased. For producers
/// that need to run concurrently with the request loop, spawn a task on the
/// runtime.
///
/// The response *initial* metadata was committed before the upgrade (by the
/// prologue), so there is no way to mutate it here. The *trailing* metadata,
/// emitted after the loop alongside `grpc-status`, is still open: write it
/// through [`response_trailers_mut`](Self::response_trailers_mut) (the bag was
/// seeded with whatever the prologue set).
pub struct Channel<'a, Req, Resp> {
    upgrade: &'a mut Upgrade,
    response_trailers: &'a mut Headers,
    state: ReadState,
    decode: fn(&[u8]) -> Result<Req, Status>,
    encode: fn(&Resp) -> Result<Bytes, Status>,
    inbound_encoding: Encoding,
    outbound_encoding: Encoding,
    max_message_size: usize,
}

impl<'a, Req, Resp> Channel<'a, Req, Resp> {
    pub(crate) fn new(
        upgrade: &'a mut Upgrade,
        response_trailers: &'a mut Headers,
        decode: fn(&[u8]) -> Result<Req, Status>,
        encode: fn(&Resp) -> Result<Bytes, Status>,
        inbound_encoding: Encoding,
        outbound_encoding: Encoding,
    ) -> Self {
        Self {
            upgrade,
            response_trailers,
            state: ReadState::new(),
            decode,
            encode,
            inbound_encoding,
            outbound_encoding,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// The response's trailing metadata, emitted alongside `grpc-status` once
    /// the loop ends. Seeded with whatever the prologue set; write to it to add
    /// trailing metadata (including `grpc-status-details-bin` error details)
    /// from inside the loop.
    pub fn response_trailers_mut(&mut self) -> &mut Headers {
        self.response_trailers
    }

    /// Read the next decoded request. `None` on clean EOF (client closed the
    /// request side); `Some(Err(_))` ends the read side.
    pub async fn recv(&mut self) -> Option<Result<Req, Status>> {
        let upgrade = &mut *self.upgrade;
        let state = &mut self.state;
        let decode = self.decode;
        let encoding = self.inbound_encoding;
        let max = self.max_message_size;
        poll_fn(|cx| poll_read_message(Pin::new(&mut *upgrade), state, cx, decode, encoding, max))
            .await
    }

    /// Frame and write one response message.
    pub async fn send(&mut self, value: Resp) -> Result<(), Status> {
        let payload = (self.encode)(&value)?;
        let frame = encode_payload(&payload, self.outbound_encoding)?;
        self.upgrade
            .write_all(&frame)
            .await
            .map_err(|e| Status::unavailable(format!("write error: {e}")))
    }
}
