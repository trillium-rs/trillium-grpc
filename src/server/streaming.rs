//! Borrowed streaming primitives handed to user closures by the streaming
//! dispatch shapes.
//!
//! All three borrow `&'a mut Upgrade` from the dispatch frame. The
//! framework retains ownership of the upgrade across the user's await
//! chain; on closure return it writes the terminating `grpc-status`
//! trailers based on the user's `Result`.
//!
//! Codec is type-erased into `fn` pointers so user-facing types don't carry
//! a codec parameter.

use crate::{
    Encoding, Status,
    encoding::DEFAULT_MAX_MESSAGE_SIZE,
    frame::{
        reader::{ReadState, poll_read_message},
        writer::encode_payload,
    },
};
use bytes::Bytes;
use futures_lite::{AsyncWriteExt, Stream};
use std::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};
use trillium::Upgrade;

/// Stream of decoded request messages over the inbound side of an upgrade.
///
/// Yielded by client-streaming dispatch. Implements
/// [`futures_lite::Stream`]; `.next()` returns the next decoded message,
/// `None` on clean EOF, or `Some(Err(_))` for a per-message error (after
/// which the stream ends).
pub struct RequestStream<'a, T> {
    upgrade: &'a mut Upgrade,
    state: ReadState,
    decode: fn(&[u8]) -> Result<T, Status>,
    encoding: Encoding,
    max_message_size: usize,
}

impl<'a, T> RequestStream<'a, T> {
    pub(crate) fn new(
        upgrade: &'a mut Upgrade,
        decode: fn(&[u8]) -> Result<T, Status>,
        encoding: Encoding,
    ) -> Self {
        Self {
            upgrade,
            state: ReadState::new(),
            decode,
            encoding,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

impl<T: 'static> Stream for RequestStream<'_, T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_read_message(
            Pin::new(&mut *this.upgrade),
            &mut this.state,
            cx,
            this.decode,
            this.encoding,
            this.max_message_size,
        )
    }
}

/// Sink for response messages over the outbound side of an upgrade.
///
/// Handed to server-streaming dispatch. Each [`send`](Self::send) frames
/// the value with the negotiated outbound encoding and writes it.
pub struct ResponseSink<'a, T> {
    upgrade: &'a mut Upgrade,
    encode: fn(&T) -> Result<Bytes, Status>,
    encoding: Encoding,
}

impl<'a, T> ResponseSink<'a, T> {
    pub(crate) fn new(
        upgrade: &'a mut Upgrade,
        encode: fn(&T) -> Result<Bytes, Status>,
        encoding: Encoding,
    ) -> Self {
        Self {
            upgrade,
            encode,
            encoding,
        }
    }

    /// Frame and write one response message. Errors are surfaced as
    /// `Status::unavailable` for transport failures or whatever the codec
    /// returns for encode failures.
    pub async fn send(&mut self, value: T) -> Result<(), Status> {
        let payload = (self.encode)(&value)?;
        let frame = encode_payload(&payload, self.encoding)?;
        self.upgrade
            .write_all(&frame)
            .await
            .map_err(|e| Status::unavailable(format!("write error: {e}")))
    }
}

/// Bidirectional channel: read decoded requests, write framed responses.
///
/// Handed to bidi dispatch. Turn-taking by construction — both
/// [`recv`](Self::recv) and [`send`](Self::send) take `&mut self`, so the
/// underlying `&mut Upgrade` is never aliased. For producers that need to
/// run concurrently with the request loop, spawn a task on the runtime.
pub struct Channel<'a, Req, Resp> {
    upgrade: &'a mut Upgrade,
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
        decode: fn(&[u8]) -> Result<Req, Status>,
        encode: fn(&Resp) -> Result<Bytes, Status>,
        inbound_encoding: Encoding,
        outbound_encoding: Encoding,
    ) -> Self {
        Self {
            upgrade,
            state: ReadState::new(),
            decode,
            encode,
            inbound_encoding,
            outbound_encoding,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Read the next decoded request. `None` on clean EOF (client closed
    /// the request side); `Some(Err(_))` ends the read side and further
    /// calls return `None`.
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
