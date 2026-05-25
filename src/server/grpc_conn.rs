//! [`GrpcServerConn`]: the per-call control surface handed to every service method.
//!
//! A `GrpcServerConn` *owns* the [`Conn`] for one RPC — value in, value out, exactly
//! like a trillium [`Handler`](trillium::Handler) owns its `Conn` (and like
//! `WebSocketConn` owns its `Upgrade`). On top of that `Conn` it exposes the
//! gRPC control interface: the request's initial metadata, mutable response
//! initial- and trailing-metadata bags, the request deadline, and — for the
//! shapes that read a request stream — a typed [`RequestStream`] via
//! [`requests`](GrpcServerConn::requests). A half-duplex gRPC method is, structurally,
//! a trillium `run()` handler: read the request body, set the response, hand
//! the `Conn` back to be flushed. That is why these shapes never touch
//! `Upgrade`.
//!
//! Response headers write straight through to the owned `Conn`'s response
//! headers, committed to the wire when the handler returns and the head is
//! flushed. Response trailers are different: trillium has no trailers field on
//! `Conn` — they're emitted dynamically by [`BodySource::trailers`] at
//! end-of-body — so they accumulate in a bag here that the framework hands to
//! the response body source. The handler never holds the `Conn` directly, so
//! there is no way to mutate headers after the head is flushed — the "exactly
//! one bite at headers" guarantee is structural.
//!
//! [`BodySource::trailers`]: trillium_http::BodySource::trailers
//!
//! `GrpcServerConn` is generic over the codec, defaulted to [`Prost`], so the common
//! signature is just `&mut GrpcServerConn`. The codec is what lets
//! [`requests`](GrpcServerConn::requests) decode the wire bytes into your message
//! type.

use crate::{
    Codec, Encoding, Prost, server::streaming::RequestStream, timeout::parse_grpc_timeout,
};
use std::{marker::PhantomData, time::Instant};
use trillium::{Conn, Headers};

/// The control surface for a single gRPC call. Owns the `Conn` for the
/// duration of one RPC — value in, value out.
pub struct GrpcServerConn<C = Prost> {
    conn: Conn,
    response_trailers: Headers,
    request_encoding: Encoding,
    deadline: Option<Instant>,
    codec: PhantomData<fn() -> C>,
}

impl<C> GrpcServerConn<C> {
    /// Take ownership of a `Conn` for the duration of one RPC.
    /// `request_encoding` is the already-validated inbound `grpc-encoding`,
    /// used to decompress request frames.
    pub(crate) fn new(conn: Conn, request_encoding: Encoding) -> Self {
        let deadline = conn
            .request_headers()
            .get_str("grpc-timeout")
            .and_then(parse_grpc_timeout)
            .map(|d| Instant::now() + d);
        Self {
            conn,
            response_trailers: Headers::new(),
            request_encoding,
            deadline,
            codec: PhantomData,
        }
    }

    /// The request's initial metadata (received headers), including any custom
    /// metadata the client attached.
    pub fn received_headers(&self) -> &Headers {
        self.conn.request_headers()
    }

    /// The response's initial metadata, written straight through to the
    /// `Conn`. Committed to the wire when the handler returns, before the first
    /// response byte.
    pub fn response_headers_mut(&mut self) -> &mut Headers {
        self.conn.response_headers_mut()
    }

    /// The response's trailing metadata, emitted alongside `grpc-status` after
    /// the response body.
    pub fn response_trailers_mut(&mut self) -> &mut Headers {
        &mut self.response_trailers
    }

    /// The deadline derived from the request's `grpc-timeout` header, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// A typed stream of decoded request messages.
    ///
    /// Borrows the connection for the lifetime of the returned stream; read it
    /// to EOF (or drop it) before touching response headers again. The codec's
    /// `decode` is selected by the `GrpcServerConn`'s codec parameter, so the only
    /// thing you supply is the message type:
    ///
    /// ```ignore
    /// let mut reqs = conn.requests::<HelloRequest>();
    /// while let Some(req) = reqs.recv().await? { /* … */ }
    /// ```
    pub fn requests<Req>(&mut self) -> RequestStream<'_, Req>
    where
        C: Codec<Req>,
        Req: 'static,
    {
        RequestStream::new(
            Box::pin(self.conn.request_body()),
            <C as Codec<Req>>::decode,
            self.request_encoding,
        )
    }

    /// Consume the wrapper, returning the owned `Conn` (with response headers
    /// already set on it) plus the accumulated response trailers for the
    /// framework to hand to the body source.
    pub(crate) fn into_parts(self) -> (Conn, Headers) {
        (self.conn, self.response_trailers)
    }
}
