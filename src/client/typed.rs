//! Typed, shape-specific call handles built on the [`GrpcClientConn`] engine.
//!
//! A generated `<Service>Client` method returns one of these rather than the
//! engine directly, so the handle you hold exposes only the operations that make
//! sense for that RPC's shape — you never need to know the proto to use it
//! correctly. Each mirrors `trillium_client::Conn`'s lifecycle: build it with
//! chainable `with_*` setters, drive it (`.await` and/or `.next()`), then read
//! the response metadata off the same value. Like a trillium `Conn`, the readers
//! behave like an `Option` that execution populates — they return `None` until
//! the call has run.
//!
//! - [`UnaryConn`] — unary and client-streaming (one response). `.await` runs the
//!   whole call; read it with [`UnaryConn::into_message`] / [`message`].
//! - [`StreamingConn`] — server-streaming. A [`Stream`] of responses; `.await` is
//!   an optional opt-in to read the head early for initial metadata.
//! - [`BidiConn`] — full-duplex bidi. Live [`send`] / [`close_send`] / [`next`].
//!
//! [`message`]: UnaryConn::message
//! [`send`]: BidiConn::send
//! [`close_send`]: BidiConn::close_send
//! [`next`]: futures_lite::StreamExt::next

use crate::{
    Codec, Metadata, Status,
    client::conn::{CancelHandle, GrpcClientConn},
    timeout::parse_grpc_timeout,
};
use futures_lite::{Stream, StreamExt};
use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use trillium::Headers;
use trillium_client::Client;

type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Read the client's configured default `grpc-timeout` into a per-call deadline.
fn default_timeout(client: &Client) -> Option<Duration> {
    client
        .default_headers()
        .get_str("grpc-timeout")
        .and_then(parse_grpc_timeout)
}

/// A unary or client-streaming call: one request (or a stream of requests) in,
/// exactly one response out.
///
/// Build it with the chainable `with_*` setters, then `.await` it — awaiting
/// sends the request(s), reads the single response, drains the stream to its
/// trailers, and enforces the one-response cardinality. Transport-level failures
/// (connection, non-200 head) surface as the `await`'s `Err`; the RPC's logical
/// `grpc-status` is read afterward via [`status`](Self::status) /
/// [`into_message`](Self::into_message), so response metadata stays readable even
/// on a logical error.
pub struct UnaryConn<Req, Resp> {
    engine: GrpcClientConn<Req, Resp>,
    /// The request source, drained when the call fires. `None` once awaited.
    requests: Option<BoxStream<Req>>,
    /// `(message, verdict)`, populated by `.await`. `None` until then.
    outcome: Option<(Option<Resp>, Result<(), Status>)>,
}

impl<Req, Resp> UnaryConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// A unary call sending the single `request`.
    pub fn unary<C>(client: &Client, path: &str, request: Req) -> Self
    where
        C: Codec<Req> + Codec<Resp>,
    {
        let mut engine = GrpcClientConn::new::<C>(
            client,
            path,
            Metadata::new(),
            default_timeout(client),
            false,
        );
        engine.buffer_request(request);
        Self {
            engine,
            requests: None,
            outcome: None,
        }
    }

    /// A client-streaming call sending each message from `requests`.
    pub fn client_streaming<C>(
        client: &Client,
        path: &str,
        requests: impl Stream<Item = Req> + Send + 'static,
    ) -> Self
    where
        C: Codec<Req> + Codec<Resp>,
    {
        let engine = GrpcClientConn::new::<C>(
            client,
            path,
            Metadata::new(),
            default_timeout(client),
            false,
        );
        Self {
            engine,
            requests: Some(Box::pin(requests)),
            outcome: None,
        }
    }

    /// Attach an ASCII request-metadata entry. Chainable; takes effect when the
    /// call fires.
    pub fn with_ascii_metadata(mut self, key: &str, value: &str) -> Self {
        self.engine.add_ascii_metadata(key, value);
        self
    }

    /// Attach a binary (`-bin`) request-metadata entry. Chainable.
    pub fn with_binary_metadata(mut self, key: &str, value: impl Into<Vec<u8>>) -> Self {
        self.engine.add_binary_metadata(key, value.into());
        self
    }

    /// Set this call's deadline, `timeout` from now. Chainable.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.engine.set_deadline_from_now(timeout);
        self
    }

    /// A handle that cancels this call from elsewhere (including before/while it
    /// is awaited).
    pub fn cancel_handle(&self) -> CancelHandle {
        self.engine.cancel_handle()
    }

    /// The response's initial metadata, once the call has run. `None` before.
    pub fn metadata(&self) -> Option<&Headers> {
        self.engine.headers()
    }

    /// The response's trailing metadata, once the call has run. `None` before.
    pub fn trailers(&self) -> Option<&Headers> {
        self.engine.trailers()
    }

    /// The response message, if the call ran and yielded one. `None` before the
    /// call, and on the error path.
    pub fn message(&self) -> Option<&Resp> {
        self.outcome.as_ref().and_then(|(m, _)| m.as_ref())
    }

    /// The RPC's logical verdict, once the call has run: `Ok(())` on success, the
    /// server's `grpc-status` (or a cardinality violation) as `Err`. `Ok(())`
    /// before the call has run.
    pub fn status(&self) -> Result<(), Status> {
        self.outcome
            .as_ref()
            .map(|(_, s)| s.clone())
            .unwrap_or(Ok(()))
    }

    /// Consume the conn and yield the response message, folding the logical
    /// verdict: `Err` on a `grpc-status` error (or cardinality violation), else
    /// the single message. Read [`metadata`](Self::metadata) /
    /// [`trailers`](Self::trailers) first if you need them on the error path.
    pub fn into_message(self) -> Result<Resp, Status> {
        match self.outcome {
            Some((message, status)) => {
                status?;
                message.ok_or_else(|| Status::internal("unary response had no message"))
            }
            None => Err(Status::internal("call has not been awaited")),
        }
    }
}

impl<Req, Resp> IntoFuture for UnaryConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    type Output = Result<UnaryConn<Req, Resp>, Status>;
    type IntoFuture = BoxFuture<Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut engine = self.engine;
            if let Some(mut requests) = self.requests.take() {
                while let Some(message) = requests.next().await {
                    engine.send(message).await?;
                }
            }
            // Fires the request (body + END_STREAM) and awaits the head; a
            // transport / non-200 / non-gRPC head fails here, before any verdict.
            engine.close_send().await?;
            let outcome = read_unary(&mut engine).await;
            Ok(UnaryConn {
                engine,
                requests: None,
                outcome: Some(outcome),
            })
        })
    }
}

/// Read exactly one response message and confirm the stream ends cleanly. The
/// second `recv` is load-bearing: `grpc-status` rides the trailing frame *after*
/// the message, so it drives the body to EOF (populating the trailers), reads the
/// verdict, and detects a cardinality-violating second message.
async fn read_unary<Req, Resp>(
    engine: &mut GrpcClientConn<Req, Resp>,
) -> (Option<Resp>, Result<(), Status>)
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    match engine.recv().await {
        Ok(Some(message)) => match engine.recv().await {
            Ok(None) => (Some(message), Ok(())),
            Ok(Some(_)) => (
                None,
                Err(Status::internal("unary response had multiple messages")),
            ),
            // A trailing error after a message: keep the message, report the error.
            Err(status) => (Some(message), Err(status)),
        },
        // Clean EOF with no message is malformed for a unary response.
        Ok(None) => (None, Err(Status::internal("unary response had no message"))),
        // A trailers-only / logical error (no message). Metadata stays readable.
        Err(status) => (None, Err(status)),
    }
}

/// A server-streaming call: one request in, a stream of responses out.
///
/// It is a [`Stream`] of decoded responses — iterating it lazily fires the
/// request and reads the head on the first poll, so no explicit `.await` is
/// required. Awaiting it *is* available as an opt-in: `.await` reads just the
/// response head, so [`metadata`](Self::metadata) is populated before you consume
/// the first message. [`trailers`](Self::trailers) populate once the stream ends.
pub struct StreamingConn<Req, Resp> {
    /// Held between polls; moved into [`in_flight`](Self::in_flight) for the
    /// duration of a single `recv` and restored when it resolves. `None` only
    /// while a poll is mid-flight (not observable through `&self` accessors).
    /// Boxed so the conn is `Unpin` despite the engine holding a `!Unpin`
    /// cancellation receiver.
    engine: Option<Box<GrpcClientConn<Req, Resp>>>,
    /// The in-progress `recv`, owning the engine while it runs so the future
    /// survives `Pending` polls. Yields `(engine, result)` so the engine returns.
    #[allow(clippy::type_complexity)]
    in_flight: Option<BoxFuture<(Box<GrpcClientConn<Req, Resp>>, Result<Option<Resp>, Status>)>>,
}

impl<Req, Resp> StreamingConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// A server-streaming call sending the single `request`.
    pub fn server_streaming<C>(client: &Client, path: &str, request: Req) -> Self
    where
        C: Codec<Req> + Codec<Resp>,
    {
        let mut engine = GrpcClientConn::new::<C>(
            client,
            path,
            Metadata::new(),
            default_timeout(client),
            false,
        );
        // The single request is buffered now; the body fires on the first recv.
        engine.buffer_request(request);
        Self {
            engine: Some(Box::new(engine)),
            in_flight: None,
        }
    }

    /// The engine between polls. Panics only if called while a poll is in
    /// flight, which `&self`/`&mut self` borrow rules make unreachable.
    fn engine(&self) -> &GrpcClientConn<Req, Resp> {
        self.engine
            .as_deref()
            .expect("engine present between polls")
    }

    fn engine_mut(&mut self) -> &mut GrpcClientConn<Req, Resp> {
        self.engine
            .as_deref_mut()
            .expect("engine present between polls")
    }

    /// Attach an ASCII request-metadata entry. Chainable.
    pub fn with_ascii_metadata(mut self, key: &str, value: &str) -> Self {
        self.engine_mut().add_ascii_metadata(key, value);
        self
    }

    /// Attach a binary (`-bin`) request-metadata entry. Chainable.
    pub fn with_binary_metadata(mut self, key: &str, value: impl Into<Vec<u8>>) -> Self {
        self.engine_mut().add_binary_metadata(key, value.into());
        self
    }

    /// Set this call's deadline, `timeout` from now. Chainable.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.engine_mut().set_deadline_from_now(timeout);
        self
    }

    /// A handle that cancels this call from elsewhere.
    pub fn cancel_handle(&self) -> CancelHandle {
        self.engine().cancel_handle()
    }

    /// The response's initial metadata, once the head has arrived. `None` before.
    pub fn metadata(&self) -> Option<&Headers> {
        self.engine().headers()
    }

    /// The response's trailing metadata, once the stream has ended. `None` before.
    pub fn trailers(&self) -> Option<&Headers> {
        self.engine().trailers()
    }

    /// Read the next response message. `Ok(None)` at end-of-stream, `Err` on RPC
    /// error. Equivalent to using the [`Stream`] impl.
    pub async fn recv(&mut self) -> Result<Option<Resp>, Status> {
        self.engine_mut().recv().await
    }
}

impl<Req, Resp> Stream for StreamingConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    type Item = Result<Resp, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.in_flight.is_none() {
            // Move the engine into a fresh recv future so it persists across
            // Pending polls; the future hands the engine back when it resolves.
            let mut engine = this.engine.take().expect("engine present between polls");
            this.in_flight = Some(Box::pin(async move {
                let result = engine.recv().await;
                (engine, result)
            }));
        }
        let fut = this.in_flight.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((engine, result)) => {
                this.engine = Some(engine);
                this.in_flight = None;
                match result {
                    Ok(None) => Poll::Ready(None),
                    Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                    Err(status) => Poll::Ready(Some(Err(status))),
                }
            }
        }
    }
}

impl<Req, Resp> IntoFuture for StreamingConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    type Output = Result<StreamingConn<Req, Resp>, Status>;
    type IntoFuture = BoxFuture<Self::Output>;

    /// Fire the request and read the head only, so initial metadata is available
    /// before the first message is consumed.
    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            self.engine_mut().open_head().await?;
            Ok(self)
        })
    }
}

/// A full-duplex bidirectional call: send and receive messages interleaved over
/// one live stream.
///
/// Drive it by turns on `&mut self` (the [`WebSocketConn`]-style single-stream
/// model): [`send`](Self::send) a request, [`next`](futures_lite::StreamExt::next)
/// (or [`recv`](Self::recv)) a response, repeat, and [`close_send`](Self::close_send)
/// the request half when done. The first send/recv flushes the request prelude
/// and reads the response head, after which sends write live on the wire.
///
/// [`WebSocketConn`]: https://docs.rs/trillium-websockets
pub struct BidiConn<Req, Resp> {
    /// See [`StreamingConn`]'s fields — same move-into-`recv`-and-back scheme.
    engine: Option<Box<GrpcClientConn<Req, Resp>>>,
    #[allow(clippy::type_complexity)]
    in_flight: Option<BoxFuture<(Box<GrpcClientConn<Req, Resp>>, Result<Option<Resp>, Status>)>>,
}

impl<Req, Resp> BidiConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// A full-duplex bidi call. Requests are sent live via [`send`](Self::send).
    pub fn bidi<C>(client: &Client, path: &str) -> Self
    where
        C: Codec<Req> + Codec<Resp>,
    {
        let engine =
            GrpcClientConn::new::<C>(client, path, Metadata::new(), default_timeout(client), true);
        Self {
            engine: Some(Box::new(engine)),
            in_flight: None,
        }
    }

    fn engine(&self) -> &GrpcClientConn<Req, Resp> {
        self.engine
            .as_deref()
            .expect("engine present between polls")
    }

    fn engine_mut(&mut self) -> &mut GrpcClientConn<Req, Resp> {
        self.engine
            .as_deref_mut()
            .expect("engine present between polls")
    }

    /// Attach an ASCII request-metadata entry, before the first send. Chainable.
    pub fn with_ascii_metadata(mut self, key: &str, value: &str) -> Self {
        self.engine_mut().add_ascii_metadata(key, value);
        self
    }

    /// Attach a binary (`-bin`) request-metadata entry, before the first send.
    pub fn with_binary_metadata(mut self, key: &str, value: impl Into<Vec<u8>>) -> Self {
        self.engine_mut().add_binary_metadata(key, value.into());
        self
    }

    /// Set this call's deadline, `timeout` from now. Chainable.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.engine_mut().set_deadline_from_now(timeout);
        self
    }

    /// A handle that cancels this call from elsewhere.
    pub fn cancel_handle(&self) -> CancelHandle {
        self.engine().cancel_handle()
    }

    /// The response's initial metadata, once the head has arrived. `None` before.
    pub fn metadata(&self) -> Option<&Headers> {
        self.engine().headers()
    }

    /// The response's trailing metadata, once the stream has ended. `None` before.
    pub fn trailers(&self) -> Option<&Headers> {
        self.engine().trailers()
    }

    /// Send one request message. The first send flushes the prelude and reads the
    /// head; later sends write live on the wire.
    pub async fn send(&mut self, message: Req) -> Result<(), Status> {
        self.engine_mut().send(message).await
    }

    /// Half-close the request side (sends END_STREAM), leaving the response side
    /// open to read.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        self.engine_mut().close_send().await
    }

    /// Read the next response message. `Ok(None)` at end-of-stream, `Err` on RPC
    /// error. Equivalent to using the [`Stream`] impl.
    pub async fn recv(&mut self) -> Result<Option<Resp>, Status> {
        self.engine_mut().recv().await
    }
}

impl<Req, Resp> Stream for BidiConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    type Item = Result<Resp, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.in_flight.is_none() {
            let mut engine = this.engine.take().expect("engine present between polls");
            this.in_flight = Some(Box::pin(async move {
                let result = engine.recv().await;
                (engine, result)
            }));
        }
        let fut = this.in_flight.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((engine, result)) => {
                this.engine = Some(engine);
                this.in_flight = None;
                match result {
                    Ok(None) => Poll::Ready(None),
                    Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                    Err(status) => Poll::Ready(Some(Err(status))),
                }
            }
        }
    }
}
