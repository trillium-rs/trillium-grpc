//! [`GrpcClientConn`] — the owned, per-RPC client connection handle.
//!
//! This is the client-side mirror of [`GrpcServerConn`](crate::GrpcServerConn),
//! and the third member of trillium's "one owned conn per protocol upgrade"
//! family alongside `trillium_websockets::WebSocketConn` and
//! `trillium_webtransport::WebTransportConnection`. It merges the per-call
//! control surface (request/response metadata, deadline, cancellation) with the
//! typed message transport (`send`/`recv`) into a single value the caller owns
//! start to finish.
//!
//! # Two transports, chosen by interaction, not papered over
//!
//! A gRPC client can write request frames *before* the response head (that's the
//! request body), but it cannot obtain a free-standing bidirectional handle to
//! *interleave* further writes with reads until the head has arrived — and a
//! half-duplex server withholds its head until it has read the request to
//! END_STREAM. trillium encodes that in the type system (you only get an
//! `Upgrade` post-head). So:
//!
//! - **Send-then-receive** (unary, server-stream, client-stream, *half-duplex*
//!   bidi): the write side is a request body terminated with END_STREAM. `send`
//!   accumulates frames, `close_send` ends the body, and only then does `recv`
//!   await the head and read responses off an owned [`ResponseBody`]. No
//!   `Upgrade`.
//! - **Full-duplex bidi** (the only interleaving shape): the request frames sent
//!   before the first `recv` form an upgrade *prelude*; the first `recv`
//!   materializes the post-head [`Upgrade`], after which `send` writes frames
//!   live and `recv` reads them back, interleaved.
//!
//! The surface (`send`/`close_send`/`recv`/`headers`/`trailers`/`cancel_handle`)
//! is uniform across both; only construction picks the transport.

use crate::{
    Code, Encoding, Status,
    encoding::DEFAULT_MAX_MESSAGE_SIZE,
    frame::{
        reader::{ReadState, poll_read_message},
        writer::encode_payload,
    },
    metadata::Metadata,
    timeout::format_grpc_timeout,
};
use bytes::Bytes;
use futures_lite::{AsyncWriteExt, future::poll_fn};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::Poll,
    time::{Duration, Instant},
};
use trillium::{Headers, KnownHeaderName, Status as HttpStatus, Transport};
use trillium_client::{Body, Client, Conn, ConnExt, Version};
use trillium_http::Upgrade as HttpUpgrade;
use trillium_server_common::Runtime;

type Upgrade = HttpUpgrade<Box<dyn Transport>>;

/// Codec/transport-agnostic parameters captured at construction. Held until the
/// request fires (lazily, on `close_send` or the first `recv`).
struct Pending {
    client: Client,
    path: String,
    content_type: String,
    request_metadata: Metadata,
    body: Vec<u8>,
    send_closed: bool,
}

/// The live response side once the head has arrived. `R` is the owned reader:
/// the [`Conn`] for half-duplex (a fresh `response_body()` view is polled per
/// read — the read state lives on the conn), the [`Upgrade`] for full-duplex.
struct Live<R> {
    reader: R,
    response_headers: Headers,
    read_state: ReadState,
    response_encoding: Encoding,
    /// `grpc-status` carried in the response head, if any. Its presence marks the
    /// response as Trailers-Only — which the gRPC spec says carries no body, so a
    /// message arriving anyway is a protocol violation. See [`read_one`] /
    /// [`finish_from_trailers`].
    head_status: Option<Result<(), Status>>,
}

/// Internal transport state. `recv`/`send`/`close_send` drive transitions out of
/// [`Inner::Pending`].
enum Inner {
    /// Accumulating request frames; no network I/O yet.
    Pending(Pending),
    /// Half-duplex: request sent with END_STREAM, reading responses off the conn.
    Reading(Box<Live<Conn>>),
    /// Full-duplex: upgraded; `send` writes live, `recv` reads, interleaved.
    Duplex(Box<Live<Upgrade>>),
    /// Terminal: the response stream has ended (or the call failed).
    Done {
        response_headers: Headers,
        trailers: Headers,
    },
    /// Terminal: opening the call failed (transport error, non-200 head, …).
    /// The error is delivered once on the next `recv`, then becomes `Done`.
    /// Distinct from `Pending` so a failed open is never retried.
    Failed(Status),
}

/// A cheaply-cloneable, `Send` handle that cancels its [`GrpcClientConn`] from
/// anywhere — including before a blocking call, or after N responses. Cancelling
/// makes the in-flight (or next) `send`/`recv` resolve `Cancelled` and resets the
/// underlying stream.
#[derive(Clone)]
pub struct CancelHandle(async_channel::Sender<()>);

impl CancelHandle {
    /// Cancel the associated RPC. Idempotent.
    pub fn cancel(&self) {
        // Closing the sender is observed by the conn's `recv`/`send` race.
        self.0.close();
    }
}

/// An owned, typed gRPC client call.
///
/// Construct one through a generated `<Service>Client` method (or
/// [`new`](Self::new)), then drive it with [`send`](Self::send) /
/// [`close_send`](Self::close_send) / [`recv`](Self::recv). Initial and trailing
/// response metadata are available via [`headers`](Self::headers) /
/// [`trailers`](Self::trailers) once they've arrived — on the error path too.
pub struct GrpcClientConn<Req, Resp> {
    inner: Inner,
    decode: fn(&[u8]) -> Result<Resp, Status>,
    encode: fn(&Req) -> Result<Bytes, Status>,
    outbound_encoding: Encoding,
    max_message_size: usize,
    full_duplex: bool,
    deadline: Option<Instant>,
    runtime: Runtime,
    cancel_rx: async_channel::Receiver<()>,
    cancel_tx: async_channel::Sender<()>,
    /// A request-side construction error (e.g. invalid metadata supplied through
    /// a typed-conn builder) deferred until the call fires, so the builders can
    /// stay infallible-chainable. Surfaced as the terminal status when the
    /// request is materialized.
    init_error: Option<Status>,
    _marker: PhantomData<fn() -> (Req, Resp)>,
}

impl<Req, Resp> GrpcClientConn<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// Open a call without firing it: captures the target and request metadata
    /// but performs no network I/O until [`close_send`](Self::close_send) or the
    /// first [`recv`](Self::recv). `full_duplex` selects the interleaving
    /// (`Upgrade`) transport; everything else uses send-then-receive.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open(
        client: &Client,
        path: &str,
        content_type: String,
        request_metadata: Metadata,
        timeout: Option<Duration>,
        encode: fn(&Req) -> Result<Bytes, Status>,
        decode: fn(&[u8]) -> Result<Resp, Status>,
        outbound_encoding: Encoding,
        full_duplex: bool,
    ) -> Self {
        let (cancel_tx, cancel_rx) = async_channel::bounded(1);
        Self {
            inner: Inner::Pending(Pending {
                client: client.clone(),
                path: path.to_string(),
                content_type,
                request_metadata,
                body: Vec::new(),
                send_closed: false,
            }),
            decode,
            encode,
            outbound_encoding,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            full_duplex,
            deadline: timeout.map(|d| Instant::now() + d),
            runtime: client.connector().runtime(),
            cancel_rx,
            cancel_tx,
            init_error: None,
            _marker: PhantomData,
        }
    }

    /// Append an ASCII request-metadata entry before the call fires. Invalid
    /// keys/values are recorded as a deferred error surfaced when the request is
    /// materialized, keeping the typed-conn builders infallible-chainable. No-op
    /// once the request is in flight.
    pub(crate) fn add_ascii_metadata(&mut self, key: &str, value: &str) {
        let result = match &mut self.inner {
            Inner::Pending(pending) => pending.request_metadata.insert_ascii(key, value),
            _ => Ok(()),
        };
        if let Err(e) = result {
            self.init_error
                .get_or_insert_with(|| Status::invalid_argument(format!("invalid metadata: {e}")));
        }
    }

    /// Append a binary (`-bin`) request-metadata entry before the call fires.
    /// See [`add_ascii_metadata`](Self::add_ascii_metadata) for error handling.
    pub(crate) fn add_binary_metadata(&mut self, key: &str, value: Vec<u8>) {
        let result = match &mut self.inner {
            Inner::Pending(pending) => pending.request_metadata.insert_binary(key, value),
            _ => Ok(()),
        };
        if let Err(e) = result {
            self.init_error
                .get_or_insert_with(|| Status::invalid_argument(format!("invalid metadata: {e}")));
        }
    }

    /// Set the per-call deadline `timeout` from now, before the call fires.
    pub(crate) fn set_deadline_from_now(&mut self, timeout: Duration) {
        self.deadline = Some(Instant::now() + timeout);
    }

    /// Frame and buffer a single request message before the call fires, without
    /// awaiting (the half-duplex `Pending` body is built synchronously). An
    /// encode failure becomes the deferred init error. Used by the
    /// single-request typed conns.
    pub(crate) fn buffer_request(&mut self, message: Req) {
        match (self.encode)(&message)
            .and_then(|payload| encode_payload(&payload, self.outbound_encoding))
        {
            Ok(frame) => {
                if let Inner::Pending(pending) = &mut self.inner {
                    pending.body.extend_from_slice(&frame);
                }
            }
            Err(status) => {
                self.init_error.get_or_insert(status);
            }
        }
    }

    /// Fire the request and read the response head, transitioning out of
    /// `Pending` without reading any message. Lets a streaming conn expose
    /// initial metadata before the first message is pulled. No-op once live.
    pub(crate) async fn open_head(&mut self) -> Result<(), Status> {
        if matches!(self.inner, Inner::Pending(_)) {
            if self.full_duplex {
                self.materialize_duplex().await
            } else {
                self.materialize_reading().await
            }
        } else {
            Ok(())
        }
    }

    /// Open a call over codec `C`, deriving the content-type and encode/decode
    /// from the [`Codec`](crate::Codec) impl and the outbound compression from
    /// the client's configured `grpc-encoding`. No network I/O happens until
    /// [`close_send`](Self::close_send) or the first [`recv`](Self::recv).
    pub fn new<C>(
        client: &Client,
        path: &str,
        metadata: Metadata,
        timeout: Option<Duration>,
        full_duplex: bool,
    ) -> Self
    where
        C: crate::Codec<Req> + crate::Codec<Resp>,
    {
        let content_type = format!(
            "application/grpc+{}",
            <C as crate::Codec<Req>>::content_type_suffix()
        );
        let outbound_encoding = client
            .default_headers()
            .get_str("grpc-encoding")
            .and_then(Encoding::from_grpc_encoding)
            .unwrap_or(Encoding::Identity);
        Self::open(
            client,
            path,
            content_type,
            metadata,
            timeout,
            <C as crate::Codec<Req>>::encode,
            decode_response::<C, Resp>,
            outbound_encoding,
            full_duplex,
        )
    }

    /// A cancellation handle for this call. Clone it freely; calling
    /// [`cancel`](CancelHandle::cancel) on any clone cancels this RPC.
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(self.cancel_tx.clone())
    }

    /// The response's initial metadata, available once the head has arrived
    /// (after the first `recv`, or after `close_send` for half-duplex). `None`
    /// before then.
    pub fn headers(&self) -> Option<&Headers> {
        match &self.inner {
            Inner::Reading(live) => Some(&live.response_headers),
            Inner::Duplex(live) => Some(&live.response_headers),
            Inner::Done {
                response_headers, ..
            } => Some(response_headers),
            Inner::Pending(_) | Inner::Failed(_) => None,
        }
    }

    /// The response's trailing metadata, available once the response stream has
    /// ended. `None` before end-of-stream.
    pub fn trailers(&self) -> Option<&Headers> {
        match &self.inner {
            Inner::Done { trailers, .. } => Some(trailers),
            _ => None,
        }
    }

    /// Frame and send one request message.
    ///
    /// Before the request has fired (the common case for half-duplex), this
    /// accumulates the frame into the request body. Once a full-duplex call is
    /// live, it writes the frame on the wire immediately.
    pub async fn send(&mut self, message: Req) -> Result<(), Status> {
        let frame = (self.encode)(&message)
            .and_then(|payload| encode_payload(&payload, self.outbound_encoding))?;

        match &mut self.inner {
            Inner::Pending(pending) => {
                if pending.send_closed {
                    return Err(Status::internal("send after close_send"));
                }
                pending.body.extend_from_slice(&frame);
                Ok(())
            }
            Inner::Duplex(live) => {
                let (deadline, runtime, cancel_rx) =
                    (self.deadline, self.runtime.clone(), self.cancel_rx.clone());
                let write = async {
                    live.reader
                        .write_all(&frame)
                        .await
                        .map_err(|e| Status::unavailable(format!("write error: {e}")))
                };
                race(deadline, &runtime, &cancel_rx, write).await
            }
            _ => Err(Status::internal("send after response started")),
        }
    }

    /// Half-close the request side (sends END_STREAM).
    ///
    /// For send-then-receive shapes this is what fires the request: the
    /// accumulated body is sent with END_STREAM and the response head is awaited.
    /// For a live full-duplex call it closes the write half, leaving the read
    /// side open.
    pub async fn close_send(&mut self) -> Result<(), Status> {
        match &mut self.inner {
            Inner::Pending(pending) => {
                pending.send_closed = true;
                if self.full_duplex {
                    self.materialize_duplex().await?;
                    if let Inner::Duplex(live) = &mut self.inner {
                        live.reader
                            .close()
                            .await
                            .map_err(|e| Status::unavailable(format!("close error: {e}")))?;
                    }
                    Ok(())
                } else {
                    self.materialize_reading().await
                }
            }
            Inner::Duplex(live) => live
                .reader
                .close()
                .await
                .map_err(|e| Status::unavailable(format!("close error: {e}"))),
            _ => Ok(()),
        }
    }

    /// Read the next response message: `Ok(Some)` for a message, `Ok(None)` at a
    /// clean end-of-stream (`grpc-status: 0`), `Err` for an RPC error (which also
    /// ends the stream). Trailing metadata is available via
    /// [`trailers`](Self::trailers) afterward.
    pub async fn recv(&mut self) -> Result<Option<Resp>, Status> {
        if matches!(self.inner, Inner::Pending(_)) {
            if self.full_duplex {
                self.materialize_duplex().await?;
            } else {
                // Half-duplex contract is "close_send, then recv"; be lenient and
                // treat recv-while-pending as an implicit close.
                self.materialize_reading().await?;
            }
        }

        match &self.inner {
            Inner::Reading(_) | Inner::Duplex(_) => self.read_one().await,
            Inner::Done { .. } => Ok(None),
            Inner::Failed(_) => {
                let Inner::Failed(status) = std::mem::replace(
                    &mut self.inner,
                    Inner::Done {
                        response_headers: Headers::new(),
                        trailers: Headers::new(),
                    },
                ) else {
                    unreachable!()
                };
                Err(status)
            }
            Inner::Pending(_) => unreachable!("materialized above"),
        }
    }

    /// Read one frame off the live transport, racing the deadline + cancellation.
    async fn read_one(&mut self) -> Result<Option<Resp>, Status> {
        let decode = self.decode;
        let max = self.max_message_size;
        let (deadline, runtime, cancel_rx) =
            (self.deadline, self.runtime.clone(), self.cancel_rx.clone());

        let read = poll_fn(|cx| match &mut self.inner {
            Inner::Reading(live) => {
                // The read state lives on the conn, so a fresh `response_body()`
                // view per poll resumes correctly (and is wired for trailers).
                let enc = live.response_encoding;
                let mut body = live.reader.response_body();
                poll_read_message(
                    Pin::new(&mut body),
                    &mut live.read_state,
                    cx,
                    decode,
                    enc,
                    max,
                )
            }
            Inner::Duplex(live) => poll_read_message(
                Pin::new(&mut live.reader),
                &mut live.read_state,
                cx,
                decode,
                live.response_encoding,
                max,
            ),
            _ => Poll::Ready(None),
        });

        match race(deadline, &runtime, &cancel_rx, async { Ok(read.await) }).await {
            Ok(Some(Ok(msg))) => {
                if self.is_trailers_only() {
                    // A Trailers-Only response (grpc-status in the head) carries no
                    // body per spec; a message here is a protocol violation. Drop the
                    // message and end the call rather than surface it.
                    let _ = self.finish_from_trailers();
                    Err(Status::internal(
                        "trailers-only response (grpc-status in headers) carried a message body",
                    ))
                } else {
                    Ok(Some(msg))
                }
            }
            Ok(Some(Err(status))) => {
                let _ = self.finish_from_trailers();
                Err(status)
            }
            Ok(None) => self.finish_from_trailers().map(|()| None),
            Err(status) => Err(status), // deadline / cancellation
        }
    }

    /// Whether the live response declared itself Trailers-Only by carrying a
    /// `grpc-status` in the head.
    fn is_trailers_only(&self) -> bool {
        match &self.inner {
            Inner::Reading(live) => live.head_status.is_some(),
            Inner::Duplex(live) => live.head_status.is_some(),
            _ => false,
        }
    }

    /// Pull trailing metadata after end-of-stream, transition to `Done`, and
    /// return the response's `grpc-status`.
    ///
    /// Precedence: a `grpc-status` in the real trailers always wins; otherwise,
    /// for a Trailers-Only response, the head's `grpc-status` (and the head
    /// metadata, surfaced as the trailers) is authoritative; otherwise a normal
    /// response ended without a trailing `grpc-status`, which `from_trailers`
    /// maps to `Unknown`.
    fn finish_from_trailers(&mut self) -> Result<(), Status> {
        let (response_headers, head_status, mut trailers) = match &mut self.inner {
            Inner::Reading(live) => (
                live.response_headers.clone(),
                live.head_status.clone(),
                live.reader.response_trailers().cloned().unwrap_or_default(),
            ),
            Inner::Duplex(live) => (
                live.response_headers.clone(),
                live.head_status.clone(),
                live.reader.received_trailers().cloned().unwrap_or_default(),
            ),
            _ => (Headers::new(), None, Headers::new()),
        };

        let status = if trailers.get_str("grpc-status").is_some() {
            Status::from_trailers(&trailers)
        } else if let Some(head_status) = head_status {
            // Trailers-Only: the trailing metadata lived in the head.
            trailers = response_headers.clone();
            head_status
        } else {
            Status::from_trailers(&trailers)
        };

        self.inner = Inner::Done {
            response_headers,
            trailers,
        };
        status
    }

    /// Fire the half-duplex request (body + END_STREAM), await the head, and
    /// transition to `Reading`.
    async fn materialize_reading(&mut self) -> Result<(), Status> {
        if let Some(status) = self.init_error.take() {
            return self.fail(status);
        }
        let body = self.take_body();
        let request = self.build_request(Body::from(body));
        let (deadline, runtime, cancel_rx) =
            (self.deadline, self.runtime.clone(), self.cancel_rx.clone());
        let conn = match race(deadline, &runtime, &cancel_rx, async {
            request.await.map_err(transport_error)
        })
        .await
        {
            Ok(conn) => conn,
            Err(status) => return self.fail(status),
        };

        match process_head(&conn) {
            Ok(Head {
                response_headers,
                response_encoding,
                head_status,
            }) => {
                self.inner = Inner::Reading(Box::new(Live {
                    reader: conn,
                    response_headers,
                    read_state: ReadState::new(),
                    response_encoding,
                    head_status,
                }));
                Ok(())
            }
            Err(status) => self.fail(status),
        }
    }

    /// Record a fatal open error as the terminal `Failed` state and return it,
    /// so the call never re-fires its request on a subsequent `recv`.
    fn fail(&mut self, status: Status) -> Result<(), Status> {
        self.inner = Inner::Failed(status.clone());
        Err(status)
    }

    /// Send the accumulated frames as an upgrade prelude (request side left
    /// open), await the head, and transition to `Duplex`.
    async fn materialize_duplex(&mut self) -> Result<(), Status> {
        if let Some(status) = self.init_error.take() {
            return self.fail(status);
        }
        let body = self.take_body();
        let request = self.build_request(Body::from(body));
        let (deadline, runtime, cancel_rx) =
            (self.deadline, self.runtime.clone(), self.cancel_rx.clone());
        let conn = match race(deadline, &runtime, &cancel_rx, async {
            request.upgrade().await.map_err(transport_error)
        })
        .await
        {
            Ok(conn) => conn,
            Err(status) => return self.fail(status),
        };

        match process_head(&conn) {
            Ok(Head {
                response_headers,
                response_encoding,
                head_status,
            }) => {
                self.inner = Inner::Duplex(Box::new(Live {
                    reader: conn.into(),
                    response_headers,
                    read_state: ReadState::new(),
                    response_encoding,
                    head_status,
                }));
                Ok(())
            }
            Err(status) => self.fail(status),
        }
    }

    /// Take the accumulated request body out of the `Pending` state.
    fn take_body(&mut self) -> Vec<u8> {
        match &mut self.inner {
            Inner::Pending(pending) => std::mem::take(&mut pending.body),
            _ => Vec::new(),
        }
    }

    /// Build the unsent request `Conn` with gRPC headers + custom metadata.
    /// Requires `self.inner` to be `Pending`.
    fn build_request(&self, body: Body) -> Conn {
        let Inner::Pending(pending) = &self.inner else {
            unreachable!("build_request requires Pending");
        };
        let mut conn = pending
            .client
            .post(pending.path.as_str())
            .with_http_version(Version::Http2)
            .with_request_header(KnownHeaderName::ContentType, pending.content_type.clone())
            .with_request_header(KnownHeaderName::Te, "trailers")
            .with_request_header("grpc-accept-encoding", Encoding::accepted_encodings());

        if !matches!(self.outbound_encoding, Encoding::Identity) {
            conn.request_headers_mut()
                .insert("grpc-encoding", self.outbound_encoding.as_grpc_encoding());
        }
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            conn.request_headers_mut()
                .insert("grpc-timeout", format_grpc_timeout(remaining));
        }
        pending
            .request_metadata
            .write_into(conn.request_headers_mut());
        conn.with_body(body)
    }
}

/// Race a future against the deadline and the cancellation signal.
async fn race<T, F>(
    deadline: Option<Instant>,
    runtime: &Runtime,
    cancel_rx: &async_channel::Receiver<()>,
    fut: F,
) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    let cancel = {
        let rx = cancel_rx.clone();
        async move {
            // recv resolves once cancel() closes the sender.
            let _ = rx.recv().await;
            Err(Status::cancelled("call cancelled"))
        }
    };

    // `or` is left-biased: poll cancel (and the deadline) *before* `fut`, so an
    // already-invoked cancel — or an elapsed deadline — wins over a response that
    // happens to be buffered and ready. The opposite order would let a fast
    // response slip through after the caller had already cancelled.
    match deadline {
        None => futures_lite::future::or(cancel, fut).await,
        Some(deadline) => {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(Status::deadline_exceeded("deadline elapsed"));
            };
            let runtime = runtime.clone();
            let timer = async move {
                runtime.delay(remaining).await;
                Err(Status::deadline_exceeded("deadline elapsed"))
            };
            futures_lite::future::or(futures_lite::future::or(cancel, timer), fut).await
        }
    }
}

/// The outcome of inspecting a response head: a valid gRPC head (HTTP 200 + gRPC
/// content-type). The body — empty for a Trailers-Only response — is read off the
/// transport afterward; `head_status` is the head's `grpc-status` (if present),
/// used as the Trailers-Only fallback in [`finish_from_trailers`].
struct Head {
    response_headers: Headers,
    response_encoding: Encoding,
    head_status: Option<Result<(), Status>>,
}

/// Validate the response head (HTTP 200 + gRPC content-type), classify it as a
/// normal or trailers-only response, and pull the inbound message encoding.
fn process_head(conn: &Conn) -> Result<Head, Status> {
    let http_status = conn.status();
    if http_status != Some(HttpStatus::Ok) {
        return Err(http_to_grpc_status(
            http_status.map(|s| s as u16).unwrap_or(0),
        ));
    }

    let ct = conn
        .response_headers()
        .get_str(KnownHeaderName::ContentType);
    if ct
        .and_then(crate::content_type::parse_grpc_content_type)
        .is_none()
    {
        // A 200 response whose content-type isn't gRPC means the peer isn't
        // speaking gRPC; the spec maps this to Unknown (not Internal).
        return Err(Status::unknown(format!(
            "unexpected response content-type: {ct:?}"
        )));
    }

    let response_encoding = match conn.response_headers().get_str("grpc-encoding") {
        None => Encoding::Identity,
        Some(s) => Encoding::from_grpc_encoding(s)
            .ok_or_else(|| Status::internal(format!("unsupported grpc-encoding {s:?}")))?,
    };

    let response_headers = conn.response_headers().clone();

    // A `grpc-status` in the head is authoritative only for a genuine Trailers-Only
    // response (HEADERS+END_STREAM, no body). We can't tell that apart from the head
    // alone — a server may also send it alongside a body or real trailers, where the
    // spec says to ignore it — so we always read the body and disambiguate at EOF in
    // `finish_from_trailers`, carrying the head status along as the no-body fallback.
    let head_status = response_headers
        .get_str("grpc-status")
        .is_some()
        .then(|| Status::from_trailers(&response_headers));

    Ok(Head {
        response_headers,
        response_encoding,
        head_status,
    })
}

fn transport_error(err: trillium_client::Error) -> Status {
    Status::unavailable(format!("transport error: {err}"))
}

/// Decode a response message, mapping a codec failure to `Internal`. The codec's
/// own `decode` reports `InvalidArgument` — correct for the server decoding a bad
/// *request*, but on the client a body we can't decode is the server's fault, so
/// it must surface as `Internal`.
fn decode_response<C, Resp>(bytes: &[u8]) -> Result<Resp, Status>
where
    C: crate::Codec<Resp>,
{
    <C as crate::Codec<Resp>>::decode(bytes)
        .map_err(|status| Status::new(Code::Internal, status.message))
}

/// Map an HTTP status code to a gRPC code per the gRPC HTTP/2 spec.
fn http_to_grpc_status(http: u16) -> Status {
    let code = match http {
        400 => Code::Internal,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::Unimplemented,
        429 | 502 | 503 | 504 => Code::Unavailable,
        _ => Code::Unknown,
    };
    Status::new(code, format!("HTTP {http}"))
}
