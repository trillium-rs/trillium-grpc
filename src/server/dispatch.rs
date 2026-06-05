//! Server-side request dispatch.
//!
//! The [`Server`] trait's methods are what generated code calls per RPC,
//! resolved through a blanket impl so `Prost::unary(conn, ...)` works without a
//! turbofish. The three half-duplex shapes (unary, client-streaming,
//! server-streaming) run entirely in `Handler::run`: they take the `Conn`,
//! drive the codec/framing around your method, and return a normal trillium
//! response whose body carries `grpc-status` in HTTP trailers — no `Upgrade`.
//! Bidi is the one shape that upgrades, because read-while-write requires the
//! response head already flushed.
//!
//! [`prepare_grpc_conn`] is the shared preflight (content-type / `te: trailers`
//! validation, response content-type / `grpc-accept-encoding`), called from
//! generated `Handler::run` after path matching.

use crate::{
    Codec, Encoding, Status,
    content_type::{has_te_trailers, parse_grpc_content_type},
    frame::writer::encode_frame,
    server::{
        bidi::{BidiResponder, BidiUpgrade},
        body::{CancelSignal, OneShotBody, StreamBody},
        grpc_conn::GrpcServerConn,
    },
    timeout::parse_grpc_timeout,
};
use futures_lite::Stream;
use std::{future::Future, time::Instant};
use trillium::{Body, Conn, Headers, KnownHeaderName, Status as HttpStatus, Swansong, Upgrade};
use trillium_http::BodySource;
use trillium_server_common::Runtime;

/// Server-side dispatch methods, available on any codec type via a blanket
/// impl. Generated code calls these as `Prost::unary(conn, ...)` etc.
///
/// The three half-duplex methods take the `Conn` by value and return the
/// finished `Conn`; the user closure is handed a [`GrpcServerConn`] control surface
/// (and, for unary / server-streaming, the decoded request). [`bidi`](Self::bidi)
/// still takes a [`trillium::Upgrade`] — it is driven from `Handler::upgrade`
/// after the head is flushed.
#[allow(async_fn_in_trait)]
pub trait Server: Sized + 'static {
    /// Unary RPC: read exactly one request, await the user function, emit one
    /// response frame followed by `grpc-status` trailers.
    async fn unary<Req, Resp>(
        conn: Conn,
        f: impl AsyncFnOnce(&mut GrpcServerConn<Self>, Req) -> Result<Resp, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        unary_impl::<Self, Req, Resp>(conn, f).await
    }

    /// Client-streaming RPC: hand the user a [`GrpcServerConn`] from which they read
    /// the request stream (`conn.requests::<Req>()`); emit the single response
    /// frame and `grpc-status` trailers.
    async fn client_streaming<Resp>(
        conn: Conn,
        f: impl AsyncFnOnce(&mut GrpcServerConn<Self>) -> Result<Resp, Status>,
    ) -> Conn
    where
        Self: Codec<Resp>,
        Resp: Send + 'static,
    {
        client_streaming_impl::<Self, Resp>(conn, f).await
    }

    /// Server-streaming RPC: read one request, await the user function for a
    /// response [`Stream`], then frame each item lazily into the response body
    /// with `grpc-status` trailers derived from how the stream ended.
    async fn server_streaming<Req, Resp, S>(
        conn: Conn,
        f: impl AsyncFnOnce(&mut GrpcServerConn<Self>, Req) -> Result<S, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        S: Stream<Item = Result<Resp, Status>> + Send + 'static,
    {
        server_streaming_impl::<Self, Req, Resp, S>(conn, f).await
    }

    /// Bidirectional-streaming RPC — the run-phase prologue. Hand the user a
    /// [`GrpcServerConn`] from which they may read early request messages (to decide
    /// response headers) and set initial metadata, then return a
    /// [`BidiResponder`] that drives the read-while-write loop after the head is
    /// flushed. Returning `Err(Status)` rejects before the flush (trailers-only,
    /// no upgrade). See [`crate::server::bidi`] for the seam mechanics.
    async fn bidi<Req, Resp, R>(
        conn: Conn,
        prologue: impl AsyncFnOnce(&mut GrpcServerConn<Self>) -> Result<R, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        R: BidiResponder<Req, Resp>,
    {
        bidi_prologue_impl::<Self, Req, Resp, R>(conn, prologue).await
    }
}

impl<T: Sized + 'static> Server for T {}

// ── half-duplex (run-phase) ────────────────────────────────────────────────

async fn unary_impl<C, Req, Resp>(
    conn: Conn,
    f: impl AsyncFnOnce(&mut GrpcServerConn<C>, Req) -> Result<Resp, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(conn.request_headers()) {
        Ok(e) => e,
        Err(status) => return error_response(conn, status),
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(c) => c,
        Err(status) => return error_response(conn, status),
    };
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let mut grpc = GrpcServerConn::<C>::new(conn, request_encoding);

    let result = cancellation
        .race(async {
            let req = read_one::<C, Req>(&mut grpc).await?;
            f(&mut grpc, req).await
        })
        .await;
    let (conn, trailers) = grpc.into_parts();
    finish_unary::<C, Resp>(conn, result, response_encoding, trailers)
}

async fn client_streaming_impl<C, Resp>(
    conn: Conn,
    f: impl AsyncFnOnce(&mut GrpcServerConn<C>) -> Result<Resp, Status>,
) -> Conn
where
    C: Codec<Resp>,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(conn.request_headers()) {
        Ok(e) => e,
        Err(status) => return error_response(conn, status),
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(c) => c,
        Err(status) => return error_response(conn, status),
    };
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let mut grpc = GrpcServerConn::<C>::new(conn, request_encoding);

    let result = cancellation.race(f(&mut grpc)).await;
    let (conn, trailers) = grpc.into_parts();
    finish_unary::<C, Resp>(conn, result, response_encoding, trailers)
}

async fn server_streaming_impl<C, Req, Resp, S>(
    conn: Conn,
    f: impl AsyncFnOnce(&mut GrpcServerConn<C>, Req) -> Result<S, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    S: Stream<Item = Result<Resp, Status>> + Send + 'static,
{
    let request_encoding = match extract_request_encoding(conn.request_headers()) {
        Ok(e) => e,
        Err(status) => return error_response(conn, status),
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(c) => c,
        Err(status) => return error_response(conn, status),
    };
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let mut grpc = GrpcServerConn::<C>::new(conn, request_encoding);

    // The setup (read one request + the user fn that produces the stream) is
    // raced against cancellation; the resulting stream is then itself made
    // cancellable so an in-flight deadline / shutdown cuts it between frames.
    let result = cancellation
        .race(async {
            let req = read_one::<C, Req>(&mut grpc).await?;
            f(&mut grpc, req).await
        })
        .await;
    let (conn, trailers) = grpc.into_parts();
    match result {
        Ok(stream) => respond(
            conn,
            StreamBody::new(
                stream,
                <C as Codec<Resp>>::encode,
                response_encoding,
                trailers,
                Some(cancellation.signal()),
            ),
        ),
        Err(status) => error_response_with_trailers(conn, status, trailers),
    }
}

/// Read exactly one request message from the conn's request body, enforcing
/// the unary / server-streaming cardinality of *exactly one* request. Zero or
/// more-than-one is a cardinality violation, which the gRPC status-code
/// guidance maps to `unimplemented`.
async fn read_one<C, Req>(grpc: &mut GrpcServerConn<C>) -> Result<Req, Status>
where
    C: Codec<Req>,
    Req: 'static,
{
    let mut requests = grpc.requests::<Req>();
    let Some(req) = requests.recv().await? else {
        return Err(Status::unimplemented(
            "expected exactly one request message, but the request stream was empty",
        ));
    };
    if requests.recv().await?.is_some() {
        return Err(Status::unimplemented(
            "expected exactly one request message, but the request stream had more than one",
        ));
    }
    Ok(req)
}

/// Encode the single-response result (unary / client-streaming) into a
/// `OneShotBody` + trailers, or an error response.
fn finish_unary<C, Resp>(
    conn: Conn,
    result: Result<Resp, Status>,
    response_encoding: Encoding,
    trailers: Headers,
) -> Conn
where
    C: Codec<Resp>,
{
    match result {
        Ok(resp) => match encode_frame::<C, Resp>(&resp, response_encoding) {
            Ok(frame) => {
                let mut trailers = trailers;
                Status::ok().write_into(&mut trailers);
                respond(conn, OneShotBody::new(frame, trailers))
            }
            Err(status) => error_response_with_trailers(conn, status, trailers),
        },
        Err(status) => error_response_with_trailers(conn, status, trailers),
    }
}

/// Set a half-duplex response body (carrying `grpc-status` in trailers) and
/// halt — this handler has fully produced the response.
fn respond(conn: Conn, body: impl BodySource) -> Conn {
    conn.with_body(Body::new_with_trailers(body, None)).halt()
}

/// An error response: response headers (already set by preflight) + empty body
/// + `grpc-status` trailers merged onto any trailing metadata the handler set.
fn error_response_with_trailers(conn: Conn, status: Status, mut trailers: Headers) -> Conn {
    status.write_into(&mut trailers);
    respond(conn, OneShotBody::new(Vec::new(), trailers))
}

/// An error response with no handler-set trailing metadata.
fn error_response(conn: Conn, status: Status) -> Conn {
    error_response_with_trailers(conn, status, Headers::new())
}

// ── bidi (run-phase prologue) ────────────────────────────────────────────────

/// Run the bidi prologue in `run()`: build the [`GrpcServerConn`], let the user read
/// early requests and set initial metadata, then either stash a [`BidiUpgrade`]
/// and mark the conn for upgrade (the responder drives the loop in `upgrade()`)
/// or, on `Err`, emit a trailers-only error without upgrading. The
/// read-while-write loop itself lives in [`crate::server::bidi`].
async fn bidi_prologue_impl<C, Req, Resp, R>(
    conn: Conn,
    prologue: impl AsyncFnOnce(&mut GrpcServerConn<C>) -> Result<R, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    R: BidiResponder<Req, Resp>,
{
    let request_encoding = match extract_request_encoding(conn.request_headers()) {
        Ok(e) => e,
        Err(status) => return error_response(conn, status),
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(c) => c,
        Err(status) => return error_response(conn, status),
    };
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let mut grpc = GrpcServerConn::<C>::new(conn, request_encoding);

    let result = cancellation.race(prologue(&mut grpc)).await;
    let deadline = grpc.deadline();
    let (conn, trailers) = grpc.into_parts();

    match result {
        Ok(responder) => {
            let bidi = BidiUpgrade::new(
                responder,
                trailers,
                <C as Codec<Req>>::decode,
                <C as Codec<Resp>>::encode,
                request_encoding,
                response_encoding,
                deadline,
            );
            conn.with_state(bidi).upgrade().halt()
        }
        Err(status) => error_response_with_trailers(conn, status, trailers),
    }
}

// ── preflight + shared helpers ───────────────────────────────────────────────

/// Validate request preflight (content-type, te:trailers) and set the gRPC
/// response headers (content-type, grpc-accept-encoding). Returns the conn with
/// those headers set, or an error-shaped conn if preflight failed.
///
/// Called from generated `Handler::run` *after* path matching has confirmed
/// this request belongs to the service.
// Both arms are the same `trillium::Conn`, so boxing only the `Err` side would be
// pointless asymmetry — the equally-large `Ok` conn moves by value regardless, and
// this runs once per request, not on a hot path.
#[allow(clippy::result_large_err)]
pub fn prepare_grpc_conn(conn: Conn, codec_suffix: &str) -> Result<Conn, Conn> {
    // The request's content-type must name a gRPC codec we actually serve;
    // `application/grpc+foo` for an unknown `foo` is rejected here rather than
    // mis-decoded as the service's codec.
    let codec_matches = conn
        .request_headers()
        .get_str(KnownHeaderName::ContentType)
        .and_then(parse_grpc_content_type)
        .is_some_and(|suffix| suffix == codec_suffix);
    if !codec_matches {
        return Err(conn.with_status(HttpStatus::UnsupportedMediaType).halt());
    }
    if !has_te_trailers(conn.request_headers()) {
        return Err(conn.with_status(HttpStatus::BadRequest).halt());
    }
    let content_type = format!("application/grpc+{codec_suffix}");
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let conn = conn
        .with_response_header(KnownHeaderName::ContentType, content_type)
        .with_response_header("grpc-accept-encoding", Encoding::accepted_encodings())
        .with_status(HttpStatus::Ok);
    Ok(if matches!(response_encoding, Encoding::Identity) {
        conn
    } else {
        conn.with_response_header("grpc-encoding", response_encoding.as_grpc_encoding())
    })
}

/// Resolve the inbound message encoding from `grpc-encoding`. Missing →
/// `Identity` (per spec). Unknown → `Unimplemented`.
fn extract_request_encoding(request_headers: &Headers) -> Result<Encoding, Status> {
    match request_headers.get_str("grpc-encoding") {
        None => Ok(Encoding::Identity),
        Some(s) => Encoding::from_grpc_encoding(s).ok_or_else(|| {
            Status::unimplemented(format!(
                "unsupported grpc-encoding {s:?}; accepted: {}",
                Encoding::accepted_encodings()
            ))
        }),
    }
}

/// Per-request cancellation handle, combining connection shutdown
/// (`Conn`/`Upgrade::swansong()`) and an optional `grpc-timeout` deadline.
pub(crate) struct Cancellation {
    swansong: Swansong,
    deadline: Option<Deadline>,
}

#[derive(Clone)]
struct Deadline {
    runtime: Runtime,
    instant: Instant,
}

impl Cancellation {
    /// Build from a `Conn` (the half-duplex run-phase path). Mirrors
    /// [`from_upgrade`](Self::from_upgrade): connection shutdown via
    /// `Conn::swansong()` and an optional `grpc-timeout` deadline.
    fn from_conn(conn: &Conn) -> Result<Self, Status> {
        let swansong = conn.swansong();
        let deadline = match conn.request_headers().get_str("grpc-timeout") {
            None => None,
            Some(header) => {
                let duration = parse_grpc_timeout(header).ok_or_else(|| {
                    Status::invalid_argument(format!("malformed grpc-timeout {header:?}"))
                })?;
                let runtime = conn
                    .shared_state::<Runtime>()
                    .expect("trillium-grpc requires a Runtime in shared state")
                    .clone();
                Some(Deadline {
                    runtime,
                    instant: Instant::now() + duration,
                })
            }
        };
        Ok(Self { swansong, deadline })
    }

    /// A future that resolves to the terminal [`Status`] when this request is
    /// cancelled (shutdown → `cancelled`, deadline → `deadline_exceeded`) and
    /// stays `Pending` otherwise. Handed to [`StreamBody`] so a server-streaming
    /// response can be cut between frames.
    fn signal(&self) -> CancelSignal {
        let swansong = self.swansong.clone();
        let deadline = self.deadline.clone();
        Box::pin(async move {
            let shutdown = async {
                swansong.interrupt(std::future::pending::<()>()).await;
                Status::cancelled("connection shutting down")
            };
            match deadline {
                None => shutdown.await,
                Some(d) => {
                    let timer = async move {
                        if let Some(remaining) = d.instant.checked_duration_since(Instant::now()) {
                            d.runtime.delay(remaining).await;
                        }
                        Status::deadline_exceeded("deadline elapsed")
                    };
                    futures_lite::future::or(shutdown, timer).await
                }
            }
        })
    }

    /// Build for the upgrade (bidi loop) phase from a deadline already computed
    /// in the prologue (so the clock isn't restarted by however long the
    /// prologue ran). Shutdown comes from the upgrade's swansong; the deadline
    /// timer reuses the request's `Runtime` from shared state.
    pub(crate) fn for_upgrade(upgrade: &Upgrade, deadline: Option<Instant>) -> Self {
        let swansong = upgrade.swansong();
        let deadline = deadline.map(|instant| {
            let runtime = upgrade
                .shared_state()
                .get::<Runtime>()
                .expect("trillium-grpc requires a Runtime in shared state")
                .clone();
            Deadline { runtime, instant }
        });
        Self { swansong, deadline }
    }

    pub(crate) async fn race<T, F>(&self, fut: F) -> Result<T, Status>
    where
        F: Future<Output = Result<T, Status>>,
    {
        let interruptible = async {
            match self.swansong.interrupt(fut).await {
                Some(result) => result,
                None => Err(Status::cancelled("connection shutting down")),
            }
        };
        let Some(deadline) = self.deadline.as_ref() else {
            return interruptible.await;
        };
        let Some(remaining) = deadline.instant.checked_duration_since(Instant::now()) else {
            return Err(Status::deadline_exceeded("deadline elapsed"));
        };
        let runtime = deadline.runtime.clone();
        let timer = async move {
            runtime.delay(remaining).await;
            Err(Status::deadline_exceeded("deadline elapsed"))
        };
        futures_lite::future::or(interruptible, timer).await
    }
}

/// Pick the response encoding by intersecting the client's
/// `grpc-accept-encoding` with `Encoding::ALL` in build order (prefer gzip,
/// deflate, zstd, then identity).
fn negotiate_response_encoding(request_headers: &Headers) -> Encoding {
    let Some(accepted) = request_headers.get_str("grpc-accept-encoding") else {
        return Encoding::Identity;
    };
    let accepted: Vec<&str> = accepted.split(',').map(str::trim).collect();
    Encoding::ALL
        .iter()
        .copied()
        .filter(|e| !matches!(e, Encoding::Identity))
        .find(|e| accepted.contains(&e.as_grpc_encoding()))
        .unwrap_or(Encoding::Identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(accept: &str) -> Headers {
        let mut h = Headers::new();
        h.insert("grpc-accept-encoding", accept.to_owned());
        h
    }

    #[test]
    fn no_accept_header_falls_back_to_identity() {
        assert_eq!(
            negotiate_response_encoding(&Headers::new()),
            Encoding::Identity
        );
    }

    #[test]
    fn identity_only_means_identity() {
        assert_eq!(
            negotiate_response_encoding(&headers_with("identity")),
            Encoding::Identity
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn picks_gzip_when_offered() {
        assert_eq!(
            negotiate_response_encoding(&headers_with("identity, gzip")),
            Encoding::Gzip
        );
    }

    #[cfg(all(feature = "gzip", feature = "zstd"))]
    #[test]
    fn prefers_build_order_over_client_order() {
        assert_eq!(
            negotiate_response_encoding(&headers_with("zstd, gzip")),
            Encoding::Gzip
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn ignores_unknown_codecs() {
        assert_eq!(
            negotiate_response_encoding(&headers_with("snappy, gzip")),
            Encoding::Gzip
        );
        assert_eq!(
            negotiate_response_encoding(&headers_with("snappy")),
            Encoding::Identity
        );
    }
}
