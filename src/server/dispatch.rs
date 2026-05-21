use crate::{
    Codec, Encoding, Status,
    frame::{reader::MessageStream, writer::encode_frame},
    server::{
        content_type::{has_te_trailers, parse_grpc_content_type},
        streaming::{Channel, RequestStream, ResponseSink},
    },
    timeout::parse_grpc_timeout,
};
use futures_lite::{AsyncWriteExt, StreamExt};
use std::{future::Future, time::Instant};
use trillium::{Conn, Headers, KnownHeaderName, Status as HttpStatus, Swansong, Upgrade};
use trillium_server_common::Runtime;

/// Server-side dispatch methods, available on any codec type via a blanket
/// impl. Generated code calls these as `Prost::unary(upgrade, ...)` etc.,
/// which resolves through the trait without requiring a turbofish.
///
/// Each method consumes a [`trillium::Upgrade`] (obtained via
/// `conn.with_upgrade()` in the service `Handler::run` and then handed off in
/// `Handler::upgrade`). The upgrade carries the AsyncRead+AsyncWrite transport,
/// the request headers, and the eventual sink for `grpc-status` trailers.
///
/// For the streaming shapes, the framework retains ownership of the upgrade
/// for the lifetime of the user closure and hands over borrowed
/// [`RequestStream`] / [`ResponseSink`] / [`Channel`] primitives. On closure
/// return the framework writes the terminating trailers based on the user's
/// `Result`.
#[allow(async_fn_in_trait)]
pub trait Server: Sized + 'static {
    /// Unary RPC: read exactly one request, await the user function, emit one
    /// response frame followed by `grpc-status` trailers.
    async fn unary<Req, Resp>(upgrade: Upgrade, f: impl AsyncFnOnce(Req) -> Result<Resp, Status>)
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        unary_impl::<Self, Req, Resp>(upgrade, f).await
    }

    /// Server-streaming RPC: read one request, hand the user a
    /// [`ResponseSink`] for emitting response messages, write trailers based
    /// on the user's `Result`.
    async fn server_streaming<Req, Resp>(
        upgrade: Upgrade,
        f: impl AsyncFnOnce(Req, ResponseSink<'_, Resp>) -> Result<(), Status>,
    ) where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        server_streaming_impl::<Self, Req, Resp>(upgrade, f).await
    }

    /// Client-streaming RPC: hand the user a [`RequestStream`] of decoded
    /// request messages, await the single response.
    async fn client_streaming<Req, Resp>(
        upgrade: Upgrade,
        f: impl AsyncFnOnce(RequestStream<'_, Req>) -> Result<Resp, Status>,
    ) where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        client_streaming_impl::<Self, Req, Resp>(upgrade, f).await
    }

    /// Bidirectional-streaming RPC: hand the user a [`Channel`] that
    /// interleaves `recv` and `send`. Turn-taking by construction; spawn a
    /// task if you need concurrent producers.
    async fn bidi<Req, Resp>(
        upgrade: Upgrade,
        f: impl AsyncFnOnce(Channel<'_, Req, Resp>) -> Result<(), Status>,
    ) where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        bidi_impl::<Self, Req, Resp>(upgrade, f).await
    }
}

impl<T: Sized + 'static> Server for T {}

async fn unary_impl<C, Req, Resp>(
    mut upgrade: Upgrade,
    f: impl AsyncFnOnce(Req) -> Result<Resp, Status>,
) where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(upgrade.request_headers()) {
        Ok(e) => e,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let cancellation = match Cancellation::from_upgrade(&upgrade) {
        Ok(c) => c,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let response_encoding = negotiate_response_encoding(upgrade.request_headers());

    let result = cancellation
        .race(async {
            let req = read_one_request::<C, Req>(&mut upgrade, request_encoding).await?;
            f(req).await
        })
        .await;

    let trailers = match result {
        Ok(resp) => match encode_frame::<C, Resp>(&resp, response_encoding) {
            Ok(frame) => match upgrade.write_all(&frame).await {
                Ok(()) => Status::ok().into_trailers(),
                // Peer hung up. Nothing useful to do with trailers — drop.
                Err(_) => return,
            },
            Err(status) => status.into_trailers(),
        },
        Err(status) => status.into_trailers(),
    };

    if let Err(e) = upgrade.send_trailers(trailers).await {
        log::warn!("trillium-grpc: send_trailers failed: {e}");
    }
}

/// Send a status-only response: no frames, just `grpc-status` trailers.
/// Used when we discover a gRPC-level error before reaching the user closure.
async fn trailers_only(upgrade: Upgrade, status: Status) {
    if let Err(e) = upgrade.send_trailers(status.into_trailers()).await {
        log::warn!("trillium-grpc: send_trailers failed: {e}");
    }
}

async fn client_streaming_impl<C, Req, Resp>(
    mut upgrade: Upgrade,
    f: impl AsyncFnOnce(RequestStream<'_, Req>) -> Result<Resp, Status>,
) where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(upgrade.request_headers()) {
        Ok(e) => e,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let cancellation = match Cancellation::from_upgrade(&upgrade) {
        Ok(c) => c,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let response_encoding = negotiate_response_encoding(upgrade.request_headers());

    let result = cancellation
        .race(async {
            let requests =
                RequestStream::new(&mut upgrade, <C as Codec<Req>>::decode, request_encoding);
            f(requests).await
        })
        .await;

    let trailers = match result {
        Ok(resp) => match encode_frame::<C, Resp>(&resp, response_encoding) {
            Ok(frame) => match upgrade.write_all(&frame).await {
                Ok(()) => Status::ok().into_trailers(),
                Err(_) => return,
            },
            Err(status) => status.into_trailers(),
        },
        Err(status) => status.into_trailers(),
    };

    if let Err(e) = upgrade.send_trailers(trailers).await {
        log::warn!("trillium-grpc: send_trailers failed: {e}");
    }
}

async fn bidi_impl<C, Req, Resp>(
    mut upgrade: Upgrade,
    f: impl AsyncFnOnce(Channel<'_, Req, Resp>) -> Result<(), Status>,
) where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(upgrade.request_headers()) {
        Ok(e) => e,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let cancellation = match Cancellation::from_upgrade(&upgrade) {
        Ok(c) => c,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let response_encoding = negotiate_response_encoding(upgrade.request_headers());

    let result = cancellation
        .race(async {
            let channel = Channel::new(
                &mut upgrade,
                <C as Codec<Req>>::decode,
                <C as Codec<Resp>>::encode,
                request_encoding,
                response_encoding,
            );
            f(channel).await
        })
        .await;

    let trailers = match result {
        Ok(()) => Status::ok().into_trailers(),
        Err(status) => status.into_trailers(),
    };

    if let Err(e) = upgrade.send_trailers(trailers).await {
        log::warn!("trillium-grpc: send_trailers failed: {e}");
    }
}

async fn server_streaming_impl<C, Req, Resp>(
    mut upgrade: Upgrade,
    f: impl AsyncFnOnce(Req, ResponseSink<'_, Resp>) -> Result<(), Status>,
) where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let request_encoding = match extract_request_encoding(upgrade.request_headers()) {
        Ok(e) => e,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let cancellation = match Cancellation::from_upgrade(&upgrade) {
        Ok(c) => c,
        Err(status) => return trailers_only(upgrade, status).await,
    };
    let response_encoding = negotiate_response_encoding(upgrade.request_headers());

    let result = cancellation
        .race(async {
            let req = read_one_request::<C, Req>(&mut upgrade, request_encoding).await?;
            let sink =
                ResponseSink::new(&mut upgrade, <C as Codec<Resp>>::encode, response_encoding);
            f(req, sink).await
        })
        .await;

    let trailers = match result {
        Ok(()) => Status::ok().into_trailers(),
        Err(status) => status.into_trailers(),
    };

    if let Err(e) = upgrade.send_trailers(trailers).await {
        log::warn!("trillium-grpc: send_trailers failed: {e}");
    }
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
        // Client lists zstd first, but our build prefers gzip. We pick gzip.
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

/// Validate request preflight (content-type, te:trailers) and set the gRPC
/// response headers (content-type, grpc-accept-encoding). Returns the conn
/// ready to be marked for upgrade, or an error-shaped conn if preflight failed.
///
/// Called from generated `Handler::run` *after* path matching has confirmed
/// this request belongs to the service.
pub fn prepare_grpc_conn(conn: Conn, codec_suffix: &str) -> Result<Conn, Conn> {
    if !has_grpc_content_type(&conn) {
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
/// `Identity` (per spec). Unknown → `Unimplemented` so the client can pick
/// a different codec from `grpc-accept-encoding`.
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

fn has_grpc_content_type(conn: &Conn) -> bool {
    conn.request_headers()
        .get_str(KnownHeaderName::ContentType)
        .and_then(parse_grpc_content_type)
        .is_some()
}

async fn read_one_request<C, Req>(upgrade: &mut Upgrade, encoding: Encoding) -> Result<Req, Status>
where
    C: Codec<Req>,
    Req: Send + 'static,
{
    let mut stream =
        MessageStream::<Req, _>::new(upgrade, <C as Codec<Req>>::decode).with_encoding(encoding);
    match stream.next().await {
        Some(Ok(req)) => Ok(req),
        Some(Err(status)) => Err(status),
        None => Err(Status::invalid_argument("missing request message")),
    }
}

/// Per-request cancellation handle. Combines two signals:
///
/// 1. Connection-level shutdown via `Upgrade::swansong()` — fires on http
///    server shutdown or h2/h3 connection teardown. Surfaces as
///    `Status::cancelled`.
/// 2. Optional deadline parsed from the `grpc-timeout` header. Surfaces
///    as `Status::deadline_exceeded`.
///
/// The whole user closure is raced against both — including the user's
/// streaming awaits inside `RequestStream::next` / `Channel::recv` etc.
struct Cancellation {
    swansong: Swansong,
    deadline: Option<Deadline>,
}

#[derive(Clone)]
struct Deadline {
    runtime: Runtime,
    instant: Instant,
}

impl Cancellation {
    /// Capture the upgrade's swansong; if `grpc-timeout` is present, parse it
    /// and pair with a runtime handle. Errors only when the header is
    /// malformed (returned as `INVALID_ARGUMENT` to the caller).
    fn from_upgrade(upgrade: &Upgrade) -> Result<Self, Status> {
        let swansong = upgrade.swansong();
        let deadline = match upgrade.request_headers().get_str("grpc-timeout") {
            None => None,
            Some(header) => {
                let duration = parse_grpc_timeout(header).ok_or_else(|| {
                    Status::invalid_argument(format!("malformed grpc-timeout {header:?}"))
                })?;
                let runtime = upgrade
                    .shared_state()
                    .get::<Runtime>()
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

    /// Race a future against shutdown and (optionally) the deadline.
    async fn race<T, F>(&self, fut: F) -> Result<T, Status>
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
/// `grpc-accept-encoding` with `Encoding::ALL`. Walks `ALL` in build order
/// and returns the first non-Identity match — i.e. preference is gzip,
/// deflate, zstd, then identity. If the client doesn't send the header or
/// only accepts identity, we don't compress.
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
