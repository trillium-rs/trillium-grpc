use crate::{
    Codec, Encoding, Status,
    frame::{reader::MessageStream, writer::StreamBody},
    server::content_type::{has_te_trailers, parse_grpc_content_type},
    timeout::parse_grpc_timeout,
};
use futures_lite::{Stream, StreamExt, stream};
use std::{future::Future, time::Instant};
use trillium::{Body, Conn, Headers, KnownHeaderName, Status as HttpStatus, Swansong};
use trillium_server_common::Runtime;

/// Server-side dispatch methods, available on any codec type via a blanket
/// impl. Generated code calls these as `Prost::unary(conn, ...)` etc., which
/// resolves through the trait without requiring a turbofish.
///
/// `async fn` is used in trait position so the method bodies stay readable;
/// `Send`-ness of the returned future is inferred at the call site (where it
/// matters — the trillium `Handler` requires its run future to be `Send`).
#[allow(async_fn_in_trait)]
pub trait Server: Sized + 'static {
    /// Unary RPC: read exactly one request, await the user function, emit one
    /// response with `grpc-status` trailers.
    async fn unary<Req, Resp>(
        conn: Conn,
        f: impl AsyncFnOnce(Req) -> Result<Resp, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        unary_impl::<Self, Req, Resp>(conn, f).await
    }

    /// Server-streaming RPC: read one request, await the user function, then
    /// frame each response message from the returned stream.
    async fn server_streaming<Req, Resp, S>(
        conn: Conn,
        f: impl AsyncFnOnce(Req) -> Result<S, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        S: Stream<Item = Result<Resp, Status>> + Send + 'static,
    {
        server_streaming_impl::<Self, Req, Resp, S>(conn, f).await
    }

    /// Client-streaming RPC: hand the user a stream of decoded request
    /// messages, await the single response.
    async fn client_streaming<Req, Resp>(
        conn: Conn,
        f: impl AsyncFnOnce(BufferedRequestStream<Req>) -> Result<Resp, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        client_streaming_impl::<Self, Req, Resp>(conn, f).await
    }

    /// Bidirectional-streaming RPC: hand the user a request stream, frame each
    /// response message from the returned stream.
    async fn bidi<Req, Resp, S>(
        conn: Conn,
        f: impl AsyncFnOnce(BufferedRequestStream<Req>) -> Result<S, Status>,
    ) -> Conn
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        S: Stream<Item = Result<Resp, Status>> + Send + 'static,
    {
        bidi_impl::<Self, Req, Resp, S>(conn, f).await
    }
}

impl<T: Sized + 'static> Server for T {}

async fn unary_impl<C, Req, Resp>(
    conn: Conn,
    f: impl AsyncFnOnce(Req) -> Result<Resp, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let mut conn = match check_preflight(conn) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let encoding = match extract_request_encoding(&conn) {
        Ok(e) => e,
        Err(status) => {
            return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status)));
        }
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(d) => d,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    let response = cancellation
        .race(async {
            let req = read_one_request::<C, Req>(&mut conn, encoding).await?;
            f(req).await
        })
        .await;
    respond_with_stream::<C, Resp, _>(conn, stream::once(response))
}

async fn server_streaming_impl<C, Req, Resp, S>(
    conn: Conn,
    f: impl AsyncFnOnce(Req) -> Result<S, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    S: Stream<Item = Result<Resp, Status>> + Send + 'static,
{
    let mut conn = match check_preflight(conn) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let encoding = match extract_request_encoding(&conn) {
        Ok(e) => e,
        Err(status) => {
            return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status)));
        }
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(d) => d,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    let response_stream = cancellation
        .race(async {
            let req = read_one_request::<C, Req>(&mut conn, encoding).await?;
            f(req).await
        })
        .await;
    let response_stream = match response_stream {
        Ok(stream) => stream,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    respond_with_stream::<C, Resp, _>(conn, cancellation.wrap_stream(response_stream))
}

// Phase 3: the request body is read to completion *before* the user function
// runs (buffered). This is a pragmatic limitation of the current trillium-http
// body API — `Body::new_with_trailers` requires a `'static` body source, but
// `ReceivedBody` borrows the `Conn`. A future `Body::new_duplex` extension on
// trillium-http will unlock true streaming with no public-API change here.
async fn client_streaming_impl<C, Req, Resp>(
    conn: Conn,
    f: impl AsyncFnOnce(BufferedRequestStream<Req>) -> Result<Resp, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let mut conn = match check_preflight(conn) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let encoding = match extract_request_encoding(&conn) {
        Ok(e) => e,
        Err(status) => {
            return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status)));
        }
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(d) => d,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    let response = cancellation
        .race(async {
            let requests = read_all_requests::<C, Req>(&mut conn, encoding).await;
            f(BufferedRequestStream::new(requests)).await
        })
        .await;
    respond_with_stream::<C, Resp, _>(conn, stream::once(response))
}

async fn bidi_impl<C, Req, Resp, S>(
    conn: Conn,
    f: impl AsyncFnOnce(BufferedRequestStream<Req>) -> Result<S, Status>,
) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    S: Stream<Item = Result<Resp, Status>> + Send + 'static,
{
    let mut conn = match check_preflight(conn) {
        Ok(c) => c,
        Err(c) => return c,
    };
    let encoding = match extract_request_encoding(&conn) {
        Ok(e) => e,
        Err(status) => {
            return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status)));
        }
    };
    let cancellation = match Cancellation::from_conn(&conn) {
        Ok(d) => d,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    let response_stream = cancellation
        .race(async {
            let requests = read_all_requests::<C, Req>(&mut conn, encoding).await;
            f(BufferedRequestStream::new(requests)).await
        })
        .await;
    let response_stream = match response_stream {
        Ok(stream) => stream,
        Err(status) => return respond_with_stream::<C, Resp, _>(conn, stream::once(Err(status))),
    };

    respond_with_stream::<C, Resp, _>(conn, cancellation.wrap_stream(response_stream))
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

/// A buffered stream of decoded request messages handed to client-streaming /
/// bidi user closures. Backed by a `Vec` for now; will be swappable for a
/// true streaming reader once `trillium-http` exposes a duplex body API.
pub struct BufferedRequestStream<T> {
    items: std::vec::IntoIter<Result<T, Status>>,
}

impl<T> BufferedRequestStream<T> {
    fn new(items: Vec<Result<T, Status>>) -> Self {
        Self {
            items: items.into_iter(),
        }
    }
}

impl<T: Unpin> Stream for BufferedRequestStream<T> {
    type Item = Result<T, Status>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.items.next())
    }
}

/// Validate request preflight (content-type, te:trailers) on a fresh request.
/// Returns `Ok(conn)` when the request is well-formed, `Err(conn)` when the
/// caller should return immediately (the conn carries the appropriate HTTP
/// error status).
fn check_preflight(conn: Conn) -> Result<Conn, Conn> {
    if !has_grpc_content_type(&conn) {
        return Err(conn.with_status(HttpStatus::UnsupportedMediaType).halt());
    }
    if !has_te_trailers(conn.request_headers()) {
        return Err(conn.with_status(HttpStatus::BadRequest).halt());
    }
    Ok(conn)
}

/// Resolve the inbound message encoding from `grpc-encoding`. Missing →
/// `Identity` (per spec). Unknown → `Unimplemented` so the client can pick
/// a different codec from `grpc-accept-encoding`.
fn extract_request_encoding(conn: &Conn) -> Result<Encoding, Status> {
    match conn.request_headers().get_str("grpc-encoding") {
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

async fn read_one_request<C, Req>(conn: &mut Conn, encoding: Encoding) -> Result<Req, Status>
where
    C: Codec<Req>,
    Req: Send + 'static,
{
    let body = conn.request_body();
    let mut stream = MessageStream::<C, Req, _>::new(body).with_encoding(encoding);
    match stream.next().await {
        Some(Ok(req)) => Ok(req),
        Some(Err(status)) => Err(status),
        None => Err(Status::invalid_argument("missing request message")),
    }
}

async fn read_all_requests<C, Req>(
    conn: &mut Conn,
    encoding: Encoding,
) -> Vec<Result<Req, Status>>
where
    C: Codec<Req>,
    Req: Send + 'static,
{
    let body = conn.request_body();
    let mut stream = MessageStream::<C, Req, _>::new(body).with_encoding(encoding);
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let stop = item.is_err();
        out.push(item);
        if stop {
            break;
        }
    }
    out
}

fn respond_with_stream<C, Resp, S>(conn: Conn, stream: S) -> Conn
where
    C: Codec<Resp>,
    Resp: Send + 'static,
    S: Stream<Item = Result<Resp, Status>> + Send + 'static,
{
    let response_encoding = negotiate_response_encoding(conn.request_headers());
    let suffix = <C as Codec<Resp>>::content_type_suffix();
    let content_type = format!("application/grpc+{suffix}");
    let body = Body::new_with_trailers(
        StreamBody::<C, Resp, _>::new(stream).with_encoding(response_encoding),
        None,
    );
    let mut conn = conn
        .with_response_header(KnownHeaderName::ContentType, content_type)
        .with_response_header("grpc-accept-encoding", Encoding::accepted_encodings())
        .with_status(HttpStatus::Ok)
        .with_body(body);
    if !matches!(response_encoding, Encoding::Identity) {
        conn.response_headers_mut()
            .insert("grpc-encoding", response_encoding.as_grpc_encoding());
    }
    conn.halt()
}

/// Per-request cancellation handle. Combines two signals:
///
/// 1. Connection-level shutdown via `conn.swansong()` — fires on http
///    server shutdown or h2/h3 connection teardown. Surfaces as
///    `Status::cancelled`.
/// 2. Optional deadline parsed from the `grpc-timeout` header. Surfaces
///    as `Status::deadline_exceeded`.
///
/// Every awaitable in the dispatch pipeline (request read, user closure,
/// each response-stream item) is raced against both.
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
    /// Capture the conn's swansong; if `grpc-timeout` is present, parse it
    /// and pair with a runtime handle. Errors only when the header is
    /// malformed (returned as `INVALID_ARGUMENT` to the caller).
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

    /// Race a future against shutdown and (optionally) the deadline.
    /// `Swansong::interrupt` handles the shutdown leg; we layer a deadline
    /// timer on top via `futures_lite::future::or` (rather than
    /// `Runtime::timeout`, which requires `Fut: Send` — dispatch is
    /// intentionally agnostic to the user closure's `Send`-ness).
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

    /// Wrap a response stream so each `next()` poll is bounded by the
    /// deadline (yielding a terminal `Err(DEADLINE_EXCEEDED)` on expiry)
    /// and by shutdown (cleanly ending the stream via
    /// `Swansong::interrupt`).
    ///
    /// Note: shutdown-cut streams end with `Poll::Ready(None)`, which
    /// [`StreamBody`](crate::frame::writer::StreamBody) encodes as
    /// `grpc-status: 0`. Peers see a partial-but-OK stream rather than
    /// `CANCELLED`. Adequate for graceful shutdown; revisit if we need
    /// strict CANCELLED semantics.
    fn wrap_stream<Resp, S>(
        &self,
        stream: S,
    ) -> impl Stream<Item = Result<Resp, Status>> + Send + 'static
    where
        S: Stream<Item = Result<Resp, Status>> + Send + 'static,
        Resp: Send + 'static,
    {
        let deadline = self.deadline.clone();
        let with_deadline = stream::unfold(
            (Box::pin(stream), deadline, false),
            |(mut stream, deadline, expired)| async move {
                if expired {
                    return None;
                }
                match deadline.as_ref() {
                    None => stream
                        .next()
                        .await
                        .map(|item| (item, (stream, deadline, false))),
                    Some(d) => match d.instant.checked_duration_since(Instant::now()) {
                        None => Some((
                            Err(Status::deadline_exceeded("deadline elapsed")),
                            (stream, deadline, true),
                        )),
                        Some(remaining) => match d.runtime.timeout(remaining, stream.next()).await {
                            Some(Some(item)) => Some((item, (stream, deadline, false))),
                            Some(None) => None,
                            None => Some((
                                Err(Status::deadline_exceeded("deadline elapsed")),
                                (stream, deadline, true),
                            )),
                        },
                    },
                }
            },
        );
        self.swansong.interrupt(with_deadline)
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
