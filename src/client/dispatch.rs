use crate::{
    Code, Codec, Encoding, Status,
    client::{ResponseStream, read_grpc_status, response_stream::race_against_deadline},
    frame::{
        reader::MessageStream,
        writer::{StreamBody, encode_frame},
    },
    server::content_type::parse_grpc_content_type,
    timeout::parse_grpc_timeout,
};
use futures_lite::{Stream, StreamExt};
use std::time::Instant;
use trillium::{Body, KnownHeaderName};
use trillium_client::{Conn, Version};
use trillium_http::Status as HttpStatus;

/// Client-side dispatch methods, available on any codec type via a blanket
/// impl. Generated code calls these as `Prost::unary_call(client, path, req)`
/// etc., which resolves through the trait without requiring a turbofish.
///
/// The `_call` suffix distinguishes these from the server-side `unary` /
/// `server_streaming` / etc. methods on the [`Server`](crate::Server) trait,
/// since both traits are blanket-implemented on every codec type.
#[allow(async_fn_in_trait)]
pub trait Client: Sized + 'static {
    /// Unary RPC: send one request, await one response.
    async fn unary_call<Req, Resp>(
        client: &trillium_client::Client,
        path: &str,
        req: Req,
    ) -> Result<Resp, Status>
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        let deadline = deadline_from_client(client);
        with_deadline(client, deadline, async {
            let conn = build_unary_conn::<Self, Req>(client, path, &req)?
                .await
                .map_err(transport_error)?;
            read_single_response::<Self, Resp>(conn).await
        })
        .await
    }

    /// Server-streaming RPC: send one request, return a stream of responses.
    async fn server_streaming_call<Req, Resp>(
        client: &trillium_client::Client,
        path: &str,
        req: Req,
    ) -> Result<ResponseStream<Self, Resp>, Status>
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
    {
        let deadline = deadline_from_client(client);
        with_deadline(client, deadline, async {
            let conn = build_unary_conn::<Self, Req>(client, path, &req)?
                .await
                .map_err(transport_error)?;
            read_streaming_response::<Self, Resp>(client, conn, deadline)
        })
        .await
    }

    /// Client-streaming RPC: send a stream of requests, await one response.
    async fn client_streaming_call<Req, Resp, S>(
        client: &trillium_client::Client,
        path: &str,
        requests: S,
    ) -> Result<Resp, Status>
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        S: Stream<Item = Req> + Send + 'static,
    {
        let deadline = deadline_from_client(client);
        with_deadline(client, deadline, async {
            let conn = build_streaming_conn::<Self, Req, S>(client, path, requests)
                .await
                .map_err(transport_error)?;
            read_single_response::<Self, Resp>(conn).await
        })
        .await
    }

    /// Bidirectional-streaming RPC: send a stream of requests, return a
    /// stream of responses. Both halves are duplexed concurrently — the
    /// request body is pulled by trillium-http's writer task while the
    /// response stream is pulled by the consumer.
    async fn bidi_call<Req, Resp, S>(
        client: &trillium_client::Client,
        path: &str,
        requests: S,
    ) -> Result<ResponseStream<Self, Resp>, Status>
    where
        Self: Codec<Req> + Codec<Resp>,
        Req: Send + 'static,
        Resp: Send + 'static,
        S: Stream<Item = Req> + Send + 'static,
    {
        let deadline = deadline_from_client(client);
        with_deadline(client, deadline, async {
            let conn = build_streaming_conn::<Self, Req, S>(client, path, requests)
                .await
                .map_err(transport_error)?;
            read_streaming_response::<Self, Resp>(client, conn, deadline)
        })
        .await
    }
}

impl<T: Sized + 'static> Client for T {}

/// Read exactly one response message and validate trailers. Used by unary
/// and client-streaming, which both expect a single response.
async fn read_single_response<C, Resp>(mut conn: Conn) -> Result<Resp, Status>
where
    C: Codec<Resp>,
    Resp: Send + 'static,
{
    validate_response_headers(&conn)?;

    if conn.response_headers().get_str("grpc-status").is_some() {
        return match Status::from_trailers(conn.response_headers()) {
            Ok(()) => Err(Status::internal("response missing message body")),
            Err(status) => Err(status),
        };
    }

    let encoding = extract_response_encoding(&conn)?;

    // Read one message AND drain to EOF so trillium-http populates
    // response_trailers. The second `next()` will see Ok(0) from the body
    // and return None, finalizing trailers; if it returns Some, the server
    // sent more than one message in violation of the contract.
    let (first, second) = {
        let body = conn.response_body();
        let mut messages = MessageStream::<C, Resp, _>::new(body).with_encoding(encoding);
        let first = messages.next().await;
        let second = messages.next().await;
        (first, second)
    };

    let response = match (first, second) {
        (Some(Ok(resp)), None) => resp,
        (Some(Ok(_)), Some(_)) => {
            return Err(Status::internal(
                "expected one response message, got multiple",
            ));
        }
        (Some(Err(status)), _) | (_, Some(Err(status))) => return Err(status),
        (None, _) => {
            return Err(read_grpc_status(&conn).err().unwrap_or_else(|| {
                Status::internal("response missing message body")
            }));
        }
    };

    read_grpc_status(&conn)?;
    Ok(response)
}

/// Validate response and hand off to a spawned ResponseStream. Used by
/// server-streaming and bidi. The deadline (if any) is forwarded into the
/// spawned reader so each message-poll is bounded.
fn read_streaming_response<C, Resp>(
    client: &trillium_client::Client,
    conn: Conn,
    deadline: Option<Instant>,
) -> Result<ResponseStream<C, Resp>, Status>
where
    C: Codec<Resp>,
    Resp: Send + 'static,
{
    validate_response_headers(&conn)?;

    if conn.response_headers().get_str("grpc-status").is_some() {
        return Ok(ResponseStream::trailers_only(Status::from_trailers(
            conn.response_headers(),
        )));
    }

    let encoding = extract_response_encoding(&conn)?;
    Ok(ResponseStream::spawn(client, conn, encoding, deadline))
}

/// Compute the per-call deadline from the client's `grpc-timeout` default
/// header. Returns `None` when no timeout is configured.
fn deadline_from_client(client: &trillium_client::Client) -> Option<Instant> {
    let header = client.default_headers().get_str("grpc-timeout")?;
    let duration = parse_grpc_timeout(header)?;
    Some(Instant::now() + duration)
}

/// Race `work` against `deadline`. Without a deadline, just runs `work`
/// to completion.
async fn with_deadline<T, F>(
    client: &trillium_client::Client,
    deadline: Option<Instant>,
    work: F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    match deadline {
        None => work.await,
        Some(deadline) => {
            let runtime = client.connector().runtime();
            race_against_deadline(&runtime, deadline, work).await
        }
    }
}

fn build_unary_conn<C, Req>(
    client: &trillium_client::Client,
    path: &str,
    req: &Req,
) -> Result<Conn, Status>
where
    C: Codec<Req>,
{
    let conn = grpc_request(client, path, <C as Codec<Req>>::content_type_suffix());
    let encoding = outbound_encoding(&conn);
    let body = encode_frame::<C, Req>(req, encoding)?;
    Ok(conn.with_body(body))
}

fn build_streaming_conn<C, Req, S>(
    client: &trillium_client::Client,
    path: &str,
    requests: S,
) -> Conn
where
    C: Codec<Req>,
    Req: Send + 'static,
    S: Stream<Item = Req> + Send + 'static,
{
    let conn = grpc_request(client, path, <C as Codec<Req>>::content_type_suffix());
    let encoding = outbound_encoding(&conn);
    // StreamBody expects Stream<Item = Result<T, Status>> because it
    // serves the server side too; map our infallible request stream into
    // the same shape. Body::new_streaming uses only the AsyncRead half, so
    // the trailers StreamBody produces internally are never read.
    let body = StreamBody::<C, Req, _>::new(requests.map(Ok::<_, Status>)).with_encoding(encoding);
    let body = Body::new_streaming(body, None);
    conn.with_body(body)
}

fn grpc_request(client: &trillium_client::Client, path: &str, suffix: &'static str) -> Conn {
    client
        .post(path)
        .with_http_version(Version::Http2)
        .with_request_header(
            KnownHeaderName::ContentType,
            format!("application/grpc+{suffix}"),
        )
        .with_request_header(KnownHeaderName::Te, "trailers")
        .with_request_header("grpc-accept-encoding", Encoding::accepted_encodings())
}

/// Resolve the inbound message encoding from the response's `grpc-encoding`.
/// Missing → `Identity`. Unknown → `Internal` (the server sent something we
/// can't decode despite advertising what we accept).
fn extract_response_encoding(conn: &Conn) -> Result<Encoding, Status> {
    match conn.response_headers().get_str("grpc-encoding") {
        None => Ok(Encoding::Identity),
        Some(s) => Encoding::from_grpc_encoding(s).ok_or_else(|| {
            Status::internal(format!("server returned unsupported grpc-encoding {s:?}"))
        }),
    }
}

/// Read the `grpc-encoding` header that
/// [`ServiceClientExt::set_outbound_compression`] stashed in the client's
/// default headers (which trillium-client copies into every new conn).
/// Missing or unrecognized → `Identity`.
fn outbound_encoding(conn: &Conn) -> Encoding {
    conn.request_headers()
        .get_str("grpc-encoding")
        .and_then(Encoding::from_grpc_encoding)
        .unwrap_or(Encoding::Identity)
}

fn validate_response_headers(conn: &Conn) -> Result<(), Status> {
    let http_status = conn.status();
    if http_status != Some(HttpStatus::Ok) {
        let n = http_status.map(|s| s as u16).unwrap_or(0);
        return Err(http_to_grpc_status(n));
    }

    let ct = conn.response_headers().get_str(KnownHeaderName::ContentType);
    if ct.and_then(parse_grpc_content_type).is_none() {
        return Err(Status::internal(format!(
            "unexpected response content-type: {ct:?}"
        )));
    }

    Ok(())
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

fn transport_error(err: trillium_client::Error) -> Status {
    Status::unavailable(format!("transport error: {err}"))
}
