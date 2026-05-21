use crate::{
    Code, Codec, Encoding, Status,
    client::{ResponseStream, response_stream::race_against_deadline},
    frame::{reader::MessageStream, writer::encode_frame},
    server::content_type::parse_grpc_content_type,
    timeout::parse_grpc_timeout,
};
use futures_lite::{AsyncWriteExt, Stream, StreamExt};
use std::time::Instant;
use trillium::{KnownHeaderName, Transport};
use trillium_client::{Conn, ConnExt, Version};
use trillium_http::{Status as HttpStatus, Upgrade as HttpUpgrade};

type Upgrade = HttpUpgrade<Box<dyn Transport>>;

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
        with_deadline(
            client,
            deadline,
            unary_call_impl::<Self, Req, Resp>(client, path, req),
        )
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
        with_deadline(
            client,
            deadline,
            server_streaming_call_impl::<Self, Req, Resp>(client, path, req, deadline),
        )
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
        with_deadline(
            client,
            deadline,
            client_streaming_call_impl::<Self, Req, Resp, S>(client, path, requests),
        )
        .await
    }

    /// Bidirectional-streaming RPC: send a stream of requests, return a
    /// stream of responses.
    ///
    /// Currently the request stream is fully drained before the response
    /// stream begins. True concurrent duplex on the client is a follow-up.
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
        with_deadline(
            client,
            deadline,
            bidi_call_impl::<Self, Req, Resp, S>(client, path, requests, deadline),
        )
        .await
    }
}

impl<T: Sized + 'static> Client for T {}

async fn unary_call_impl<C, Req, Resp>(
    client: &trillium_client::Client,
    path: &str,
    req: Req,
) -> Result<Resp, Status>
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let outbound_encoding = outbound_encoding_from_client(client);
    let frame = encode_frame::<C, Req>(&req, outbound_encoding)?;
    let (mut upgrade, response_encoding) = open_upgrade::<C, Req>(client, path)
        .await?
        .into_streaming()?;

    write_frame(&mut upgrade, &frame).await?;
    close_outbound(&mut upgrade).await?;

    let response = read_one_response::<C, Resp>(&mut upgrade, response_encoding).await?;
    finish_with_trailers(&upgrade)?;
    Ok(response)
}

async fn server_streaming_call_impl<C, Req, Resp>(
    client: &trillium_client::Client,
    path: &str,
    req: Req,
    deadline: Option<Instant>,
) -> Result<ResponseStream<C, Resp>, Status>
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let outbound_encoding = outbound_encoding_from_client(client);
    let frame = encode_frame::<C, Req>(&req, outbound_encoding)?;
    let opened = open_upgrade::<C, Req>(client, path).await?;
    let response_stream = match opened {
        OpenUpgrade::Streaming(mut upgrade, response_encoding) => {
            write_frame(&mut upgrade, &frame).await?;
            close_outbound(&mut upgrade).await?;
            ResponseStream::spawn(client, upgrade, response_encoding, deadline)
        }
        OpenUpgrade::TrailersOnly(result) => ResponseStream::trailers_only(result),
    };
    Ok(response_stream)
}

async fn client_streaming_call_impl<C, Req, Resp, S>(
    client: &trillium_client::Client,
    path: &str,
    requests: S,
) -> Result<Resp, Status>
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    S: Stream<Item = Req> + Send + 'static,
{
    let outbound_encoding = outbound_encoding_from_client(client);
    let (mut upgrade, response_encoding) = open_upgrade::<C, Req>(client, path)
        .await?
        .into_streaming()?;

    write_request_stream::<C, Req, S>(&mut upgrade, requests, outbound_encoding).await?;
    close_outbound(&mut upgrade).await?;

    let response = read_one_response::<C, Resp>(&mut upgrade, response_encoding).await?;
    finish_with_trailers(&upgrade)?;
    Ok(response)
}

async fn bidi_call_impl<C, Req, Resp, S>(
    client: &trillium_client::Client,
    path: &str,
    requests: S,
    deadline: Option<Instant>,
) -> Result<ResponseStream<C, Resp>, Status>
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send + 'static,
    S: Stream<Item = Req> + Send + 'static,
{
    let outbound_encoding = outbound_encoding_from_client(client);
    let opened = open_upgrade::<C, Req>(client, path).await?;
    let response_stream = match opened {
        OpenUpgrade::Streaming(mut upgrade, response_encoding) => {
            write_request_stream::<C, Req, S>(&mut upgrade, requests, outbound_encoding).await?;
            close_outbound(&mut upgrade).await?;
            ResponseStream::spawn(client, upgrade, response_encoding, deadline)
        }
        OpenUpgrade::TrailersOnly(result) => ResponseStream::trailers_only(result),
    };
    Ok(response_stream)
}

/// Result of awaiting an Upgrade-marked conn. Either the upgrade is live
/// (body + trailers to follow) or the server returned a trailers-only
/// response (HEADERS+END_STREAM with `grpc-status` in the initial frame).
enum OpenUpgrade {
    Streaming(Upgrade, Encoding),
    TrailersOnly(Result<(), Status>),
}

impl OpenUpgrade {
    /// Force the streaming variant. Used by unary + client-streaming which
    /// always expect a body. Trailers-only responses bypass the upgrade,
    /// surfacing as a Status error.
    fn into_streaming(self) -> Result<(Upgrade, Encoding), Status> {
        match self {
            OpenUpgrade::Streaming(u, enc) => Ok((u, enc)),
            OpenUpgrade::TrailersOnly(Ok(())) => {
                Err(Status::internal("response missing message body"))
            }
            OpenUpgrade::TrailersOnly(Err(status)) => Err(status),
        }
    }
}

async fn open_upgrade<C, Req>(
    client: &trillium_client::Client,
    path: &str,
) -> Result<OpenUpgrade, Status>
where
    C: Codec<Req>,
{
    let conn = grpc_request(client, path, <C as Codec<Req>>::content_type_suffix())
        .upgrade()
        .await
        .map_err(transport_error)?;

    validate_response_headers(&conn)?;
    let response_encoding = extract_response_encoding(&conn)?;

    if conn.response_headers().get_str("grpc-status").is_some() {
        return Ok(OpenUpgrade::TrailersOnly(Status::from_trailers(
            conn.response_headers(),
        )));
    }

    let upgrade: Upgrade = conn.into();
    Ok(OpenUpgrade::Streaming(upgrade, response_encoding))
}

async fn write_frame(upgrade: &mut Upgrade, frame: &[u8]) -> Result<(), Status> {
    upgrade
        .write_all(frame)
        .await
        .map_err(|e| Status::unavailable(format!("write error: {e}")))
}

/// Signal end-of-request-body to the server by closing the upgrade's write
/// side (which translates to an END_STREAM flag on the h2 stream). We have
/// no trailers to send, so this is the only way to mark "no more requests
/// coming" without dropping the upgrade entirely (we still need to read the
/// response).
async fn close_outbound(upgrade: &mut Upgrade) -> Result<(), Status> {
    upgrade
        .close()
        .await
        .map_err(|e| Status::unavailable(format!("close error: {e}")))
}

async fn write_request_stream<C, Req, S>(
    upgrade: &mut Upgrade,
    requests: S,
    outbound_encoding: Encoding,
) -> Result<(), Status>
where
    C: Codec<Req>,
    Req: Send + 'static,
    S: Stream<Item = Req> + Send + 'static,
{
    let mut requests = Box::pin(requests);
    while let Some(req) = requests.next().await {
        let frame = encode_frame::<C, Req>(&req, outbound_encoding)?;
        write_frame(upgrade, &frame).await?;
    }
    Ok(())
}

/// Read exactly one response message and confirm the body ended cleanly.
/// The second `next()` call drives the reader to EOF so trillium-http can
/// populate `received_trailers()`.
async fn read_one_response<C, Resp>(
    upgrade: &mut Upgrade,
    encoding: Encoding,
) -> Result<Resp, Status>
where
    C: Codec<Resp>,
    Resp: Send + 'static,
{
    let (first, second) = {
        let mut messages = MessageStream::<Resp, _>::new(&mut *upgrade, <C as Codec<Resp>>::decode)
            .with_encoding(encoding);
        let first = messages.next().await;
        let second = messages.next().await;
        (first, second)
    };

    match (first, second) {
        (Some(Ok(resp)), None) => Ok(resp),
        (Some(Ok(_)), Some(_)) => Err(Status::internal(
            "expected one response message, got multiple",
        )),
        (Some(Err(status)), _) | (_, Some(Err(status))) => Err(status),
        (None, _) => Err(finish_with_trailers(upgrade)
            .err()
            .unwrap_or_else(|| Status::internal("response missing message body"))),
    }
}

/// Inspect the trailers populated by trillium-http after read-to-EOF.
fn finish_with_trailers(upgrade: &Upgrade) -> Result<(), Status> {
    match upgrade.received_trailers() {
        Some(trailers) => Status::from_trailers(trailers),
        None => Err(Status::internal("stream ended without grpc-status trailer")),
    }
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

fn grpc_request(client: &trillium_client::Client, path: &str, suffix: &'static str) -> Conn {
    // TEMP: read a version override from a default-header marker so tests can
    // opt into h3 without a public ServiceClientExt method. If we ship h3
    // support this gets a real opt-in (set_http3 etc.) and this falls away.
    let version = match client.default_headers().get_str("x-trillium-grpc-version") {
        Some("h3") => Version::Http3,
        _ => Version::Http2,
    };
    client
        .post(path)
        .with_http_version(version)
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
/// default headers. Missing or unrecognized → `Identity`.
fn outbound_encoding_from_client(client: &trillium_client::Client) -> Encoding {
    client
        .default_headers()
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

    let ct = conn
        .response_headers()
        .get_str(KnownHeaderName::ContentType);
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
