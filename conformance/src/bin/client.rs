//! Conformance client-under-test harness.
//!
//! The `connectconformance` runner writes one length-prefixed
//! [`ClientCompatRequest`] per test case to our stdin, each describing an RPC to
//! issue against the runner's reference server. We issue it through a
//! [`GrpcClientConn`], observe the result, and write back a length-prefixed
//! [`ClientCompatResponse`]. Messages are framed with a 4-byte big-endian length
//! prefix (matching the conformance protoDecoder/protoEncoder), same as the
//! server harness.
//!
//! We only speak gRPC over h2c with the protobuf codec; the accompanying config
//! YAML restricts the runner to that flavor.
//!
//! [`ClientCompatRequest`]: trillium_grpc_conformance::pb::ClientCompatRequest
//! [`ClientCompatResponse`]: trillium_grpc_conformance::pb::ClientCompatResponse

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD as BASE64};
use prost::Message;
use prost_types::Any;
use std::{
    io::{self, Read, Write},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use trillium::Headers;
use trillium_client::Client;
use trillium_grpc::{GrpcClientConn, Metadata, Prost};
use trillium_grpc_conformance::pb::{
    self, ClientCompatRequest, ClientCompatResponse, ClientResponseResult, ConformancePayload,
    StreamType, client_compat_request::cancel::CancelTiming, client_compat_response,
};

const SERVICE: &str = "connectrpc.conformance.v1.ConformanceService";
const TYPE_PREFIX: &str = "connectrpc.conformance.v1.";

/// A minimal `google.rpc.Status`, as carried in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, ::prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

fn read_delimited<M: Message + Default>(input: &mut impl Read) -> io::Result<M> {
    let mut len_buf = [0u8; 4];
    input.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    input.read_exact(&mut buf)?;
    M::decode(&buf[..]).map_err(io::Error::other)
}

fn write_delimited(output: &mut impl Write, msg: &impl Message) -> io::Result<()> {
    let bytes = msg.encode_to_vec();
    output.write_all(&(bytes.len() as u32).to_be_bytes())?;
    output.write_all(&bytes)?;
    output.flush()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    env_logger::init();

    // Process each case on its own task: the runner allows out-of-order results,
    // and concurrency keeps one slow/hung RPC from blocking every case behind it.
    // Completed tasks write their response immediately; a hung task only ever
    // fails its own case (the runner times it out and SIGTERMs us at the end).
    //
    // Bound in-flight cases with a semaphore: the suite has 400+ cases, and each
    // opens its own client/connection to the reference server. Letting all of
    // them race at once swamps the server and produces spurious, rotating
    // failures (timeouts, resets); a modest cap keeps the run stable while still
    // pipelining enough to stay fast and isolate hangs.
    const MAX_IN_FLIGHT: usize = 32;
    let limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT));

    let mut tasks = Vec::new();
    loop {
        let request: ClientCompatRequest = match read_delimited(&mut io::stdin().lock()) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let permit = Arc::clone(&limit);
        tasks.push(tokio::spawn(async move {
            // Held for the duration of the case; released on completion.
            let _permit = permit.acquire_owned().await.expect("semaphore open");
            let test_name = request.test_name.clone();
            let result = handle(request).await;
            let response = ClientCompatResponse {
                test_name,
                result: Some(result),
            };
            // io::stdout()'s lock serializes concurrent writes; each frame is
            // written + flushed atomically under it, so they never interleave.
            let _ = write_delimited(&mut io::stdout().lock(), &response);
        }));
    }

    for task in tasks {
        let _ = task.await;
    }
    Ok(())
}

/// A failure that prevented issuing the RPC at all (bad request, unknown method).
fn client_error(message: impl Into<String>) -> client_compat_response::Result {
    client_compat_response::Result::Error(pb::ClientErrorResult {
        message: message.into(),
    })
}

/// Build a TLS connector that trusts the runner's self-signed server cert and
/// advertises ALPN `h2`. Layered over the plain tokio TCP connector.
fn tls_connector(
    server_cert_pem: &[u8],
) -> trillium_rustls::RustlsConfig<trillium_tokio::ClientConfig> {
    use std::sync::Arc;
    use trillium_rustls::rustls::{
        ClientConfig, RootCertStore, crypto::aws_lc_rs, pki_types::CertificateDer,
    };

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(server_cert_pem))
            .collect::<Result<_, _>>()
            .expect("server_tls_cert is valid PEM");
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certs);

    let mut config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];

    trillium_rustls::RustlsConfig::new(config, trillium_tokio::ClientConfig::default())
}

/// Dispatch one conformance request to the appropriate typed call.
async fn handle(req: ClientCompatRequest) -> client_compat_response::Result {
    let service = req.service.as_deref().unwrap_or(SERVICE);
    if service != SERVICE {
        return client_error(format!("unknown service {service}"));
    }

    let stream_type = req.stream_type();
    let method = req
        .method
        .clone()
        .unwrap_or_else(|| default_method(stream_type).to_string());

    // TLS when the runner hands us the server's self-signed cert; plain h2c otherwise.
    // GrpcClientConn pins HTTP/2, so over TLS the connection is h2-over-TLS.
    let mut client = if req.server_tls_cert.is_empty() {
        Client::new(trillium_tokio::ClientConfig::default())
            .with_base(format!("http://{}:{}", req.host, req.port))
    } else {
        Client::new(tls_connector(&req.server_tls_cert))
            .with_base(format!("https://{}:{}", req.host, req.port))
    };
    // Honor the requested outbound compression — GrpcClientConn reads it from the
    // client's configured grpc-encoding.
    if req.compression() == pb::Compression::Gzip {
        client.default_headers_mut().insert("grpc-encoding", "gzip");
    }
    let path = format!("/{SERVICE}/{method}");
    let full_duplex = stream_type == StreamType::FullDuplexBidiStream;

    // Per method: (single request message?, single response message?).
    macro_rules! drive_method {
        ($Req:ty, $Resp:ty, $short:literal, $single_req:expr, $single_resp:expr, $extract:expr) => {
            drive::<$Req, $Resp>(
                client,
                path,
                req,
                $short,
                $single_req,
                $single_resp,
                full_duplex,
                $extract,
            )
            .await
        };
    }

    match method.as_str() {
        "Unary" => {
            drive_method!(
                pb::UnaryRequest,
                pb::UnaryResponse,
                "UnaryRequest",
                true,
                true,
                |r| { r.payload }
            )
        }
        "IdempotentUnary" => drive_method!(
            pb::IdempotentUnaryRequest,
            pb::IdempotentUnaryResponse,
            "IdempotentUnaryRequest",
            true,
            true,
            |r| r.payload
        ),
        // Unimplemented always errors; its response carries no payload.
        "Unimplemented" => drive_method!(
            pb::UnimplementedRequest,
            pb::UnimplementedResponse,
            "UnimplementedRequest",
            true,
            true,
            |_r| None
        ),
        "ClientStream" => drive_method!(
            pb::ClientStreamRequest,
            pb::ClientStreamResponse,
            "ClientStreamRequest",
            false,
            true,
            |r| r.payload
        ),
        "ServerStream" => drive_method!(
            pb::ServerStreamRequest,
            pb::ServerStreamResponse,
            "ServerStreamRequest",
            true,
            false,
            |r| r.payload
        ),
        "BidiStream" => drive_method!(
            pb::BidiStreamRequest,
            pb::BidiStreamResponse,
            "BidiStreamRequest",
            false,
            false,
            |r| r.payload
        ),
        other => client_error(format!("unknown method {other}")),
    }
}

fn default_method(stream_type: StreamType) -> &'static str {
    match stream_type {
        StreamType::ClientStream => "ClientStream",
        StreamType::ServerStream => "ServerStream",
        StreamType::HalfDuplexBidiStream | StreamType::FullDuplexBidiStream => "BidiStream",
        _ => "Unary",
    }
}

/// Issue one RPC and build its [`ClientResponseResult`]. Generic over the
/// request/response types; `resp_to_payload` extracts the (uniform) payload
/// field from each response message.
async fn drive<Req, Resp>(
    client: Client,
    path: String,
    req: ClientCompatRequest,
    req_type_short: &str,
    single_request: bool,
    single_response: bool,
    full_duplex: bool,
    resp_to_payload: fn(Resp) -> Option<ConformancePayload>,
) -> client_compat_response::Result
where
    Req: Message + Default + Send + 'static,
    Resp: Message + Default + Send + 'static,
{
    // Decode the request messages out of their `Any` wrappers.
    let expected = format!("{TYPE_PREFIX}{req_type_short}");
    let mut messages = Vec::with_capacity(req.request_messages.len());
    for any in &req.request_messages {
        let type_name = any.type_url.rsplit('/').next().unwrap_or(&any.type_url);
        if type_name != expected {
            return client_error(format!("expected request type {expected}, got {type_name}"));
        }
        match Req::decode(any.value.as_slice()) {
            Ok(m) => messages.push(m),
            Err(e) => return client_error(format!("failed to decode request: {e}")),
        }
    }
    if single_request && messages.len() != 1 {
        return client_error(format!(
            "expected exactly one request message, got {}",
            messages.len()
        ));
    }

    let total = messages.len();
    let metadata = metadata_from_headers(&req.request_headers);
    let timeout = req.timeout_ms.map(|ms| Duration::from_millis(ms as u64));
    let request_delay = Duration::from_millis(req.request_delay_ms as u64);
    let cancel = req.cancel.and_then(|c| c.cancel_timing);
    let runtime = client.connector().runtime();

    let mut conn =
        GrpcClientConn::<Req, Resp>::new::<Prost>(&client, &path, metadata, timeout, full_duplex);
    let cancel_handle = conn.cancel_handle();

    let mut payloads = Vec::new();
    let mut error = None;
    let mut num_unsent = 0;
    // Set once the response stream ends (clean EOF, an error, or a send failure)
    // — no more responses to read.
    let mut done = false;

    // Receive one response message, recording the payload (an empty message
    // still counts as one empty payload) or the terminal error. Returns true
    // when the stream is finished.
    macro_rules! recv_one {
        () => {
            match conn.recv().await {
                Ok(Some(resp)) => {
                    payloads.push(resp_to_payload(resp).unwrap_or_default());
                    if let Some(CancelTiming::AfterNumResponses(n)) = cancel
                        && payloads.len() as u32 == n
                    {
                        cancel_handle.cancel();
                    }
                    false
                }
                Ok(None) => true,
                Err(status) => {
                    // grpc-status-details-bin lives on the raw trailers (Metadata
                    // reserves grpc-* keys), so read details from there.
                    error = Some(rpc_error(status, conn.trailers()));
                    true
                }
            }
        };
    }

    // Send each request message (delaying first). For full-duplex, receive one
    // response after each send (the first recv flushes the prelude + upgrades,
    // later sends write live) — that interleaving is what lets after-N-responses
    // and before/after-close-send cancellation observe responses.
    for (i, message) in messages.into_iter().enumerate() {
        if !request_delay.is_zero() {
            runtime.delay(request_delay).await;
        }
        if let Err(status) = conn.send(message).await {
            num_unsent = (total - i) as i32;
            error = Some(rpc_error(status, conn.trailers()));
            done = true;
            break;
        }
        if full_duplex && recv_one!() {
            done = true;
            break;
        }
    }

    if !done {
        if matches!(cancel, Some(CancelTiming::BeforeCloseSend(()))) {
            // Cancel instead of closing the send side.
            cancel_handle.cancel();
        } else {
            let _ = conn.close_send().await;
        }

        // Cancel a fixed time after closing the send side.
        if let Some(CancelTiming::AfterCloseSendMs(ms)) = cancel {
            if single_response {
                // The single response is fast and already buffered by close_send,
                // so a concurrently-scheduled cancel never wins the read. Apply it
                // inline before recv: the call must report Cancelled with no
                // payloads, which is exactly what the runner expects here.
                runtime.delay(Duration::from_millis(ms as u64)).await;
                cancel_handle.cancel();
            } else {
                // Streaming: cancel concurrently so the read loop can still observe
                // responses that arrive during the delay window. trillium spawns
                // detach on drop, so the handle dropped here keeps running.
                let handle = cancel_handle.clone();
                let rt = runtime.clone();
                runtime.clone().spawn(async move {
                    rt.delay(Duration::from_millis(ms as u64)).await;
                    handle.cancel();
                });
            }
        }

        // Drain any remaining responses.
        while !recv_one!() {}
    }

    // Unary and client-stream must yield exactly one response message; anything
    // else (with no RPC error) is a protocol violation the client reports as
    // unimplemented, matching the reference client. The partial messages are not
    // a valid result, so they're discarded (the runner expects zero payloads).
    if error.is_none() && single_response && payloads.len() != 1 {
        error = Some(pb::Error {
            code: trillium_grpc::Code::Unimplemented as u8 as i32,
            message: Some(format!(
                "expected exactly one response message, got {}",
                payloads.len()
            )),
            details: Vec::new(),
        });
        payloads.clear();
    }

    client_compat_response::Result::Response(ClientResponseResult {
        response_headers: headers_to_proto(conn.headers()),
        payloads,
        error,
        response_trailers: headers_to_proto(conn.trailers()),
        num_unsent_requests: num_unsent,
        http_status_code: None,
        feedback: Vec::new(),
    })
}

/// Build request metadata from the conformance request headers. These are
/// custom metadata only; transport headers are added by the conn itself.
fn metadata_from_headers(headers: &[pb::Header]) -> Metadata {
    let mut metadata = Metadata::new();
    for header in headers {
        // HTTP/2 header names are lowercase; the runner may hand us canonical
        // case (e.g. "X-Conformance-Test"), so normalize before inserting (our
        // Metadata enforces lowercase keys).
        let name = header.name.to_ascii_lowercase();
        for value in &header.value {
            if name.ends_with("-bin") {
                // `-bin` values arrive base64-encoded (unpadded); decode to bytes
                // so Metadata re-encodes them consistently on the wire.
                if let Ok(bytes) = BASE64.decode(value) {
                    let _ = metadata.insert_binary(&name, bytes);
                }
            } else {
                let _ = metadata.insert_ascii(&name, value);
            }
        }
    }
    metadata
}

/// Build a conformance `Error` from a [`Status`](trillium_grpc::Status) and the
/// raw trailers it rode in on (for `grpc-status-details-bin` details).
fn rpc_error(status: trillium_grpc::Status, trailers: Option<&Headers>) -> pb::Error {
    pb::Error {
        code: status.code as u8 as i32,
        message: Some(status.message),
        details: trailers.map(error_details).unwrap_or_default(),
    }
}

/// Recover the structured error details from a `grpc-status-details-bin` trailer
/// (a base64-encoded `google.rpc.Status`).
fn error_details(headers: &Headers) -> Vec<Any> {
    headers
        .get_str("grpc-status-details-bin")
        .and_then(|v| BASE64.decode(v).ok())
        .and_then(|bytes| RpcStatus::decode(bytes.as_slice()).ok())
        .map(|s| s.details)
        .unwrap_or_default()
}

/// Convert observed response metadata to conformance `Header`s, dropping the
/// transport/protocol headers that aren't part of the application metadata.
fn headers_to_proto(headers: Option<&Headers>) -> Vec<pb::Header> {
    let Some(headers) = headers else {
        return Vec::new();
    };
    headers
        .iter()
        .filter_map(|(name, values)| {
            let name = name.to_string();
            if is_transport_header(&name) {
                return None;
            }
            let value: Vec<String> = values
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            (!value.is_empty()).then_some(pb::Header { name, value })
        })
        .collect()
}

fn is_transport_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "content-type"
            | "grpc-encoding"
            | "grpc-accept-encoding"
            | "grpc-status"
            | "grpc-message"
            | "grpc-status-details-bin"
            | "grpc-timeout"
    )
}
