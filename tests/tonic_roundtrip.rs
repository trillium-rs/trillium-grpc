//! End-to-end conformance smoke test: a tonic-generated client speaks to our
//! `trillium-grpc` server over h2c. If tonic — a known-good gRPC implementation
//! — can complete an RPC against us, we are framing, header-setting, and
//! trailer-emitting per spec.
//!
//! Covers all four call shapes: unary, server-streaming, client-streaming, bidi.
//!
//! Server side uses the codegen output committed at `tests/generated/greeter_v1.rs`;
//! client side uses tonic's own generated stub. Both are prost-generated from
//! the same .proto, so wire compatibility flows from that — they don't share
//! Rust types.

mod proto {
    include!("proto/gen/greeter.v1.rs");
}

#[allow(dead_code)] // committed codegen output; not every RPC is exercised here
mod greeter_v1 {
    include!("generated/greeter_v1.rs");
}

use crate::greeter_v1::{Greeter, GreeterServer, HelloReply, HelloRequest};
use crate::proto::greeter_client::GreeterClient;
use futures_lite::StreamExt;
use trillium_grpc::{Channel, RequestStream, ResponseSink, Status};

struct MyGreeter;

impl Greeter for MyGreeter {
    async fn say_hello(&self, req: HelloRequest) -> Result<HelloReply, Status> {
        Ok(HelloReply {
            message: format!("Hello, {}", req.name),
        })
    }

    async fn say_hello_stream(
        &self,
        req: HelloRequest,
        mut responses: ResponseSink<'_, HelloReply>,
    ) -> Result<(), Status> {
        for i in 1..=3 {
            responses
                .send(HelloReply {
                    message: format!("Hello {i}, {}", req.name),
                })
                .await?;
        }
        Ok(())
    }

    async fn say_hello_many(
        &self,
        mut reqs: RequestStream<'_, HelloRequest>,
    ) -> Result<HelloReply, Status> {
        let mut names = Vec::new();
        while let Some(req) = reqs.next().await {
            names.push(req?.name);
        }
        Ok(HelloReply {
            message: format!("Hello, {}", names.join(" and ")),
        })
    }

    async fn say_hello_chat(
        &self,
        mut channel: Channel<'_, HelloRequest, HelloReply>,
    ) -> Result<(), Status> {
        while let Some(req) = channel.recv().await {
            let req = req?;
            channel
                .send(HelloReply {
                    message: format!("Hi back, {}", req.name),
                })
                .await?;
        }
        Ok(())
    }
}

macro_rules! start_server {
    () => {{
        let server = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(GreeterServer::new(MyGreeter));
        let port = server.info().await.tcp_socket_addr().unwrap().port();
        (server, port)
    }};
}

async fn connect(port: u16) -> GreeterClient<tonic::transport::Channel> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).unwrap();
    GreeterClient::connect(endpoint).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_roundtrip_against_tonic_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let mut client = connect(port).await;

    let response = client
        .say_hello(tonic::Request::new(proto::HelloRequest {
            name: "world".into(),
        }))
        .await
        .unwrap();

    assert_eq!(response.into_inner().message, "Hello, world");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_with_gzip_compressed_request() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let mut client = connect(port)
        .await
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);

    // Repeat the name so the message is well above gzip's overhead floor
    // and we're confident the wire payload is genuinely compressed.
    let name = "world ".repeat(200);
    let response = client
        .say_hello(tonic::Request::new(proto::HelloRequest {
            name: name.clone(),
        }))
        .await
        .unwrap();

    assert_eq!(response.into_inner().message, format!("Hello, {name}"));

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_auto_compresses_response_when_client_accepts_gzip() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    // Configure tonic to accept gzip — this sets `grpc-accept-encoding: gzip`
    // on outgoing requests, which our server should pick up and use.
    let mut client = connect(port)
        .await
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

    // Long string so the compressed response is meaningfully smaller than
    // the uncompressed one.
    let name = "world ".repeat(200);
    let response = client
        .say_hello(tonic::Request::new(proto::HelloRequest {
            name: name.clone(),
        }))
        .await
        .unwrap();

    // The metadata map exposes response headers. Our server must have
    // selected gzip and announced it.
    assert_eq!(
        response
            .metadata()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("gzip"),
    );
    assert_eq!(response.into_inner().message, format!("Hello, {name}"));

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_streaming_roundtrip_against_tonic_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let mut client = connect(port).await;

    let response = client
        .say_hello_stream(tonic::Request::new(proto::HelloRequest {
            name: "world".into(),
        }))
        .await
        .unwrap();

    let mut stream = response.into_inner();
    let mut messages = Vec::new();
    while let Some(reply) = stream.message().await.unwrap() {
        messages.push(reply.message);
    }

    assert_eq!(
        messages,
        vec!["Hello 1, world", "Hello 2, world", "Hello 3, world"]
    );

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_streaming_roundtrip_against_tonic_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let mut client = connect(port).await;

    let request_stream = tokio_stream::iter(vec![
        proto::HelloRequest {
            name: "alice".into(),
        },
        proto::HelloRequest { name: "bob".into() },
    ]);

    let response = client
        .say_hello_many(tonic::Request::new(request_stream))
        .await
        .unwrap();

    assert_eq!(response.into_inner().message, "Hello, alice and bob");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bidi_roundtrip_against_tonic_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let mut client = connect(port).await;

    let request_stream = tokio_stream::iter(vec![
        proto::HelloRequest {
            name: "alice".into(),
        },
        proto::HelloRequest { name: "bob".into() },
        proto::HelloRequest {
            name: "carol".into(),
        },
    ]);

    let response = client
        .say_hello_chat(tonic::Request::new(request_stream))
        .await
        .unwrap();

    let mut stream = response.into_inner();
    let mut messages = Vec::new();
    while let Some(reply) = stream.message().await.unwrap() {
        messages.push(reply.message);
    }

    assert_eq!(
        messages,
        vec!["Hi back, alice", "Hi back, bob", "Hi back, carol"]
    );

    server.shut_down().await;
}

/// Regenerates the checked-in tonic fixture at `tests/proto/gen/greeter.v1.rs`
/// from `tests/proto/greeter.proto`. This is committed (not built via build.rs)
/// so downstream consumers don't pull in tonic-prost-build as a build-dependency.
///
/// Run after editing greeter.proto:
///   cargo test --test tonic_roundtrip regenerate_tonic_fixture -- --ignored
#[test]
#[ignore = "regenerates a checked-in fixture; run manually after editing greeter.proto"]
fn regenerate_tonic_fixture() {
    tonic_prost_build::configure()
        .build_server(false)
        .out_dir("tests/proto/gen")
        .compile_protos(&["tests/proto/greeter.proto"], &["tests/proto"])
        .expect("failed to compile greeter.proto");
}
