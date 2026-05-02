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
    tonic::include_proto!("greeter.v1");
}

#[path = "generated/greeter_v1.rs"]
mod greeter_v1;

use crate::greeter_v1::{Greeter, GreeterServer, HelloReply, HelloRequest};
use crate::proto::greeter_client::GreeterClient;
use futures_lite::{Stream, StreamExt, stream};
use trillium_grpc::{BufferedRequestStream, Status};

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
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<>, Status> {
        let name = req.name;
        Ok(stream::iter((1..=3).map(move |i| {
            Ok(HelloReply {
                message: format!("Hello {i}, {name}"),
            })
        })))
    }

    async fn say_hello_many(
        &self,
        mut reqs: BufferedRequestStream<HelloRequest>,
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
        mut reqs: BufferedRequestStream<HelloRequest>,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<>, Status> {
        let mut replies = Vec::new();
        while let Some(req) = reqs.next().await {
            let req = req?;
            replies.push(Ok(HelloReply {
                message: format!("Hi back, {}", req.name),
            }));
        }
        Ok(stream::iter(replies))
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
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap();
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
    let mut client = connect(port).await.send_compressed(tonic::codec::CompressionEncoding::Gzip);

    // Repeat the name so the message is well above gzip's overhead floor
    // and we're confident the wire payload is genuinely compressed.
    let name = "world ".repeat(200);
    let response = client
        .say_hello(tonic::Request::new(proto::HelloRequest { name: name.clone() }))
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
        .say_hello(tonic::Request::new(proto::HelloRequest { name: name.clone() }))
        .await
        .unwrap();

    // The metadata map exposes response headers. Our server must have
    // selected gzip and announced it.
    assert_eq!(
        response.metadata().get("grpc-encoding").and_then(|v| v.to_str().ok()),
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
        proto::HelloRequest { name: "alice".into() },
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
        proto::HelloRequest { name: "alice".into() },
        proto::HelloRequest { name: "bob".into() },
        proto::HelloRequest { name: "carol".into() },
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
