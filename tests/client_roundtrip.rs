//! Own-world tests: spin up our generated `GreeterServer`, then call it
//! through our generated `GreeterClient`. Server conformance against tonic
//! is covered separately by `tonic_roundtrip.rs`; this file proves the
//! round-trip works end-to-end through generated code on both sides.

#[allow(dead_code)] // committed codegen output; not every RPC is exercised here
mod greeter_v1 {
    include!("generated/greeter_v1.rs");
}

use crate::greeter_v1::{Greeter, GreeterClient, GreeterServer, HelloReply, HelloRequest};
use futures_lite::{StreamExt, stream};
use trillium_grpc::{
    BidiResponder, Channel, Code, Encoding, GrpcServerConn, Metadata, ServiceClientExt, Status,
    Stream,
};

struct MyGreeter;

/// Bidi responder: echo each request back with a greeting.
struct ChatResponder;
impl BidiResponder<HelloRequest, HelloReply> for ChatResponder {
    async fn respond(
        self,
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

/// Sentinel name that makes `say_hello` return an Err with custom trailing
/// metadata, used to exercise the metadata round-trip path.
const FAIL_NAME: &str = "fail";

impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<HelloReply, Status> {
        if req.name == FAIL_NAME {
            let mut metadata = Metadata::new();
            metadata.insert_ascii("retry-after", "30").unwrap();
            metadata
                .insert_binary("debug-bin", vec![0xDE, 0xAD, 0xBE, 0xEF])
                .unwrap();
            return Err(Status::resource_exhausted("slow down").with_metadata(metadata));
        }
        Ok(HelloReply {
            message: format!("Hello, {}", req.name),
        })
    }

    async fn say_hello_stream(
        &self,
        _conn: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + use<>, Status> {
        let name = req.name;
        Ok(stream::iter((1..=3).map(move |i| {
            Ok(HelloReply {
                message: format!("Hello {i}, {name}"),
            })
        })))
    }

    async fn say_hello_many(&self, conn: &mut GrpcServerConn) -> Result<HelloReply, Status> {
        let mut names = Vec::new();
        let mut reqs = conn.requests::<HelloRequest>();
        while let Some(req) = reqs.recv().await? {
            names.push(req.name);
        }
        Ok(HelloReply {
            message: format!("Hello, {}", names.join(" and ")),
        })
    }

    async fn say_hello_chat(
        &self,
        _conn: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
        Ok(ChatResponder)
    }
}

macro_rules! start_pair {
    () => {{
        let server = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(GreeterServer::new(MyGreeter));
        let port = server.info().await.tcp_socket_addr().unwrap().port();
        let client = GreeterClient::from(
            trillium_client::Client::new(trillium_tokio::ClientConfig::default())
                .with_base(format!("http://127.0.0.1:{port}")),
        );
        (server, client)
    }};
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_via_generated_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();

    let resp = greeter
        .say_hello(HelloRequest {
            name: "world".into(),
        })
        .await
        .unwrap()
        .into_message()
        .unwrap();

    assert_eq!(resp.message, "Hello, world");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_error_carries_trailing_metadata() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();

    // The HTTP exchange succeeds (await is Ok); the logical error surfaces when
    // the message is taken. Response metadata stays readable either way.
    let err = greeter
        .say_hello(HelloRequest {
            name: FAIL_NAME.into(),
        })
        .await
        .unwrap()
        .into_message()
        .unwrap_err();

    assert_eq!(err.code, Code::ResourceExhausted);
    assert_eq!(err.message, "slow down");
    assert_eq!(err.metadata.get_ascii("retry-after"), Some("30"));
    assert_eq!(
        err.metadata.get_binary("debug-bin"),
        Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]),
    );

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_with_gzip_outbound_compression() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();
    let greeter = greeter.with_outbound_compression(Encoding::Gzip);
    assert_eq!(greeter.outbound_compression(), Encoding::Gzip);

    // Repeat the name so the request body is well above gzip's overhead
    // floor — confirms compression is actually applied and decoded.
    let name = "world ".repeat(200);
    let resp = greeter
        .say_hello(HelloRequest { name: name.clone() })
        .await
        .unwrap()
        .into_message()
        .unwrap();

    assert_eq!(resp.message, format!("Hello, {name}"));

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bidi_with_gzip_outbound_compression() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();
    let greeter = greeter.with_outbound_compression(Encoding::Gzip);

    let mut conn = greeter.say_hello_chat();
    for name in ["alice", "bob"] {
        conn.send(HelloRequest { name: name.into() }).await.unwrap();
    }
    conn.close_send().await.unwrap();

    let mut messages = Vec::new();
    while let Some(item) = conn.next().await {
        messages.push(item.unwrap().message);
    }

    assert_eq!(messages, vec!["Hi back, alice", "Hi back, bob"]);

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_streaming_via_generated_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();

    let mut stream = greeter
        .say_hello_stream(HelloRequest {
            name: "world".into(),
        })
        .await
        .unwrap();

    let mut messages = Vec::new();
    while let Some(item) = stream.next().await {
        messages.push(item.unwrap().message);
    }

    assert_eq!(
        messages,
        vec!["Hello 1, world", "Hello 2, world", "Hello 3, world"]
    );

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_streaming_via_generated_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();

    let resp = greeter
        .say_hello_many(stream::iter([
            HelloRequest {
                name: "alice".into(),
            },
            HelloRequest { name: "bob".into() },
        ]))
        .await
        .unwrap()
        .into_message()
        .unwrap();

    assert_eq!(resp.message, "Hello, alice and bob");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bidi_via_generated_client() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, greeter) = start_pair!();

    // Full-duplex: interleave send and recv on the live conn.
    let mut conn = greeter.say_hello_chat();
    let mut messages = Vec::new();
    for name in ["alice", "bob", "carol"] {
        conn.send(HelloRequest { name: name.into() }).await.unwrap();
        let reply = conn.recv().await.unwrap().unwrap();
        messages.push(reply.message);
    }
    conn.close_send().await.unwrap();
    assert!(conn.recv().await.unwrap().is_none());

    assert_eq!(
        messages,
        vec!["Hi back, alice", "Hi back, bob", "Hi back, carol"]
    );

    server.shut_down().await;
}
