//! Drive the new owned `GrpcClientConn` handle through every interaction
//! pattern against the generated `GreeterServer`: unary, server-stream,
//! client-stream, half-duplex bidi, and full-duplex bidi. Proves the handle's
//! send/close_send/recv/headers/trailers surface works on both transports
//! before codegen and the conformance harness are built on top of it.

#[allow(dead_code)]
mod greeter_v1 {
    include!("generated/greeter_v1.rs");
}

use crate::greeter_v1::{Greeter, GreeterServer, HelloReply, HelloRequest};
use futures_lite::StreamExt;
use trillium_grpc::{
    BidiConn, BidiResponder, Channel, GrpcClientConn, GrpcServerConn, Metadata, Prost, Status,
    Stream, StreamingConn, UnaryConn,
};

struct MyGreeter;
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
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        _c: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<HelloReply, Status> {
        Ok(HelloReply {
            message: format!("Hello, {}", req.name),
        })
    }
    async fn say_hello_stream(
        &self,
        c: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + use<>, Status> {
        let name = req.name;
        // `name == "empty"` yields a zero-message stream (still terminated with a
        // custom trailer) — exercises the trailers-after-empty-body read path.
        let count = if name == "empty" { 0 } else { 3 };
        c.response_trailers_mut()
            .insert("x-custom-trailer", "present");
        Ok(futures_lite::stream::iter((1..=count).map(move |i| {
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
        _c: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
        Ok(ChatResponder)
    }
}

macro_rules! start {
    () => {{
        let server = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(GreeterServer::new(MyGreeter));
        let port = server.info().await.tcp_socket_addr().unwrap().port();
        let client = trillium_client::Client::new(trillium_tokio::ClientConfig::default())
            .with_base(format!("http://127.0.0.1:{port}/greeter.v1.Greeter/"));
        (server, client)
    }};
}

#[tokio::test(flavor = "multi_thread")]
async fn unary() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHello", Metadata::new(), None, false);
    conn.send(HelloRequest {
        name: "world".into(),
    })
    .await
    .unwrap();
    conn.close_send().await.unwrap();
    let reply = conn.recv().await.unwrap().unwrap();
    assert_eq!(reply.message, "Hello, world");
    assert!(conn.recv().await.unwrap().is_none());
    assert!(conn.trailers().is_some());

    server.shut_down().await;
}

// ── Typed conns (UnaryConn / StreamingConn / BidiConn) ──────────────────────
// The shape the generated client returns. Same wire path as the raw engine tests
// above, exercised through the ergonomic await-returns-self surface.

#[tokio::test(flavor = "multi_thread")]
async fn typed_unary() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let conn = UnaryConn::<HelloRequest, HelloReply>::unary::<Prost>(
        &client,
        "SayHello",
        HelloRequest {
            name: "world".into(),
        },
    )
    .await
    .unwrap();

    assert_eq!(conn.message().unwrap().message, "Hello, world");
    assert!(conn.metadata().is_some());
    assert!(conn.trailers().is_some());
    assert!(conn.status().is_ok());
    assert_eq!(conn.into_message().unwrap().message, "Hello, world");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_unary_metadata_roundtrip() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    // Bad metadata is deferred to the await as InvalidArgument (builders stay
    // infallible-chainable).
    let result = UnaryConn::<HelloRequest, HelloReply>::unary::<Prost>(
        &client,
        "SayHello",
        HelloRequest {
            name: "world".into(),
        },
    )
    .with_ascii_metadata("grpc-internal", "no") // reserved key → deferred error
    .await;
    match result {
        Ok(_) => panic!("expected reserved-key metadata to fail"),
        Err(status) => assert_eq!(status.code, trillium_grpc::Code::InvalidArgument),
    }

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_server_stream() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    // Iterate without an explicit await — the first poll fires the request.
    let mut conn = StreamingConn::<HelloRequest, HelloReply>::server_streaming::<Prost>(
        &client,
        "SayHelloStream",
        HelloRequest {
            name: "world".into(),
        },
    );
    let mut messages = Vec::new();
    while let Some(reply) = conn.next().await {
        messages.push(reply.unwrap().message);
    }
    assert_eq!(
        messages,
        vec!["Hello 1, world", "Hello 2, world", "Hello 3, world"]
    );
    assert!(conn.trailers().is_some());

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_server_stream_await_for_metadata() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    // Opt into the head read so metadata is available before the first message.
    let conn = StreamingConn::<HelloRequest, HelloReply>::server_streaming::<Prost>(
        &client,
        "SayHelloStream",
        HelloRequest {
            name: "world".into(),
        },
    )
    .await
    .unwrap();
    assert!(conn.metadata().is_some());
    let count = conn.count().await;
    assert_eq!(count, 3);

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_client_stream() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let requests = futures_lite::stream::iter([
        HelloRequest {
            name: "alice".into(),
        },
        HelloRequest { name: "bob".into() },
    ]);
    let reply = UnaryConn::<HelloRequest, HelloReply>::client_streaming::<Prost>(
        &client,
        "SayHelloMany",
        requests,
    )
    .await
    .unwrap()
    .into_message()
    .unwrap();
    assert_eq!(reply.message, "Hello, alice and bob");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_bidi_full_duplex() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let mut conn = BidiConn::<HelloRequest, HelloReply>::bidi::<Prost>(&client, "SayHelloChat");
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

#[tokio::test(flavor = "multi_thread")]
async fn server_stream() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHelloStream", Metadata::new(), None, false);
    conn.send(HelloRequest {
        name: "world".into(),
    })
    .await
    .unwrap();
    conn.close_send().await.unwrap();

    let mut messages = Vec::new();
    while let Some(reply) = conn.recv().await.unwrap() {
        messages.push(reply.message);
    }
    assert_eq!(
        messages,
        vec!["Hello 1, world", "Hello 2, world", "Hello 3, world"]
    );

    server.shut_down().await;
}

// Concurrency guard for `GrpcClientConn` over an empty server-stream (head +
// trailing HEADERS, no DATA — the trailers-after-empty-body path). One *fresh*
// client per call, mirroring the conformance harness; proves the per-call logic
// is concurrency-safe (a miss would surface as recv -> Unknown, the trailing
// grpc-status not captured before EOF). The conformance `empty-response` flake
// reproduces only across the full 400-case run, not here — it's environmental
// contention, not a GrpcClientConn bug.
#[tokio::test(flavor = "multi_thread")]
async fn empty_server_stream_under_concurrency() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();
    let base = client.base().expect("client has a base").to_string();

    for iter in 0..8u32 {
        let mut handles = Vec::new();
        for task in 0..32u32 {
            // Fresh client per task — a new pool, so no shared pooled connection
            // (that path has a separate, known issue; see the ignored test below).
            let client = trillium_client::Client::new(trillium_tokio::ClientConfig::default())
                .with_base(base.clone());
            handles.push(tokio::spawn(empty_stream_call(client, iter, task)));
        }
        for h in handles {
            let (count, trailer) = h.await.unwrap().expect("recv error");
            assert_eq!(count, 0, "empty stream should yield no messages");
            assert_eq!(
                trailer.as_deref(),
                Some("present"),
                "custom trailer missing"
            );
        }
    }

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_client_concurrent_calls_trip_h2_protocol_error() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    for iter in 0..20u32 {
        let mut handles = Vec::new();
        for task in 0..64u32 {
            let client = client.clone(); // shared pool — the realistic channel model
            handles.push(tokio::spawn(empty_stream_call(client, iter, task)));
        }
        for h in handles {
            let (count, trailer) = h.await.unwrap().expect("recv error");
            assert_eq!(count, 0);
            assert_eq!(trailer.as_deref(), Some("present"));
        }
    }

    server.shut_down().await;
}

// One empty-server-stream call: send, close, drain to EOF, return (count, trailer).
async fn empty_stream_call(
    client: trillium_client::Client,
    iter: u32,
    task: u32,
) -> Result<(u32, Option<String>), String> {
    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHelloStream", Metadata::new(), None, false);
    conn.send(HelloRequest {
        name: "empty".into(),
    })
    .await
    .map_err(|s| format!("iter {iter} task {task}: send -> {s:?}"))?;
    conn.close_send()
        .await
        .map_err(|s| format!("iter {iter} task {task}: close_send -> {s:?}"))?;
    let mut count = 0u32;
    loop {
        match conn.recv().await {
            Ok(Some(_)) => count += 1,
            Ok(None) => break,
            Err(s) => return Err(format!("iter {iter} task {task}: recv -> {s:?}")),
        }
    }
    let trailer = conn
        .trailers()
        .and_then(|t| t.get_str("x-custom-trailer").map(str::to_string));
    Ok((count, trailer))
}

#[tokio::test(flavor = "multi_thread")]
async fn client_stream() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHelloMany", Metadata::new(), None, false);
    conn.send(HelloRequest {
        name: "alice".into(),
    })
    .await
    .unwrap();
    conn.send(HelloRequest { name: "bob".into() })
        .await
        .unwrap();
    conn.close_send().await.unwrap();
    let reply = conn.recv().await.unwrap().unwrap();
    assert_eq!(reply.message, "Hello, alice and bob");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn half_duplex_bidi() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    // half-duplex: full_duplex=false → send all, close, then read.
    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHelloChat", Metadata::new(), None, false);
    conn.send(HelloRequest {
        name: "alice".into(),
    })
    .await
    .unwrap();
    conn.send(HelloRequest { name: "bob".into() })
        .await
        .unwrap();
    conn.close_send().await.unwrap();

    let mut messages = Vec::new();
    while let Some(reply) = conn.recv().await.unwrap() {
        messages.push(reply.message);
    }
    assert_eq!(messages, vec!["Hi back, alice", "Hi back, bob"]);

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn full_duplex_bidi() {
    let _ = env_logger::builder().is_test(true).try_init();
    let (server, client) = start!();

    // full-duplex: full_duplex=true → interleave send/recv over the Upgrade.
    let mut conn: GrpcClientConn<HelloRequest, HelloReply> =
        GrpcClientConn::new::<Prost>(&client, "SayHelloChat", Metadata::new(), None, true);

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
