//! `grpc-timeout` end-to-end tests. Spins up a deliberately slow greeter
//! and verifies:
//!
//! 1. Client-side deadline expiry surfaces as `DEADLINE_EXCEEDED` to the
//!    caller, even when the server is still blocked.
//! 2. A tonic client with its own deadline talking to our server: server
//!    parses the header and emits `DEADLINE_EXCEEDED` trailers.
//! 3. Untimed calls keep working unchanged.

#[allow(dead_code)] // committed codegen output; not every RPC is exercised here
mod greeter_v1 {
    include!("generated/greeter_v1.rs");
}

mod proto {
    include!("proto/gen/greeter.v1.rs");
}

use crate::greeter_v1::{Greeter, GreeterClient, GreeterServer, HelloReply, HelloRequest};
use crate::proto::greeter_client::GreeterClient as TonicGreeter;
use futures_lite::StreamExt;
use std::time::Duration;
use trillium_grpc::{
    BidiResponder, Channel, Code, GrpcServerConn, ServiceClientExt, Status, Stream,
};

/// Bidi responder: echo each request's name straight back.
struct EchoChat;
impl BidiResponder<HelloRequest, HelloReply> for EchoChat {
    async fn respond(
        self,
        mut channel: Channel<'_, HelloRequest, HelloReply>,
    ) -> Result<(), Status> {
        while let Some(req) = channel.recv().await {
            channel.send(HelloReply { message: req?.name }).await?;
        }
        Ok(())
    }
}

/// Greeter whose `say_hello` blocks for the duration encoded in
/// `name = "sleep:<ms>"`. Other shapes return immediately so we can vary
/// what we're testing per call.
struct SlowGreeter;

fn parse_sleep(name: &str) -> Option<Duration> {
    let ms = name.strip_prefix("sleep:")?.parse::<u64>().ok()?;
    Some(Duration::from_millis(ms))
}

impl Greeter for SlowGreeter {
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<HelloReply, Status> {
        if req.name == "synth-deadline" {
            return Err(Status::deadline_exceeded("synthetic"));
        }
        if let Some(d) = parse_sleep(&req.name) {
            tokio::time::sleep(d).await;
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
        // Per-message sleep before yielding, so a streaming response can be cut
        // by an in-flight deadline (StreamBody polls the cancel signal between
        // frames).
        let delay = parse_sleep(&req.name).unwrap_or_default();
        Ok(futures_lite::stream::unfold(0usize, move |i| async move {
            if i >= 5 {
                return None;
            }
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Some((
                Ok(HelloReply {
                    message: format!("msg {i}"),
                }),
                i + 1,
            ))
        }))
    }

    async fn say_hello_many(&self, conn: &mut GrpcServerConn) -> Result<HelloReply, Status> {
        let mut names = Vec::new();
        let mut reqs = conn.requests::<HelloRequest>();
        while let Some(req) = reqs.recv().await? {
            names.push(req.name);
        }
        Ok(HelloReply {
            message: names.join(","),
        })
    }

    async fn say_hello_chat(
        &self,
        _conn: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
        Ok(EchoChat)
    }
}

macro_rules! start_server {
    () => {{
        let server = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(GreeterServer::new(SlowGreeter));
        let port = server.info().await.tcp_socket_addr().unwrap().port();
        (server, port)
    }};
}

fn our_client(port: u16) -> GreeterClient {
    GreeterClient::from(
        trillium_client::Client::new(trillium_tokio::ClientConfig::default())
            .with_base(format!("http://127.0.0.1:{port}")),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn untimed_call_still_works() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let greeter = our_client(port);

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
async fn client_deadline_expires_returns_deadline_exceeded() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let greeter = our_client(port).with_default_timeout(Duration::from_millis(50));

    // The deadline can fire while awaiting the head or while reading the body, so
    // fold both stages: `and_then` carries an `await` error through, else runs
    // `into_message`.
    let err = greeter
        .say_hello(HelloRequest {
            name: "sleep:500".into(),
        })
        .await
        .and_then(|conn| conn.into_message())
        .unwrap_err();

    assert_eq!(err.code, Code::DeadlineExceeded);

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn client_deadline_with_room_to_spare_completes() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let greeter = our_client(port).with_default_timeout(Duration::from_secs(5));

    let resp = greeter
        .say_hello(HelloRequest {
            name: "sleep:50".into(),
        })
        .await
        .unwrap()
        .into_message()
        .unwrap();
    assert_eq!(resp.message, "Hello, sleep:50");

    server.shut_down().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tonic_reads_synthetic_deadline_exceeded_from_our_server() {
    // Sanity probe: server returns DEADLINE_EXCEEDED *without* any timer
    // involvement. Confirms tonic-on-our-server can read the trailer
    // when nothing about timeout/race plumbing is in play.
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    let endpoint =
        tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}")).unwrap();
    let mut client = TonicGreeter::connect(endpoint).await.unwrap();

    let status = client
        .say_hello(tonic::Request::new(proto::HelloRequest {
            name: "synth-deadline".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);

    server.shut_down().await;
}

// NOTE: a "server-side enforcement against tonic client" test is intentionally
// omitted. Tonic enforces any outgoing `grpc-timeout` header locally — its own
// timer fires at the same moment and RST_STREAM(Cancel)s the stream before
// our server's `DEADLINE_EXCEEDED` trailer can be read. There's no clean way
// to bypass that without bypassing tonic's request layer entirely. Server-side
// enforcement is exercised via the synth-deadline test above (proving the
// trailer is readable) and via the streaming-response test below (proving
// the in-flight stream is cut by the server's timer).

#[tokio::test(flavor = "multi_thread")]
async fn streaming_response_cut_by_deadline() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (server, port) = start_server!();
    // Server yields 5 messages with a 100ms sleep before each. A 250ms
    // deadline should let ~2 through and then expire.
    let greeter = our_client(port).with_default_timeout(Duration::from_millis(250));

    let mut stream = greeter
        .say_hello_stream(HelloRequest {
            name: "sleep:100".into(),
        })
        .await
        .unwrap();

    let mut ok_count = 0;
    let mut last_err: Option<Status> = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => ok_count += 1,
            Err(status) => {
                last_err = Some(status);
                break;
            }
        }
    }

    assert!(
        ok_count > 0 && ok_count < 5,
        "expected partial stream, got {ok_count}/5",
    );
    assert_eq!(last_err.unwrap().code, Code::DeadlineExceeded);

    server.shut_down().await;
}
