//! Cancellation tests. Verifies that initiating server shutdown while an
//! RPC is in flight drops the user's in-flight work.
//!
//! - Unary: handler future is dropped server-side when shutdown fires.
//!   We observe this via a drop-tracker rather than asserting the client
//!   sees CANCELLED, because trillium-http currently does not flush the
//!   response when shutdown is initiated mid-RPC (separate issue, will
//!   surface as `Code::Cancelled` to the caller once fixed).
//! - Server-streaming: the response stream ends cleanly mid-flight per
//!   `Swansong::interrupt`'s stream semantics; peer sees a partial-but-OK
//!   stream. (Documented trade-off in `Cancellation::wrap_stream`.)

#[path = "generated/greeter_v1.rs"]
mod greeter_v1;

use crate::greeter_v1::{Greeter, GreeterClient, GreeterServer, HelloReply, HelloRequest};
use futures_lite::{Stream, StreamExt, stream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use trillium_grpc::{BufferedRequestStream, Status};

/// Drop sentinel: when its containing future is dropped, the flag flips.
/// Lets us assert that a long-running handler future was actually
/// cancelled rather than hung.
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Greeter that blocks forever on every call. The streaming response
/// shape sleeps between messages so we can race shutdown against an
/// in-flight stream.
struct SleepyGreeter {
    unary_dropped: Arc<AtomicBool>,
}

impl SleepyGreeter {
    fn new() -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (Self { unary_dropped: flag.clone() }, flag)
    }
}

impl Greeter for SleepyGreeter {
    async fn say_hello(&self, _req: HelloRequest) -> Result<HelloReply, Status> {
        let _track = DropFlag(self.unary_dropped.clone());
        std::future::pending::<()>().await;
        unreachable!()
    }

    async fn say_hello_stream(
        &self,
        _req: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<>, Status>
    {
        Ok(stream::unfold(0usize, |i| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Some((
                Ok(HelloReply { message: format!("msg {i}") }),
                i + 1,
            ))
        }))
    }

    async fn say_hello_many(
        &self,
        _reqs: BufferedRequestStream<HelloRequest>,
    ) -> Result<HelloReply, Status> {
        std::future::pending::<()>().await;
        unreachable!()
    }

    async fn say_hello_chat(
        &self,
        _reqs: BufferedRequestStream<HelloRequest>,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + 'static + use<>, Status>
    {
        Ok(stream::unfold((), |()| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Some((Ok(HelloReply { message: "tick".into() }), ()))
        }))
    }
}

macro_rules! start_server {
    ($greeter:expr) => {{
        let server = trillium_tokio::config()
            .with_host("127.0.0.1")
            .with_port(0)
            .spawn(GreeterServer::new($greeter));
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

/// Poll `flag` until it becomes true or `deadline` passes. Yields
/// between checks so the runtime can drive the server's shutdown.
async fn await_flag(flag: &AtomicBool, deadline: Instant) -> bool {
    while Instant::now() < deadline {
        if flag.load(Ordering::SeqCst) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    flag.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_drops_in_flight_unary_handler() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (greeter_impl, dropped) = SleepyGreeter::new();
    let (server, port) = start_server!(greeter_impl);
    let greeter = our_client(port);

    // Fire and forget — once cancellation lands server-side, the handler
    // future is dropped, which is what we're asserting on.
    let _call = tokio::spawn(async move {
        let _ = greeter.say_hello(HelloRequest { name: "world".into() }).await;
    });

    // Let the call reach the server.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !dropped.load(Ordering::SeqCst),
        "handler future dropped before shutdown",
    );

    // Initiate shutdown in a task so it doesn't deadlock against the
    // hanging conn.
    let _shutdown = tokio::spawn(async move { server.shut_down().await });

    let was_dropped = await_flag(&dropped, Instant::now() + Duration::from_secs(2)).await;
    assert!(
        was_dropped,
        "handler future was not dropped within 2s of server shutdown",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_in_flight_server_stream() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (greeter_impl, _) = SleepyGreeter::new();
    let (server, port) = start_server!(greeter_impl);
    let greeter = our_client(port);

    let mut stream = greeter
        .say_hello_stream(HelloRequest { name: "world".into() })
        .await
        .expect("stream open");

    // Receive at least one message so the stream is genuinely in-flight.
    let first = stream.next().await.expect("first message");
    assert!(first.is_ok(), "first message: {first:?}");

    // Shut down in a task so we can keep draining the stream.
    let shutdown =
        tokio::spawn(async move { server.shut_down().await });

    // Drain remaining items. Per `Swansong::interrupt`'s stream semantics,
    // the response stream ends cleanly with `None`; peer sees OK trailers.
    // We only assert: the stream terminates promptly (no infinite loop)
    // and we don't see a CANCELLED error item.
    let drain = async {
        let mut tail_ok = 0;
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => tail_ok += 1,
                Err(s) => panic!("unexpected error draining: {s:?}"),
            }
        }
        tail_ok
    };
    let tail_ok = tokio::time::timeout(Duration::from_secs(2), drain)
        .await
        .expect("stream did not terminate");

    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("shutdown did not complete")
        .unwrap();

    // Sanity: a 30s/100ms-per-message stream cut by ~50ms of shutdown
    // delay should not have produced anywhere near as many messages as
    // the user fn would have produced unmolested.
    assert!(tail_ok < 50, "stream did not end promptly: {tail_ok} more after first");
}
