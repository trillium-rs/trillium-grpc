//! Verify the `trillium_grpc::generate!` proc macro produces working,
//! end-to-end code: a unary roundtrip against a server built from the
//! macro-generated trait + Server, called through the macro-generated
//! Client. Mirrors the simplest case in `client_roundtrip.rs`, but with
//! the codegen done by the proc macro instead of the CLI.

trillium_grpc::generate!("tests/proto/greeter.proto");

use greeter::v1::{Greeter, GreeterClient, GreeterServer, HelloReply, HelloRequest};
use trillium_grpc::{BidiResponder, Channel, GrpcServerConn, Status, Stream};

struct MyGreeter;

/// Bidi responder that does nothing — just exercises the macro-generated shape.
struct NoopChat;
impl BidiResponder<HelloRequest, HelloReply> for NoopChat {
    async fn respond(self, _channel: Channel<'_, HelloRequest, HelloReply>) -> Result<(), Status> {
        Ok(())
    }
}

impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        req: HelloRequest,
    ) -> Result<HelloReply, Status> {
        Ok(HelloReply {
            message: format!("Hello, {}", req.name),
        })
    }

    async fn say_hello_stream(
        &self,
        _conn: &mut GrpcServerConn,
        _req: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + use<>, Status> {
        Ok(futures_lite::stream::empty())
    }

    async fn say_hello_many(&self, _conn: &mut GrpcServerConn) -> Result<HelloReply, Status> {
        Ok(HelloReply::default())
    }

    async fn say_hello_chat(
        &self,
        _conn: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
        Ok(NoopChat)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unary_roundtrip_through_macro_generated_code() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = trillium_tokio::config()
        .with_host("127.0.0.1")
        .with_port(0)
        .spawn(GreeterServer::new(MyGreeter));
    let port = server.info().await.tcp_socket_addr().unwrap().port();
    let greeter = GreeterClient::from(
        trillium_client::Client::new(trillium_tokio::ClientConfig::default())
            .with_base(format!("http://127.0.0.1:{port}")),
    );

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
