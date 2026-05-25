//! A complete greeter service — server and client, all four call shapes — that
//! runs end to end in one process. Run it with:
//!
//! ```text
//! cargo run --example greeter --features macros
//! ```
//!
//! The service is generated at compile time from `examples/greeter.proto` by the
//! [`generate!`](trillium_grpc::generate) macro. The same `.proto` can instead be
//! turned into a checked-in module with the `trillium grpc` CLI, or compiled in a
//! build script — see the crate docs for the trade-offs.

use futures_lite::StreamExt;
use trillium_grpc::{BidiResponder, Channel, GrpcServerConn, Status, Stream};

trillium_grpc::generate!("examples/greeter.proto");

use greeter::v1::{Greeter, GreeterClient, GreeterServer, HelloReply, HelloRequest};

struct MyGreeter;

impl Greeter for MyGreeter {
    /// Unary: receive one request, return one response.
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> Result<HelloReply, Status> {
        Ok(HelloReply {
            message: format!("Hello, {}", request.name),
        })
    }

    /// Server-streaming: return a `Stream` the framework pulls responses from.
    async fn say_hello_stream(
        &self,
        _conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + use<>, Status> {
        Ok(futures_lite::stream::iter((1..=3).map(move |i| {
            Ok(HelloReply {
                message: format!("Hello #{i}, {}", request.name),
            })
        })))
    }

    /// Client-streaming: drain the request stream off the conn, return one response.
    async fn say_hello_many(&self, conn: &mut GrpcServerConn) -> Result<HelloReply, Status> {
        let mut names = Vec::new();
        let mut requests = conn.requests::<HelloRequest>();
        while let Some(request) = requests.recv().await? {
            names.push(request.name);
        }
        Ok(HelloReply {
            message: format!("Hello, {}", names.join(" and ")),
        })
    }

    /// Bidirectional-streaming: return a responder that runs the read-while-write
    /// loop after the response head is on the wire.
    async fn say_hello_chat(
        &self,
        _conn: &mut GrpcServerConn,
    ) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
        Ok(Chat)
    }
}

struct Chat;

impl BidiResponder<HelloRequest, HelloReply> for Chat {
    async fn respond(
        self,
        mut channel: Channel<'_, HelloRequest, HelloReply>,
    ) -> Result<(), Status> {
        while let Some(request) = channel.recv().await.transpose()? {
            channel
                .send(HelloReply {
                    message: format!("Hey, {}", request.name),
                })
                .await?;
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let server = trillium_tokio::config()
        .with_host("127.0.0.1")
        .with_port(0)
        .spawn(GreeterServer::new(MyGreeter));
    let port = server.info().await.tcp_socket_addr().unwrap().port();

    let client = GreeterClient::from(
        trillium_client::Client::new(trillium_tokio::ClientConfig::default())
            .with_base(format!("http://127.0.0.1:{port}")),
    );

    // Unary.
    let reply = client
        .say_hello(HelloRequest {
            name: "world".into(),
        })
        .await
        .unwrap()
        .into_message()
        .unwrap();
    println!("unary:            {}", reply.message);

    // Server-streaming — iterate the response handle as a Stream.
    let mut stream = client.say_hello_stream(HelloRequest {
        name: "world".into(),
    });
    while let Some(reply) = stream.next().await {
        println!("server-streaming: {}", reply.unwrap().message);
    }

    // Client-streaming — pass a Stream of requests, get one reply.
    let names =
        futures_lite::stream::iter(["Alice", "Bob"].map(|name| HelloRequest { name: name.into() }));
    let reply = client
        .say_hello_many(names)
        .await
        .unwrap()
        .into_message()
        .unwrap();
    println!("client-streaming: {}", reply.message);

    // Bidirectional — live send/recv on one handle.
    let mut chat = client.say_hello_chat();
    chat.send(HelloRequest {
        name: "Carol".into(),
    })
    .await
    .unwrap();
    chat.send(HelloRequest {
        name: "Dave".into(),
    })
    .await
    .unwrap();
    chat.close_send().await.unwrap();
    while let Some(reply) = chat.recv().await.unwrap() {
        println!("bidirectional:    {}", reply.message);
    }

    server.shut_down().await;
}
