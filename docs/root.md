A gRPC server and client for [trillium](https://trillium.rs), built as a thin
layer on `trillium-http`'s HTTP/2, h2c (HTTP/2 over cleartext), and HTTP/3
support. If you have a `.proto` service definition and want to serve it — or call
it — from a trillium application, this generates the glue and handles the wire
format. It is a from-scratch, spec-conformant implementation of the [gRPC over
HTTP/2 protocol].

All four call shapes are supported — unary, server-streaming, client-streaming,
and bidirectional-streaming — along with protobuf messages via
[`prost`](https://docs.rs/prost) (and optional JSON behind the `json` feature)
and per-message compression (`gzip` by default, `deflate` and `zstd` behind
features).

# A worked example

You write a `.proto`:

```proto
syntax = "proto3";
package greeter.v1;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Codegen turns it into a Rust module: the [`prost`](https://docs.rs/prost) message
types, a `Greeter` trait you implement, a `GreeterServer<T>` handler that mounts
into a trillium handler chain, and a `GreeterClient` for calling the service.
Every service method receives a [`GrpcServerConn`] — the per-call control surface
for request metadata, the deadline, and response metadata — and returns its
result as a `Result<_, Status>`; the framework writes the terminating
`grpc-status` from that result.

```rust,ignore
use trillium_grpc::{GrpcServerConn, Status};
use greeter::v1::{Greeter, GreeterServer, HelloReply, HelloRequest};

struct MyGreeter;

impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> Result<HelloReply, Status> {
        Ok(HelloReply { message: format!("Hello, {}", request.name) })
    }
}

// GreeterServer<T> is an ordinary trillium Handler — mount it like any other.
trillium_tokio::run(GreeterServer::new(MyGreeter));
```

The generated `GreeterClient` wraps a [`trillium_client::Client`]. Its `From`
constructor appends the service path to the client's base URL; each method returns
a typed call handle you drive like a [`trillium_client::Conn`] — configure it,
then `.await` it:

```rust,ignore
use greeter::v1::{GreeterClient, HelloRequest};

let client = GreeterClient::from(
    trillium_client::Client::new(trillium_tokio::ClientConfig::default())
        .with_base("http://127.0.0.1:8080"),
);

let reply = client
    .say_hello(HelloRequest { name: "world".into() })
    .await?
    .into_message()?;
assert_eq!(reply.message, "Hello, world");
```

A complete, runnable version covering all four shapes is in
[`examples/greeter.rs`](https://github.com/trillium-rs/trillium-grpc/blob/main/examples/greeter.rs)
(`cargo run --example greeter --features macros`).

# Guide

| Module | Topic |
|--------|-------|
| [`generating`] | Turning a `.proto` into Rust — the CLI, the `generate!` macro, and build scripts |
| [`serving`] | Implementing a service: the four server-side shapes and [`GrpcServerConn`] |
| [`calling`] | Calling a service: the typed client handles and per-client configuration |
| [`advanced`] | Driving the library types directly for shape-dynamic and unusual work |

[gRPC over HTTP/2 protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
[`generating`]: crate::generating
[`serving`]: crate::serving
[`calling`]: crate::calling
[`advanced`]: crate::advanced
