//! A gRPC server and client for [trillium](https://trillium.rs), built as a
//! thin layer on `trillium-http`'s HTTP/2, h2c (HTTP/2 over cleartext), and
//! HTTP/3 support. If you have a `.proto` service definition and want to serve
//! it — or call it — from a trillium application, this generates the glue and
//! handles the wire format. It is a from-scratch, spec-conformant
//! implementation of the [gRPC over HTTP/2 protocol].
//!
//! All four call shapes are supported — unary, server-streaming,
//! client-streaming, and bidirectional-streaming — along with protobuf
//! messages via [`prost`](https://docs.rs/prost) (and optional JSON behind the
//! `json` feature) and per-message compression (`gzip` by default, `deflate`
//! and `zstd` behind features).
//!
//! # Generating service code
//!
//! You write a `.proto`; codegen produces a Rust module containing the prost
//! message types, a service trait you implement, a `<Service>Server` handler
//! that mounts into a trillium handler chain, and a `<Service>Client` for
//! calling the service. There are three ways to run that codegen, and they all
//! produce the same output:
//!
//! **The `trillium grpc` CLI** writes the generated module to a path you choose
//! and check in:
//!
//! ```text
//! cargo install trillium-cli --features grpc --no-default-features
//! trillium grpc path/to/service.proto src/generated/service.rs -I path/to/include
//! ```
//!
//! Checking the output in is a feature, not a workaround — the generated code
//! is meant to be read, reviewed, and diffed like any other source file, the
//! way you'd treat a committed `Cargo.lock`. You bring it into your crate with
//! a plain module:
//!
//! ```rust,ignore
//! mod service {
//!     include!("generated/service.rs");
//! }
//! ```
//!
//! **The [`generate!`] macro** (requires the `macros` feature) runs the same
//! codegen at compile time and inlines the result, so there's no file to
//! manage:
//!
//! ```rust,ignore
//! trillium_grpc::generate!("proto/service.proto");
//! ```
//!
//! **A build script** (requires the `codegen` feature) writes the module into
//! `OUT_DIR` and re-runs when the `.proto` changes. This is the most
//! conventional option for prost users but the least transparent — the output
//! isn't visible in your tree — so reach for it only if the CLI and macro don't
//! fit:
//!
//! ```rust,ignore
//! // build.rs
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     trillium_grpc::codegen::compile_protos(&["proto/service.proto"], &["proto"])?;
//!     Ok(())
//! }
//! ```
//!
//! ```rust,ignore
//! // src/lib.rs
//! mod service {
//!     include!(concat!(env!("OUT_DIR"), "/service.v1.rs"));
//! }
//! ```
//!
//! # A worked example
//!
//! Given this service:
//!
//! ```proto
//! syntax = "proto3";
//! package greeter.v1;
//!
//! service Greeter {
//!   rpc SayHello(HelloRequest) returns (HelloReply);
//!   rpc SayHelloStream(HelloRequest) returns (stream HelloReply);
//! }
//!
//! message HelloRequest { string name = 1; }
//! message HelloReply { string message = 1; }
//! ```
//!
//! codegen emits a `Greeter` trait. Unary methods take the request and return
//! the response; server-streaming methods receive a [`ResponseSink`] to push
//! responses into. You implement it on your own type:
//!
//! ```rust,ignore
//! use trillium_grpc::{ResponseSink, Status};
//! use greeter::v1::{Greeter, HelloReply, HelloRequest};
//!
//! struct MyGreeter;
//!
//! impl Greeter for MyGreeter {
//!     async fn say_hello(&self, req: HelloRequest) -> Result<HelloReply, Status> {
//!         Ok(HelloReply { message: format!("Hello, {}", req.name) })
//!     }
//!
//!     async fn say_hello_stream(
//!         &self,
//!         req: HelloRequest,
//!         mut responses: ResponseSink<'_, HelloReply>,
//!     ) -> Result<(), Status> {
//!         for i in 1..=3 {
//!             responses.send(HelloReply { message: format!("Hello {i}, {}", req.name) }).await?;
//!         }
//!         Ok(())
//!     }
//! }
//! ```
//!
//! `GreeterServer::new(MyGreeter)` is an ordinary trillium
//! [`Handler`](trillium::Handler) — mount it like any other:
//!
//! ```rust,ignore
//! trillium_tokio::run(greeter::v1::GreeterServer::new(MyGreeter));
//! ```
//!
//! The generated `GreeterClient` wraps a [`trillium_client::Client`]. Its
//! constructor takes the client by `From`, appending the service path to the
//! base URL; each method then issues one RPC:
//!
//! ```rust,ignore
//! use greeter::v1::{GreeterClient, HelloRequest};
//!
//! let client = GreeterClient::from(
//!     trillium_client::Client::new(trillium_tokio::ClientConfig::default())
//!         .with_base("http://127.0.0.1:8080"),
//! );
//! let reply = client.say_hello(HelloRequest { name: "world".into() }).await?;
//! ```
//!
//! Per-client options — outbound compression, a default deadline — live on the
//! [`ServiceClientExt`] trait, which every generated client implements.
//!
//! # The streaming shapes
//!
//! The server trait method signature follows the RPC's directionality. The
//! streaming directions are expressed as borrowed primitives whose lifetime is
//! tied to the method call: you read requests and push responses through them,
//! and the framework writes the terminating status from your returned
//! `Result`:
//!
//! | RPC shape          | server receives                                | server returns         |
//! |--------------------|------------------------------------------------|------------------------|
//! | unary              | `Req`                                          | `Result<Resp, Status>` |
//! | server-streaming   | `Req`, [`ResponseSink`]`<'_, Resp>`            | `Result<(), Status>`   |
//! | client-streaming   | [`RequestStream`]`<'_, Req>`                   | `Result<Resp, Status>` |
//! | bidirectional      | [`Channel`]`<'_, Req, Resp>`                   | `Result<(), Status>`   |
//!
//! On the client side, request streams are taken as `impl Stream<Item = Req>`
//! and response streams are returned as `impl Stream<Item = Result<Resp,
//! Status>>` backed by [`ResponseStream`].
//!
//! [gRPC over HTTP/2 protocol]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md

pub mod client;
pub mod codec;
pub mod encoding;
pub mod frame;
pub mod metadata;
pub mod server;
pub mod status;
pub mod timeout;

#[cfg(feature = "codegen")]
pub use trillium_grpc_codegen as codegen;

#[cfg(feature = "macros")]
pub use trillium_grpc_macros::generate;

pub use client::{Client, ResponseStream, ServiceClient, ServiceClientExt, with_service_prefix};
pub use codec::{Codec, Prost};
pub use encoding::Encoding;
pub use futures_lite::Stream;
pub use metadata::{Metadata, MetadataError, MetadataValue};
pub use server::{Channel, RequestStream, ResponseSink, Server, dispatch::prepare_grpc_conn};
pub use status::{Code, Status};
