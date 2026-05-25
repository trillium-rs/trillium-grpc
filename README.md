# trillium-grpc — gRPC for Trillium

[![ci][ci-badge]][ci]
[![crates.io version][version-badge]][crate]
[![docs.rs][docs-badge]][docs]
[![codecov][codecov-badge]][codecov]

[ci]: https://github.com/trillium-rs/trillium-grpc/actions?query=workflow%3ACI
[ci-badge]: https://github.com/trillium-rs/trillium-grpc/workflows/CI/badge.svg
[version-badge]: https://img.shields.io/crates/v/trillium-grpc.svg?style=flat-square
[crate]: https://crates.io/crates/trillium-grpc
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs]: https://docs.rs/trillium-grpc
[codecov-badge]: https://codecov.io/gh/trillium-rs/trillium-grpc/graph/badge.svg
[codecov]: https://codecov.io/gh/trillium-rs/trillium-grpc

A spec-conformant gRPC server and client for [trillium](https://trillium.rs), built as a thin layer
on `trillium-http`'s HTTP/2 / h2c / HTTP/3 support. Supports all four call shapes (unary,
server-streaming, client-streaming, bidirectional), protobuf and optional JSON codecs, and
per-message compression.

You write a `.proto`; codegen produces the [`prost`](https://docs.rs/prost) message types, a
service trait you implement, a server handler that mounts into a trillium handler chain, and a
typed client. There are three ways to run codegen — the `trillium grpc` CLI (output committed to
your tree), the `generate!` macro, or a build script — all producing the same service code.

```rust,ignore
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        _conn: &mut GrpcServerConn,
        request: HelloRequest,
    ) -> Result<HelloReply, Status> {
        Ok(HelloReply { message: format!("Hello, {}", request.name) })
    }
}

trillium_tokio::run(GreeterServer::new(MyGreeter));
```

See the [API documentation](https://docs.rs/trillium-grpc) for the full guide, and
[`examples/greeter.rs`](examples/greeter.rs) for a complete, runnable server and client covering
all four shapes (`cargo run --example greeter --features macros`).

## License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br/>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
