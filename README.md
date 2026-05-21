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
