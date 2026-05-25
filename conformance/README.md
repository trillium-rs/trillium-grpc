# trillium-grpc conformance harness

Drives the [connectrpc conformance suite][conformance] against trillium-grpc in
both directions:

- **server** — `src/bin/server.rs` speaks the runner's
  `ServerCompatRequest`/`ServerCompatResponse` protocol and serves
  `connectrpc.conformance.v1.ConformanceService` (`src/service.rs`, generated
  from the vendored protos by `build.rs`) over h2c.
- **client** — `src/bin/client.rs` speaks the runner's
  `ClientCompatRequest`/`ClientCompatResponse` protocol, driving a
  [`GrpcClientConn`] per stream type against the runner's reference server. Each
  case runs on its own task (out-of-order results are allowed) so one slow/hung
  RPC can't block the rest.

[`GrpcClientConn`]: ../src/client/conn.rs

## Running

The runner (`connectconformance`) is a Go binary. Build it from a checkout of
the conformance repo (we develop against `../grpc-conformance`):

```sh
# in ../grpc-conformance
go build -o /tmp/connectconformance ./cmd/connectconformance
```

Then build the harness and run the suite:

```sh
cargo build                                    # builds target/debug/conformance-server
/tmp/connectconformance --mode server \
    --conf config/grpc-h2c.yaml \
    --known-failing @config/grpc-known-failing.txt \
    -- ../target/debug/conformance-server
```

`config/grpc-h2c.yaml` restricts the run to what we speak today: gRPC over
HTTP/2 (h2c), the protobuf codec, identity + gzip compression, no TLS.

To run the **client** suite (the runner stands up a reference server and drives
our client):

```sh
/tmp/connectconformance --mode client \
    --conf config/grpc-h2c-client.yaml \
    --known-failing @config/grpc-known-failing-client.txt \
    -- ../target/debug/conformance-client
```

## Status

**Server: 324 / 324** — all four call shapes, including bidi-stream (closed by the
prologue + `BidiResponder` redesign, PLAN task #10). `config/grpc-known-failing.txt`
is empty.

**Client: 630 / 632**, stable run-to-run. The only remaining failures are the two
variants (TLS on/off) of `nonexistent-http-status-code`, parked on a trillium-http
limitation (its `Status` type can't carry an unrecognized HTTP status code); see
`config/grpc-known-failing-client.txt`. The previously-rotating "empty trailers →
Unknown" flake was a trillium-http bug — an h2 response carrying `content-length`
ended its body on the declared length before the trailing `grpc-status` HEADERS
arrived — now fixed (the `Raw` body ends on `END_STREAM`, not `content-length`),
carried via the `../trillium` path patch in the root `Cargo.toml`.

[conformance]: https://github.com/connectrpc/conformance
