Driving the library types directly.

If you're calling or serving a known service, you want the generated client and
server and the [`calling`](crate::calling) / [`serving`](crate::serving) guides —
not this page. The types here are the engine the generated code is built on,
exposed for the cases the typed handles can't express: a single piece of code
that has to handle *any* RPC shape chosen at runtime, a service whose method set
isn't known until run time, or a test harness that exercises the protocol
directly.

# The client engine: `GrpcClientConn`

[`GrpcClientConn`] is an owned, per-RPC connection handle — the client-side mirror
of [`GrpcServerConn`], and a sibling of `trillium_websockets::WebSocketConn`. The
generated [`UnaryConn`] / [`StreamingConn`] / [`BidiConn`] are thin typed views
over it; reach for the engine when you need one code path to serve every shape.

You construct it with the codec, the RPC path, request metadata, an optional
timeout, and whether the call is full-duplex:

```rust,ignore
use trillium_grpc::{GrpcClientConn, Metadata, Prost};

let mut conn = GrpcClientConn::<HelloRequest, HelloReply>::new::<Prost>(
    &client,
    "/greeter.v1.Greeter/SayHello",
    Metadata::new(),
    None,        // no per-call deadline
    false,       // not full-duplex
);
```

The handle is uniform across shapes: [`send`](GrpcClientConn::send) a request,
[`close_send`](GrpcClientConn::close_send) the request side,
[`recv`](GrpcClientConn::recv) responses until it yields `Ok(None)`, and read
[`headers`](GrpcClientConn::headers) / [`trailers`](GrpcClientConn::trailers)
once they've arrived. No network I/O happens until `close_send` or the first
`recv`.

The `full_duplex` flag is load-bearing. It selects between the two transports the
protocol mandates: a full-duplex call interleaves sends and receives over one
long-lived stream, while every other shape sends its requests as a body that ends
before the response head arrives. The typed handles set this for you; driving the
engine yourself means choosing correctly — `true` only for genuinely
read-while-write bidirectional calls.

[`cancel_handle`](GrpcClientConn::cancel_handle) returns a cheaply-cloneable,
`Send` [`CancelHandle`]; [`cancel`](CancelHandle::cancel) on any clone cancels the
RPC, so you can cancel from another task or after a side condition.

# The server control surface

On the server, [`GrpcServerConn`] is already the value your generated methods
receive, and the dispatch entry points it flows through —
[`Server::unary`](crate::server::Server), `server_streaming`, `client_streaming`,
`bidi` — are public on the [`Server`] trait. A hand-written
[`Handler`](trillium::Handler) can match a path itself, call
[`prepare_grpc_conn`](crate::server::dispatch::prepare_grpc_conn) to validate the
preflight and build the conn, and dispatch to whichever shape it likes — which is
exactly what the generated `<Service>Server` does. The generated code is the
reference for wiring this by hand.

# Worked example: the conformance harness

The [connectrpc conformance suite] driver in the repo's `conformance/` directory
is the realest example of the engine in use. The runner hands the harness a
description of each case — the stream type, the messages, the metadata to send,
cancellation timing — and the harness builds one [`GrpcClientConn`] per case,
branching on the stream type at run time to decide `full_duplex` and the
send/recv ordering. The typed handles can't express that (each is fixed to one
shape), so the harness drives the engine directly. If you're building something
that dispatches over arbitrary RPC shapes, that binary is the pattern to copy.

[connectrpc conformance suite]: https://github.com/connectrpc/conformance
