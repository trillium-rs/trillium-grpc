Calling a service.

The generated `<Service>Client` wraps a [`trillium_client::Client`]. Build it with
`From`, passing a client whose base URL points at the server — the constructor
appends the service path, so each method only names its own RPC:

```rust,ignore
let client = GreeterClient::from(
    trillium_client::Client::new(trillium_tokio::ClientConfig::default())
        .with_base("http://127.0.0.1:8080"),
);
```

Each method returns a typed call handle whose surface fits the RPC's shape, so
you never need the `.proto` to use it correctly. A handle is built lazily: you
configure it with chainable setters, then `.await` and/or iterate it to run the
call. Initial metadata and trailing metadata are readable off the handle once the
call has run — on the error path too.

| RPC shape        | method returns    | drive it with                                          |
|------------------|-------------------|--------------------------------------------------------|
| unary            | [`UnaryConn`]     | `.await?`, then [`into_message`](UnaryConn::into_message) |
| server-streaming | [`StreamingConn`] | iterate it as a [`Stream`]                             |
| client-streaming | [`UnaryConn`]     | pass `impl Stream<Item = Req>`, then `.await?`         |
| bidirectional    | [`BidiConn`]      | live [`send`](BidiConn::send) / [`recv`](BidiConn::recv) |

# Unary

Awaiting a [`UnaryConn`] runs the whole call — sends the request, reads the one
response, reads the trailers. [`into_message`](UnaryConn::into_message) then hands
you the decoded message or the `grpc-status` as a `Status`:

```rust,ignore
let reply = client
    .say_hello(HelloRequest { name: "world".into() })
    .await?
    .into_message()?;
```

# Server-streaming

A [`StreamingConn`] *is* a [`Stream`] of `Result<Resp, Status>`; the first poll
fires the request and reads the head. Iterate it to completion:

```rust,ignore
use futures_lite::StreamExt;

let mut stream = client.say_hello_stream(HelloRequest { name: "world".into() });
while let Some(reply) = stream.next().await {
    println!("{}", reply?.message);
}
```

To read the initial metadata *before* the first message, `.await` the handle
first — that reads the head only and hands the handle back, still iterable.

# Client-streaming

Pass an `impl Stream<Item = Req>`; awaiting the returned [`UnaryConn`] drains it
and reads the single response:

```rust,ignore
let names = futures_lite::stream::iter(
    ["Alice", "Bob"].map(|name| HelloRequest { name: name.into() }),
);
let reply = client.say_hello_many(names).await?.into_message()?;
```

# Bidirectional

A [`BidiConn`] is a live full-duplex handle: [`send`](BidiConn::send) requests,
[`close_send`](BidiConn::close_send) when done sending, and
[`recv`](BidiConn::recv) responses — interleaved, turn-taking on `&mut self`. It
also implements [`Stream`] for the receive side:

```rust,ignore
let mut chat = client.say_hello_chat();
chat.send(HelloRequest { name: "Carol".into() }).await?;
chat.close_send().await?;
while let Some(reply) = chat.recv().await? {
    println!("{}", reply.message);
}
```

# Per-call configuration

Every handle carries the same chainable builders before you drive it:

- [`with_ascii_metadata`](UnaryConn::with_ascii_metadata) /
  [`with_binary_metadata`](UnaryConn::with_binary_metadata) — request metadata.
  Binary keys (suffix `-bin`) carry arbitrary bytes; ASCII keys carry
  printable-ASCII strings.
- [`with_timeout`](UnaryConn::with_timeout) — a `grpc-timeout` deadline for this
  one call.
- [`cancel_handle`](UnaryConn::cancel_handle) — a cheaply-cloneable
  [`CancelHandle`] you can move elsewhere and trigger to cancel the call.

Read the response metadata with `metadata()` (initial) and `trailers()`, both
available once the call has run.

# Error surfacing

Errors split by stage. Transport failures and a non-gRPC response head surface
from the `.await` (the `?` above). The logical `grpc-status` surfaces at read time
— [`into_message`](UnaryConn::into_message), [`status`](UnaryConn::status), or a
stream item — so the response metadata stays readable even when the call failed.
A unary deadline can fire at either stage; to treat both as one error, write
`call.await.and_then(|c| c.into_message())`.

# Per-client configuration

Options that apply to every call a client makes live on the [`ServiceClientExt`]
trait, which every generated client implements:

- [`with_outbound_compression`](ServiceClientExt::with_outbound_compression) —
  compress request messages with an [`Encoding`].
- [`with_default_timeout`](ServiceClientExt::with_default_timeout) — a fallback
  `grpc-timeout` for calls that don't set their own.

```rust,ignore
use trillium_grpc::{Encoding, ServiceClientExt};

let client = GreeterClient::from(/* … */)
    .with_outbound_compression(Encoding::Gzip)
    .with_default_timeout(std::time::Duration::from_secs(5));
```
