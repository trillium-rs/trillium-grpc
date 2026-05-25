Implementing a service.

Codegen emits a trait with one method per RPC and a `<Service>Server<T>` that
wraps your implementation. The server is an ordinary trillium
[`Handler`](trillium::Handler): it matches the service's path prefix, validates
the gRPC request preflight, decodes and frames messages, enforces the deadline,
and writes the terminating `grpc-status` from the `Result` your method returns.
You implement the trait; the rest is the generated wrapper.

```rust,ignore
trillium_tokio::run(GreeterServer::new(MyGreeter));
```

# The control surface

Every method receives a [`GrpcServerConn`] by `&mut`. It is the per-call control
surface — it *owns* the connection for one RPC, value in and value out, the way a
trillium [`Handler`](trillium::Handler) owns its `Conn`:

- [`received_headers`](GrpcServerConn::received_headers) — the request's initial
  metadata.
- [`response_headers_mut`](GrpcServerConn::response_headers_mut) — initial
  response metadata. It commits when the first message is written (or when the
  method returns), so set it before then.
- [`response_trailers_mut`](GrpcServerConn::response_trailers_mut) — trailing
  response metadata, sent alongside `grpc-status` after the last message.
- [`deadline`](GrpcServerConn::deadline) — the instant derived from the request's
  `grpc-timeout`, if any. The framework already races your method against it; read
  it if you want to budget your own work.
- [`requests`](GrpcServerConn::requests) — for the request-streaming shapes, the
  inbound [`RequestStream`] of decoded messages.

# The four shapes

The method signature follows the RPC's directionality:

| RPC shape          | method receives                | method returns                                       |
|--------------------|--------------------------------|------------------------------------------------------|
| unary              | `&mut GrpcServerConn`, `Req`   | `Result<Resp, Status>`                               |
| server-streaming   | `&mut GrpcServerConn`, `Req`   | `Result<impl Stream<Item = Result<Resp, Status>>, Status>` |
| client-streaming   | `&mut GrpcServerConn`          | `Result<Resp, Status>`                               |
| bidirectional      | `&mut GrpcServerConn`          | `Result<impl `[`BidiResponder`]`, Status>`           |

**Unary** is one request in, one response out:

```rust,ignore
async fn say_hello(
    &self,
    _conn: &mut GrpcServerConn,
    request: HelloRequest,
) -> Result<HelloReply, Status> {
    Ok(HelloReply { message: format!("Hello, {}", request.name) })
}
```

**Server-streaming** returns a [`Stream`] the framework pulls responses from until
it ends. The returned stream is `+ use<>` so it doesn't capture `&self`:

```rust,ignore
async fn say_hello_stream(
    &self,
    _conn: &mut GrpcServerConn,
    request: HelloRequest,
) -> Result<impl Stream<Item = Result<HelloReply, Status>> + Send + use<>, Status> {
    Ok(futures_lite::stream::iter((1..=3).map(move |i| {
        Ok(HelloReply { message: format!("Hello #{i}, {}", request.name) })
    })))
}
```

**Client-streaming** reads the inbound messages off the conn with
[`requests`](GrpcServerConn::requests), then returns one response.
[`RequestStream::recv`] yields `Ok(None)` at end of stream:

```rust,ignore
async fn say_hello_many(&self, conn: &mut GrpcServerConn) -> Result<HelloReply, Status> {
    let mut names = Vec::new();
    let mut requests = conn.requests::<HelloRequest>();
    while let Some(request) = requests.recv().await? {
        names.push(request.name);
    }
    Ok(HelloReply { message: format!("Hello, {}", names.join(" and ")) })
}
```

**Bidirectional-streaming** needs to read and write at the same time, which can
only happen after the response head is on the wire. So the trait method is a
*prologue*: it runs first, may read early requests and set initial metadata, and
returns a [`BidiResponder`]. The responder's
[`respond`](BidiResponder::respond) method gets a [`Channel`] and drives the
read-while-write loop afterward — [`Channel::recv`] for requests,
[`Channel::send`] for responses:

```rust,ignore
async fn say_hello_chat(
    &self,
    _conn: &mut GrpcServerConn,
) -> Result<impl BidiResponder<HelloRequest, HelloReply> + use<>, Status> {
    Ok(Chat)
}

struct Chat;
impl BidiResponder<HelloRequest, HelloReply> for Chat {
    async fn respond(self, mut channel: Channel<'_, HelloRequest, HelloReply>) -> Result<(), Status> {
        while let Some(request) = channel.recv().await.transpose()? {
            channel.send(HelloReply { message: format!("Hey, {}", request.name) }).await?;
        }
        Ok(())
    }
}
```

The [`server::bidi`](crate::server::bidi) module explains why bidi splits into two
functions and what crosses between them.

# Returning errors

Return `Err(Status)` from any shape and the framework ends the call with that
`grpc-status` and message — for unary and the streaming shapes alike, on the
prologue or mid-loop. [`Status`] has a constructor per gRPC code
([`Status::not_found`], [`Status::invalid_argument`], …). Trailing metadata you
set via [`response_trailers_mut`](GrpcServerConn::response_trailers_mut) (or
[`Channel::response_trailers_mut`]) still rides the error trailers.
