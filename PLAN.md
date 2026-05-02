# trillium-grpc — implementation plan

## Goal

A spec-conformant gRPC implementation that fits trillium's architecture: server
and client built as a thin layer on `trillium-http`'s h2/h3/h2c support. No
dependency on `tonic` or its codegen.

In scope for v1:
- Server-side handler with all four call shapes (unary / server-stream /
  client-stream / bidi)
- Client-side as a `trillium-client` extension
- Codegen: pure-Rust, library API in `trillium-grpc` + CLI in `trillium-cli`
- Default codec: protobuf via `prost`. Optional JSON via `serde_json` behind a
  feature flag.
- Compression: `identity` + `gzip` (per-message)
- `Status` / `Code` types, error trailer formatting
- Path dispatch, content-type validation, `te: trailers` enforcement

Deferred (named so we don't accidentally design them in):
- Cancellation propagation (layered via Conn's swansong when needed)
- `grpc-timeout` enforcement (handler-space, easy to add)
- gRPC-Web (separate sibling crate)
- Standard reflection service (`grpc.reflection.v1.ServerReflection`)
- Standard health service (`grpc.health.v1.Health`)
- `snappy` / `deflate` compression
- `application/grpc-web` and any other non-h2/h3 transports
- Custom `google.rpc.Status` details (`grpc-status-details-bin`)

## Architecture

### What lives below us, untouched

`trillium-http` provides:
- h2, h3, h2c (with prior knowledge support) — gRPC adds zero new HTTP/2 frame
  types
- HEADERS parsing → `Headers`
- Trailers as a first-class concept, including dynamic accumulation via
  `BodySource::trailers()`
- `ReceivedBody` (request body as `AsyncRead`)
- `Body::new_with_trailers(BodySource, len)` for streaming responses with
  computed-on-completion trailers — exactly the shape we need for `grpc-status`
- `Conn::swansong()` for cancellation when we wire it up

### What `trillium-grpc` adds

```
trillium-grpc/
├── src/
│   ├── lib.rs              — re-exports, prelude
│   ├── status.rs           — Status, Code, trailer (de)serialization
│   ├── codec/
│   │   ├── mod.rs          — Codec<T> trait
│   │   ├── prost.rs        — Prost impl (default)
│   │   └── json.rs         — Json impl (feature = "json")
│   ├── encoding.rs         — Encoding enum, per-message gzip compress/decompress
│   ├── frame/
│   │   ├── reader.rs       — MessageStream<T>: AsyncRead → Stream<Item=Result<T, Status>>
│   │   └── writer.rs       — message-stream → AsyncRead body source
│   ├── server/
│   │   ├── mod.rs          — public dispatch fns: unary, server_streaming, …
│   │   ├── content_type.rs — request-side validation
│   │   └── trailers.rs     — response trailer construction (status, metadata)
│   ├── client/             — trillium-client extension (Phase 5+)
│   ├── metadata.rs         — -bin/percent-encoded custom metadata helpers
│   └── codegen/            — feature = "codegen"
│       ├── mod.rs          — pub fn generate_from_proto(...)
│       ├── parse.rs        — wraps protox to load FileDescriptorSet
│       ├── messages.rs     — drives prost-build for message types
│       ├── services.rs     — emits service trait + Server struct
│       └── render.rs       — quote!/prettyplease formatting
├── tests/
│   ├── tonic_roundtrip.rs  — tonic client → our server (wire conformance)
│   ├── codegen_snapshots.rs — insta snapshots of generated code
│   ├── compiles/           — generated outputs checked in, compiled as tests
│   └── proto/              — .proto fixtures
└── examples/
    └── greeter.rs
```

## API surface

### User experience for a hand-written service

```rust
// What codegen would emit, hand-written for clarity in this sketch:

pub trait Greeter: Send + Sync + 'static {
    fn say_hello(&self, req: HelloRequest)
        -> impl Future<Output = Result<HelloReply, Status>> + Send;

    fn say_hello_stream(&self, req: HelloRequest)
        -> impl Future<Output = Result<impl Stream<Item = Result<HelloReply, Status>> + Send, Status>> + Send;

    fn say_hello_many(&self, reqs: impl Stream<Item = Result<HelloRequest, Status>> + Send)
        -> impl Future<Output = Result<HelloReply, Status>> + Send;

    fn say_hello_chat(&self, reqs: impl Stream<Item = Result<HelloRequest, Status>> + Send)
        -> impl Future<Output = Result<impl Stream<Item = Result<HelloReply, Status>> + Send, Status>> + Send;
}

pub struct GreeterServer<T>(Arc<T>);

impl<T: Greeter> Handler for GreeterServer<T> {
    async fn run(&self, conn: Conn) -> Conn {
        const PREFIX: &str = "/myapp.v1.Greeter";
        let Some(method) = conn.path().strip_prefix(PREFIX) else { return conn; };
        if conn.method() != Method::Post { return conn; }
        if !is_grpc_content_type(conn.headers()) { return conn; }

        match method {
            "/SayHello"       => unary(conn, |req|  self.0.say_hello(req)).await,
            "/SayHelloStream" => server_streaming(conn, |req|  self.0.say_hello_stream(req)).await,
            "/SayHelloMany"   => client_streaming(conn, |reqs| self.0.say_hello_many(reqs)).await,
            "/SayHelloChat"   => bidi(conn, |reqs| self.0.say_hello_chat(reqs)).await,
            _ => respond_unimplemented(conn).await,
        }
    }
}
```

### Free dispatch functions (no turbofish needed by callers)

```rust
pub async fn unary<C, Req, Resp, F, Fut>(conn: Conn, f: F) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    Req: Send + 'static,
    Resp: Send,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Result<Resp, Status>>;

pub async fn server_streaming<C, Req, Resp, S, F, Fut>(conn: Conn, f: F) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    S: Stream<Item = Result<Resp, Status>> + Send,
    F: FnOnce(Req) -> Fut,
    Fut: Future<Output = Result<S, Status>>;

pub async fn client_streaming<C, Req, Resp, F, Fut>(conn: Conn, f: F) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    F: FnOnce(MessageStream<C, Req>) -> Fut,
    Fut: Future<Output = Result<Resp, Status>>;

pub async fn bidi<C, Req, Resp, S, F, Fut>(conn: Conn, f: F) -> Conn
where
    C: Codec<Req> + Codec<Resp>,
    S: Stream<Item = Result<Resp, Status>> + Send,
    F: FnOnce(MessageStream<C, Req>) -> Fut,
    Fut: Future<Output = Result<S, Status>>;
```

The `C` codec parameter defaults to `Prost` in codegen. The user closure's
input/output types pin `Req`/`Resp` via inference. If inference proves cranky in
practice, codegen falls back to `unary::<Prost, HelloRequest, HelloReply, _, _>(...)`.

### `Codec<T>` trait

```rust
pub trait Codec<T>: 'static {
    fn content_type_suffix() -> &'static str;        // "proto" | "json" | ...
    fn encode(value: &T) -> Result<Bytes, Status>;
    fn decode(bytes: &[u8]) -> Result<T, Status>;
}

pub struct Prost;
impl<T: prost::Message + Default> Codec<T> for Prost { /* ... */ }

#[cfg(feature = "json")]
pub struct Json;
#[cfg(feature = "json")]
impl<T: Serialize + DeserializeOwned + 'static> Codec<T> for Json { /* ... */ }
```

Future-proofing for asymmetric encode/decode (not in v1):
```rust
pub struct CrossCodec<E, D>(PhantomData<(E, D)>);
impl<T, E: Codec<T>, D: Codec<T>> Codec<T> for CrossCodec<E, D> { /* delegate */ }
```

### `Status` / `Code`

```rust
pub struct Status { pub code: Code, pub message: String, pub details: Option<Bytes> }

#[repr(u8)]
pub enum Code {
    Ok = 0, Cancelled = 1, Unknown = 2, InvalidArgument = 3, DeadlineExceeded = 4,
    NotFound = 5, AlreadyExists = 6, PermissionDenied = 7, ResourceExhausted = 8,
    FailedPrecondition = 9, Aborted = 10, OutOfRange = 11, Unimplemented = 12,
    Internal = 13, Unavailable = 14, DataLoss = 15, Unauthenticated = 16,
}

impl Status {
    pub fn ok() -> Self;
    pub fn invalid_argument(msg: impl Into<String>) -> Self;
    pub fn unimplemented(msg: impl Into<String>) -> Self;
    /* ... one constructor per code ... */

    pub(crate) fn into_trailers(self) -> Headers; // grpc-status, grpc-message, [grpc-status-details-bin]
    pub(crate) fn from_trailers(h: &Headers) -> Result<(), Self>; // client side
}
```

### `MessageStream<C, T>`

Internal type the framework instantiates from `ReceivedBody`. Implements
`Stream<Item = Result<T, Status>>`. Users see it as `impl Stream<...>` in their
trait method signatures. Parameterized on the codec so decoding is statically
dispatched.

### `Encoding`

```rust
pub enum Encoding { Identity, Gzip }

impl Encoding {
    pub fn from_grpc_encoding(s: &str) -> Option<Self>;
    pub fn as_grpc_encoding(&self) -> &'static str;
    pub async fn compress(&self, msg: &[u8]) -> Result<Vec<u8>, Status>;
    pub async fn decompress(&self, msg: &[u8], max_size: usize) -> Result<Vec<u8>, Status>;
}
```

`max_size` on decompress is mandatory (zip-bomb defense). Default cap from
`grpc-max-recv-message-length` if set, else a conservative default (e.g., 4 MiB
matching grpc-go's default).

## Codegen

### Library API (gated on `codegen` feature)

```rust
pub mod codegen {
    pub struct Options {
        pub package_path_prefix: String,  // e.g. "crate::proto"
        pub include_paths: Vec<PathBuf>,
        pub codec: CodecChoice,           // Prost | Json | Custom("...")
        // ...
    }

    pub struct GeneratedFiles { pub files: BTreeMap<PathBuf, String> }

    pub fn generate_from_proto(src: &Path, opts: &Options) -> Result<GeneratedFiles>;
    pub fn generate_from_descriptors(fds: &FileDescriptorSet, opts: &Options) -> Result<GeneratedFiles>;
}
```

`generate_from_proto` parses .proto with `protox` (pure-Rust), drives
`prost-build` for message types, emits service traits + Server structs via
`quote!` + `prettyplease`.

### CLI

Lives in `trillium-cli` behind a `grpc` feature. Cargo cell:
```
trillium grpc codegen ./greeter.proto ./src/greeter
```
Reads .proto(s), calls `trillium_grpc::codegen::generate_from_proto`, writes
output files to the target directory. Output is plain `rustfmt`'d Rust, intended
to be committed.

### Output shape

For a .proto with multiple services, one file per service plus one shared
messages file:
```
src/greeter/
├── mod.rs              — re-exports the service modules + messages
├── messages.rs         — prost-generated message structs
└── greeter.rs          — Greeter trait + GreeterServer<T> + GreeterClient (later)
```

## Testing strategy

### Codegen — insta snapshots + compile-tests

Two complementary test targets:
1. **Snapshot tests** (`tests/codegen_snapshots.rs`): for each fixture .proto in
   `tests/proto/`, run `generate_from_proto` and snapshot the rendered output
   with `insta`. Catches regressions in formatting and structure.
2. **Compile tests** (`tests/compiles/`): pre-generated outputs of the same
   fixtures, checked into the repo, included as a sub-module of the test crate
   so they're typechecked on every build. Regenerated via the CLI when codegen
   changes; mismatches between snapshot and checked-in output flag a missed
   regen.

Fixture .protos to cover:
- `unary_only.proto` — single service, single unary RPC
- `all_shapes.proto` — one service, all four call shapes
- `multi_service.proto` — two services in one file
- `complex_messages.proto` — oneofs, nested types, repeated, packed, optional,
  enums, well-known types
- `naming.proto` — services and methods that exercise PascalCase ↔ snake_case
  conversion, packages with dots, reserved-word fields
- `imports.proto` — cross-file imports

### Runtime — bootstrap via tonic, transition to own-world

**Phase A — wire conformance via tonic.** While we're proving correctness,
write integration tests where:
- Server: our `trillium-grpc` impl serving a hand-written or generated service
- Client: tonic's generated client of the same .proto
- Tests assert request/response correctness over the wire
- Both ends are on `localhost`, h2c. TLS variants come later.

This phase ensures we're spec-conformant against a known-good implementation —
if tonic can talk to us, we are speaking gRPC.

**Phase B — own-world tests once client lands.** Once `trillium-grpc` has a
client side (Phase 5 in the milestone list below), most new tests use both
ends from our crate. Both sides share types and traits, so tests can assert
fluently:
```rust
let server = GreeterServer::new(MyGreeter);
let client = GreeterClient::connect(addr).await?;
let reply = client.say_hello(HelloRequest { name: "world".into() }).await?;
assert_eq!(reply.message, "Hello, world");
```
The tonic-roundtrip tests stay as a conformance backstop on the CI matrix.

**Phase C (later) — official gRPC interop.** The grpc/grpc repo publishes a
canonical interop test runner. We can implement the standard interop service
in trillium-grpc and have CI run the official runner against it. This is the
gold-standard correctness check.

### Test matrix per call shape

For each of unary / server-stream / client-stream / bidi:
- Happy path
- User method returns `Err(Status)` → trailer-only error
- Decode error on wire → INVALID_ARGUMENT
- Compression: identity, gzip (request-side, response-side, both)
- Custom metadata round-trips (request → method, method → response trailers)
- (Phase 4+) `grpc-timeout` honored

### Transport matrix

- h2c with prior knowledge (primary; what most service-mesh deployments use)
- h2 over TLS via `trillium-rustls`
- h3 (lower priority — gRPC-over-h3 is real but not common yet)

## Phasing / milestone sequence

Each phase is roughly one PR's worth of work.

**Phase 1 — Foundations.** Cargo.toml, `[patch.crates-io]` cell, module
skeleton, `Status`/`Code`, `Codec<T>` trait, `Prost` impl, frame reader/writer
with no compression, content-type/te validation. No call-shape dispatch yet —
just the primitives. Unit tests on framing.

**Phase 2 — Unary end-to-end.** `unary()` dispatch fn. Hand-rolled `Greeter`
trait + `GreeterServer<T>` (no codegen). One tonic-client roundtrip test.

**Phase 3 — Streaming shapes.** `server_streaming`, `client_streaming`, `bidi`.
`MessageStream<C, T>`. Tonic-roundtrip tests for each.

**Phase 4 — Codegen.** `codegen` feature in trillium-grpc. CLI integration in
trillium-cli (`grpc` feature). Snapshot tests. Replace hand-rolled `Greeter` in
tests with generated.

**Phase 5 — Client.** `trillium-client` extension. Generated client stubs.
Migrate tests to dual-trillium-grpc. Keep tonic tests as conformance.

**Phase 6 — Metadata + compression + timeout.** Custom metadata helpers (-bin
encoding, percent encoding). Per-message gzip via `async-compression`.
`grpc-timeout` parse + race with deadline.

**Phase 7 — Cancellation.** Wire `Conn::swansong()` through user-facing futures
and streams so peer-initiated RST_STREAM cancels in-flight work. Likely just a
cancellation token threaded into the `MessageStream` and a future-races-deadline
helper.

**Phase 8+ (deferred, separate work).** gRPC-Web (sibling crate), reflection
service, health service, snappy/deflate compression, official interop runner.

## Cross-workspace dev setup

`trillium-grpc` lives in its own repo (`/Users/jbr/code/trillium-grpc`) outside
the trillium workspace. During pre-1.0 iteration on the underlying protocol
crates, `Cargo.toml` carries a `[patch.crates-io]` block pointing at
`../trillium/{crate}` — same convention trillium-cli already uses.

Once `trillium-grpc` is stable enough to publish, the patch block becomes
optional (only used when developing against unreleased trillium core).

## Open questions / risks

- **Type inference on dispatch fns.** We're betting full inference will work
  with `unary(conn, |req| inner.say_hello(req))`. If it doesn't, codegen falls
  back to turbofish — generated code, no user impact.
- **`Stream<Item = Result<T, Status>> + Send` in trait return position.** AFIT
  + RPITIT is stable but exotic enough that generated code may need explicit
  `+ Send` bounds (or `trait_variant`). Confirm during Phase 3.
- **Trailers-only responses on h2/h3.** trillium-http will emit
  HEADERS+empty-DATA+trailers HEADERS rather than a single trailers-only HEADERS
  frame. Spec-compliant but suboptimal. Optimization deferred.
- **Cancellation correctness.** Deferred to Phase 7. Risk: discovering that
  some component of trillium-http doesn't propagate stream cancellation.
  Fallback is to address that in trillium-http first.
- **MSRV.** AFIT + RPITIT need 1.75+; using `Send`-bounded RPIT in trait may
  push to a later version. Pin in `Cargo.toml`.
