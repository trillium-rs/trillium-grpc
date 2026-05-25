Turning a `.proto` into Rust.

Codegen produces, for each service in the file, a `prost` message type per
message, a service trait you implement, a `<Service>Server<T>` handler, and a
`<Service>Client`. There are three front-ends, and they run the *same* codegen
over the same parsed descriptors, so the generated service code is identical
across all three. They differ only in *where* the output goes and *how* it lands
in your module tree.

| Front-end | Feature | Output | Reach for it when |
|-----------|---------|--------|-------------------|
| `trillium grpc` CLI | — (in `trillium-cli`) | a file you check in | you want the generated code visible and reviewable |
| [`generate!`] macro | `macros` | inlined at the call site | you'd rather not manage a file |
| build script | `codegen` | a file in `OUT_DIR` | you already drive `prost` from a build script |

# The CLI

The `trillium grpc` subcommand lives in `trillium-cli`:

```text
cargo install trillium-cli --features grpc --no-default-features
trillium grpc proto/greeter.proto src/generated --include proto
```

The first argument is the `.proto`; the second is an output **directory**
(default `./src`, created if missing); `--include`/`-I` adds import search paths
(the `.proto`'s own directory is always searched). One `<package>.rs` file is
written per proto package — `proto/greeter.proto` with `package greeter.v1;`
produces `src/generated/greeter.v1.rs`.

The file holds the generated items directly, with no enclosing module, so you
choose where they sit in your tree by wrapping the `include!`:

```rust,ignore
mod greeter {
    pub mod v1 {
        include!("generated/greeter.v1.rs");
    }
}
// greeter::v1::Greeter, greeter::v1::GreeterClient, …
```

Checking the output in is a feature, not a workaround: the generated code is
meant to be read, reviewed, and diffed like any other source file — the way
you'd treat a committed `Cargo.lock` — and you get a git history of how it
changed when you regenerate.

# The `generate!` macro

[`generate!`] runs the same codegen at compile time and inlines the result, so
there's no file to manage. It takes one or more comma-separated `.proto` paths,
resolved relative to your crate's `CARGO_MANIFEST_DIR`:

```rust,ignore
trillium_grpc::generate!("proto/greeter.proto");
// expands to: pub mod greeter { pub mod v1 { /* trait + Server + Client + messages */ } }
```

Unlike the file-based front-ends, the macro nests the output into a module tree
matching the package — `package greeter.v1;` becomes `greeter::v1` — so you
reach the items at `greeter::v1::Greeter` without wrapping anything. Cargo
re-expands the macro when a referenced `.proto` changes.

# A build script

With the `codegen` feature, [`compile_protos`](crate::codegen::compile_protos)
writes one `<package>.rs` per package into `OUT_DIR` and emits the
`cargo:rerun-if-changed` lines that re-run the build when a `.proto` changes:

```rust,ignore
// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    trillium_grpc::codegen::compile_protos(&["proto/greeter.proto"], &["proto"])?;
    Ok(())
}
```

```rust,ignore
// src/lib.rs
mod greeter {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/greeter.v1.rs"));
    }
}
```

This is the least transparent option — the output isn't in your tree — so reach
for it when you're already generating `prost` types from a build script and want
the gRPC glue to ride along. [`configure`](crate::codegen::configure) exposes the
one knob, whether to pretty-print the `OUT_DIR` output; for output to a custom
location or feeding the descriptors to another tool, drop to
[`generate_from_proto`](crate::codegen::generate_from_proto) and write the files
yourself.

[`generate!`]: crate::generate
