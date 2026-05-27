//! `.proto` → Rust codegen for trillium-grpc services.
//!
//! Gated on the `codegen` feature. Pulls in `protox` (pure-Rust .proto parser),
//! `prost-build` (message types + service-generator hook), `quote!` /
//! `prettyplease` (service trait + `Server` struct emission and formatting).
//!
//! Output shape: one Rust file per .proto package, e.g. `greeter.v1.rs` for
//! `package greeter.v1;`. The trait, the `Server<T>` struct, and the
//! prost-generated message types all live in the same module — that's the
//! natural prost-build shape and means `Method::input_type` / `output_type`
//! resolve as bare names.
//!
//! Generated code uses unqualified type names (`Conn`, `Handler`, `Status`,
//! …) and a `use` block at the top of each module to bring them into scope.
//! Downstream crates only need to depend on `trillium-grpc` — `Stream`,
//! `RequestStream`, `ResponseSink`, and `Channel` are all re-exported there.
//!
//! Streaming server-side trait methods take borrowed primitives
//! (`RequestStream<'_, Req>`, `ResponseSink<'_, Resp>`, `Channel<'_, Req, Resp>`)
//! whose lifetime is tied to the user closure: responses are pushed through the
//! sink and the framework writes `grpc-status` trailers based on the user's
//! `Result`. The client-facing methods expose `impl Stream`-based ergonomics on
//! top of a spawned reader.

use prost_build::{Config, Method, Module, Service, ServiceGenerator};
use prost_types::FileDescriptorSet;
use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
};

/// Anything that can go wrong while generating Rust from `.proto` sources.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// protox failed to parse or resolve the `.proto` sources.
    #[error("failed to compile .proto sources: {0}")]
    Protox(#[from] protox::Error),

    /// An I/O error from prost-build or from writing output files.
    #[error("prost-build failed: {0}")]
    ProstBuild(#[from] std::io::Error),

    /// The generated source didn't parse as Rust — a codegen bug.
    #[error("generated code did not parse as valid Rust: {0}")]
    Syn(#[from] syn::Error),

    /// A build-script helper was called outside a build script (no `OUT_DIR`).
    #[error("OUT_DIR is not set; the build-script helpers must run from a build.rs")]
    NoOutDir,
}

/// Settings for a [`generate_from_proto`] / [`generate_from_descriptors`] run.
#[derive(Debug, Clone)]
pub struct Options {
    /// Include paths handed to protox for resolving `import` statements.
    pub include_paths: Vec<PathBuf>,

    /// Run the generated code through `prettyplease`. Leave this on when the
    /// output is written to a file a human will read; turn it off when the
    /// result is fed straight back into the compiler (as the proc-macro path
    /// does), where pretty-printing is wasted work.
    pub format: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            include_paths: Vec::new(),
            format: true,
        }
    }
}

/// The Rust modules produced by a codegen run — one entry per proto package.
#[derive(Debug, Default)]
pub struct GeneratedFiles {
    /// Map of `<package>.rs` → file contents. Pretty-printed iff
    /// [`Options::format`] was set (default: `true`).
    pub files: BTreeMap<PathBuf, String>,
}

/// Parse the given `.proto` files and emit Rust modules.
///
/// `srcs` are the source files to compile; `opts.include_paths` are passed to
/// protox for resolving `import` statements.
pub fn generate_from_proto<P: AsRef<Path>>(
    srcs: &[P],
    opts: &Options,
) -> Result<GeneratedFiles, Error> {
    let includes: Vec<&Path> = opts.include_paths.iter().map(PathBuf::as_path).collect();
    let srcs: Vec<&Path> = srcs.iter().map(P::as_ref).collect();
    let fds = protox::compile(srcs, includes)?;
    generate_from_descriptors(fds, opts)
}

/// Emit Rust modules from an already-parsed `FileDescriptorSet`. Useful when
/// the caller has already produced descriptors via another tool.
pub fn generate_from_descriptors(
    fds: FileDescriptorSet,
    opts: &Options,
) -> Result<GeneratedFiles, Error> {
    let mut config = Config::new();
    config.service_generator(Box::new(TrilliumServiceGenerator::default()));

    let requests: Vec<(Module, prost_types::FileDescriptorProto)> = fds
        .file
        .into_iter()
        .map(|fdp| {
            let pkg = fdp.package.as_deref().unwrap_or_default();
            (Module::from_protobuf_package_name(pkg), fdp)
        })
        .collect();

    let raw = config.generate(requests)?;

    let mut files = BTreeMap::new();
    for (module, code) in raw {
        let filename = module.to_file_name_or("_");
        let path = PathBuf::from(filename);
        let body = if opts.format {
            format_rust(&code)
        } else {
            code
        };
        files.insert(path, body);
    }

    Ok(GeneratedFiles { files })
}

/// Compile `.proto` files from a build script, writing one Rust module per
/// package into `OUT_DIR` and emitting `cargo:rerun-if-changed` for every
/// compiled file (including transitive imports).
///
/// `protos` are the source files; `includes` are the directories used to
/// resolve `import` statements — pass the root(s) of your proto tree. Each
/// generated `<package>.rs` is pulled into your crate with
/// `include!(concat!(env!("OUT_DIR"), "/<package>.rs"))`.
///
/// This is the zero-config shorthand for [`configure`]. For anything outside
/// the standard build-script flow — output to a custom location, or feeding
/// the result to another tool — drop down to [`generate_from_proto`] and write
/// the files yourself; the OUT_DIR write loop and `rerun-if-changed` emission
/// are the only things this layer adds.
pub fn compile_protos<P: AsRef<Path>, I: AsRef<Path>>(
    protos: &[P],
    includes: &[I],
) -> Result<(), Error> {
    configure().compile(protos, includes)
}

/// Start configuring a build-script codegen run. See [`compile_protos`] for the
/// zero-config shorthand.
pub fn configure() -> Builder {
    Builder::default()
}

/// Builder for build-script codegen. Construct via [`configure`], set options,
/// and finish with [`Builder::compile`]. The one option is whether to
/// pretty-print the output.
#[derive(Debug, Clone)]
pub struct Builder {
    format: bool,
}

impl Default for Builder {
    fn default() -> Self {
        Self { format: true }
    }
}

impl Builder {
    /// Run generated code through `prettyplease`. On by default: OUT_DIR output
    /// is occasionally inspected while debugging, and the formatting cost is
    /// paid only when the build script actually re-runs.
    pub fn format(mut self, yes: bool) -> Self {
        self.format = yes;
        self
    }

    /// Compile `protos`, resolving imports against `includes`, write one
    /// `<package>.rs` per package into `OUT_DIR`, and emit
    /// `cargo:rerun-if-changed` for every compiled file. See [`compile_protos`].
    pub fn compile<P: AsRef<Path>, I: AsRef<Path>>(
        self,
        protos: &[P],
        includes: &[I],
    ) -> Result<(), Error> {
        let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or(Error::NoOutDir)?);

        // Drive protox::Compiler with the exact settings protox::compile uses
        // (source info + imports), so the FileDescriptorSet — and therefore the
        // generated output — is identical to the CLI and proc-macro paths. The
        // only reason to use the Compiler directly is `files()`, which gives us
        // the resolved filesystem paths to feed `rerun-if-changed`, imports
        // included.
        let mut compiler = protox::Compiler::new(includes)?;
        compiler
            .include_source_info(true)
            .include_imports(true)
            .open_files(protos.iter().map(AsRef::as_ref))?;

        for file in compiler.files() {
            if let Some(path) = file.path() {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }

        let opts = Options {
            include_paths: Vec::new(),
            format: self.format,
        };
        let generated = generate_from_descriptors(compiler.file_descriptor_set(), &opts)?;

        for (rel_path, content) in &generated.files {
            std::fs::write(out_dir.join(rel_path), content)?;
        }

        Ok(())
    }
}

/// Format generated source via prettyplease. On parse failure (which would
/// indicate a codegen bug) fall back to the unformatted string so the failure
/// is debuggable rather than swallowed.
fn format_rust(src: &str) -> String {
    match syn::parse_file(src) {
        Ok(file) => prettyplease::unparse(&file),
        Err(_) => src.to_owned(),
    }
}

#[derive(Default)]
struct TrilliumServiceGenerator {
    services_in_package: u32,
    needs: Needs,
}

/// Which trillium-grpc types this package's generated code references.
/// Drives the `use trillium_grpc::{...}` block at the top of the module.
#[derive(Default)]
struct Needs {
    /// `impl Stream<...>` shows up in client-streaming/bidi client *inputs*,
    /// server-streaming/bidi client *outputs*, and the server-streaming
    /// server-side return.
    stream: bool,
    /// `GrpcServerConn` — the control surface every server method receives (the three
    /// half-duplex shapes and the bidi prologue).
    grpc_conn: bool,
    /// `BidiResponder<Req, Resp>` — the bidi prologue's return type. Also implies
    /// the `trillium::Upgrade` import (bidi is the only shape that upgrades).
    bidi: bool,
    /// `UnaryConn` — the client return type for unary and client-streaming RPCs.
    unary_conn: bool,
    /// `StreamingConn` — the client return type for server-streaming RPCs.
    streaming_conn: bool,
    /// `BidiConn` — the client return type for bidi RPCs.
    bidi_conn: bool,
}

impl ServiceGenerator for TrilliumServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        self.services_in_package += 1;
        for m in &service.methods {
            // `impl Stream` appears only in the half-streaming shapes:
            // client-streaming's client *input* and server-streaming's server
            // trait *return*. Bidi uses BidiResponder/BidiConn, unary neither.
            match (m.client_streaming, m.server_streaming) {
                (false, false) => self.needs.unary_conn = true,
                (true, false) => {
                    self.needs.unary_conn = true;
                    self.needs.stream = true;
                }
                (false, true) => {
                    self.needs.streaming_conn = true;
                    self.needs.stream = true;
                }
                (true, true) => {
                    self.needs.bidi = true;
                    self.needs.bidi_conn = true;
                }
            }
            // Every server method — including the bidi prologue — receives a
            // `&mut GrpcServerConn`.
            self.needs.grpc_conn = true;
        }
        let trait_def = render_trait(&service);
        let server_def = render_server(&service);
        let client_def = render_client(&service);
        let combined = quote::quote! {
            #trait_def
            #server_def
            #client_def
        };
        buf.push_str(&combined.to_string());
    }

    fn finalize_package(&mut self, _package: &str, buf: &mut String) {
        if self.services_in_package > 0 {
            buf.insert_str(0, &render_imports(&self.needs));
        }
        // Reset for the next package.
        *self = Self::default();
    }
}

/// Build the `use` block prepended to a module that contains at least one
/// generated service. `Future`/`Result`/`Send`/`Sync`/`Sized` are in the
/// prelude and don't need importing; streaming types are imported only when
/// the service actually references them.
///
/// `trillium_client::Client` (the connection-pool struct) is referenced
/// fully-qualified in generated types, so it isn't imported here.
fn render_imports(needs: &Needs) -> String {
    let mut grpc_items: Vec<&str> = vec![
        "Prost",
        "Server",
        "ServiceClient",
        "Status",
        "prepare_grpc_conn",
    ];
    if needs.grpc_conn {
        grpc_items.push("GrpcServerConn");
    }
    if needs.bidi {
        grpc_items.push("BidiResponder");
    }
    if needs.stream {
        grpc_items.push("Stream");
    }
    if needs.unary_conn {
        grpc_items.push("UnaryConn");
    }
    if needs.streaming_conn {
        grpc_items.push("StreamingConn");
    }
    if needs.bidi_conn {
        grpc_items.push("BidiConn");
    }
    grpc_items.sort_unstable();

    // `Upgrade` is only referenced by the bidi handler path (`has_upgrade`).
    let trillium_items = if needs.bidi {
        "Conn, Handler, Method, Upgrade"
    } else {
        "Conn, Handler, Method"
    };

    format!(
        "use std::sync::Arc;\nuse trillium::{{{trillium_items}}};\nuse trillium_grpc::{{{}}};\n\n",
        grpc_items.join(", ")
    )
}

fn render_trait(service: &Service) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let trait_name = format_ident!("{}", service.name);
    let methods = service.methods.iter().map(render_trait_method);
    quote! {
        pub trait #trait_name: Send + Sync + 'static {
            #(#methods)*
        }
    }
}

fn render_trait_method(method: &Method) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let name = format_ident!("{}", method.name);
    let input: syn::Type = syn::parse_str(&method.input_type).expect("valid Rust type from prost");
    let output: syn::Type =
        syn::parse_str(&method.output_type).expect("valid Rust type from prost");

    // The three half-duplex shapes run in `Handler::run` and take a `GrpcServerConn`
    // control surface (read the request stream via `conn.requests::<Req>()`,
    // set response metadata via `conn.response_headers_mut()` /
    // `response_trailers_mut()`). Bidi is the run-phase prologue: it also takes a
    // `GrpcServerConn` and returns a `BidiResponder` that drives the read-while-write
    // loop from `Handler::upgrade`. The `use<Self>` keeps the responder from
    // capturing the `&self` borrow so it stays `'static` across the seam.
    match (method.client_streaming, method.server_streaming) {
        (false, false) => quote! {
            fn #name(
                &self,
                conn: &mut GrpcServerConn,
                request: #input,
            ) -> impl Future<Output = Result<#output, Status>> + Send;
        },
        (false, true) => quote! {
            fn #name(
                &self,
                conn: &mut GrpcServerConn,
                request: #input,
            ) -> impl Future<
                Output = Result<
                    impl Stream<Item = Result<#output, Status>> + Send + use<Self>,
                    Status,
                >,
            > + Send;
        },
        (true, false) => quote! {
            fn #name(
                &self,
                conn: &mut GrpcServerConn,
            ) -> impl Future<Output = Result<#output, Status>> + Send;
        },
        (true, true) => quote! {
            fn #name(
                &self,
                conn: &mut GrpcServerConn,
            ) -> impl Future<
                Output = Result<
                    impl BidiResponder<#input, #output> + use<Self>,
                    Status,
                >,
            > + Send;
        },
    }
}

fn render_server(service: &Service) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let trait_name = format_ident!("{}", service.name);
    let server_name = format_ident!("{}Server", service.name);
    let dispatch_name = format_ident!("{}Dispatch", service.name);
    let prefix = if service.package.is_empty() {
        format!("/{}", service.proto_name)
    } else {
        format!("/{}.{}", service.package, service.proto_name)
    };

    let dispatch_variants = service.methods.iter().map(|m| {
        let v = format_ident!("{}", m.proto_name);
        quote! { #v }
    });
    let dispatch_match_arms = service.methods.iter().map(|m| {
        let path = format!("/{}", m.proto_name);
        let v = format_ident!("{}", m.proto_name);
        quote! { #path => #dispatch_name::#v, }
    });
    let run_arms = service
        .methods
        .iter()
        .map(|m| render_run_arm(m, &dispatch_name));

    let has_bidi = service
        .methods
        .iter()
        .any(|m| m.client_streaming && m.server_streaming);

    // Every run arm forwards to `inner` (the bidi arm runs its prologue too).
    let run_inner = quote! { let inner = Arc::clone(&self.0); };

    // Only emit the upgrade plumbing when the service has a bidi RPC. Both
    // hooks are method-independent: the run-phase prologue has already stashed a
    // type-erased driver in the conn's state, so the upgrade just drives it.
    let upgrade_impl = if has_bidi {
        quote! {
            fn has_upgrade(&self, upgrade: &Upgrade) -> bool {
                trillium_grpc::has_bidi_upgrade(upgrade)
            }

            async fn upgrade(&self, upgrade: Upgrade) {
                trillium_grpc::drive_bidi_upgrade(upgrade).await;
            }
        }
    } else {
        quote! {}
    };

    quote! {
        pub struct #server_name<T>(Arc<T>);

        impl<T> #server_name<T> {
            pub fn new(inner: T) -> Self {
                Self(Arc::new(inner))
            }
        }

        // Variants mirror the proto's RPC method names, which commonly share a
        // prefix (e.g. `SayHello`, `SayHelloStream`); we can't rename them.
        #[allow(clippy::enum_variant_names)]
        #[derive(Debug, Clone, Copy)]
        enum #dispatch_name {
            #(#dispatch_variants,)*
        }

        impl<T: #trait_name> Handler for #server_name<T> {
            async fn run(&self, conn: Conn) -> Conn {
                const PREFIX: &str = #prefix;
                let Some(method) = conn.path().strip_prefix(PREFIX) else {
                    return conn;
                };
                if conn.method() != Method::Post {
                    return conn;
                }
                let dispatch = match method {
                    #(#dispatch_match_arms)*
                    _ => return conn,
                };
                let conn = match prepare_grpc_conn(conn, "proto") {
                    Ok(c) => c,
                    Err(c) => return c,
                };
                #run_inner
                match dispatch {
                    #(#run_arms)*
                }
            }

            #upgrade_impl
        }
    }
}

/// A `run()` match arm. Every shape calls its dispatch fn with the `Conn` and
/// returns the finished `Conn`. For bidi that dispatch fn is the prologue: it
/// runs the user's method, and on success stashes a type-erased driver in the
/// conn's state and marks it for upgrade (driven later from `upgrade()`).
///
/// The closures use `async move |...|` (an async closure) rather than
/// `move |...| async move {...}` because the latter doesn't infer the
/// higher-ranked `AsyncFnOnce` bound over the `&mut GrpcServerConn` lifetime.
fn render_run_arm(method: &Method, dispatch_name: &proc_macro2::Ident) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let v = format_ident!("{}", method.proto_name);
    let rust_name = format_ident!("{}", method.name);

    match (method.client_streaming, method.server_streaming) {
        (false, false) => quote! {
            #dispatch_name::#v => {
                Prost::unary(conn, async move |grpc, req| inner.#rust_name(grpc, req).await).await
            }
        },
        (false, true) => quote! {
            #dispatch_name::#v => {
                Prost::server_streaming(conn, async move |grpc, req| inner.#rust_name(grpc, req).await).await
            }
        },
        (true, false) => quote! {
            #dispatch_name::#v => {
                Prost::client_streaming(conn, async move |grpc| inner.#rust_name(grpc).await).await
            }
        },
        (true, true) => {
            let input: syn::Type =
                syn::parse_str(&method.input_type).expect("valid Rust type from prost");
            let output: syn::Type =
                syn::parse_str(&method.output_type).expect("valid Rust type from prost");
            // Turbofish pins Req/Resp; only the responder type `R` is inferred
            // from the prologue closure's return.
            quote! {
                #dispatch_name::#v => {
                    Prost::bidi::<#input, #output, _>(
                        conn,
                        async move |grpc| inner.#rust_name(grpc).await,
                    )
                    .await
                }
            }
        }
    }
}

fn render_client(service: &Service) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let client_name = format_ident!("{}Client", service.name);
    // The service prefix lives in the client's base url; methods then use
    // just the bare RPC name as a relative path.
    let prefix = if service.package.is_empty() {
        service.proto_name.clone()
    } else {
        format!("{}.{}", service.package, service.proto_name)
    };
    let methods = service.methods.iter().map(render_client_method);

    quote! {
        pub struct #client_name(trillium_client::Client);

        impl From<trillium_client::Client> for #client_name {
            fn from(client: trillium_client::Client) -> Self {
                Self(trillium_grpc::with_service_prefix(client, #prefix))
            }
        }

        impl ServiceClient for #client_name {
            fn client(&self) -> &trillium_client::Client { &self.0 }
            fn client_mut(&mut self) -> &mut trillium_client::Client { &mut self.0 }
        }

        impl #client_name {
            #(#methods)*
        }
    }
}

/// Emit one client method. Each returns a typed conn handle whose surface fits
/// the RPC's shape — `.await` it (unary / client-streaming), iterate it
/// (server-streaming), or drive it live (bidi). Construction is synchronous; the
/// request fires when the handle is awaited or first polled.
fn render_client_method(method: &Method) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let name = format_ident!("{}", method.name);
    let input: syn::Type = syn::parse_str(&method.input_type).expect("valid Rust type from prost");
    let output: syn::Type =
        syn::parse_str(&method.output_type).expect("valid Rust type from prost");
    let rpc = method.proto_name.as_str();

    match (method.client_streaming, method.server_streaming) {
        (false, false) => quote! {
            pub fn #name(&self, request: #input) -> UnaryConn<#input, #output> {
                UnaryConn::unary::<Prost>(&self.0, #rpc, request)
            }
        },
        (true, false) => quote! {
            pub fn #name(
                &self,
                requests: impl Stream<Item = #input> + Send + 'static,
            ) -> UnaryConn<#input, #output> {
                UnaryConn::client_streaming::<Prost>(&self.0, #rpc, requests)
            }
        },
        (false, true) => quote! {
            pub fn #name(&self, request: #input) -> StreamingConn<#input, #output> {
                StreamingConn::server_streaming::<Prost>(&self.0, #rpc, request)
            }
        },
        (true, true) => quote! {
            pub fn #name(&self) -> BidiConn<#input, #output> {
                BidiConn::bidi::<Prost>(&self.0, #rpc)
            }
        },
    }
}
