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
    /// `impl Stream<...>` shows up in client-streaming/bidi client *inputs*
    /// and server-streaming/bidi client *outputs*.
    stream: bool,
    /// `RequestStream<'_, Req>` — client-streaming server-side parameter.
    request_stream: bool,
    /// `ResponseSink<'_, Resp>` — server-streaming server-side parameter.
    response_sink: bool,
    /// `Channel<'_, Req, Resp>` — bidi server-side parameter.
    channel: bool,
}

impl ServiceGenerator for TrilliumServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        self.services_in_package += 1;
        for m in &service.methods {
            if m.server_streaming || m.client_streaming {
                self.needs.stream = true;
            }
            match (m.client_streaming, m.server_streaming) {
                (false, true) => self.needs.response_sink = true,
                (true, false) => self.needs.request_stream = true,
                (true, true) => self.needs.channel = true,
                (false, false) => {}
            }
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
/// Note: `Client` here is the trillium-grpc dispatch trait (in scope so
/// `Prost::*_call` resolves through it); `trillium_client::Client` (the
/// connection-pool struct) is referenced fully-qualified in generated
/// types to avoid the name collision.
fn render_imports(needs: &Needs) -> String {
    let mut grpc_items: Vec<&str> = vec![
        "Client",
        "Prost",
        "Server",
        "ServiceClient",
        "Status",
        "prepare_grpc_conn",
    ];
    if needs.request_stream {
        grpc_items.push("RequestStream");
    }
    if needs.response_sink {
        grpc_items.push("ResponseSink");
    }
    if needs.channel {
        grpc_items.push("Channel");
    }
    if needs.stream {
        grpc_items.push("Stream");
    }
    grpc_items.sort_unstable();

    format!(
        "use std::sync::Arc;\nuse trillium::{{Conn, Handler, Method, Upgrade}};\nuse trillium_grpc::{{{}}};\n\n",
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

    // For the four call shapes, generate trait signatures that match the
    // borrowed-primitive dispatch shape: framework owns the upgrade and
    // hands the user closure RequestStream/ResponseSink/Channel borrows
    // tied to that lifetime.
    match (method.client_streaming, method.server_streaming) {
        (false, false) => quote! {
            fn #name(
                &self,
                request: #input,
            ) -> impl Future<Output = Result<#output, Status>> + Send;
        },
        (false, true) => quote! {
            fn #name(
                &self,
                request: #input,
                responses: ResponseSink<'_, #output>,
            ) -> impl Future<Output = Result<(), Status>> + Send;
        },
        (true, false) => quote! {
            fn #name(
                &self,
                requests: RequestStream<'_, #input>,
            ) -> impl Future<Output = Result<#output, Status>> + Send;
        },
        (true, true) => quote! {
            fn #name(
                &self,
                channel: Channel<'_, #input, #output>,
            ) -> impl Future<Output = Result<(), Status>> + Send;
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
    let upgrade_match_arms = service
        .methods
        .iter()
        .map(|m| render_upgrade_arm(m, &dispatch_name));

    quote! {
        pub struct #server_name<T>(Arc<T>);

        impl<T> #server_name<T> {
            pub fn new(inner: T) -> Self {
                Self(Arc::new(inner))
            }
        }

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
                conn.with_state(dispatch).upgrade().halt()
            }

            fn has_upgrade(&self, upgrade: &Upgrade) -> bool {
                upgrade.state().get::<#dispatch_name>().is_some()
            }

            async fn upgrade(&self, mut upgrade: Upgrade) {
                let dispatch = upgrade.state_mut().take::<#dispatch_name>().unwrap();
                let inner = Arc::clone(&self.0);
                match dispatch {
                    #(#upgrade_match_arms)*
                }
            }
        }
    }
}

fn render_upgrade_arm(
    method: &Method,
    dispatch_name: &proc_macro2::Ident,
) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let v = format_ident!("{}", method.proto_name);
    let rust_name = format_ident!("{}", method.name);

    // Closure shape mirrors the dispatch fn it's handed to. The streaming
    // shapes use `async move |...|` (an async closure) rather than
    // `move |...| async move {...}` because the latter doesn't infer a
    // higher-ranked AsyncFnOnce bound over the borrowed primitive's
    // lifetime — the returned future ends up tied to a specific lifetime,
    // and dispatch needs `for<'a> AsyncFnOnce(...<'a>...)`.
    let (dispatch_method, closure) = match (method.client_streaming, method.server_streaming) {
        (false, false) => (
            format_ident!("unary"),
            quote! { async move |req| inner.#rust_name(req).await },
        ),
        (false, true) => (
            format_ident!("server_streaming"),
            quote! { async move |req, sink| inner.#rust_name(req, sink).await },
        ),
        (true, false) => (
            format_ident!("client_streaming"),
            quote! { async move |reqs| inner.#rust_name(reqs).await },
        ),
        (true, true) => (
            format_ident!("bidi"),
            quote! { async move |channel| inner.#rust_name(channel).await },
        ),
    };

    quote! {
        #dispatch_name::#v => {
            Prost::#dispatch_method(upgrade, #closure).await
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

fn render_client_method(method: &Method) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let name = format_ident!("{}", method.name);
    let input: syn::Type = syn::parse_str(&method.input_type).expect("valid Rust type from prost");
    let output: syn::Type =
        syn::parse_str(&method.output_type).expect("valid Rust type from prost");
    let rpc = method.proto_name.as_str();

    let dispatch_method = match (method.client_streaming, method.server_streaming) {
        (false, false) => format_ident!("unary_call"),
        (false, true) => format_ident!("server_streaming_call"),
        (true, false) => format_ident!("client_streaming_call"),
        (true, true) => format_ident!("bidi_call"),
    };

    let request_param = if method.client_streaming {
        quote! { requests: impl Stream<Item = #input> + Send + 'static }
    } else {
        quote! { request: #input }
    };

    let arg = if method.client_streaming {
        format_ident!("requests")
    } else {
        format_ident!("request")
    };

    let result_ty = if method.server_streaming {
        quote! {
            Result<impl Stream<Item = Result<#output, Status>> + Send + 'static, Status>
        }
    } else {
        quote! { Result<#output, Status> }
    };

    quote! {
        pub async fn #name(&self, #request_param) -> #result_ty {
            Prost::#dispatch_method(&self.0, #rpc, #arg).await
        }
    }
}
