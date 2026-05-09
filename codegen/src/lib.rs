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
//! Downstream crates only need to depend on `trillium-grpc` — `Stream` is
//! re-exported there.
//!
//! Streaming method return types use `+ use<Self>` precise-capture clauses so
//! the inner `impl Stream` only "captures" the `Self` type parameter. Because
//! the trait bound is `Send + Sync + 'static`, this is functionally equivalent
//! to `+ use<>` (no captures), but rustc requires `Self` in the list.

use prost_build::{Config, Method, Module, Service, ServiceGenerator};
use prost_types::FileDescriptorSet;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to compile .proto sources: {0}")]
    Protox(#[from] protox::Error),

    #[error("prost-build failed: {0}")]
    ProstBuild(#[from] std::io::Error),

    #[error("generated code did not parse as valid Rust: {0}")]
    Syn(#[from] syn::Error),
}

#[derive(Debug, Default, Clone)]
pub struct Options {
    /// Include paths handed to protox for resolving `import` statements.
    pub include_paths: Vec<PathBuf>,
}

#[derive(Debug, Default)]
pub struct GeneratedFiles {
    /// Map of `<package>.rs` → file contents (formatted Rust).
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
    _opts: &Options,
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
        let formatted = format_rust(&code);
        files.insert(path, formatted);
    }

    Ok(GeneratedFiles { files })
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
    needs_stream: bool,
    needs_buffered_request_stream: bool,
}

impl ServiceGenerator for TrilliumServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        self.services_in_package += 1;
        for m in &service.methods {
            // `Stream` shows up in both server-streaming server returns
            // and client-streaming/bidi client method parameters.
            if m.server_streaming || m.client_streaming {
                self.needs_stream = true;
            }
            if m.client_streaming {
                self.needs_buffered_request_stream = true;
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
            buf.insert_str(
                0,
                &render_imports(self.needs_stream, self.needs_buffered_request_stream),
            );
        }
        // Reset for the next package.
        *self = Self::default();
    }
}

/// Build the `use` block prepended to a module that contains at least one
/// generated service. `Future`/`Result`/`Send`/`Sync`/`Sized` are in the
/// prelude and don't need importing; `Stream` and `BufferedRequestStream` are
/// only emitted when the service actually uses them.
///
/// Note: `Client` here is the trillium-grpc dispatch trait (in scope so
/// `Prost::*_call` resolves through it); `trillium_client::Client` (the
/// connection-pool struct) is referenced fully-qualified in generated
/// types to avoid the name collision.
fn render_imports(needs_stream: bool, needs_buffered_request_stream: bool) -> String {
    let mut grpc_items: Vec<&str> = vec!["Client", "Prost", "Server", "ServiceClient", "Status"];
    if needs_buffered_request_stream {
        grpc_items.push("BufferedRequestStream");
    }
    if needs_stream {
        grpc_items.push("Stream");
    }
    grpc_items.sort_unstable();

    format!(
        "use std::sync::Arc;\nuse trillium::{{Conn, Handler, Method}};\nuse trillium_grpc::{{{}}};\n\n",
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
    let output: syn::Type = syn::parse_str(&method.output_type).expect("valid Rust type from prost");

    let request_param = if method.client_streaming {
        quote! { requests: BufferedRequestStream<#input> }
    } else {
        quote! { request: #input }
    };

    let result_ty = if method.server_streaming {
        quote! {
            Result<
                impl Stream<Item = Result<#output, Status>> + Send + 'static + use<Self>,
                Status,
            >
        }
    } else {
        quote! { Result<#output, Status> }
    };

    quote! {
        fn #name(
            &self,
            #request_param,
        ) -> impl Future<Output = #result_ty> + Send;
    }
}

fn render_server(service: &Service) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let trait_name = format_ident!("{}", service.name);
    let server_name = format_ident!("{}Server", service.name);
    let prefix = if service.package.is_empty() {
        format!("/{}", service.proto_name)
    } else {
        format!("/{}.{}", service.package, service.proto_name)
    };
    let arms = service.methods.iter().map(render_server_arm);

    quote! {
        pub struct #server_name<T>(Arc<T>);

        impl<T> #server_name<T> {
            pub fn new(inner: T) -> Self {
                Self(Arc::new(inner))
            }
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
                match method {
                    #(#arms)*
                    _ => conn,
                }
            }
        }
    }
}

fn render_server_arm(method: &Method) -> proc_macro2::TokenStream {
    use quote::{format_ident, quote};
    let path = format!("/{}", method.proto_name);
    let rust_name = format_ident!("{}", method.name);

    // Dispatch via the `Server` trait method on the codec type — no turbofish
    // needed because all generic parameters are inferable from the closure
    // and `conn`.
    let dispatch_method = match (method.client_streaming, method.server_streaming) {
        (false, false) => format_ident!("unary"),
        (false, true) => format_ident!("server_streaming"),
        (true, false) => format_ident!("client_streaming"),
        (true, true) => format_ident!("bidi"),
    };
    let arg = if method.client_streaming {
        format_ident!("reqs")
    } else {
        format_ident!("req")
    };

    quote! {
        #path => {
            let inner = Arc::clone(&self.0);
            Prost::#dispatch_method(
                conn,
                move |#arg| async move { inner.#rust_name(#arg).await },
            ).await
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
