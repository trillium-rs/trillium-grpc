//! Proc-macro front-end for [`trillium_grpc_codegen`].
//!
//! Exposes a single macro, [`generate!`], which reads one or more `.proto`
//! files at compile time, runs the same codegen used by the `trillium grpc
//! codegen` CLI, and inlines the result into the call site. The shape of
//! the inlined output mirrors the `<package>.rs` files the CLI would have
//! written: one `pub mod <segment> { … }` per dotted segment of each
//! package, merged into a single tree so packages that share a prefix
//! collapse into the same outer module.
//!
//! ```ignore
//! trillium_grpc::generate!("proto/greeter.proto");
//! // expands to (roughly):
//! // pub mod greeter { pub mod v1 { /* trait + Server + Client + messages */ } }
//! ```
//!
//! Paths are resolved relative to the consuming crate's `CARGO_MANIFEST_DIR`
//! (the directory containing its `Cargo.toml`); absolute paths are accepted
//! verbatim. To force cargo to re-expand the macro when a `.proto` changes,
//! the macro emits a `const _: &[u8] = include_bytes!("…")` shim per source
//! file alongside the generated code.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use std::{collections::BTreeMap, path::PathBuf};
use syn::{
    LitStr, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

struct Args {
    paths: Punctuated<LitStr, Token![,]>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(Self {
            paths: Punctuated::parse_terminated(input)?,
        })
    }
}

/// Generate trillium-grpc service modules from one or more `.proto` files.
///
/// Accepts a comma-separated list of string-literal paths. See the crate
/// docs for details on path resolution and module shape.
#[proc_macro]
pub fn generate(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as Args);
    match expand(args) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand(args: Args) -> syn::Result<TokenStream2> {
    if args.paths.is_empty() {
        return Err(syn::Error::new(
            Span::call_site(),
            "trillium_grpc::generate! requires at least one .proto path",
        ));
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
        syn::Error::new(
            Span::call_site(),
            "CARGO_MANIFEST_DIR not set; trillium_grpc::generate! must run under cargo",
        )
    })?;
    let manifest_dir = PathBuf::from(manifest_dir);

    let mut srcs: Vec<PathBuf> = Vec::with_capacity(args.paths.len());
    for lit in &args.paths {
        let raw = PathBuf::from(lit.value());
        let resolved = if raw.is_absolute() {
            raw
        } else {
            manifest_dir.join(raw)
        };
        if !resolved.exists() {
            return Err(syn::Error::new(
                lit.span(),
                format!(".proto file not found: {}", resolved.display()),
            ));
        }
        srcs.push(resolved);
    }

    // Default include path: the parent of each .proto, so sibling imports
    // resolve without the user having to spell them out. Order-preserving
    // dedup so explicit precedence (later flag, if added) would win.
    let mut includes: Vec<PathBuf> = Vec::new();
    for src in &srcs {
        if let Some(parent) = src.parent() {
            let parent = parent.to_path_buf();
            if !includes.contains(&parent) {
                includes.push(parent);
            }
        }
    }

    let opts = trillium_grpc_codegen::Options {
        include_paths: includes,
        format: false,
    };

    let generated = trillium_grpc_codegen::generate_from_proto(&srcs, &opts).map_err(|e| {
        syn::Error::new(
            Span::call_site(),
            format!("trillium-grpc codegen failed: {e}"),
        )
    })?;

    let tree = build_tree(generated.files)?;
    let modules = emit_tree(&tree);

    let tracking = srcs.iter().map(|p| {
        let s = p.to_string_lossy().into_owned();
        quote! { const _: &[u8] = include_bytes!(#s); }
    });

    Ok(quote! {
        #(#tracking)*
        #modules
    })
}

#[derive(Default)]
struct ModTree {
    children: BTreeMap<String, ModTree>,
    contents: Option<String>,
}

/// Walk the codegen output (one entry per package, keyed by `<pkg>.rs`)
/// and bucket each entry into a tree of nested modules so dotted segments
/// become nested `pub mod` blocks. Multi-package outputs that share a
/// prefix collapse onto the same intermediate node.
fn build_tree(files: BTreeMap<PathBuf, String>) -> syn::Result<ModTree> {
    let mut root = ModTree::default();
    for (path, content) in files {
        let stem = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
            syn::Error::new(
                Span::call_site(),
                format!("unexpected codegen output filename: {}", path.display()),
            )
        })?;
        let mut cursor = &mut root;
        for seg in stem.split('.') {
            cursor = cursor.children.entry(seg.to_string()).or_default();
        }
        if cursor.contents.replace(content).is_some() {
            return Err(syn::Error::new(
                Span::call_site(),
                format!("duplicate codegen output for package {stem}"),
            ));
        }
    }
    Ok(root)
}

fn emit_tree(tree: &ModTree) -> TokenStream2 {
    let mut tokens = TokenStream2::new();
    if let Some(content) = &tree.contents {
        match content.parse::<TokenStream2>() {
            Ok(ts) => tokens.extend(ts),
            Err(e) => tokens.extend(syn::Error::from(e).to_compile_error()),
        }
    }
    for (name, child) in &tree.children {
        let ident = quote::format_ident!("{}", name);
        let inner = emit_tree(child);
        tokens.extend(quote! {
            pub mod #ident {
                #inner
            }
        });
    }
    tokens
}
