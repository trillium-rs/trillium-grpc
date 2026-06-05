#![doc = include_str!("../docs/root.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(doc)]
#[doc = include_str!("../docs/generating.md")]
pub mod generating {}

#[cfg(doc)]
#[doc = include_str!("../docs/serving.md")]
pub mod serving {}

#[cfg(doc)]
#[doc = include_str!("../docs/calling.md")]
pub mod calling {}

#[cfg(doc)]
#[doc = include_str!("../docs/advanced.md")]
pub mod advanced {}

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub mod client;
pub mod codec;
pub mod content_type;
pub mod encoding;
pub mod frame;
pub mod metadata;
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub mod server;
pub mod status;
pub mod timeout;

#[cfg(feature = "codegen")]
pub use trillium_grpc_codegen as codegen;

#[cfg(feature = "macros")]
pub use trillium_grpc_macros::{generate, generate_client, generate_server};

#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use client::{
    BidiConn, CancelHandle, GrpcClientConn, ServiceClient, ServiceClientExt, StreamingConn,
    UnaryConn, with_service_prefix,
};
pub use codec::{Codec, Prost};
pub use encoding::Encoding;
pub use futures_lite::Stream;
pub use metadata::{Metadata, MetadataError, MetadataValue};
#[cfg(feature = "server")]
#[cfg_attr(docsrs, doc(cfg(feature = "server")))]
pub use server::{
    BidiResponder, Channel, GrpcServerConn, RequestStream, Server, dispatch::prepare_grpc_conn,
    drive_bidi_upgrade, has_bidi_upgrade,
};
pub use status::{Code, Status};

/// Re-export of the [`prost`] runtime.
///
/// Generated message types derive `::trillium_grpc::prost::Message`, so a crate
/// that depends only on `trillium-grpc` can use the generated code without a
/// direct `prost` dependency.
pub use prost;

/// Re-export of [`trillium_client`].
///
/// Generated service clients name [`trillium_client::Client`] through this
/// re-export, so a crate that depends only on `trillium-grpc` can use a
/// generated client without a direct `trillium-client` dependency.
#[cfg(feature = "client")]
#[cfg_attr(docsrs, doc(cfg(feature = "client")))]
pub use trillium_client;

#[cfg(test)]
#[doc = include_str!("../README.md")]
mod readme {}
