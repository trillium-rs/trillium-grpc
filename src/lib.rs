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

pub mod client;
pub mod codec;
pub mod encoding;
pub mod frame;
pub mod metadata;
pub mod server;
pub mod status;
pub mod timeout;

#[cfg(feature = "codegen")]
pub use trillium_grpc_codegen as codegen;

#[cfg(feature = "macros")]
pub use trillium_grpc_macros::generate;

pub use client::{
    BidiConn, CancelHandle, GrpcClientConn, ServiceClient, ServiceClientExt, StreamingConn,
    UnaryConn, with_service_prefix,
};
pub use codec::{Codec, Prost};
pub use encoding::Encoding;
pub use futures_lite::Stream;
pub use metadata::{Metadata, MetadataError, MetadataValue};
pub use server::{
    BidiResponder, Channel, GrpcServerConn, RequestStream, Server, dispatch::prepare_grpc_conn,
    drive_bidi_upgrade, has_bidi_upgrade,
};
pub use status::{Code, Status};

#[cfg(test)]
#[doc = include_str!("../README.md")]
mod readme {}
