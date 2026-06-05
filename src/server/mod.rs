//! The server half of trillium-grpc.
//!
//! A generated `<Service>Server<T>` is a trillium [`Handler`](trillium::Handler):
//! it matches the service's path prefix, validates the gRPC preflight, and
//! hands each request to one of the [`Server`] dispatch methods, which drives
//! the codec, framing, cancellation, and `grpc-status` trailers around your
//! service-trait method. The streaming shapes hand your method a borrowed
//! [`RequestStream`] or [`Channel`].
//!
//! You implement the generated service trait and wrap it in the generated
//! server; the items here are the machinery that wrapper is built from.

pub mod bidi;
mod body;
pub mod dispatch;
mod grpc_conn;
pub mod streaming;

pub use bidi::{BidiResponder, drive_bidi_upgrade, has_bidi_upgrade};
pub use dispatch::Server;
pub use grpc_conn::GrpcServerConn;
pub use streaming::{Channel, RequestStream};
