//! The server half of trillium-grpc.
//!
//! A generated `<Service>Server<T>` is a trillium [`Handler`](trillium::Handler):
//! it matches the service's path prefix, validates the gRPC preflight, and
//! hands each request to one of the [`Server`] dispatch methods, which drives
//! the codec, framing, cancellation, and `grpc-status` trailers around your
//! service-trait method. The streaming shapes hand your method a borrowed
//! [`RequestStream`], [`ResponseSink`], or [`Channel`].
//!
//! You implement the generated service trait and wrap it in the generated
//! server; the items here are the machinery that wrapper is built from.

pub mod content_type;
pub mod dispatch;
pub mod streaming;

pub use dispatch::Server;
pub use streaming::{Channel, RequestStream, ResponseSink};
