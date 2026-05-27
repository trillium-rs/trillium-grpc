//! The client half of trillium-grpc.
//!
//! A generated `<Service>Client` wraps a [`trillium_client::Client`] and exposes
//! one method per RPC. Each returns a typed, shape-specific call handle —
//! [`UnaryConn`], [`StreamingConn`], or [`BidiConn`] — built on the
//! [`GrpcClientConn`] engine: configure it with chainable `with_*` setters, then
//! `.await` and/or iterate it to run the call and read the response, its initial
//! metadata, and its `grpc-status` trailers. Per-client configuration
//! (compression, deadlines) lives on [`ServiceClientExt`].

mod conn;
mod service_client;
mod typed;

pub use conn::{CancelHandle, GrpcClientConn};
pub use service_client::{ServiceClient, ServiceClientExt};
pub use typed::{BidiConn, StreamingConn, UnaryConn};

/// Append a service-prefix segment to the client's base URL. Used by
/// generated `From<trillium_client::Client>` impls so that each generated
/// method only needs to specify its own RPC name as a relative path.
///
/// Mutates the base in place — `trillium_client::Client::base_mut` is
/// clone-on-write across clones of the same client, so this doesn't leak
/// to other holders.
///
/// Panics if the client has no base URL set.
pub fn with_service_prefix(
    mut client: trillium_client::Client,
    prefix: &str,
) -> trillium_client::Client {
    let base = client
        .base_mut()
        .expect("trillium_client::Client must have a base url set");
    let new_path = format!("{}/{prefix}/", base.path().trim_end_matches('/'));
    base.set_path(&new_path);
    client
}
