//! The client half of trillium-grpc.
//!
//! A generated `<Service>Client` wraps a [`trillium_client::Client`] and
//! exposes one async method per RPC. Each method calls through the [`Client`]
//! dispatch trait, which encodes the request, opens an HTTP/2 stream, and reads
//! the response and its `grpc-status` trailers back. Streaming responses arrive
//! as a [`ResponseStream`]. Per-client configuration (compression, deadlines)
//! lives on [`ServiceClientExt`].

mod dispatch;
mod response_stream;
mod service_client;

pub use dispatch::Client;
pub use response_stream::ResponseStream;
pub use service_client::{ServiceClient, ServiceClientExt};

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
