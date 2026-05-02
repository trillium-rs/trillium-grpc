mod dispatch;
mod response_stream;
mod service_client;

pub use dispatch::Client;
pub use response_stream::ResponseStream;
pub use service_client::{ServiceClient, ServiceClientExt};

use crate::Status;

/// Extract the gRPC status from a finished client conn. Looks at trailers
/// first; falls back to response headers for trailers-only responses (where
/// the server sent HEADERS+END_STREAM with no body).
pub(crate) fn read_grpc_status(conn: &trillium_client::Conn) -> Result<(), Status> {
    if let Some(trailers) = conn.response_trailers()
        && trailers.get_str("grpc-status").is_some()
    {
        return Status::from_trailers(trailers);
    }
    Status::from_trailers(conn.response_headers())
}

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
