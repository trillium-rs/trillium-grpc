//! Extension surface for generated `<Service>Client` newtypes.
//!
//! Codegen emits `impl ServiceClient for <Service>Client` for every service
//! it produces. The blanket [`ServiceClientExt`] then provides the
//! builder/setter methods every service client gets for free.
//!
//! New options live on [`ServiceClientExt`] — adding one is a single
//! method here, no codegen change.

use crate::{Encoding, timeout::format_grpc_timeout};
use std::time::Duration;

/// Generated `<Service>Client` newtypes implement this so extension traits
/// can configure the underlying [`trillium_client::Client`].
pub trait ServiceClient {
    /// The underlying connection client.
    fn client(&self) -> &trillium_client::Client;
    /// The underlying connection client, mutably — the hook the
    /// [`ServiceClientExt`] setters write through.
    fn client_mut(&mut self) -> &mut trillium_client::Client;
}

/// Builder-style configuration available on every service client.
/// Implemented for any `T: ServiceClient + Sized`, so service clients
/// don't reimplement these — they just `impl ServiceClient` and inherit
/// the full set.
pub trait ServiceClientExt: ServiceClient + Sized {
    /// Compress every outgoing request body with this codec. Sent on the
    /// wire as `grpc-encoding: <codec>`.
    ///
    /// The server is required by spec to handle whatever the client sends
    /// — including failing with `Unimplemented` — so picking a codec your
    /// server is known to support is the caller's responsibility. The
    /// server's `grpc-accept-encoding` (visible after the first response)
    /// can be inspected by the caller to pick a future-safe codec.
    ///
    /// Setting `Encoding::Identity` clears any previously-set compression.
    fn with_outbound_compression(mut self, encoding: Encoding) -> Self {
        self.set_outbound_compression(encoding);
        self
    }

    /// `&mut` form of [`with_outbound_compression`](Self::with_outbound_compression).
    fn set_outbound_compression(&mut self, encoding: Encoding) -> &mut Self {
        let headers = self.client_mut().default_headers_mut();
        if matches!(encoding, Encoding::Identity) {
            headers.remove("grpc-encoding");
        } else {
            headers.insert("grpc-encoding", encoding.as_grpc_encoding());
        }
        self
    }

    /// The currently-configured outbound compression. `Identity` if none
    /// has been set.
    fn outbound_compression(&self) -> Encoding {
        self.client()
            .default_headers()
            .get_str("grpc-encoding")
            .and_then(Encoding::from_grpc_encoding)
            .unwrap_or(Encoding::Identity)
    }

    /// Apply this duration as the default deadline on every call. Sent on
    /// the wire as `grpc-timeout: <unit>` so the server can enforce the
    /// same deadline; the client also races its own dispatch future
    /// against a local timer so we fail fast even if the server is unable
    /// to respond.
    ///
    /// Setting `Duration::ZERO` clears any previously-set deadline.
    fn with_default_timeout(mut self, duration: Duration) -> Self {
        self.set_default_timeout(duration);
        self
    }

    /// `&mut` form of [`with_default_timeout`](Self::with_default_timeout).
    fn set_default_timeout(&mut self, duration: Duration) -> &mut Self {
        let headers = self.client_mut().default_headers_mut();
        if duration.is_zero() {
            headers.remove("grpc-timeout");
        } else {
            headers.insert("grpc-timeout", format_grpc_timeout(duration));
        }
        self
    }

    /// The currently-configured default timeout, if any.
    fn default_timeout(&self) -> Option<Duration> {
        self.client()
            .default_headers()
            .get_str("grpc-timeout")
            .and_then(crate::timeout::parse_grpc_timeout)
    }
}

impl<T: ServiceClient> ServiceClientExt for T {}
