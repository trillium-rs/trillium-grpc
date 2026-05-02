use crate::Status;
use bytes::Bytes;

mod prost_codec;
pub use prost_codec::Prost;

/// Per-RPC value codec — encodes a request/response message to bytes and back.
///
/// The default for codegen is [`Prost`]. Other codecs (JSON, custom)
/// implement this trait so the same call-shape dispatch functions can drive
/// them without runtime dispatch.
pub trait Codec<T>: 'static {
    /// The fragment after `application/grpc+` in the content-type header
    /// (e.g. `"proto"`, `"json"`). For raw `application/grpc`, return `"proto"`
    /// — the spec says the bare type implies protobuf.
    fn content_type_suffix() -> &'static str;

    fn encode(value: &T) -> Result<Bytes, Status>;
    fn decode(bytes: &[u8]) -> Result<T, Status>;
}
