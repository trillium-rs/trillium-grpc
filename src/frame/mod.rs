//! gRPC message framing.
//!
//! Every gRPC message on the wire is a 5-byte prefix — one Compressed-Flag
//! byte followed by a big-endian `u32` length — then that many payload bytes.
//! A single HTTP/2 body carries a sequence of these frames.
//!
//! [`reader`] turns an `AsyncRead` body into a [`Stream`](futures_lite::Stream)
//! of decoded messages; [`writer`] encodes a message (optionally compressed)
//! into a frame. Codec and compression are both pushed in from the caller, so
//! this layer is agnostic to message type and encoding.

pub mod reader;
pub mod writer;
