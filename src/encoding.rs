//! Per-message compression negotiation and codecs.
//!
//! The wire surface is two HTTP/2 headers plus the per-message
//! Compressed-Flag byte:
//!
//! - `grpc-encoding`: encoding the sender used for *its* messages
//!   (request → request body messages; response → response body messages).
//! - `grpc-accept-encoding`: comma-separated list the sender will accept
//!   on the *peer's* messages.
//!
//! [`Encoding`] enumerates the codecs trillium-grpc was built with. The
//! `Identity` variant is always present; `Gzip`, `Deflate`, and `Zstd` are
//! cfg-gated on their respective Cargo features. `gzip` is on by default
//! because it's the de-facto baseline for gRPC compression in the wild.
//!
//! Compression is one-shot (bytes → bytes) on per-message buffers, so we
//! use the synchronous codec crates directly (`flate2` for gzip+deflate,
//! `zstd` for zstd). The async-compression wrappers would only add
//! AsyncRead-shaped ceremony for what is fundamentally a small in-memory
//! transformation.

use crate::Status;

/// Default cap on a single decompressed message, matching grpc-go's default
/// and the per-frame `max_message_size` in [`crate::frame::reader`].
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    Identity,
    #[cfg(feature = "gzip")]
    Gzip,
    #[cfg(feature = "deflate")]
    Deflate,
    #[cfg(feature = "zstd")]
    Zstd,
}

impl Encoding {
    /// Every codec compiled into this build, including `Identity`. Order
    /// is the order presented in `grpc-accept-encoding`.
    pub const ALL: &'static [Self] = &[
        Self::Identity,
        #[cfg(feature = "gzip")]
        Self::Gzip,
        #[cfg(feature = "deflate")]
        Self::Deflate,
        #[cfg(feature = "zstd")]
        Self::Zstd,
    ];

    /// Parse a single `grpc-encoding` token. Returns `None` for codecs not
    /// compiled in or values outside the spec set.
    pub fn from_grpc_encoding(s: &str) -> Option<Self> {
        match s {
            "identity" => Some(Self::Identity),
            #[cfg(feature = "gzip")]
            "gzip" => Some(Self::Gzip),
            #[cfg(feature = "deflate")]
            "deflate" => Some(Self::Deflate),
            #[cfg(feature = "zstd")]
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    pub fn as_grpc_encoding(&self) -> &'static str {
        match self {
            Self::Identity => "identity",
            #[cfg(feature = "gzip")]
            Self::Gzip => "gzip",
            #[cfg(feature = "deflate")]
            Self::Deflate => "deflate",
            #[cfg(feature = "zstd")]
            Self::Zstd => "zstd",
        }
    }

    /// Comma-separated list of every codec in this build, suitable for the
    /// `grpc-accept-encoding` response header. Memoized — the value is
    /// constant for a given build.
    pub fn accepted_encodings() -> &'static str {
        static LIST: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        LIST.get_or_init(|| {
            Self::ALL
                .iter()
                .map(|e| e.as_grpc_encoding())
                .collect::<Vec<_>>()
                .join(",")
        })
    }

    /// Compress `data` with this codec. `Identity` returns a copy.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Status> {
        match self {
            Self::Identity => Ok(data.to_vec()),
            #[cfg(feature = "gzip")]
            Self::Gzip => gzip_compress(data),
            #[cfg(feature = "deflate")]
            Self::Deflate => deflate_compress(data),
            #[cfg(feature = "zstd")]
            Self::Zstd => zstd_compress(data),
        }
    }

    /// Decompress `data` with this codec, capping the decompressed size at
    /// `max_size` bytes (zip-bomb defense). `Identity` returns a copy and
    /// errors if `data.len() > max_size`.
    pub fn decompress(&self, data: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
        match self {
            Self::Identity => {
                if data.len() > max_size {
                    return Err(oversize(max_size));
                }
                Ok(data.to_vec())
            }
            #[cfg(feature = "gzip")]
            Self::Gzip => gzip_decompress(data, max_size),
            #[cfg(feature = "deflate")]
            Self::Deflate => deflate_decompress(data, max_size),
            #[cfg(feature = "zstd")]
            Self::Zstd => zstd_decompress(data, max_size),
        }
    }
}

fn oversize(max_size: usize) -> Status {
    Status::resource_exhausted(format!(
        "decompressed message exceeds limit of {max_size} bytes"
    ))
}

#[cfg(feature = "gzip")]
fn gzip_compress(data: &[u8]) -> Result<Vec<u8>, Status> {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    let mut enc = GzEncoder::new(Vec::with_capacity(data.len()), Compression::default());
    enc.write_all(data)
        .map_err(|e| Status::internal(format!("gzip compress: {e}")))?;
    enc.finish()
        .map_err(|e| Status::internal(format!("gzip compress: {e}")))
}

#[cfg(feature = "gzip")]
fn gzip_decompress(data: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
    use flate2::read::GzDecoder;
    read_capped(GzDecoder::new(data), max_size, "gzip decompress")
}

#[cfg(feature = "deflate")]
fn deflate_compress(data: &[u8]) -> Result<Vec<u8>, Status> {
    use flate2::{Compression, write::DeflateEncoder};
    use std::io::Write;
    let mut enc = DeflateEncoder::new(Vec::with_capacity(data.len()), Compression::default());
    enc.write_all(data)
        .map_err(|e| Status::internal(format!("deflate compress: {e}")))?;
    enc.finish()
        .map_err(|e| Status::internal(format!("deflate compress: {e}")))
}

#[cfg(feature = "deflate")]
fn deflate_decompress(data: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
    use flate2::read::DeflateDecoder;
    read_capped(DeflateDecoder::new(data), max_size, "deflate decompress")
}

#[cfg(feature = "zstd")]
fn zstd_compress(data: &[u8]) -> Result<Vec<u8>, Status> {
    zstd::stream::encode_all(data, 0)
        .map_err(|e| Status::internal(format!("zstd compress: {e}")))
}

#[cfg(feature = "zstd")]
fn zstd_decompress(data: &[u8], max_size: usize) -> Result<Vec<u8>, Status> {
    let dec = zstd::stream::Decoder::new(data)
        .map_err(|e| Status::internal(format!("zstd decompress: {e}")))?;
    read_capped(dec, max_size, "zstd decompress")
}

/// Read at most `max_size + 1` bytes from `r` into a fresh `Vec`. If we
/// hit the +1 byte, the message blew the cap → `ResourceExhausted`.
#[cfg(any(feature = "gzip", feature = "deflate", feature = "zstd"))]
fn read_capped<R: std::io::Read>(r: R, max_size: usize, ctx: &str) -> Result<Vec<u8>, Status> {
    use std::io::Read;
    let mut out = Vec::new();
    r.take(max_size as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| Status::internal(format!("{ctx}: {e}")))?;
    if out.len() > max_size {
        return Err(oversize(max_size));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_roundtrip() {
        let data = b"hello world";
        let compressed = Encoding::Identity.compress(data).unwrap();
        let decompressed = Encoding::Identity
            .decompress(&compressed, DEFAULT_MAX_MESSAGE_SIZE)
            .unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn identity_decompress_respects_max_size() {
        let data = vec![0u8; 100];
        let err = Encoding::Identity.decompress(&data, 50).unwrap_err();
        assert_eq!(err.code, crate::Code::ResourceExhausted);
    }

    #[test]
    fn from_grpc_encoding_identity_always_recognized() {
        assert_eq!(
            Encoding::from_grpc_encoding("identity"),
            Some(Encoding::Identity)
        );
    }

    #[test]
    fn from_grpc_encoding_unknown_returns_none() {
        assert!(Encoding::from_grpc_encoding("snappy").is_none());
        assert!(Encoding::from_grpc_encoding("").is_none());
        assert!(Encoding::from_grpc_encoding("GZIP").is_none()); // case-sensitive per spec
    }

    #[test]
    fn accepted_encodings_starts_with_identity() {
        assert!(Encoding::accepted_encodings().starts_with("identity"));
    }

    #[cfg(feature = "gzip")]
    mod gzip {
        use super::*;

        #[test]
        fn parse_and_serialize() {
            assert_eq!(Encoding::from_grpc_encoding("gzip"), Some(Encoding::Gzip));
            assert_eq!(Encoding::Gzip.as_grpc_encoding(), "gzip");
            assert!(Encoding::accepted_encodings().contains("gzip"));
        }

        #[test]
        fn roundtrip() {
            let data = b"hello, gzip-compressed world! ".repeat(100);
            let compressed = Encoding::Gzip.compress(&data).unwrap();
            assert!(compressed.len() < data.len(), "compression had effect");
            let decompressed = Encoding::Gzip
                .decompress(&compressed, DEFAULT_MAX_MESSAGE_SIZE)
                .unwrap();
            assert_eq!(decompressed, data);
        }

        #[test]
        fn decompress_respects_max_size() {
            // 100 KB of 'a' compresses well; decompressed size > 1 KB cap.
            let data = vec![b'a'; 100 * 1024];
            let compressed = Encoding::Gzip.compress(&data).unwrap();
            let err = Encoding::Gzip.decompress(&compressed, 1024).unwrap_err();
            assert_eq!(err.code, crate::Code::ResourceExhausted);
        }
    }

    #[cfg(feature = "deflate")]
    mod deflate {
        use super::*;

        #[test]
        fn parse_and_serialize() {
            assert_eq!(
                Encoding::from_grpc_encoding("deflate"),
                Some(Encoding::Deflate)
            );
            assert_eq!(Encoding::Deflate.as_grpc_encoding(), "deflate");
        }

        #[test]
        fn roundtrip() {
            let data = b"hello, deflate-compressed world! ".repeat(100);
            let compressed = Encoding::Deflate.compress(&data).unwrap();
            let decompressed = Encoding::Deflate
                .decompress(&compressed, DEFAULT_MAX_MESSAGE_SIZE)
                .unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd {
        use super::*;

        #[test]
        fn parse_and_serialize() {
            assert_eq!(Encoding::from_grpc_encoding("zstd"), Some(Encoding::Zstd));
            assert_eq!(Encoding::Zstd.as_grpc_encoding(), "zstd");
        }

        #[test]
        fn roundtrip() {
            let data = b"hello, zstd-compressed world! ".repeat(100);
            let compressed = Encoding::Zstd.compress(&data).unwrap();
            let decompressed = Encoding::Zstd
                .decompress(&compressed, DEFAULT_MAX_MESSAGE_SIZE)
                .unwrap();
            assert_eq!(decompressed, data);
        }
    }
}
