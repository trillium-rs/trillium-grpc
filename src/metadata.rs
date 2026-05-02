//! Custom request, response, and trailing metadata.
//!
//! gRPC distinguishes "ASCII metadata" (printable-ASCII values) from "binary
//! metadata" (key ends in `-bin`, value base64-encoded on the wire). Reserved
//! keys (`grpc-*`, `te`, `content-type`, `user-agent`, HTTP/2 pseudo-headers)
//! are owned by the framework and rejected on insert.
//!
//! [`Metadata`] is an ordered map that round-trips through
//! `trillium::Headers`. In Phase 6a it backs `Status::metadata` (Err-path
//! trailing metadata). Initial request/response metadata will be plumbed
//! through Request/Response wrappers in a follow-up phase.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use trillium::Headers;

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    entries: Vec<(String, MetadataValue)>,
}

#[derive(Debug, Clone)]
pub enum MetadataValue {
    Ascii(String),
    Binary(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("metadata key {0:?} contains invalid characters (must match [0-9a-z_\\-.]+)")]
    InvalidKey(String),
    #[error("metadata key {0:?} is reserved by the gRPC framework")]
    ReservedKey(String),
    #[error("ASCII metadata key {0:?} must not end in -bin")]
    AsciiKeyHasBinSuffix(String),
    #[error("binary metadata key {0:?} must end in -bin")]
    BinaryKeyMissingBinSuffix(String),
    #[error("ASCII metadata value contains non-printable bytes")]
    InvalidAsciiValue,
}

impl MetadataValue {
    pub fn as_ascii(&self) -> Option<&str> {
        match self {
            Self::Ascii(s) => Some(s),
            Self::Binary(_) => None,
        }
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            Self::Ascii(_) => None,
        }
    }
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Insert an ASCII metadata entry. Key must be lowercase
    /// `[0-9a-z_\-.]+`, must not be reserved, and must not end in `-bin`.
    /// Value must be printable ASCII (0x20–0x7E).
    pub fn insert_ascii(
        &mut self,
        key: &str,
        value: impl Into<String>,
    ) -> Result<(), MetadataError> {
        validate_key(key)?;
        if is_reserved(key) {
            return Err(MetadataError::ReservedKey(key.to_owned()));
        }
        if key.ends_with("-bin") {
            return Err(MetadataError::AsciiKeyHasBinSuffix(key.to_owned()));
        }
        let value = value.into();
        if !is_valid_ascii_value(&value) {
            return Err(MetadataError::InvalidAsciiValue);
        }
        self.entries
            .push((key.to_owned(), MetadataValue::Ascii(value)));
        Ok(())
    }

    /// Insert a binary metadata entry. Key must end in `-bin` and otherwise
    /// follow the same rules as ASCII keys. Value bytes are base64-encoded
    /// at write time.
    pub fn insert_binary(
        &mut self,
        key: &str,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), MetadataError> {
        validate_key(key)?;
        if is_reserved(key) {
            return Err(MetadataError::ReservedKey(key.to_owned()));
        }
        if !key.ends_with("-bin") {
            return Err(MetadataError::BinaryKeyMissingBinSuffix(key.to_owned()));
        }
        self.entries
            .push((key.to_owned(), MetadataValue::Binary(value.into())));
        Ok(())
    }

    /// Return the first ASCII value for `key`, if any.
    pub fn get_ascii(&self, key: &str) -> Option<&str> {
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Ascii(s) if k == key => Some(s.as_str()),
            _ => None,
        })
    }

    /// Return the first binary value for `key`, if any.
    pub fn get_binary(&self, key: &str) -> Option<&[u8]> {
        self.entries.iter().find_map(|(k, v)| match v {
            MetadataValue::Binary(b) if k == key => Some(b.as_slice()),
            _ => None,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &MetadataValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Pluck non-reserved entries out of `headers`. Binary `-bin` entries
    /// are base64-decoded; entries that fail to decode or whose ASCII values
    /// are not valid UTF-8 are skipped silently (the spec is lenient here —
    /// we'd rather drop unparseable user metadata than fail the whole RPC).
    ///
    /// Header names are normalized to lowercase. trillium's `KnownHeaderName`
    /// table presents canonical-case names (`Content-Type`, `Retry-After`,
    /// …) even though the HTTP/2 wire is lowercase-only, so we lowercase
    /// before matching against the reserved set and storing.
    pub fn from_headers(headers: &Headers) -> Self {
        let mut out = Self::new();
        for (name, values) in headers.iter() {
            let key = name.as_ref().to_ascii_lowercase();
            if is_reserved(&key) || !is_valid_key(&key) {
                continue;
            }
            let is_bin = key.ends_with("-bin");
            for value in values.iter() {
                if is_bin {
                    if let Ok(decoded) = BASE64.decode(value.as_ref()) {
                        out.entries
                            .push((key.clone(), MetadataValue::Binary(decoded)));
                    }
                } else if let Some(s) = value.as_str() {
                    out.entries
                        .push((key.clone(), MetadataValue::Ascii(s.to_owned())));
                }
            }
        }
        out
    }

    /// Append every entry to `headers`. Binary values are base64-encoded.
    /// Multiple values for the same key produce multiple appended entries,
    /// preserving wire order.
    pub fn write_into(&self, headers: &mut Headers) {
        for (key, value) in &self.entries {
            match value {
                MetadataValue::Ascii(v) => {
                    headers.append(key.clone(), v.clone());
                }
                MetadataValue::Binary(b) => {
                    headers.append(key.clone(), BASE64.encode(b));
                }
            }
        }
    }
}

fn validate_key(key: &str) -> Result<(), MetadataError> {
    if is_valid_key(key) {
        Ok(())
    } else {
        Err(MetadataError::InvalidKey(key.to_owned()))
    }
}

fn is_valid_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'z' | b'_' | b'-' | b'.'))
}

fn is_valid_ascii_value(value: &str) -> bool {
    value.bytes().all(|b| (0x20..=0x7E).contains(&b))
}

/// Keys owned by the gRPC framework or HTTP transport. The `grpc-` prefix
/// catches the documented family (`grpc-status`, `grpc-message`,
/// `grpc-status-details-bin`, `grpc-timeout`, `grpc-encoding`,
/// `grpc-accept-encoding`) plus any future additions.
fn is_reserved(key: &str) -> bool {
    key.starts_with("grpc-")
        || matches!(
            key,
            "te" | "content-type" | "user-agent" | "host" | "connection"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_ascii() {
        let mut m = Metadata::new();
        m.insert_ascii("trace-id", "abc123").unwrap();
        assert_eq!(m.get_ascii("trace-id"), Some("abc123"));
        assert_eq!(m.get_binary("trace-id"), None);
    }

    #[test]
    fn insert_and_get_binary() {
        let mut m = Metadata::new();
        m.insert_binary("token-bin", vec![1, 2, 3, 0xFF]).unwrap();
        assert_eq!(m.get_binary("token-bin"), Some(&[1, 2, 3, 0xFF][..]));
        assert_eq!(m.get_ascii("token-bin"), None);
    }

    #[test]
    fn rejects_uppercase_key() {
        let mut m = Metadata::new();
        let err = m.insert_ascii("Trace-Id", "x").unwrap_err();
        assert!(matches!(err, MetadataError::InvalidKey(_)));
    }

    #[test]
    fn rejects_invalid_key_chars() {
        let mut m = Metadata::new();
        assert!(matches!(
            m.insert_ascii("trace id", "x"),
            Err(MetadataError::InvalidKey(_))
        ));
        assert!(matches!(
            m.insert_ascii("trace/id", "x"),
            Err(MetadataError::InvalidKey(_))
        ));
        assert!(matches!(
            m.insert_ascii("", "x"),
            Err(MetadataError::InvalidKey(_))
        ));
    }

    #[test]
    fn rejects_reserved_keys() {
        let mut m = Metadata::new();
        assert!(matches!(
            m.insert_ascii("grpc-status", "0"),
            Err(MetadataError::ReservedKey(_))
        ));
        assert!(matches!(
            m.insert_ascii("content-type", "x"),
            Err(MetadataError::ReservedKey(_))
        ));
        assert!(matches!(
            m.insert_binary("grpc-status-details-bin", vec![0]),
            Err(MetadataError::ReservedKey(_))
        ));
    }

    #[test]
    fn ascii_key_cannot_end_in_bin() {
        let mut m = Metadata::new();
        let err = m.insert_ascii("token-bin", "x").unwrap_err();
        assert!(matches!(err, MetadataError::AsciiKeyHasBinSuffix(_)));
    }

    #[test]
    fn binary_key_must_end_in_bin() {
        let mut m = Metadata::new();
        let err = m.insert_binary("token", vec![1, 2, 3]).unwrap_err();
        assert!(matches!(err, MetadataError::BinaryKeyMissingBinSuffix(_)));
    }

    #[test]
    fn rejects_non_printable_ascii_value() {
        let mut m = Metadata::new();
        assert!(matches!(
            m.insert_ascii("trace-id", "line1\nline2"),
            Err(MetadataError::InvalidAsciiValue)
        ));
        assert!(matches!(
            m.insert_ascii("trace-id", "café"),
            Err(MetadataError::InvalidAsciiValue)
        ));
    }

    #[test]
    fn round_trip_through_headers() {
        let mut m = Metadata::new();
        m.insert_ascii("trace-id", "abc").unwrap();
        m.insert_ascii("trace-id", "def").unwrap();
        m.insert_binary("token-bin", vec![0, 1, 2, 0xFF]).unwrap();

        let mut headers = Headers::new();
        m.write_into(&mut headers);

        let parsed = Metadata::from_headers(&headers);
        let entries: Vec<_> = parsed
            .iter()
            .map(|(k, v)| (k, v.clone()))
            .collect();

        let trace_ids: Vec<_> = entries
            .iter()
            .filter(|(k, _)| *k == "trace-id")
            .filter_map(|(_, v)| v.as_ascii())
            .collect();
        assert_eq!(trace_ids, vec!["abc", "def"]);

        let token = entries
            .iter()
            .find(|(k, _)| *k == "token-bin")
            .and_then(|(_, v)| v.as_binary())
            .unwrap();
        assert_eq!(token, &[0, 1, 2, 0xFF]);
    }

    #[test]
    fn from_headers_skips_reserved() {
        let mut headers = Headers::new();
        headers.append("grpc-status", "0");
        headers.append("content-type", "application/grpc");
        headers.append("trace-id", "abc");
        let m = Metadata::from_headers(&headers);
        assert_eq!(m.len(), 1);
        assert_eq!(m.get_ascii("trace-id"), Some("abc"));
    }

    #[test]
    fn from_headers_skips_undecodable_bin() {
        let mut headers = Headers::new();
        headers.append("token-bin", "not!valid!base64!");
        let m = Metadata::from_headers(&headers);
        assert!(m.is_empty());
    }
}
