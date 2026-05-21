use crate::Metadata;
use trillium::Headers;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl Code {
    pub fn from_u8(n: u8) -> Option<Self> {
        Some(match n {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => return None,
        })
    }

    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone)]
pub struct Status {
    pub code: Code,
    pub message: String,
    pub metadata: Metadata,
}

macro_rules! status_constructors {
    ($($name:ident => $variant:ident),* $(,)?) => {
        $(
            pub fn $name(message: impl Into<String>) -> Self {
                Self {
                    code: Code::$variant,
                    message: message.into(),
                    metadata: Metadata::new(),
                }
            }
        )*
    };
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            metadata: Metadata::new(),
        }
    }

    pub fn ok() -> Self {
        Self {
            code: Code::Ok,
            message: String::new(),
            metadata: Metadata::new(),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self.code, Code::Ok)
    }

    /// Attach trailing metadata. Carried in the `grpc-status` trailers
    /// alongside `grpc-message`.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    status_constructors! {
        cancelled            => Cancelled,
        unknown              => Unknown,
        invalid_argument     => InvalidArgument,
        deadline_exceeded    => DeadlineExceeded,
        not_found            => NotFound,
        already_exists       => AlreadyExists,
        permission_denied    => PermissionDenied,
        resource_exhausted   => ResourceExhausted,
        failed_precondition  => FailedPrecondition,
        aborted              => Aborted,
        out_of_range         => OutOfRange,
        unimplemented        => Unimplemented,
        internal             => Internal,
        unavailable          => Unavailable,
        data_loss            => DataLoss,
        unauthenticated      => Unauthenticated,
    }

    pub fn into_trailers(self) -> Headers {
        let mut headers = Headers::new();
        self.write_into(&mut headers);
        headers
    }

    pub fn write_into(&self, headers: &mut Headers) {
        headers.insert("grpc-status", self.code.as_u8().to_string());
        if !self.message.is_empty() {
            headers.insert("grpc-message", percent_encode(&self.message));
        }
        self.metadata.write_into(headers);
    }

    /// Read a Status from response trailers (or trailer-only response headers).
    /// Returns `Ok(())` for `grpc-status: 0`, `Err(Status)` otherwise.
    /// Missing `grpc-status` is treated as `Unknown` per spec. On the Err
    /// path the returned `Status` carries any custom trailing metadata
    /// extracted from the same headers.
    pub fn from_trailers(headers: &Headers) -> Result<(), Self> {
        let code = headers
            .get_str("grpc-status")
            .and_then(|s| s.parse::<u8>().ok())
            .and_then(Code::from_u8)
            .unwrap_or(Code::Unknown);

        if matches!(code, Code::Ok) {
            return Ok(());
        }

        let message = headers
            .get_str("grpc-message")
            .map(percent_decode)
            .unwrap_or_default();

        Err(Self {
            code,
            message,
            metadata: Metadata::from_headers(headers),
        })
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Status {}

/// Percent-encode a `grpc-message` per the gRPC HTTP/2 spec.
///
/// Bytes 0x20–0x7E except `%` (0x25) pass through literally; everything else
/// (control chars, non-ASCII UTF-8 continuation bytes, and `%` itself) becomes
/// `%XX` with uppercase hex.
fn percent_encode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if (0x20..=0x7E).contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_nibble(b >> 4));
            out.push(hex_nibble(b & 0x0F));
        }
    }
    out
}

/// Percent-decode a `grpc-message`. Invalid `%XX` sequences are passed through
/// literally per spec ("non-spec-compliant messages should be returned without
/// modification").
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|e| {
        // bytes that don't form valid UTF-8: fall back to lossy
        String::from_utf8_lossy(&e.into_bytes()).into_owned()
    })
}

fn hex_nibble(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'A' + n - 10) as char,
        _ => unreachable!(),
    }
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_roundtrip() {
        for n in 0u8..=16 {
            let code = Code::from_u8(n).unwrap();
            assert_eq!(code.as_u8(), n);
        }
        assert!(Code::from_u8(17).is_none());
        assert!(Code::from_u8(255).is_none());
    }

    #[test]
    fn status_into_from_trailers_ok() {
        let trailers = Status::ok().into_trailers();
        assert_eq!(trailers.get_str("grpc-status"), Some("0"));
        assert_eq!(trailers.get_str("grpc-message"), None);
        Status::from_trailers(&trailers).unwrap();
    }

    #[test]
    fn status_into_from_trailers_err() {
        let original = Status::not_found("user 42 missing");
        let trailers = original.clone().into_trailers();
        assert_eq!(trailers.get_str("grpc-status"), Some("5"));
        assert_eq!(trailers.get_str("grpc-message"), Some("user 42 missing"));

        let parsed = Status::from_trailers(&trailers).unwrap_err();
        assert_eq!(parsed.code, original.code);
        assert_eq!(parsed.message, original.message);
    }

    #[test]
    fn missing_grpc_status_is_unknown() {
        let headers = Headers::new();
        let err = Status::from_trailers(&headers).unwrap_err();
        assert_eq!(err.code, Code::Unknown);
        assert!(err.message.is_empty());
    }

    #[test]
    fn unknown_grpc_status_value_is_unknown() {
        let mut headers = Headers::new();
        headers.insert("grpc-status", "999");
        let err = Status::from_trailers(&headers).unwrap_err();
        assert_eq!(err.code, Code::Unknown);
    }

    #[test]
    fn percent_encode_roundtrip() {
        let cases = [
            ("hello", "hello"),
            ("hello world", "hello world"), // space is 0x20, allowed literal
            ("100%", "100%25"),
            ("\n\r\t", "%0A%0D%09"),
            ("café", "caf%C3%A9"),
            ("emoji: 🎉", "emoji: %F0%9F%8E%89"),
        ];
        for (raw, encoded) in cases {
            assert_eq!(percent_encode(raw), encoded, "encoding {raw:?}");
            assert_eq!(percent_decode(encoded), raw, "decoding {encoded:?}");
        }
    }

    #[test]
    fn percent_decode_passes_through_invalid_sequences() {
        // Per spec: malformed %-escapes left as-is rather than erroring.
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("100%2"), "100%2");
        assert_eq!(percent_decode("100%ZZ"), "100%ZZ");
    }

    #[test]
    fn message_omitted_when_empty() {
        let trailers = Status::cancelled("").into_trailers();
        assert_eq!(trailers.get_str("grpc-status"), Some("1"));
        assert_eq!(trailers.get_str("grpc-message"), None);
    }

    #[test]
    fn status_round_trip_preserves_metadata() {
        let mut metadata = Metadata::new();
        metadata.insert_ascii("retry-after", "30").unwrap();
        metadata
            .insert_binary("debug-bin", vec![0xDE, 0xAD])
            .unwrap();

        let original = Status::resource_exhausted("slow down").with_metadata(metadata);
        let trailers = original.clone().into_trailers();

        let parsed = Status::from_trailers(&trailers).unwrap_err();
        assert_eq!(parsed.code, original.code);
        assert_eq!(parsed.message, original.message);
        assert_eq!(parsed.metadata.get_ascii("retry-after"), Some("30"));
        assert_eq!(
            parsed.metadata.get_binary("debug-bin"),
            Some(&[0xDE, 0xAD][..]),
        );
    }
}
