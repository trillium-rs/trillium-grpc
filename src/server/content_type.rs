use trillium::{Headers, KnownHeaderName};

/// Parse a gRPC content-type header.
///
/// Returns `Some(suffix)` if the value is a valid gRPC content-type. The bare
/// `application/grpc` returns `Some("proto")` since the spec treats it as an
/// alias for `application/grpc+proto`.
///
/// Examples:
/// - `application/grpc` → `Some("proto")`
/// - `application/grpc+proto` → `Some("proto")`
/// - `application/grpc+json` → `Some("json")`
/// - `application/grpc+json; charset=utf-8` → `Some("json")` (parameters ignored)
/// - `application/json` → `None`
pub fn parse_grpc_content_type(value: &str) -> Option<&str> {
    let value = value.trim();
    let media_type = value.split(';').next().unwrap_or(value).trim();

    let prefix = "application/grpc";
    if !media_type.get(..prefix.len())?.eq_ignore_ascii_case(prefix) {
        return None;
    }

    match &media_type[prefix.len()..] {
        "" => Some("proto"),
        rest if rest.starts_with('+') => Some(&rest[1..]),
        _ => None,
    }
}

/// Validate that the request carries `te: trailers`. The gRPC HTTP/2 spec
/// requires this so HTTP/2 intermediaries don't strip response trailers.
///
/// Returns `true` if any value of the `te` header equals `trailers`
/// (case-insensitive). `te` is a list-valued header, so multiple comma-separated
/// values or multiple header lines are both accepted.
pub fn has_te_trailers(headers: &Headers) -> bool {
    let Some(values) = headers.get_values(KnownHeaderName::Te) else {
        return false;
    };
    values
        .iter()
        .filter_map(|v| v.as_str())
        .flat_map(|s| s.split(','))
        .any(|tok| tok.trim().eq_ignore_ascii_case("trailers"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_application_grpc_is_proto() {
        assert_eq!(parse_grpc_content_type("application/grpc"), Some("proto"));
    }

    #[test]
    fn proto_suffix() {
        assert_eq!(
            parse_grpc_content_type("application/grpc+proto"),
            Some("proto")
        );
    }

    #[test]
    fn json_suffix() {
        assert_eq!(
            parse_grpc_content_type("application/grpc+json"),
            Some("json")
        );
    }

    #[test]
    fn case_insensitive_prefix() {
        assert_eq!(
            parse_grpc_content_type("Application/GRPC+proto"),
            Some("proto")
        );
    }

    #[test]
    fn parameters_are_ignored() {
        assert_eq!(
            parse_grpc_content_type("application/grpc+proto; charset=utf-8"),
            Some("proto")
        );
        assert_eq!(
            parse_grpc_content_type("application/grpc; encoding=identity"),
            Some("proto")
        );
    }

    #[test]
    fn surrounding_whitespace_trimmed() {
        assert_eq!(
            parse_grpc_content_type("  application/grpc+json  "),
            Some("json")
        );
    }

    #[test]
    fn rejects_non_grpc_types() {
        assert_eq!(parse_grpc_content_type("application/json"), None);
        assert_eq!(parse_grpc_content_type("text/plain"), None);
        assert_eq!(parse_grpc_content_type(""), None);
        assert_eq!(parse_grpc_content_type("application/grpc-web"), None);
        assert_eq!(parse_grpc_content_type("application/grpcfoo"), None);
    }

    #[test]
    fn te_trailers_present() {
        let mut headers = Headers::new();
        headers.insert(KnownHeaderName::Te, "trailers");
        assert!(has_te_trailers(&headers));
    }

    #[test]
    fn te_trailers_case_insensitive() {
        let mut headers = Headers::new();
        headers.insert(KnownHeaderName::Te, "Trailers");
        assert!(has_te_trailers(&headers));
    }

    #[test]
    fn te_trailers_among_other_values() {
        let mut headers = Headers::new();
        headers.insert(KnownHeaderName::Te, "gzip, trailers");
        assert!(has_te_trailers(&headers));
    }

    #[test]
    fn te_missing_or_wrong() {
        let headers = Headers::new();
        assert!(!has_te_trailers(&headers));

        let mut headers = Headers::new();
        headers.insert(KnownHeaderName::Te, "gzip");
        assert!(!has_te_trailers(&headers));
    }
}
