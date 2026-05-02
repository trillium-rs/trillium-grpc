//! `grpc-timeout` header codec.
//!
//! The wire format is `<positive_integer><unit>` where unit is one of
//! `H`, `M`, `S`, `m`, `u`, `n` (hour, minute, second, millisecond,
//! microsecond, nanosecond). The integer is at most 8 ASCII digits.
//!
//! When formatting a `Duration` we pick the smallest unit that fits in 8
//! digits and round *up*, so the wire value is always ≥ the requested
//! duration — we never advertise a tighter deadline than asked.

use std::time::Duration;

const MAX_VALUE: u128 = 99_999_999;

/// (unit_nanos, suffix) ordered smallest → largest. The first entry whose
/// ceil(nanos / unit_nanos) fits in 8 digits is chosen.
const UNITS: &[(u128, char)] = &[
    (1, 'n'),
    (1_000, 'u'),
    (1_000_000, 'm'),
    (1_000_000_000, 'S'),
    (60 * 1_000_000_000, 'M'),
    (3600 * 1_000_000_000, 'H'),
];

/// Format a [`Duration`] as a `grpc-timeout` header value.
///
/// Picks the smallest unit such that the rounded-up integer value fits in
/// 8 digits. Durations longer than 99,999,999 hours saturate to that.
pub fn format_grpc_timeout(d: Duration) -> String {
    let nanos = d.as_nanos();
    for &(unit_nanos, suffix) in UNITS {
        let value = nanos.div_ceil(unit_nanos);
        if value <= MAX_VALUE {
            return format!("{value}{suffix}");
        }
    }
    format!("{MAX_VALUE}H")
}

/// Parse a `grpc-timeout` header value into a [`Duration`]. Returns `None`
/// for malformed input (empty, missing/unknown unit, non-digit body, or
/// arithmetic overflow).
pub fn parse_grpc_timeout(s: &str) -> Option<Duration> {
    let bytes = s.as_bytes();
    let (&suffix, digits) = bytes.split_last()?;
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    let unit_nanos: u64 = match suffix {
        b'n' => 1,
        b'u' => 1_000,
        b'm' => 1_000_000,
        b'S' => 1_000_000_000,
        b'M' => 60 * 1_000_000_000,
        b'H' => 3600 * 1_000_000_000,
        _ => return None,
    };
    // Use u128 to avoid overflow at the hours-times-99_999_999 extreme,
    // then split into (secs, subsec_nanos) for Duration::new.
    let total_nanos = (value as u128).checked_mul(unit_nanos as u128)?;
    let secs = u64::try_from(total_nanos / 1_000_000_000).ok()?;
    let nanos = (total_nanos % 1_000_000_000) as u32;
    Some(Duration::new(secs, nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_picks_smallest_fitting_unit() {
        assert_eq!(format_grpc_timeout(Duration::from_nanos(500)), "500n");
        assert_eq!(format_grpc_timeout(Duration::from_micros(500)), "500000n");
        // 500ms = 500_000_000n, doesn't fit (9 digits) → step up to micros.
        assert_eq!(format_grpc_timeout(Duration::from_millis(500)), "500000u");
        // 5s = 5_000_000us, fits in micros (7 digits).
        assert_eq!(format_grpc_timeout(Duration::from_secs(5)), "5000000u");
        // 5min = 5_000_000_000us doesn't fit, 5_000_000ms doesn't fit (10
        // digits), 5_000ms doesn't... wait: 300_000ms fits → "300000m".
        assert_eq!(format_grpc_timeout(Duration::from_secs(300)), "300000m");
        // 1 hour = 3_600_000ms, fits as ms.
        assert_eq!(format_grpc_timeout(Duration::from_secs(3600)), "3600000m");
        // 1 day = 86_400s, fits as seconds.
        assert_eq!(
            format_grpc_timeout(Duration::from_secs(86_400)),
            "86400000m"
        );
    }

    #[test]
    fn format_rounds_up() {
        // 1500ns → 2us (round up, never under-promise).
        assert_eq!(format_grpc_timeout(Duration::from_nanos(1500)), "1500n");
        // Construct a duration that doesn't fit in nanos and isn't a
        // whole number of micros: 99_999_999_500 ns = 99_999_999.5 us.
        // Doesn't fit as nanos (12 digits). As micros: ceil = 100_000_000,
        // doesn't fit. As ms: ceil(99_999_999_500 / 1_000_000) = 100_000.
        assert_eq!(
            format_grpc_timeout(Duration::from_nanos(99_999_999_500)),
            "100000m"
        );
    }

    #[test]
    fn format_zero() {
        assert_eq!(format_grpc_timeout(Duration::ZERO), "0n");
    }

    #[test]
    fn format_saturates_at_max_hours() {
        // u64::MAX nanos ≈ 213 years; well under 99_999_999 hours
        // (~11_400 years). Construct something larger via from_secs:
        let huge = Duration::from_secs(u64::MAX); // ~5.8e11 years
        assert_eq!(format_grpc_timeout(huge), "99999999H");
    }

    #[test]
    fn parse_each_unit() {
        assert_eq!(parse_grpc_timeout("500n"), Some(Duration::from_nanos(500)));
        assert_eq!(parse_grpc_timeout("500u"), Some(Duration::from_micros(500)));
        assert_eq!(parse_grpc_timeout("500m"), Some(Duration::from_millis(500)));
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("2M"), Some(Duration::from_secs(120)));
        assert_eq!(parse_grpc_timeout("1H"), Some(Duration::from_secs(3600)));
    }

    #[test]
    fn parse_extreme_value() {
        // 99_999_999H = 99_999_999 * 3600 seconds. Should parse cleanly.
        let d = parse_grpc_timeout("99999999H").unwrap();
        assert_eq!(d.as_secs(), 99_999_999u64 * 3600);
    }

    #[test]
    fn parse_rejects_malformed() {
        assert_eq!(parse_grpc_timeout(""), None);
        assert_eq!(parse_grpc_timeout("S"), None); // no digits
        assert_eq!(parse_grpc_timeout("5"), None); // no unit
        assert_eq!(parse_grpc_timeout("5s"), None); // lowercase s is not a unit
        assert_eq!(parse_grpc_timeout("5x"), None); // bad unit
        assert_eq!(parse_grpc_timeout("-1S"), None); // sign not allowed
        assert_eq!(parse_grpc_timeout("1.5S"), None); // not an integer
        assert_eq!(parse_grpc_timeout(" 5S"), None); // whitespace not allowed
    }

    #[test]
    fn parse_zero_is_valid() {
        assert_eq!(parse_grpc_timeout("0n"), Some(Duration::ZERO));
        assert_eq!(parse_grpc_timeout("0S"), Some(Duration::ZERO));
    }

    #[test]
    fn roundtrip_typical_durations() {
        for d in [
            Duration::from_millis(1),
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(30),
            Duration::from_secs(300),
        ] {
            let formatted = format_grpc_timeout(d);
            let parsed = parse_grpc_timeout(&formatted).unwrap();
            assert_eq!(parsed, d, "roundtrip failed for {d:?} → {formatted:?}");
        }
    }
}
