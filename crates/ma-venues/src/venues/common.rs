//! Parsing helpers shared by more than one venue.

use std::time::{Duration, SystemTime};

use ma_core::Level;

use crate::sync::VenueError;

/// Parse a `(price_str, qty_str)` pair into a [`Level`].
///
/// Used by Coinbase's `price_level`/`new_quantity` fields, and by Bitstamp's
/// `[price, qty]` wire pairs via [`levels_from_str_pairs`] below.
pub fn level_from_str_pair(price: &str, qty: &str) -> Result<Level, VenueError> {
    let price = price
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad price {price:?}")))?;
    let qty = qty
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad qty {qty:?}")))?;
    Ok(Level::new(price, qty))
}

/// Bitstamp sends every side of every book message as an array of
/// `[price, qty]` pairs, on both the diff channel and the REST snapshot.
pub fn levels_from_str_pairs(pairs: &[[String; 2]]) -> Result<Vec<Level>, VenueError> {
    pairs
        .iter()
        .map(|[p, q]| level_from_str_pair(p, q))
        .collect()
}

/// Microseconds since the Unix epoch, as Bitstamp sends them.
pub fn system_time_from_micros(micros: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_micros(micros)
}

/// Parse the RFC 3339 timestamps Coinbase and Kraken put on their frames.
///
/// # Why this is hand-rolled rather than a date crate
///
/// `ma-venues` carries `serde`, `serde_json`, `crc32fast` and `rust_decimal`
/// and nothing else, and that thinness is load-bearing — it is what keeps the
/// venue layer a pure state machine that the offline suite can drive. Pulling
/// in a full calendar library to read one field would be a poor trade, because
/// the field is only ever *reported*, never used for a decision.
///
/// That last point is what makes this safe to hand-roll. `docs/DESIGN.md` §6 is
/// categorical: venue timestamps are never used for ordering, windowing, or
/// book age, because venues disagree by seconds and some are simply wrong. So
/// the worst a bug here can do is misreport observed clock skew and write a
/// wrong `venue_ts` column beside a correct `ingest_ts` one. Nothing reorders,
/// nothing desyncs, no book changes. A parse failure returns `None` and the
/// event carries no venue timestamp at all, which is the honest answer.
///
/// Accepts `YYYY-MM-DDTHH:MM:SS[.fraction][Z|+HH:MM|-HH:MM]`. Returns `None`
/// for anything else rather than erroring: a venue that changes this format has
/// not broken our book, and failing the whole frame over a decorative field
/// would turn a cosmetic drift into a data outage.
pub fn parse_rfc3339(raw: &str) -> Option<SystemTime> {
    let bytes = raw.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    // `T` per the spec; a space is tolerated because it is the one deviation
    // that shows up in the wild and it is unambiguous.
    if bytes[10] != b'T' && bytes[10] != b't' && bytes[10] != b' ' {
        return None;
    }
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }

    let year: i64 = raw.get(0..4)?.parse().ok()?;
    let month: i64 = raw.get(5..7)?.parse().ok()?;
    let day: i64 = raw.get(8..10)?.parse().ok()?;
    let hour: u64 = raw.get(11..13)?.parse().ok()?;
    let minute: u64 = raw.get(14..16)?.parse().ok()?;
    let second: u64 = raw.get(17..19)?.parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    let mut rest = raw.get(19..)?;

    // Optional fractional seconds, to nanosecond resolution. Extra digits are
    // truncated rather than rejected — a venue publishing picoseconds is not a
    // reason to drop the timestamp.
    let mut subsec_nanos = 0_u32;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let digits: String = after_dot.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            return None;
        }
        rest = rest.get(1 + digits.len()..)?;
        let mut scaled = digits.get(..9.min(digits.len()))?.to_owned();
        while scaled.len() < 9 {
            scaled.push('0');
        }
        subsec_nanos = scaled.parse().ok()?;
    }

    // Offset. `Z` (or absence, which every venue here means as UTC) is zero.
    let offset_secs: i64 = match rest.as_bytes().first() {
        None | Some(b'Z' | b'z') => 0,
        Some(sign @ (b'+' | b'-')) => {
            let sign = if *sign == b'-' { -1 } else { 1 };
            let body = rest.get(1..)?;
            let (h, m) = match body.split_once(':') {
                Some((h, m)) => (h, m),
                // Compact `+HHMM`, also legal.
                None if body.len() == 4 => body.split_at(2),
                _ => return None,
            };
            let h: i64 = h.parse().ok()?;
            let m: i64 = m.parse().ok()?;
            if !(0..=23).contains(&h) || !(0..=59).contains(&m) {
                return None;
            }
            sign * (h * 3600 + m * 60)
        }
        _ => return None,
    };

    // A leap second (`:60`) is clamped to `:59`. `SystemTime` has no way to
    // represent one, and the alternatives — rejecting the frame, or rolling
    // into the next minute — are both worse than being one second off on a
    // field that is only ever displayed.
    let second = second.min(59);

    let days = days_from_civil(year, month, day);
    let utc_secs = days * 86_400 + (hour * 3600 + minute * 60 + second) as i64 - offset_secs;

    let whole = Duration::new(utc_secs.unsigned_abs(), 0);
    let base = if utc_secs >= 0 {
        SystemTime::UNIX_EPOCH.checked_add(whole)?
    } else {
        SystemTime::UNIX_EPOCH.checked_sub(whole)?
    };
    base.checked_add(Duration::from_nanos(u64::from(subsec_nanos)))
}

/// Days since 1970-01-01 for a proleptic-Gregorian civil date.
///
/// Howard Hinnant's `days_from_civil`, which is the standard formulation and
/// the one worth copying rather than re-deriving: it shifts the year so that
/// March is the first month, which makes the leap day the *last* day of the
/// year and removes the special case that hand-written versions get wrong.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400; // [0, 399]
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn unix_nanos(t: SystemTime) -> i128 {
        match t.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as i128,
            Err(e) => -(e.duration().as_nanos() as i128),
        }
    }

    fn at(raw: &str) -> i128 {
        unix_nanos(parse_rfc3339(raw).unwrap_or_else(|| panic!("failed to parse {raw:?}")))
    }

    #[test]
    fn the_epoch_is_zero() {
        assert_eq!(at("1970-01-01T00:00:00Z"), 0);
    }

    #[test]
    fn known_timestamps_match_their_unix_seconds() {
        // Cross-checked against `date -u -d ... +%s`.
        assert_eq!(at("2024-01-01T00:00:00Z"), 1_704_067_200_000_000_000);
        assert_eq!(at("2026-08-09T12:34:56Z"), 1_786_278_896_000_000_000);
    }

    #[test]
    fn leap_days_and_century_rules() {
        // 2000 is a leap year (divisible by 400), 1900 is not (divisible by
        // 100 but not 400). This is the case a hand-rolled implementation
        // usually gets wrong, and the reason `days_from_civil` is copied
        // rather than invented.
        assert_eq!(
            at("2000-03-01T00:00:00Z") - at("2000-02-28T00:00:00Z"),
            2 * 86_400 * 1_000_000_000
        );
        assert_eq!(
            at("1900-03-01T00:00:00Z") - at("1900-02-28T00:00:00Z"),
            86_400 * 1_000_000_000
        );
        assert_eq!(
            at("2024-02-29T00:00:00Z") - at("2024-02-28T00:00:00Z"),
            86_400 * 1_000_000_000
        );
    }

    #[test]
    fn fractional_seconds_scale_to_nanoseconds() {
        let base = at("2024-01-01T00:00:00Z");
        assert_eq!(at("2024-01-01T00:00:00.5Z") - base, 500_000_000);
        assert_eq!(at("2024-01-01T00:00:00.123Z") - base, 123_000_000);
        assert_eq!(at("2024-01-01T00:00:00.000000001Z") - base, 1);
        // Coinbase's actual precision.
        assert_eq!(at("2024-01-01T00:00:00.123456789Z") - base, 123_456_789);
        // More digits than we can hold are truncated, not rejected.
        assert_eq!(at("2024-01-01T00:00:00.1234567891234Z") - base, 123_456_789);
    }

    #[test]
    fn offsets_shift_in_the_right_direction() {
        // +01:00 means local is ahead of UTC, so the UTC instant is earlier.
        assert_eq!(
            at("2024-01-01T01:00:00+01:00"),
            at("2024-01-01T00:00:00Z"),
            "a positive offset moved the instant the wrong way"
        );
        assert_eq!(at("2023-12-31T23:00:00-01:00"), at("2024-01-01T00:00:00Z"));
        assert_eq!(at("2024-01-01T01:00:00+0100"), at("2024-01-01T00:00:00Z"));
    }

    #[test]
    fn pre_epoch_dates_go_backwards_rather_than_wrapping() {
        // Not a real venue timestamp, but an unsigned subtraction bug here
        // would produce a date in 2554 rather than an obviously wrong one.
        assert_eq!(at("1969-12-31T23:59:59Z"), -1_000_000_000);
    }

    #[test]
    fn a_leap_second_is_clamped_rather_than_rejected() {
        assert_eq!(at("2016-12-31T23:59:60Z"), at("2016-12-31T23:59:59Z"));
    }

    #[test]
    fn malformed_input_yields_none_rather_than_a_wrong_instant() {
        // Returning None keeps a cosmetic venue change from becoming a data
        // outage: the event simply carries no venue timestamp.
        for bad in [
            "",
            "not a date",
            "2024-01-01",
            "2024-01-01T00:00",
            "2024-13-01T00:00:00Z",
            "2024-00-01T00:00:00Z",
            "2024-01-32T00:00:00Z",
            "2024-01-01T24:00:00Z",
            "2024-01-01T00:60:00Z",
            "2024-01-01T00:00:00.Z",
            "2024-01-01T00:00:00+99:00",
            "2024-01-01T00:00:00 QQ",
            "2024/01/01T00:00:00Z",
        ] {
            assert!(
                parse_rfc3339(bad).is_none(),
                "{bad:?} parsed into something"
            );
        }
    }

    #[test]
    fn micros_round_trip_for_bitstamp() {
        let t = system_time_from_micros(1_786_000_000_123_456);
        assert_eq!(unix_nanos(t), 1_786_000_000_123_456_000);
    }
}
