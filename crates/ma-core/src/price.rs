//! Exact decimal price and quantity types.

use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Error parsing a price or quantity off the wire.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("not a decimal number: {0}")]
    NotDecimal(String),
    #[error("quantity may not be negative: {0}")]
    NegativeQty(Decimal),
}

/// A price level, represented exactly.
///
/// Deliberately not `f64`, for two independent reasons:
///
/// 1. **Kraken's book checksum.** Kraken validates a local book by CRC32 over
///    the venue's own decimal digits with the point removed. A representation
///    that cannot reproduce the exact digits Kraken sent will produce checksum
///    mismatches indistinguishable from genuine book corruption — the most
///    expensive kind of false alarm this system can raise.
///
/// 2. **Book keys.** Levels are keyed by price. Under `f64`, two prices that
///    are equal on the wire can compare unequal after a round trip and occupy
///    two separate levels, quietly doubling the depth at that price.
///
/// [`Decimal`] preserves scale, so `45000.10` stays two places and renders back
/// to the bytes the venue hashed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(Decimal);

impl Price {
    pub const fn from_decimal(d: Decimal) -> Self {
        Self(d)
    }

    pub const fn as_decimal(&self) -> Decimal {
        self.0
    }

    /// Render with exactly `scale` digits after the point.
    ///
    /// Venue checksums are computed over a fixed precision per trading pair,
    /// which is not always the precision the venue chose to transmit — it will
    /// send `45000.1` and expect `45000.10` to be hashed. This is the seam
    /// where that gets normalised.
    pub fn to_fixed_string(&self, scale: u32) -> String {
        format!("{:.*}", scale as usize, self.0)
    }
}

impl FromStr for Price {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Decimal::from_str(s)
            .or_else(|_| Decimal::from_scientific(s))
            .map(Self)
            .map_err(|_| ParseError::NotDecimal(s.to_owned()))
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Price({})", self.0)
    }
}

/// A size at a price level. Never negative; zero is meaningful.
///
/// A zero quantity in a delta is a **deletion**, not an empty level. That
/// convention is shared by all three venues and is relied on by
/// [`Book::apply_delta`](crate::Book::apply_delta).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Qty(Decimal);

impl Qty {
    pub const ZERO: Qty = Qty(Decimal::ZERO);

    pub fn from_decimal(d: Decimal) -> Result<Self, ParseError> {
        if d.is_sign_negative() && !d.is_zero() {
            return Err(ParseError::NegativeQty(d));
        }
        Ok(Self(d))
    }

    pub const fn as_decimal(&self) -> Decimal {
        self.0
    }

    /// True when this quantity means "remove the level".
    pub fn is_delete(&self) -> bool {
        self.0.is_zero()
    }

    pub fn to_fixed_string(&self, scale: u32) -> String {
        format!("{:.*}", scale as usize, self.0)
    }
}

impl FromStr for Qty {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let d = Decimal::from_str(s)
            .or_else(|_| Decimal::from_scientific(s))
            .map_err(|_| ParseError::NotDecimal(s.to_owned()))?;
        Self::from_decimal(d)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Qty({})", self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn price_round_trips_exactly() {
        // The property Kraken's checksum depends on.
        for raw in ["45000.10", "0.00000001", "123456.78901234", "1", "0.1"] {
            let p: Price = raw.parse().unwrap();
            assert_eq!(p.to_string(), raw, "lost digits round-tripping {raw}");
        }
    }

    #[test]
    fn price_preserves_trailing_zeros() {
        // `45000.10` and `45000.1` are numerically equal but hash differently
        // once the decimal point is stripped, so scale must survive parsing.
        let padded: Price = "45000.10".parse().unwrap();
        let bare: Price = "45000.1".parse().unwrap();

        assert_eq!(padded, bare, "should compare numerically equal");
        assert_eq!(padded.to_string(), "45000.10");
        assert_eq!(bare.to_string(), "45000.1");
        // ...and the checksum seam normalises them back together.
        assert_eq!(padded.to_fixed_string(2), bare.to_fixed_string(2));
    }

    #[test]
    fn equal_prices_are_one_level() {
        // The f64 hazard: these must be the same key, not two levels.
        use std::collections::BTreeMap;
        let mut book: BTreeMap<Price, Qty> = BTreeMap::new();
        book.insert("45000.10".parse().unwrap(), "1".parse().unwrap());
        book.insert("45000.1".parse().unwrap(), "2".parse().unwrap());
        assert_eq!(book.len(), 1, "same price occupied two levels");
    }

    #[test]
    fn prices_order_numerically_not_lexically() {
        let mut prices: Vec<Price> = ["9.5", "10.1", "100", "9.45"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        prices.sort();
        let rendered: Vec<String> = prices.iter().map(|p| p.to_string()).collect();
        assert_eq!(rendered, ["9.45", "9.5", "10.1", "100"]);
    }

    #[test]
    fn zero_qty_is_a_delete() {
        let q: Qty = "0".parse().unwrap();
        assert!(q.is_delete());
        let q: Qty = "0.00000000".parse().unwrap();
        assert!(q.is_delete(), "scaled zero is still a delete");
        let q: Qty = "0.00000001".parse().unwrap();
        assert!(!q.is_delete());
    }

    #[test]
    fn negative_qty_is_rejected() {
        assert!("-1".parse::<Qty>().is_err());
        // Negative zero is not an error; it is still a delete.
        assert!("-0".parse::<Qty>().unwrap().is_delete());
    }

    #[test]
    fn garbage_is_rejected_rather_than_defaulted() {
        // A venue sending `null` or an empty string must not silently become 0.
        for raw in ["", "null", "NaN", "Infinity", "abc"] {
            assert!(raw.parse::<Price>().is_err(), "{raw} parsed as a price");
        }
    }
}
