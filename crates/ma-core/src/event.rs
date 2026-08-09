//! The single normalised event type.
//!
//! Everything venue-specific is resolved at the edge, in `ma-venues`. Nothing
//! downstream of this type knows which venue an event came from except by
//! reading [`MarketEvent::venue`].

use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::price::{Price, Qty};
use crate::time::IngestTime;

/// The venues this build knows how to speak to.
///
/// An enum rather than a string: adding a venue should not compile until its
/// sync strategy and its integrity guarantee have both been decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VenueId {
    Coinbase,
    Kraken,
    Bitstamp,
    /// Scripted venue used by the offline suite. Present in release builds on
    /// purpose: replay is a first-class mode, not a test-only affordance.
    Fake,
}

impl VenueId {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Coinbase => "coinbase",
            Self::Kraken => "kraken",
            Self::Bitstamp => "bitstamp",
            Self::Fake => "fake",
        }
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A normalised instrument identifier, e.g. `BTC-USD`.
///
/// Venues disagree on spelling (`BTC-USD`, `XBT/USD`, `btcusd`); translation
/// happens in each venue's parser so that everything downstream compares equal.
/// `Arc<str>` because events are cloned into fan-out and the symbol is the only
/// variable-length field on the hot path.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(Arc<str>);

impl Symbol {
    pub fn new(s: impl AsRef<str>) -> Self {
        Self(Arc::from(s.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

/// One price level in a snapshot or delta.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Price,
    /// Zero means delete this level. See [`Qty::is_delete`].
    pub qty: Qty,
}

impl Level {
    pub fn new(price: Price, qty: Qty) -> Self {
        Self { price, qty }
    }
}

/// What actually happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// Replace the book wholesale. Ends a resync.
    Snapshot { bids: Vec<Level>, asks: Vec<Level> },
    /// Incremental change. Zero quantity deletes.
    Delta { bids: Vec<Level>, asks: Vec<Level> },
    Trade {
        price: Price,
        qty: Qty,
        /// Which side took liquidity. Some venues do not say; then this is
        /// `None` rather than a guess.
        taker_side: Option<Side>,
    },
    /// The venue's own claim about the state of our book, if it makes one.
    /// Only Kraken does. Verified by `ma-venues`, never ignored.
    Checksum { value: u32 },
    /// Liveness only. Carries no book content, but its absence is meaningful:
    /// Coinbase closes sparse subscriptions after 60–90s of silence.
    Heartbeat { counter: Option<u64> },
}

/// A normalised event, stamped with both clocks.
///
/// Note the absence of `Serialize`. It is not an oversight: [`IngestTime`]
/// holds an `Instant`, which is meaningless outside this process. The Parquet
/// writer converts explicitly at its own boundary, which forces the question
/// "which clock is this column?" to be answered once, in writing.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketEvent {
    pub venue: VenueId,
    pub symbol: Symbol,
    /// The venue's own timestamp, as sent. `None` when the venue omits one.
    ///
    /// **Never use this for windowing or ordering.** Venues disagree by
    /// seconds and some are simply wrong. It is retained so that clock skew can
    /// be measured and reported, not so it can be trusted.
    pub venue_ts: Option<SystemTime>,
    /// When we received it. This is the clock every window uses.
    pub ingest_ts: IngestTime,
    pub kind: EventKind,
}

impl MarketEvent {
    /// Observed skew: how far ahead (positive) or behind (negative) the venue's
    /// clock ran relative to ours at ingest.
    ///
    /// Reported as a metric, never used to reorder anything.
    pub fn clock_skew(&self) -> Option<SkewObservation> {
        let venue_ts = self.venue_ts?;
        let local = self.ingest_ts.wall();
        Some(match venue_ts.duration_since(local) {
            Ok(ahead) => SkewObservation::VenueAhead(ahead),
            Err(e) => SkewObservation::VenueBehind(e.duration()),
        })
    }
}

/// Direction matters here: a venue clock running *ahead* of ours means events
/// appear to arrive before they were sent, which is the case that breaks naive
/// latency dashboards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkewObservation {
    VenueAhead(std::time::Duration),
    VenueBehind(std::time::Duration),
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::time::{Clock, TestClock};
    use std::time::Duration;

    fn event_with_venue_ts(venue_ts: Option<SystemTime>) -> MarketEvent {
        let clock = TestClock::new();
        MarketEvent {
            venue: VenueId::Fake,
            symbol: Symbol::new("BTC-USD"),
            venue_ts,
            ingest_ts: clock.now(),
            kind: EventKind::Heartbeat { counter: None },
        }
    }

    #[test]
    fn skew_is_none_when_venue_omits_its_clock() {
        assert!(event_with_venue_ts(None).clock_skew().is_none());
    }

    #[test]
    fn skew_reports_direction() {
        let ev = event_with_venue_ts(None);
        let local = ev.ingest_ts.wall();

        let ahead = MarketEvent {
            venue_ts: Some(local + Duration::from_secs(2)),
            ..ev.clone()
        };
        assert_eq!(
            ahead.clock_skew(),
            Some(SkewObservation::VenueAhead(Duration::from_secs(2)))
        );

        let behind = MarketEvent {
            venue_ts: Some(local - Duration::from_secs(3)),
            ..ev
        };
        assert_eq!(
            behind.clock_skew(),
            Some(SkewObservation::VenueBehind(Duration::from_secs(3)))
        );
    }

    #[test]
    fn symbols_compare_by_value_across_clones() {
        assert_eq!(Symbol::new("BTC-USD"), Symbol::new("BTC-USD"));
        assert_ne!(Symbol::new("BTC-USD"), Symbol::new("ETH-USD"));
    }
}
