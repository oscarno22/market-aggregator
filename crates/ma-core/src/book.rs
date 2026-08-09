//! The order book, and the trust level attached to it.
//!
//! The central claim of this crate is that a consumer can always distinguish
//! three different situations:
//!
//! - **no data** — [`BookState::Uninitialized`]
//! - **data I don't trust** — [`BookState::Desynced`]
//! - **data I trust, to a stated degree** — [`BookState::Live`]
//!
//! The third is the one the original design brief missed. Not every venue can
//! make the same promise about a synced book, and flattening them into one
//! "live" flag would make a Bitstamp book look exactly as reliable as a
//! checksum-verified Kraken book. It is not.

use std::collections::BTreeMap;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::event::{Level, Side, Symbol, VenueId};
use crate::price::{Price, Qty};
use crate::time::IngestTime;

/// How strong a venue's "your book is correct" guarantee actually is.
///
/// Ordered weakest to strongest, and `Ord` is derived deliberately: a view that
/// combines venues can take the minimum and report the truth about the whole.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Integrity {
    /// **Bitstamp.** Diffs carry a microtimestamp and nothing else. We can tell
    /// that messages arrived in order; we cannot tell that they all arrived. A
    /// dropped diff leaves no trace and the book is silently wrong from then
    /// on. This is the weakest claim the system makes, and the reason the v2
    /// plan includes a periodic REST re-snapshot audit.
    OrderOnly,
    /// **Coinbase.** Contiguous `sequence_num` per connection, so a dropped
    /// message is detected the instant the next one arrives. Detection only —
    /// there is no way to fetch the missing message, so recovery is a
    /// resubscribe.
    GapDetectable,
    /// **Kraken.** CRC32 over the top 10 levels of the book we actually built,
    /// checked on every update. This validates the resulting *state*, not
    /// merely the path taken to it, and so catches misapplication bugs that
    /// sequence numbers cannot.
    Verified,
}

impl Integrity {
    /// Whether a gap in the stream would be noticed at all.
    pub const fn detects_loss(&self) -> bool {
        !matches!(self, Self::OrderOnly)
    }
}

/// Why a book stopped being trustworthy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesyncReason {
    /// Sequence numbers skipped. Coinbase, and Bitstamp's REST splice.
    SequenceGap { expected: u64, got: u64 },
    /// Our book does not hash to what the venue says it should.
    ChecksumMismatch { expected: u32, computed: u32 },
    /// Timestamps went backwards, which means reordering we cannot undo.
    TimestampRegression { last_micros: u64, got_micros: u64 },
    /// Best bid met or crossed best ask. Impossible within a single venue's
    /// book, so it is proof of misapplication — and the only loss signal
    /// available at all for [`Integrity::OrderOnly`] venues.
    CrossedBook { best_bid: Price, best_ask: Price },
    /// Socket closed, or heartbeats stopped.
    ConnectionLost,
    /// Resync in progress: buffering deltas, waiting on a snapshot.
    AwaitingSnapshot,
    /// A periodic REST depth audit disagreed with our book, repeatedly.
    ///
    /// The only loss signal available to a venue that publishes no checksum
    /// and — for Bitstamp — no sequence number either. `consecutive` is
    /// carried because a single disagreement is not evidence: the fetch races
    /// the stream, so one finding may be timing. See [`crate::audit`].
    AuditMismatch {
        price: crate::price::Price,
        consecutive: u32,
    },
    /// The buffered deltas did not join onto the snapshot cleanly. Everything
    /// is discarded and the resync restarts.
    SnapshotGap,
}

impl DesyncReason {
    /// Whether repairing this needs a **new stream** from the venue.
    ///
    /// Every venue here recovers by receiving a fresh snapshot, and only sends
    /// one on a new subscription — so most reasons here mean "reconnect".
    /// Two do not, and getting that wrong costs a connection every time:
    ///
    /// - [`Self::AwaitingSnapshot`] is Bitstamp's *normal* opening state, not
    ///   a fault. The socket is fine and a REST fetch is already in flight.
    ///   Treating it as a failure reconnects on every single startup —
    ///   observed doing exactly that against the live venue — throwing away a
    ///   healthy connection and restarting a handshake that was going to
    ///   succeed, against a venue that can rate-limit for it.
    /// - [`Self::ConnectionLost`] already means a reconnect is underway.
    ///   Asking for another would mean one request per reconnect.
    ///
    /// The distinction is "is anyone already fixing this?", not "is this bad?"
    pub const fn needs_fresh_stream(&self) -> bool {
        !matches!(self, Self::AwaitingSnapshot | Self::ConnectionLost)
    }
}

/// Whether the book can be believed, and how much.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookState {
    /// Never synced. There is no data — which is different from bad data.
    Uninitialized,
    /// Synced, with the strength of that claim attached.
    Live {
        integrity: Integrity,
        /// When this run of being synced began.
        since: IngestTime,
        /// Last time the venue's own checksum matched. Only ever `Some` for
        /// [`Integrity::Verified`]. A verified book whose last check is old is
        /// not really verified any more, which is why the timestamp is kept
        /// rather than a bare boolean.
        last_verified: Option<IngestTime>,
    },
    /// Known bad, or mid-recovery.
    Desynced {
        since: IngestTime,
        reason: DesyncReason,
    },
}

impl BookState {
    /// The trust level, or `None` if there is nothing to trust.
    pub const fn integrity(&self) -> Option<Integrity> {
        match self {
            Self::Live { integrity, .. } => Some(*integrity),
            _ => None,
        }
    }

    pub const fn is_live(&self) -> bool {
        matches!(self, Self::Live { .. })
    }

    /// Whether two readings describe the same *status*, ignoring bookkeeping
    /// that moves without anything having happened.
    ///
    /// # Why this is not `PartialEq`
    ///
    /// [`Self::Live::last_verified`] advances on every Kraken checksum match —
    /// several times a second on a live book — while nothing about the book's
    /// status has changed. A consumer that watches for transitions by
    /// comparing whole states therefore sees a transition per message, on
    /// exactly the venue whose guarantee is strongest.
    ///
    /// That is not hypothetical. Before this existed, replaying a three-minute
    /// live tape produced 1006 "book is live" transition logs for Kraken's
    /// 1108 messages, and the aggregator's "live for" clock reset on each one,
    /// so a Kraken book that had been healthy for two minutes reported having
    /// been live for however long since its last update. Coinbase and Bitstamp
    /// publish no checksum, so neither showed it, and no fixture did either:
    /// it takes a venue that verifies continuously.
    ///
    /// `PartialEq` stays exact, because "are these the same value" is a
    /// question worth being able to ask. This is the other question.
    pub fn same_status(self, other: Self) -> bool {
        match (self, other) {
            (Self::Uninitialized, Self::Uninitialized) => true,
            (
                Self::Live {
                    integrity: a,
                    since: since_a,
                    ..
                },
                Self::Live {
                    integrity: b,
                    since: since_b,
                    ..
                },
            ) => a == b && since_a == since_b,
            (
                Self::Desynced {
                    since: since_a,
                    reason: reason_a,
                },
                Self::Desynced {
                    since: since_b,
                    reason: reason_b,
                },
            ) => since_a == since_b && reason_a == reason_b,
            _ => false,
        }
    }
}

/// Operations that a caller got wrong, as opposed to data the venue got wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    #[error("cannot apply a delta to a book that is not live (state: {0:?})")]
    NotLive(BookStateKind),
}

/// [`BookState`] without its payload, for error messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookStateKind {
    Uninitialized,
    Live,
    Desynced,
}

/// Top of book, with its trust level welded on.
///
/// Returning prices without state would let a caller render a Desynced book as
/// though it were fine, which is the exact failure the brief calls out as worse
/// than being obviously down. There is no accessor that hands out prices alone.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TopOfBook {
    pub bid: Option<Level>,
    pub ask: Option<Level>,
    pub state: BookState,
    /// Time since the last applied update. `None` if nothing has ever applied.
    pub age: Option<Duration>,
}

impl TopOfBook {
    /// Spread, if both sides exist. Says nothing about whether to believe it —
    /// read [`TopOfBook::state`] for that.
    pub fn spread(&self) -> Option<Decimal> {
        let bid = self.bid?.price.as_decimal();
        let ask = self.ask?.price.as_decimal();
        Some(ask - bid)
    }

    /// Mid price, if both sides exist.
    pub fn mid(&self) -> Option<Decimal> {
        let bid = self.bid?.price.as_decimal();
        let ask = self.ask?.price.as_decimal();
        Some((ask + bid) / Decimal::TWO)
    }
}

/// A single venue's order book for a single symbol.
///
/// Owned exclusively by the aggregator task. No interior mutability, no locks —
/// single ownership is what buys that.
#[derive(Clone, Debug)]
pub struct Book {
    venue: VenueId,
    symbol: Symbol,
    /// Ascending by price; best bid is the last entry.
    bids: BTreeMap<Price, Qty>,
    /// Ascending by price; best ask is the first entry.
    asks: BTreeMap<Price, Qty>,
    state: BookState,
    last_update: Option<IngestTime>,
    max_depth: Option<usize>,
}

impl Book {
    pub fn new(venue: VenueId, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            state: BookState::Uninitialized,
            last_update: None,
            max_depth: None,
        }
    }

    /// Cap retained depth per side.
    ///
    /// **Caveat, and it is a real one:** a pruned book cannot be repaired by
    /// deltas alone. Once the 51st level is discarded, a delta deleting a level
    /// inside the top 50 exposes a level we no longer know. The prune boundary
    /// must therefore sit well beyond the depth actually served, and a pruned
    /// book is only ever correct near the top.
    pub fn with_max_depth(mut self, depth: usize) -> Self {
        self.max_depth = Some(depth);
        self
    }

    pub fn venue(&self) -> VenueId {
        self.venue
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn state(&self) -> BookState {
        self.state
    }

    /// Number of levels currently held, per side.
    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Replace the book wholesale and declare it live at the given integrity.
    ///
    /// This is the end of every resync: the snapshot is the ground truth, so
    /// prior contents are discarded rather than merged.
    pub fn apply_snapshot(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        integrity: Integrity,
        at: IngestTime,
    ) -> Result<(), DesyncReason> {
        self.bids.clear();
        self.asks.clear();

        for level in bids {
            if !level.qty.is_delete() {
                self.bids.insert(level.price, level.qty);
            }
        }
        for level in asks {
            if !level.qty.is_delete() {
                self.asks.insert(level.price, level.qty);
            }
        }

        self.state = BookState::Live {
            integrity,
            since: at,
            last_verified: None,
        };
        self.last_update = Some(at);
        self.prune();
        self.guard_crossed(at)
    }

    /// Apply an incremental update. Zero quantity deletes the level.
    ///
    /// Refuses to apply to a book that is not live: deltas arriving during a
    /// resync belong in the venue layer's buffer, not in the book. Applying
    /// them here is precisely how a book ends up silently wrong.
    pub fn apply_delta(
        &mut self,
        bids: &[Level],
        asks: &[Level],
        at: IngestTime,
    ) -> Result<(), BookError> {
        if !self.state.is_live() {
            return Err(BookError::NotLive(self.state_kind()));
        }

        apply_levels(&mut self.bids, bids);
        apply_levels(&mut self.asks, asks);

        self.last_update = Some(at);
        self.prune();

        // A crossed book is a desync, not a caller error, so it changes state
        // rather than returning Err — the caller cannot "handle" it any other
        // way, and forgetting to check must not be possible.
        let _ = self.guard_crossed(at);
        Ok(())
    }

    /// Record that the venue's checksum matched.
    ///
    /// Only meaningful for [`Integrity::Verified`]; ignored otherwise, since a
    /// venue that publishes no checksum can never earn the claim.
    pub fn mark_verified(&mut self, at: IngestTime) {
        if let BookState::Live {
            integrity: Integrity::Verified,
            last_verified,
            ..
        } = &mut self.state
        {
            *last_verified = Some(at);
        }
    }

    /// Declare the book untrustworthy. Contents are retained for debugging but
    /// will not be served as live.
    pub fn mark_desynced(&mut self, reason: DesyncReason, at: IngestTime) {
        self.state = BookState::Desynced { since: at, reason };
    }

    /// Top of book with trust and age attached.
    ///
    /// Takes `now` rather than reading a clock, so that staleness is testable
    /// without sleeping.
    pub fn top_of_book(&self, now: IngestTime) -> TopOfBook {
        TopOfBook {
            bid: self.best(Side::Bid),
            ask: self.best(Side::Ask),
            state: self.state,
            age: self.last_update.map(|last| now.since(last)),
        }
    }

    /// Best level on a side, ignoring trust. Internal and checksum use; callers
    /// outside this crate should go through [`Book::top_of_book`].
    pub fn best(&self, side: Side) -> Option<Level> {
        match side {
            Side::Bid => self.bids.iter().next_back(),
            Side::Ask => self.asks.iter().next(),
        }
        .map(|(price, qty)| Level::new(*price, *qty))
    }

    /// The `n` best levels on a side, best first.
    ///
    /// Ordering matters beyond presentation: Kraken's checksum is computed over
    /// the top 10 in exactly this order, so this is the function its correctness
    /// rests on.
    pub fn top_levels(&self, side: Side, n: usize) -> Vec<Level> {
        let iter: Box<dyn Iterator<Item = (&Price, &Qty)>> = match side {
            Side::Bid => Box::new(self.bids.iter().rev()),
            Side::Ask => Box::new(self.asks.iter()),
        };
        iter.take(n)
            .map(|(price, qty)| Level::new(*price, *qty))
            .collect()
    }

    fn state_kind(&self) -> BookStateKind {
        match self.state {
            BookState::Uninitialized => BookStateKind::Uninitialized,
            BookState::Live { .. } => BookStateKind::Live,
            BookState::Desynced { .. } => BookStateKind::Desynced,
        }
    }

    /// Detect a crossed book and desync if found.
    ///
    /// Within one venue's book, best bid >= best ask is impossible in reality,
    /// so observing it proves we misapplied something. For `OrderOnly` venues
    /// this is the *only* loss detector available, which makes it worth the
    /// check on every update rather than periodically.
    fn guard_crossed(&mut self, at: IngestTime) -> Result<(), DesyncReason> {
        let (Some(bid), Some(ask)) = (self.best(Side::Bid), self.best(Side::Ask)) else {
            return Ok(());
        };
        if bid.price >= ask.price {
            let reason = DesyncReason::CrossedBook {
                best_bid: bid.price,
                best_ask: ask.price,
            };
            self.mark_desynced(reason, at);
            return Err(reason);
        }
        Ok(())
    }

    fn prune(&mut self) {
        let Some(max) = self.max_depth else { return };

        while self.bids.len() > max {
            // Bids are ascending, so the worst is first.
            let Some(worst) = self.bids.keys().next().copied() else {
                break;
            };
            self.bids.remove(&worst);
        }
        while self.asks.len() > max {
            let Some(worst) = self.asks.keys().next_back().copied() else {
                break;
            };
            self.asks.remove(&worst);
        }
    }
}

fn apply_levels(side: &mut BTreeMap<Price, Qty>, updates: &[Level]) {
    for level in updates {
        if level.qty.is_delete() {
            side.remove(&level.price);
        } else {
            side.insert(level.price, level.qty);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::time::{Clock, TestClock};

    fn lv(price: &str, qty: &str) -> Level {
        Level::new(price.parse().unwrap(), qty.parse().unwrap())
    }

    fn book() -> (Book, TestClock) {
        (
            Book::new(VenueId::Fake, Symbol::new("BTC-USD")),
            TestClock::new(),
        )
    }

    fn synced() -> (Book, TestClock) {
        let (mut b, clock) = book();
        b.apply_snapshot(
            &[lv("100", "1"), lv("99", "2")],
            &[lv("101", "1"), lv("102", "3")],
            Integrity::GapDetectable,
            clock.now(),
        )
        .unwrap();
        (b, clock)
    }

    #[test]
    fn fresh_book_reports_no_data_not_bad_data() {
        let (b, clock) = book();
        let top = b.top_of_book(clock.now());
        assert_eq!(top.state, BookState::Uninitialized);
        assert!(top.bid.is_none() && top.ask.is_none());
        assert!(top.age.is_none(), "never-updated book has no age");
    }

    #[test]
    fn snapshot_establishes_best_levels() {
        let (b, clock) = synced();
        let top = b.top_of_book(clock.now());
        assert_eq!(top.bid, Some(lv("100", "1")));
        assert_eq!(top.ask, Some(lv("101", "1")));
        assert_eq!(top.spread(), Some(Decimal::ONE));
        assert_eq!(top.state.integrity(), Some(Integrity::GapDetectable));
    }

    #[test]
    fn snapshot_replaces_rather_than_merges() {
        let (mut b, clock) = synced();
        b.apply_snapshot(
            &[lv("50", "1")],
            &[lv("51", "1")],
            Integrity::Verified,
            clock.now(),
        )
        .unwrap();
        assert_eq!(b.depth(), (1, 1), "old levels survived a snapshot");
        assert_eq!(b.best(Side::Bid), Some(lv("50", "1")));
    }

    #[test]
    fn zero_quantity_deletes_the_level() {
        let (mut b, clock) = synced();
        b.apply_delta(&[lv("100", "0")], &[], clock.now()).unwrap();
        assert_eq!(
            b.best(Side::Bid),
            Some(lv("99", "2")),
            "deleting the best bid should expose the next one"
        );
    }

    #[test]
    fn delta_updates_quantity_in_place() {
        let (mut b, clock) = synced();
        b.apply_delta(&[lv("100", "7")], &[], clock.now()).unwrap();
        assert_eq!(b.best(Side::Bid), Some(lv("100", "7")));
        assert_eq!(b.depth(), (2, 2), "in-place update changed the level count");
    }

    #[test]
    fn desynced_book_refuses_deltas() {
        let (mut b, clock) = synced();
        b.mark_desynced(DesyncReason::ConnectionLost, clock.now());

        let err = b
            .apply_delta(&[lv("100", "5")], &[], clock.now())
            .unwrap_err();
        assert_eq!(err, BookError::NotLive(BookStateKind::Desynced));
    }

    #[test]
    fn uninitialized_book_refuses_deltas() {
        // The bug this prevents: applying deltas to an empty book produces a
        // plausible-looking book that is missing everything before the first
        // delta, and nothing downstream can tell.
        let (mut b, clock) = book();
        let err = b
            .apply_delta(&[lv("100", "5")], &[], clock.now())
            .unwrap_err();
        assert_eq!(err, BookError::NotLive(BookStateKind::Uninitialized));
    }

    #[test]
    fn crossed_book_desyncs_itself() {
        let (mut b, clock) = synced();
        // Bid crosses through the ask: impossible within one venue.
        b.apply_delta(&[lv("105", "1")], &[], clock.now()).unwrap();

        match b.state() {
            BookState::Desynced {
                reason: DesyncReason::CrossedBook { best_bid, best_ask },
                ..
            } => {
                assert_eq!(best_bid, "105".parse().unwrap());
                assert_eq!(best_ask, "101".parse().unwrap());
            }
            other => panic!("expected CrossedBook desync, got {other:?}"),
        }
    }

    #[test]
    fn touching_book_is_crossed_too() {
        // bid == ask is equally impossible, and an off-by-one here would let
        // the one case a venue actually produces slip through.
        let (mut b, clock) = synced();
        b.apply_delta(&[lv("101", "1")], &[], clock.now()).unwrap();
        assert!(matches!(
            b.state(),
            BookState::Desynced {
                reason: DesyncReason::CrossedBook { .. },
                ..
            }
        ));
    }

    #[test]
    fn only_checksum_venues_can_claim_verification() {
        let (mut b, clock) = synced(); // GapDetectable
        b.mark_verified(clock.now());
        match b.state() {
            BookState::Live { last_verified, .. } => assert!(
                last_verified.is_none(),
                "a venue with no checksum claimed verification"
            ),
            other => panic!("expected Live, got {other:?}"),
        }
    }

    #[test]
    fn verified_venue_records_when_it_was_last_checked() {
        let (mut b, clock) = book();
        b.apply_snapshot(
            &[lv("100", "1")],
            &[lv("101", "1")],
            Integrity::Verified,
            clock.now(),
        )
        .unwrap();
        clock.advance(Duration::from_secs(5));
        b.mark_verified(clock.now());

        match b.state() {
            BookState::Live {
                last_verified: Some(at),
                since,
                ..
            } => {
                assert_eq!(at.since(since), Duration::from_secs(5));
            }
            other => panic!("expected a verified Live book, got {other:?}"),
        }
    }

    #[test]
    fn integrity_is_ordered_weakest_first() {
        // The v3 cross-venue view depends on this: combining books takes the
        // minimum and reports the truth about the weakest input.
        assert!(Integrity::OrderOnly < Integrity::GapDetectable);
        assert!(Integrity::GapDetectable < Integrity::Verified);

        let combined = [Integrity::Verified, Integrity::OrderOnly]
            .into_iter()
            .min()
            .unwrap();
        assert_eq!(combined, Integrity::OrderOnly);

        assert!(!Integrity::OrderOnly.detects_loss());
        assert!(Integrity::GapDetectable.detects_loss());
    }

    #[test]
    fn age_measures_time_since_last_update_not_since_sync() {
        let (mut b, clock) = synced();
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            b.top_of_book(clock.now()).age,
            Some(Duration::from_secs(10))
        );

        b.apply_delta(&[lv("100", "3")], &[], clock.now()).unwrap();
        assert_eq!(b.top_of_book(clock.now()).age, Some(Duration::ZERO));
    }

    #[test]
    fn top_levels_are_ordered_best_first() {
        let (mut b, clock) = book();
        b.apply_snapshot(
            &[lv("97", "1"), lv("99", "1"), lv("98", "1")],
            &[lv("103", "1"), lv("101", "1"), lv("102", "1")],
            Integrity::Verified,
            clock.now(),
        )
        .unwrap();

        let bids: Vec<String> = b
            .top_levels(Side::Bid, 10)
            .iter()
            .map(|l| l.price.to_string())
            .collect();
        assert_eq!(bids, ["99", "98", "97"], "bids must descend from best");

        let asks: Vec<String> = b
            .top_levels(Side::Ask, 10)
            .iter()
            .map(|l| l.price.to_string())
            .collect();
        assert_eq!(asks, ["101", "102", "103"], "asks must ascend from best");
    }

    #[test]
    fn pruning_discards_the_worst_levels_on_each_side() {
        let mut b = Book::new(VenueId::Fake, Symbol::new("BTC-USD")).with_max_depth(2);
        let clock = TestClock::new();
        b.apply_snapshot(
            &[lv("97", "1"), lv("99", "1"), lv("98", "1")],
            &[lv("103", "1"), lv("101", "1"), lv("102", "1")],
            Integrity::Verified,
            clock.now(),
        )
        .unwrap();

        assert_eq!(b.depth(), (2, 2));
        assert_eq!(b.best(Side::Bid), Some(lv("99", "1")));
        assert_eq!(b.best(Side::Ask), Some(lv("101", "1")));
        // The levels furthest from the touch are the ones dropped.
        assert_eq!(
            b.top_levels(Side::Bid, 10).last().unwrap().price,
            "98".parse().unwrap()
        );
    }
}
