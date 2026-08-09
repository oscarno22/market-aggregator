//! The consolidated touch across venues, and the arbitrage it appears to show.
//!
//! Take the highest bid and the lowest ask across every venue tracking one
//! symbol. Usually the result is an ordinary, tighter-than-any-single-venue
//! spread. Occasionally the highest bid is *above* the lowest ask, and the two
//! books are crossed — someone appears to be bidding more on one venue than
//! someone else is offering on another.
//!
//! This is the most misreadable number this system publishes, and everything
//! in this module exists to make it hard to misread.
//!
//! # Why the obvious implementation lies
//!
//! Exactly the shape of the depth audit's problem in §5 of `docs/DESIGN.md`,
//! one layer up. There, a REST snapshot from instant `T` was compared against
//! a websocket book at `T + δ`, and the naive comparison disagreed almost
//! every time. Here, two *different venues'* books are compared, and they were
//! never observed at the same instant either: they arrive over different
//! sockets, with different network paths, and are applied by different tasks.
//!
//! So a consolidated cross is not, on its own, evidence of anything. Three
//! rules keep it from being noise:
//!
//! **1. Only trusted books participate.** A `Desynced` book still holds its
//! last contents — deliberately, for debugging — and those contents are
//! exactly what a naive `max` over bids would pick up. A book that is
//! mid-recovery holding a stale aggressive bid would show a permanent
//! arbitrage against every healthy venue beside it.
//!
//! **2. A book older than [`CrossPolicy::max_age`] does not participate.** Not
//! because a quiet book is wrong, but because a *stalled* one is: on the pairs
//! this runs against, a live feed updates many times a second, so a book that
//! has not moved in seconds is one whose socket has gone quiet, and its touch
//! is a quote from a market that has since moved. The idle watchdog will
//! reconnect it shortly; until then it must not be one leg of a spread.
//!
//! **3. Every derived number states the weakest guarantee behind it.** An
//! "edge" computed from a Kraken book verified by checksum and a Bitstamp book
//! that may have silently lost a message is a Bitstamp-grade number, not a
//! Kraken-grade one. [`CrossVenue::integrity_floor`] is the minimum over the
//! legs *actually used*, not over every venue configured, because the number
//! is derived from two books and not from the set.
//!
//! # What was measured, and the wrong turn it rules out
//!
//! The tempting refinement is to measure staleness as *time since the touch
//! last moved*, rather than time since any level was applied. It sounds
//! strictly better and it is wrong, and the committed tape is the counter-
//! example: across 60 seconds of Coinbase BTC-USD it carries 49,940 level
//! updates and the touch never changes once — 288 of those updates land within
//! $5 of the touch and none of them move it. Touch age there would read as a
//! minute while the feed was healthy the entire time.
//!
//! The reason is that on a gap-free incremental feed a touch that has not
//! moved *has not changed*: the venue would have sent a delta if it had. Time
//! since the last applied update is what actually distinguishes "quiet" from
//! "stalled", and that is what [`TopOfBook::age`] already carries.
//!
//! # This is not an execution signal, and the naming says so
//!
//! [`CrossVenue::spread_bps`] is signed, and negative means crossed. The
//! magnitude of a negative reading is an **apparent** edge, gross of every
//! reason it is usually not real: taker fees on both legs (which alone exceed
//! most crossings seen here), the latency between observing it and acting,
//! withdrawal and transfer times between venues, and the fact that the two
//! quotes were never simultaneous. The project takes no orders and holds no
//! key that could — see CLAUDE.md's non-goals — so this is a market-structure
//! reading, not a trading one.
//!
//! # A cross here is not a desync
//!
//! Within one venue, best bid ≥ best ask is impossible and
//! [`DesyncReason::CrossedBook`](crate::DesyncReason::CrossedBook) treats it as
//! proof of misapplication. Across venues it is an ordinary market state.
//! Wiring this signal into the desync path would reconnect every venue in the
//! process every time two exchanges disagreed by a basis point.

use std::fmt;
use std::time::Duration;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::{BookState, Integrity, TopOfBook};
use crate::event::VenueId;
use crate::price::{Price, Qty};

/// Which books are allowed to be a leg of the consolidated touch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrossPolicy {
    /// A book whose last applied update is older than this does not
    /// participate.
    ///
    /// Two seconds by default, which on these pairs is two orders of magnitude
    /// beyond a healthy inter-update gap and still well inside the tightest
    /// idle watchdog (Coinbase, 15s). The gap between those two numbers is
    /// deliberate: it is the window in which a socket has gone quiet but
    /// nothing has yet reconnected it, and it is precisely the window in which
    /// a stale quote would otherwise be picked up as an arbitrage.
    pub max_age: Duration,
}

impl Default for CrossPolicy {
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(2),
        }
    }
}

/// One side of the consolidated touch, and where it came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossLeg {
    pub venue: VenueId,
    pub price: Price,
    pub qty: Qty,
    /// Age of the book this leg came from. Published per leg because the two
    /// legs are rarely equally fresh, and the older one bounds the claim.
    pub age_ms: u64,
    /// What this venue's protocol can prove about the book behind this leg.
    pub integrity: Integrity,
}

/// Why a venue is not part of the consolidated touch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExclusionReason {
    /// Not `Live`. Its contents are retained for debugging and are not a quote.
    Untrusted,
    /// `Live`, but nothing has been applied for longer than
    /// [`CrossPolicy::max_age`].
    Stale { age: Duration },
    /// `Live` and fresh, but holds no levels on either side.
    NoQuote,
}

impl fmt::Display for ExclusionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Untrusted => f.write_str("book is not trusted"),
            Self::Stale { age } => write!(f, "no update for {}ms", age.as_millis()),
            Self::NoQuote => f.write_str("book holds no levels"),
        }
    }
}

/// A venue that was configured for this symbol and did not contribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exclusion {
    pub venue: VenueId,
    pub reason: ExclusionReason,
}

/// The consolidated touch across venues for one symbol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrossVenue {
    /// Highest bid across participating venues.
    pub bid: Option<CrossLeg>,
    /// Lowest ask across participating venues.
    pub ask: Option<CrossLeg>,
    /// `ask - bid`, exactly. **Signed**: negative means the books are crossed.
    pub spread: Option<Decimal>,
    /// The same, in basis points of the consolidated mid. Negative is the
    /// apparent arbitrage — see the module docs on why "apparent" is the whole
    /// of the claim.
    pub spread_bps: Option<Decimal>,
    pub mid: Option<Decimal>,
    /// Weakest guarantee among the legs used. `None` when no leg was used.
    ///
    /// Over the legs, not over the configured venues: this number came from at
    /// most two books, and a desynced third venue neither strengthens nor
    /// weakens it.
    pub integrity_floor: Option<Integrity>,
    /// Age of the older of the two legs — the bound on how simultaneous this
    /// reading is.
    pub oldest_leg_ms: Option<u64>,
    /// Venues that contributed at least one side.
    pub venues_used: usize,
    /// Everyone who did not, and why. Published rather than dropped: a
    /// consolidated touch that quietly narrows to one venue looks exactly like
    /// one drawn from three, and the difference is the whole of its value.
    pub excluded: Vec<Exclusion>,
}

impl CrossVenue {
    /// True when the highest bid is at or above the lowest ask.
    ///
    /// Reported rather than left to a caller comparing `spread_bps` against
    /// zero, so that the boundary case — a bid exactly equal to an ask, which
    /// is a crossing of zero width and still not a normal book — is decided
    /// once, here.
    pub fn is_crossed(&self) -> bool {
        self.spread.is_some_and(|s| s <= Decimal::ZERO)
    }

    /// True when both legs came from the same venue.
    ///
    /// Then this is that venue's own spread wearing a cross-venue label, and
    /// cannot show an arbitrage: a single venue's book that crossed itself
    /// would already have been desynced by
    /// [`DesyncReason::CrossedBook`](crate::DesyncReason::CrossedBook).
    pub fn is_single_venue(&self) -> bool {
        match (self.bid, self.ask) {
            (Some(bid), Some(ask)) => bid.venue == ask.venue,
            _ => false,
        }
    }

    fn empty(excluded: Vec<Exclusion>) -> Self {
        Self {
            bid: None,
            ask: None,
            spread: None,
            spread_bps: None,
            mid: None,
            integrity_floor: None,
            oldest_leg_ms: None,
            venues_used: 0,
            excluded,
        }
    }
}

/// Build the consolidated touch from one `(venue, top of book)` pair per
/// venue tracking a symbol.
///
/// `now` is passed rather than read from a clock so that the staleness rule is
/// testable without sleeping — the same reason
/// [`Book::top_of_book`](crate::Book::top_of_book) takes one.
pub fn consolidate(
    quotes: impl IntoIterator<Item = (VenueId, TopOfBook)>,
    policy: CrossPolicy,
) -> CrossVenue {
    let mut best_bid: Option<CrossLeg> = None;
    let mut best_ask: Option<CrossLeg> = None;
    let mut excluded = Vec::new();
    let mut used = 0usize;

    for (venue, top) in quotes {
        // Trust first. A `Desynced` book still holds its last contents, and
        // those contents are exactly what an unguarded `max` would find.
        let BookState::Live { integrity, .. } = top.state else {
            excluded.push(Exclusion {
                venue,
                reason: ExclusionReason::Untrusted,
            });
            continue;
        };

        // `None` means nothing has ever been applied, which cannot be fresh.
        let age = top.age.unwrap_or(Duration::MAX);
        if age > policy.max_age {
            excluded.push(Exclusion {
                venue,
                reason: ExclusionReason::Stale { age },
            });
            continue;
        }

        if top.bid.is_none() && top.ask.is_none() {
            excluded.push(Exclusion {
                venue,
                reason: ExclusionReason::NoQuote,
            });
            continue;
        }
        used += 1;

        let age_ms = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
        if let Some(level) = top.bid {
            let leg = CrossLeg {
                venue,
                price: level.price,
                qty: level.qty,
                age_ms,
                integrity,
            };
            // Strictly greater, so ties keep the venue seen first. Callers
            // iterate a `BTreeMap`, so "first" is a stable order rather than
            // whichever task happened to publish last — a tie that flapped
            // between two venues every tick would look like the book moving.
            if best_bid.is_none_or(|b| leg.price > b.price) {
                best_bid = Some(leg);
            }
        }
        if let Some(level) = top.ask {
            let leg = CrossLeg {
                venue,
                price: level.price,
                qty: level.qty,
                age_ms,
                integrity,
            };
            if best_ask.is_none_or(|a| leg.price < a.price) {
                best_ask = Some(leg);
            }
        }
    }

    let mut out = CrossVenue::empty(excluded);
    out.bid = best_bid;
    out.ask = best_ask;
    out.venues_used = used;

    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => {
            out.integrity_floor = Some(bid.integrity.min(ask.integrity));
            out.oldest_leg_ms = Some(bid.age_ms.max(ask.age_ms));

            let bid_price = bid.price.as_decimal();
            let ask_price = ask.price.as_decimal();
            let spread = ask_price - bid_price;
            let mid = (ask_price + bid_price) / Decimal::TWO;
            out.spread = Some(spread);
            out.mid = Some(mid);
            if !mid.is_zero() {
                out.spread_bps =
                    Some((spread * Decimal::from(10_000_u32) / mid).round_dp(BPS_SCALE));
            }
        }
        // One-sided: there is a best bid or a best ask but not both, so there
        // is no spread to report. The leg that exists is still published —
        // "the highest bid anyone is showing" is a real answer even when
        // nobody is offering.
        (bid, ask) => {
            out.integrity_floor = bid.map(|l| l.integrity).or(ask.map(|l| l.integrity));
            out.oldest_leg_ms = bid.map(|l| l.age_ms).or(ask.map(|l| l.age_ms));
        }
    }

    out
}

/// Four decimal places, matching [`crate::window`]. A hundredth of a basis
/// point is finer than any of these readings means anything to.
const BPS_SCALE: u32 = 4;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::book::DesyncReason;
    use crate::event::Level;
    use crate::time::{Clock, IngestTime, TestClock};
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn at() -> IngestTime {
        TestClock::new().now()
    }

    fn level(price: &str) -> Level {
        Level::new(Price::from_str(price).unwrap(), Qty::from_str("1").unwrap())
    }

    /// A live two-sided book of the given age and integrity.
    fn quote(bid: &str, ask: &str, age_ms: u64, integrity: Integrity) -> TopOfBook {
        TopOfBook {
            bid: Some(level(bid)),
            ask: Some(level(ask)),
            state: BookState::Live {
                integrity,
                since: at(),
                last_verified: None,
            },
            age: Some(Duration::from_millis(age_ms)),
        }
    }

    fn fresh(bid: &str, ask: &str) -> TopOfBook {
        quote(bid, ask, 10, Integrity::Verified)
    }

    fn desynced(bid: &str, ask: &str) -> TopOfBook {
        TopOfBook {
            state: BookState::Desynced {
                since: at(),
                reason: DesyncReason::SequenceGap {
                    expected: 1,
                    got: 9,
                },
            },
            ..fresh(bid, ask)
        }
    }

    #[test]
    fn the_consolidated_touch_is_the_best_of_each_side_across_venues() {
        let out = consolidate(
            [
                (VenueId::Coinbase, fresh("100", "102")),
                (VenueId::Kraken, fresh("101", "103")),
                (VenueId::Bitstamp, fresh("99", "101")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.bid.unwrap().venue, VenueId::Kraken);
        assert_eq!(out.bid.unwrap().price.to_string(), "101");
        assert_eq!(out.ask.unwrap().venue, VenueId::Bitstamp);
        assert_eq!(out.ask.unwrap().price.to_string(), "101");
        assert_eq!(out.venues_used, 3);
        assert!(out.excluded.is_empty());

        // Bid equals ask: a crossing of zero width, which is still not a
        // normal book. The boundary is decided in `is_crossed`, once.
        assert_eq!(out.spread, Some(Decimal::ZERO));
        assert!(out.is_crossed());
        assert!(!out.is_single_venue());
    }

    #[test]
    fn a_crossed_consolidation_reports_a_negative_spread() {
        let out = consolidate(
            [
                (VenueId::Kraken, fresh("10010", "10020")),
                (VenueId::Coinbase, fresh("9990", "10000")),
            ],
            CrossPolicy::default(),
        );

        // Best bid 10010 (Kraken) above best ask 10000 (Coinbase): 10 wide on
        // a 10005 mid, so just under 10 bps of apparent edge.
        assert_eq!(out.spread, Some(dec("-10")));
        assert_eq!(out.spread_bps, Some(dec("-9.9950")));
        assert!(out.is_crossed());
        assert_eq!(out.bid.unwrap().venue, VenueId::Kraken);
        assert_eq!(out.ask.unwrap().venue, VenueId::Coinbase);
    }

    #[test]
    fn an_untrusted_book_is_not_a_quote() {
        // The failure this rule exists to stop: a desynced book keeps its last
        // contents on purpose, and an unguarded `max` over bids would treat
        // that frozen aggressive bid as a live one and show a permanent
        // arbitrage against every healthy venue beside it.
        let out = consolidate(
            [
                (VenueId::Coinbase, desynced("99999", "100000")),
                (VenueId::Kraken, fresh("100", "102")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.bid.unwrap().venue, VenueId::Kraken);
        assert!(
            !out.is_crossed(),
            "a desynced book manufactured an arbitrage"
        );
        assert_eq!(out.venues_used, 1);
        assert_eq!(
            out.excluded,
            vec![Exclusion {
                venue: VenueId::Coinbase,
                reason: ExclusionReason::Untrusted,
            }]
        );
    }

    #[test]
    fn a_stalled_book_is_excluded_and_says_how_old_it_was() {
        let policy = CrossPolicy {
            max_age: Duration::from_secs(2),
        };
        let out = consolidate(
            [
                // Live, nothing has invalidated it, and five seconds old. Its
                // touch is a quote from a market that has since moved.
                (
                    VenueId::Bitstamp,
                    quote("99999", "100000", 5_000, Integrity::OrderOnly),
                ),
                (VenueId::Kraken, fresh("100", "102")),
            ],
            policy,
        );

        assert_eq!(out.venues_used, 1);
        assert!(!out.is_crossed());
        assert_eq!(
            out.excluded,
            vec![Exclusion {
                venue: VenueId::Bitstamp,
                reason: ExclusionReason::Stale {
                    age: Duration::from_secs(5)
                },
            }]
        );
        assert_eq!(
            out.excluded[0].reason.to_string(),
            "no update for 5000ms",
            "the exclusion must say how stale, or an operator cannot tell a \
             misconfigured guard from a dead feed"
        );
    }

    #[test]
    fn the_integrity_floor_is_taken_over_the_legs_used_not_the_venues_present() {
        // Kraken supplies the bid and is Verified; Bitstamp supplies the ask
        // and is OrderOnly. The derived spread is an OrderOnly-grade number.
        let out = consolidate(
            [
                (
                    VenueId::Kraken,
                    quote("100", "105", 10, Integrity::Verified),
                ),
                (
                    VenueId::Bitstamp,
                    quote("98", "102", 10, Integrity::OrderOnly),
                ),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.bid.unwrap().venue, VenueId::Kraken);
        assert_eq!(out.ask.unwrap().venue, VenueId::Bitstamp);
        assert_eq!(
            out.integrity_floor,
            Some(Integrity::OrderOnly),
            "a spread built on a book that cannot detect a lost message \
             claimed checksum-grade trust"
        );
    }

    #[test]
    fn a_desynced_venue_does_not_weaken_a_floor_it_contributed_nothing_to() {
        // The other half of "over the legs, not over the set": Bitstamp is the
        // weakest venue present and is excluded, so it must not drag the floor
        // of a number derived entirely from Kraken and Coinbase.
        let out = consolidate(
            [
                (
                    VenueId::Kraken,
                    quote("100", "102", 10, Integrity::Verified),
                ),
                (
                    VenueId::Coinbase,
                    quote("99", "101", 10, Integrity::GapDetectable),
                ),
                (VenueId::Bitstamp, desynced("50", "150")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.integrity_floor, Some(Integrity::GapDetectable));
    }

    #[test]
    fn the_older_leg_bounds_the_reading() {
        let out = consolidate(
            [
                (
                    VenueId::Kraken,
                    quote("100", "105", 40, Integrity::Verified),
                ),
                (
                    VenueId::Coinbase,
                    quote("98", "102", 900, Integrity::GapDetectable),
                ),
            ],
            CrossPolicy::default(),
        );

        // Bid from Kraken (40ms), ask from Coinbase (900ms). The claim is only
        // as simultaneous as the worse of the two.
        assert_eq!(out.oldest_leg_ms, Some(900));
    }

    #[test]
    fn both_legs_from_one_venue_is_flagged_as_not_an_arbitrage() {
        let out = consolidate(
            [
                (VenueId::Kraken, fresh("100", "101")),
                (VenueId::Coinbase, fresh("98", "103")),
            ],
            CrossPolicy::default(),
        );

        assert!(out.is_single_venue());
        assert_eq!(out.spread, Some(dec("1")));
        assert!(!out.is_crossed());
    }

    #[test]
    fn no_participating_venue_gives_nothing_rather_than_zero() {
        let out = consolidate(
            [
                (VenueId::Coinbase, desynced("100", "101")),
                (VenueId::Kraken, desynced("100", "101")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.bid, None);
        assert_eq!(out.ask, None);
        assert_eq!(out.spread, None);
        assert_eq!(out.spread_bps, None);
        assert_eq!(out.integrity_floor, None);
        assert!(!out.is_crossed(), "an absent spread read as a crossing");
        assert_eq!(out.excluded.len(), 2);
    }

    #[test]
    fn a_one_sided_venue_still_contributes_the_side_it_has() {
        let one_sided = TopOfBook {
            ask: None,
            ..fresh("105", "0")
        };
        let out = consolidate(
            [
                (VenueId::Coinbase, one_sided),
                (VenueId::Kraken, fresh("100", "102")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.bid.unwrap().venue, VenueId::Coinbase);
        assert_eq!(out.ask.unwrap().venue, VenueId::Kraken);
        assert_eq!(out.venues_used, 2);
    }

    #[test]
    fn a_book_that_has_never_applied_anything_is_not_fresh() {
        // `age: None` means nothing has ever been applied. Treating a missing
        // age as zero would make an empty book the freshest thing in the view.
        let never = TopOfBook {
            age: None,
            ..fresh("99999", "100000")
        };
        let out = consolidate(
            [
                (VenueId::Coinbase, never),
                (VenueId::Kraken, fresh("100", "102")),
            ],
            CrossPolicy::default(),
        );

        assert_eq!(out.venues_used, 1);
        assert_eq!(out.bid.unwrap().venue, VenueId::Kraken);
    }

    #[test]
    fn ties_keep_the_first_venue_so_the_view_does_not_flap() {
        let out = consolidate(
            [
                (VenueId::Bitstamp, fresh("100", "102")),
                (VenueId::Coinbase, fresh("100", "102")),
            ],
            CrossPolicy::default(),
        );
        assert_eq!(out.bid.unwrap().venue, VenueId::Bitstamp);
        assert_eq!(out.ask.unwrap().venue, VenueId::Bitstamp);
    }
}
