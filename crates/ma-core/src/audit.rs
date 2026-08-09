//! Checking a book we built against a snapshot the venue served.
//!
//! # The gap this exists to close
//!
//! Kraken hashes the book we actually built and sends the hash with every
//! message, so a Kraken book is [`Integrity::Verified`](crate::Integrity) —
//! continuously, against the venue's own opinion. The other two venues make no
//! such claim:
//!
//! - **Bitstamp** is `OrderOnly`. A dropped diff leaves no trace at all. The
//!   book is silently wrong from that moment and nothing in the protocol will
//!   ever say so.
//! - **Coinbase** is `GapDetectable`. It notices a *lost message*, but nothing
//!   checks that the messages we did receive were applied correctly. A delta
//!   written to the wrong side, or a level dropped by our own bug, is
//!   invisible.
//!
//! A periodic REST depth fetch is the only independent evidence available for
//! either. This module is the comparison; fetching is `ma-pipeline`'s job.
//!
//! # Why a naive comparison would be worse than none
//!
//! A REST snapshot describes the venue's book at some instant `T`. Our book,
//! by the time the response arrives and is compared, is at `T + δ` and has
//! applied every delta in between. On a liquid pair δ is a hundred
//! milliseconds and the touch moves several times inside it. Comparing the two
//! directly would report a disagreement almost every time, and a check that
//! cries wolf continuously is worse than no check: it trains whoever reads it
//! to ignore the one occasion it is right, and — if it desynced the book — it
//! would produce a permanent reconnect loop against a venue that bans for
//! exactly that.
//!
//! Two properties make the check sound anyway, and they are the whole design:
//!
//! **1. Drift near the touch self-repairs; drift deep in the book does not.**
//! A lost delta leaves one price level wrong. That level stays wrong until
//! something else updates that same price. Near the touch, prices are
//! rewritten constantly, so damage there is erased within seconds. Ten levels
//! out on a major pair, a price can sit untouched for minutes. So the deep
//! book is both where real corruption *accumulates* and where the timing race
//! *doesn't reach* — which is why [`AuditPolicy::guard`] skips the levels
//! nearest the touch rather than trying to compare them cleverly.
//!
//! **2. Genuine drift is permanent; a race is not.** A discrepancy caused by
//! timing is gone by the next fetch, because the book has moved on. A
//! discrepancy caused by a lost message is still there, at the same price,
//! forever. So a single disagreement proves nothing and consecutive
//! disagreements prove a great deal — see
//! [`AuditPolicy::consecutive_before_desync`].
//!
//! Together these make the audit **advisory first and authoritative second**:
//! every comparison feeds a counter, which is the honest primary output, and
//! only a repeated finding is allowed to declare the book untrustworthy.

use std::collections::BTreeMap;

use crate::book::{Book, DesyncReason};
use crate::event::{Level, Side};
use crate::price::{Price, Qty};

/// How strict an audit is, and how much evidence it needs before it acts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditPolicy {
    /// Levels nearest the touch to ignore, per side.
    ///
    /// These are the levels the fetch/apply race actually reaches. Skipping
    /// them removes nearly every false positive and costs nearly no real
    /// coverage, because damage this close to the touch is overwritten within
    /// seconds anyway — see the module docs.
    pub guard: usize,
    /// Levels per side to consider in total, including the guarded ones.
    ///
    /// The compared window is therefore `guard..depth`. Bounded because a
    /// full-book comparison on Coinbase would be tens of thousands of levels
    /// four times a minute, and the deep tail adds no signal the near-deep
    /// levels do not already carry.
    pub depth: usize,
    /// How many audits in a row must find a disagreement before the book is
    /// declared untrustworthy.
    ///
    /// `1` would act on a single finding, which the module docs argue is not
    /// evidence. The default is `2`: a race resolves by the next fetch, real
    /// drift does not.
    pub consecutive_before_desync: u32,
}

impl AuditPolicy {
    pub const DEFAULT: Self = Self {
        guard: 5,
        depth: 50,
        consecutive_before_desync: 2,
    };

    /// The window actually compared, as `(skip, take)`.
    const fn window(&self) -> (usize, usize) {
        (self.guard, self.depth.saturating_sub(self.guard))
    }
}

impl Default for AuditPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One concrete disagreement, named precisely enough to debug from a log line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditFinding {
    pub side: Side,
    pub price: Price,
    /// What our book holds at that price. `None` means we have no such level
    /// and the venue does.
    pub ours: Option<Qty>,
    /// What the venue's snapshot holds. `None` means we hold a level the venue
    /// does not — a delete we never applied.
    pub theirs: Option<Qty>,
}

/// What one comparison concluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    /// Nothing comparable. Either book was too shallow to reach past the
    /// guard, or their price ranges did not overlap — which happens on a
    /// genuinely fast-moving book and is not evidence of anything.
    Inconclusive,
    /// Every compared level agreed.
    Match { compared: usize },
    /// At least one level disagreed. The first finding, in book order, is
    /// carried; there is no value in listing all of them, because one wrong
    /// level already means the book cannot be trusted.
    Mismatch {
        finding: AuditFinding,
        compared: usize,
    },
}

impl AuditOutcome {
    pub const fn is_mismatch(&self) -> bool {
        matches!(self, Self::Mismatch { .. })
    }

    /// How many levels the comparison actually looked at.
    pub const fn compared(&self) -> usize {
        match self {
            Self::Inconclusive => 0,
            Self::Match { compared } | Self::Mismatch { compared, .. } => *compared,
        }
    }
}

/// Compare a book against a venue-served snapshot under `policy`.
///
/// Comparison is **by price, not by index**. Indexing would misalign the two
/// windows the moment one side holds a level the other does not, and then
/// report every remaining level as wrong — turning one real discrepancy into
/// forty-five spurious ones, and burying the actual price under them.
pub fn audit(
    book: &Book,
    snapshot_bids: &[Level],
    snapshot_asks: &[Level],
    policy: AuditPolicy,
) -> AuditOutcome {
    let mut compared = 0;
    let mut first: Option<AuditFinding> = None;

    for (side, theirs) in [(Side::Bid, snapshot_bids), (Side::Ask, snapshot_asks)] {
        let (outcome, n) = audit_side(book, side, theirs, policy);
        compared += n;
        if first.is_none() {
            first = outcome;
        }
    }

    match (first, compared) {
        (Some(finding), _) => AuditOutcome::Mismatch { finding, compared },
        (None, 0) => AuditOutcome::Inconclusive,
        (None, _) => AuditOutcome::Match { compared },
    }
}

fn audit_side(
    book: &Book,
    side: Side,
    theirs: &[Level],
    policy: AuditPolicy,
) -> (Option<AuditFinding>, usize) {
    let (skip, take) = policy.window();

    let ours: BTreeMap<Price, Qty> = book
        .top_levels(side, policy.depth)
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|l| (l.price, l.qty))
        .collect();

    // The venue's own ordering is not guaranteed to be ours, so sort into
    // best-first order before applying the same window.
    let mut sorted: Vec<Level> = theirs
        .iter()
        .copied()
        .filter(|l| !l.qty.is_delete())
        .collect();
    match side {
        Side::Bid => sorted.sort_by(|a, b| b.price.cmp(&a.price)),
        Side::Ask => sorted.sort_by(|a, b| a.price.cmp(&b.price)),
    }
    let theirs: BTreeMap<Price, Qty> = sorted
        .into_iter()
        .skip(skip)
        .take(take)
        .map(|l| (l.price, l.qty))
        .collect();

    if ours.is_empty() || theirs.is_empty() {
        return (None, 0);
    }

    // Only the price range both windows cover can be compared. Outside it, one
    // side simply looked further into the book than the other, which says
    // nothing about agreement. Restricting to the overlap is what keeps a
    // shallower REST response from reading as forty missing levels.
    //
    // `unwrap_or` is unreachable: both maps were just checked non-empty.
    let lo = ours
        .keys()
        .next()
        .copied()
        .max(theirs.keys().next().copied())
        .unwrap_or_else(|| Price::from_decimal(rust_decimal::Decimal::ZERO));
    let hi = ours
        .keys()
        .next_back()
        .copied()
        .min(theirs.keys().next_back().copied())
        .unwrap_or_else(|| Price::from_decimal(rust_decimal::Decimal::ZERO));
    if lo > hi {
        return (None, 0);
    }

    let mut compared = 0;
    let mut finding = None;

    let prices: std::collections::BTreeSet<Price> = ours
        .range(lo..=hi)
        .map(|(p, _)| *p)
        .chain(theirs.range(lo..=hi).map(|(p, _)| *p))
        .collect();

    for price in prices {
        compared += 1;
        let ours_qty = ours.get(&price).copied();
        let theirs_qty = theirs.get(&price).copied();
        if ours_qty != theirs_qty && finding.is_none() {
            finding = Some(AuditFinding {
                side,
                price,
                ours: ours_qty,
                theirs: theirs_qty,
            });
        }
    }

    (finding, compared)
}

/// Tracks consecutive findings so a single race cannot desync a book.
///
/// See the module docs: one disagreement is not evidence, because the fetch
/// races the stream. Two in a row is, because a race would have resolved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuditTrail {
    consecutive: u32,
    /// Lifetime totals, for the metrics that are the audit's primary output.
    pub audits: u64,
    pub mismatches: u64,
}

impl AuditTrail {
    /// Fold in one outcome, returning a desync reason if the evidence is now
    /// strong enough to distrust the book.
    pub fn observe(&mut self, outcome: AuditOutcome, policy: AuditPolicy) -> Option<DesyncReason> {
        match outcome {
            // An inconclusive audit is not a clean bill of health, so it must
            // not reset the streak — otherwise a book could alternate
            // mismatch/inconclusive forever and never accumulate two in a row.
            // It is not evidence against the book either, so it is not counted
            // as a mismatch.
            AuditOutcome::Inconclusive => {
                self.audits += 1;
                None
            }
            AuditOutcome::Match { .. } => {
                self.audits += 1;
                self.consecutive = 0;
                None
            }
            AuditOutcome::Mismatch { finding, .. } => {
                self.audits += 1;
                self.mismatches += 1;
                self.consecutive += 1;
                (self.consecutive >= policy.consecutive_before_desync).then_some(
                    DesyncReason::AuditMismatch {
                        price: finding.price,
                        consecutive: self.consecutive,
                    },
                )
            }
        }
    }

    /// Forget the streak. Called on reconnect: the book that disagreed no
    /// longer exists, so the evidence against it does not carry over.
    pub fn reset(&mut self) {
        self.consecutive = 0;
    }

    pub const fn consecutive(&self) -> u32 {
        self.consecutive
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::book::Integrity;
    use crate::event::{Symbol, VenueId};
    use crate::time::{Clock, TestClock};

    fn lv(price: &str, qty: &str) -> Level {
        Level::new(price.parse().unwrap(), qty.parse().unwrap())
    }

    /// A book with `n` levels a side, one unit apart, centred on 1000/1001.
    fn ladder(n: usize) -> (Vec<Level>, Vec<Level>) {
        let bids = (0..n)
            .map(|i| lv(&format!("{}", 1000 - i as i64), "1"))
            .collect();
        let asks = (0..n)
            .map(|i| lv(&format!("{}", 1001 + i as i64), "1"))
            .collect();
        (bids, asks)
    }

    fn book_of(bids: &[Level], asks: &[Level]) -> Book {
        let mut b = Book::new(VenueId::Fake, Symbol::new("BTC-USD"));
        b.apply_snapshot(bids, asks, Integrity::OrderOnly, TestClock::new().now())
            .unwrap();
        b
    }

    fn policy() -> AuditPolicy {
        AuditPolicy {
            guard: 2,
            depth: 10,
            consecutive_before_desync: 2,
        }
    }

    #[test]
    fn an_identical_book_matches() {
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);
        let outcome = audit(&book, &bids, &asks, policy());
        assert!(
            matches!(outcome, AuditOutcome::Match { .. }),
            "got {outcome:?}"
        );
        assert!(outcome.compared() > 0, "nothing was actually compared");
    }

    #[test]
    fn a_wrong_quantity_outside_the_guard_is_caught() {
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);

        // Level index 4 on the bid side: outside a guard of 2.
        let mut theirs = bids.clone();
        theirs[4] = lv("996", "7");

        match audit(&book, &theirs, &asks, policy()) {
            AuditOutcome::Mismatch { finding, .. } => {
                assert_eq!(finding.side, Side::Bid);
                assert_eq!(finding.price, "996".parse().unwrap());
                assert_eq!(finding.ours, Some("1".parse().unwrap()));
                assert_eq!(finding.theirs, Some("7".parse().unwrap()));
            }
            other => panic!("expected a mismatch, got {other:?}"),
        }
    }

    #[test]
    fn disagreement_inside_the_guard_band_is_deliberately_ignored() {
        // The levels nearest the touch are where the fetch races the stream,
        // and where real damage is overwritten within seconds anyway. Flagging
        // them would make the audit cry wolf on every liquid pair.
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);

        let mut theirs = bids.clone();
        theirs[0] = lv("1000", "99");
        theirs[1] = lv("999", "99");

        assert!(
            matches!(
                audit(&book, &theirs, &asks, policy()),
                AuditOutcome::Match { .. }
            ),
            "the guard band did not absorb a near-touch difference"
        );
    }

    #[test]
    fn a_level_we_hold_that_the_venue_does_not_is_a_mismatch() {
        // The signature of a delete we never applied — the exact damage an
        // OrderOnly venue cannot otherwise reveal.
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);

        let theirs: Vec<Level> = bids
            .iter()
            .copied()
            .filter(|l| l.price != "996".parse().unwrap())
            .collect();

        match audit(&book, &theirs, &asks, policy()) {
            AuditOutcome::Mismatch { finding, .. } => {
                assert_eq!(finding.price, "996".parse().unwrap());
                assert!(finding.ours.is_some());
                assert!(finding.theirs.is_none(), "we should hold a phantom level");
            }
            other => panic!("expected a mismatch, got {other:?}"),
        }
    }

    #[test]
    fn comparison_is_by_price_so_one_extra_level_is_one_finding() {
        // Indexing would misalign both windows after the inserted level and
        // report every subsequent level as wrong. One real discrepancy must
        // not become forty-five, or the log buries the price that matters.
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);

        let mut theirs = bids.clone();
        theirs.insert(4, lv("996.5", "2"));

        let outcome = audit(&book, &theirs, &asks, policy());
        assert!(outcome.is_mismatch());

        // Count how many prices actually disagree, rather than trusting the
        // first finding alone.
        let disagreements = {
            let ours: BTreeMap<Price, Qty> = book
                .top_levels(Side::Bid, 10)
                .into_iter()
                .skip(2)
                .map(|l| (l.price, l.qty))
                .collect();
            let mut sorted = theirs.clone();
            sorted.sort_by(|a, b| b.price.cmp(&a.price));
            let t: BTreeMap<Price, Qty> = sorted
                .into_iter()
                .skip(2)
                .take(8)
                .map(|l| (l.price, l.qty))
                .collect();
            let lo = *ours.keys().next().unwrap().max(t.keys().next().unwrap());
            let hi = *ours
                .keys()
                .next_back()
                .unwrap()
                .min(t.keys().next_back().unwrap());
            ours.range(lo..=hi)
                .map(|(p, _)| *p)
                .chain(t.range(lo..=hi).map(|(p, _)| *p))
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .filter(|p| ours.get(p) != t.get(p))
                .count()
        };
        assert_eq!(
            disagreements, 1,
            "an inserted level misaligned the comparison"
        );
    }

    #[test]
    fn a_book_too_shallow_to_reach_past_the_guard_is_inconclusive() {
        // Not a match. Reporting "everything agreed" after comparing nothing
        // would be the audit lying in the most comfortable direction.
        let (bids, asks) = ladder(2);
        let book = book_of(&bids, &asks);
        assert_eq!(
            audit(&book, &bids, &asks, policy()),
            AuditOutcome::Inconclusive
        );
    }

    #[test]
    fn a_shallower_venue_response_compares_only_the_overlap() {
        // The venue answering with 6 levels when we hold 10 is not evidence
        // that we invented 4 — it is evidence that we asked for less depth.
        let (bids, asks) = ladder(10);
        let book = book_of(&bids, &asks);

        let short_bids: Vec<Level> = bids.iter().copied().take(6).collect();
        let short_asks: Vec<Level> = asks.iter().copied().take(6).collect();

        let outcome = audit(&book, &short_bids, &short_asks, policy());
        assert!(
            matches!(outcome, AuditOutcome::Match { .. }),
            "a shallower response read as a disagreement: {outcome:?}"
        );
    }

    #[test]
    fn one_finding_is_not_enough_to_desync_but_two_in_a_row_are() {
        // The core rule. A disagreement caused by the fetch racing the stream
        // is gone by the next fetch; one caused by a lost message is still
        // there, at the same price, forever.
        let mut trail = AuditTrail::default();
        let finding = AuditFinding {
            side: Side::Bid,
            price: "996".parse().unwrap(),
            ours: Some("1".parse().unwrap()),
            theirs: Some("7".parse().unwrap()),
        };
        let mismatch = AuditOutcome::Mismatch {
            finding,
            compared: 8,
        };

        assert_eq!(trail.observe(mismatch, policy()), None, "acted on one race");
        match trail.observe(mismatch, policy()) {
            Some(DesyncReason::AuditMismatch { price, consecutive }) => {
                assert_eq!(price, "996".parse().unwrap());
                assert_eq!(consecutive, 2);
            }
            other => panic!("persistent drift went unreported: {other:?}"),
        }
        assert_eq!(trail.mismatches, 2);
        assert_eq!(trail.audits, 2);
    }

    #[test]
    fn a_clean_audit_clears_the_streak() {
        // Which is exactly what a timing race produces: disagree, then agree.
        let mut trail = AuditTrail::default();
        let mismatch = AuditOutcome::Mismatch {
            finding: AuditFinding {
                side: Side::Bid,
                price: "996".parse().unwrap(),
                ours: None,
                theirs: Some("1".parse().unwrap()),
            },
            compared: 8,
        };

        assert_eq!(trail.observe(mismatch, policy()), None);
        assert_eq!(
            trail.observe(AuditOutcome::Match { compared: 8 }, policy()),
            None
        );
        assert_eq!(trail.consecutive(), 0);
        // A third audit disagreeing is now the *first* of a new streak, not
        // the second of the old one.
        assert_eq!(trail.observe(mismatch, policy()), None);
        assert_eq!(trail.audits, 3);
        assert_eq!(trail.mismatches, 2);
    }

    #[test]
    fn an_inconclusive_audit_does_not_launder_a_streak() {
        // If Inconclusive reset the counter, a book could alternate
        // mismatch/inconclusive indefinitely and never reach two in a row —
        // genuine drift would go unreported forever, while every individual
        // reading looked defensible.
        let mut trail = AuditTrail::default();
        let mismatch = AuditOutcome::Mismatch {
            finding: AuditFinding {
                side: Side::Ask,
                price: "1005".parse().unwrap(),
                ours: Some("1".parse().unwrap()),
                theirs: None,
            },
            compared: 8,
        };

        assert_eq!(trail.observe(mismatch, policy()), None);
        assert_eq!(trail.observe(AuditOutcome::Inconclusive, policy()), None);
        assert!(
            trail.observe(mismatch, policy()).is_some(),
            "an inconclusive reading cleared evidence it had no bearing on"
        );
        assert_eq!(
            trail.mismatches, 2,
            "an inconclusive audit must not count as a mismatch"
        );
    }

    #[test]
    fn a_reconnect_forgets_the_evidence() {
        // The book that disagreed no longer exists after a resync, so the case
        // against it does not carry over to its replacement.
        let mut trail = AuditTrail::default();
        let mismatch = AuditOutcome::Mismatch {
            finding: AuditFinding {
                side: Side::Bid,
                price: "996".parse().unwrap(),
                ours: None,
                theirs: None,
            },
            compared: 8,
        };
        trail.observe(mismatch, policy());
        trail.reset();
        assert_eq!(trail.consecutive(), 0);
        assert_eq!(
            trail.observe(mismatch, policy()),
            None,
            "a fresh book was desynced by its predecessor's evidence"
        );
    }
}
