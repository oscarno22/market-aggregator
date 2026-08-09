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
//! rewritten constantly, so damage there is erased within seconds. Far enough
//! out, a price can sit untouched for minutes. So the deep book is both where
//! real corruption *accumulates* and where the timing race *doesn't reach* —
//! which is why [`AuditPolicy::guard_bps`] excludes the region nearest the
//! touch rather than trying to compare it cleverly.
//!
//! ## "Deep" means price distance, not level count — measured, not assumed
//!
//! The first version of this guard counted *levels*, and it was wrong in a way
//! only live data revealed: it declared every Coinbase and Bitstamp book
//! untrustworthy within two minutes of going live.
//!
//! On a dense book with a small tick, a level count is not a distance. The top
//! fifty levels of Coinbase BTC-USD span **2.4 basis points** — about $15 on a
//! $64,000 book — and five levels in is 0.2 bps. The touch moves further than
//! that during the REST round trip, so a level-counted guard of five was
//! entirely inside the churn it was meant to exclude.
//!
//! The numbers that settled it, taken against the live venues:
//!
//! | Measurement | Result |
//! |---|---|
//! | Two REST fetches 150 ms apart, 5–100 bps band | 0 of 668 levels disagreed |
//! | Our websocket book vs REST, ask side, top 12 | 0 of 12 disagreed |
//! | Our websocket book vs REST, bid side, top 12 | 1 disagreed — a level 0.2 bps out |
//!
//! So the book was never wrong; the window was. Beyond a few basis points the
//! book is genuinely quiet, and that is where the comparison belongs. The guard
//! is therefore expressed in basis points from the touch, and the REST requests
//! ask for enough depth to reach past it.
//!
//! **2. Genuine drift is permanent *and stays at one price*; a race is not.**
//! This is the property that actually does the work, and the first
//! implementation used a weaker version of it that live data disproved.
//!
//! Requiring merely "two mismatching audits in a row" is not enough, because
//! *some* level disagreeing is the normal state of affairs. The measured churn
//! is ~2% of levels in the 1–10 bps band, and an audit comparing several
//! hundred levels will therefore find a disagreement somewhere nearly every
//! time — at a **different price each time**. Two such audits in a row prove
//! nothing at all, and the first version desynced every book on schedule
//! because of it.
//!
//! A lost delta is different in kind: it corrupts *one specific price*, and
//! that price stays wrong until something rewrites it. So the test is not "did
//! two audits disagree" but **"did two consecutive audits disagree about the
//! same price"**. Noise moves; damage does not. [`AuditTrail`] intersects the
//! finding sets of successive audits and acts only on a survivor.
//!
//! Together these make the audit **advisory first and authoritative second**:
//! every comparison feeds a counter, which is the honest primary output, and
//! only a repeated finding is allowed to declare the book untrustworthy.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::book::{Book, DesyncReason};
use crate::event::{Level, Side};
use crate::price::{Price, Qty};

/// How strict an audit is, and how much evidence it needs before it acts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditPolicy {
    /// Ignore levels within this many **basis points** of the touch.
    ///
    /// Basis points rather than a level count, because on a dense book those
    /// are wildly different quantities — see the module docs. `10` puts the
    /// compared region an order of magnitude beyond the churn measured against
    /// the live venues, while still leaving hundreds of levels to compare.
    pub guard_bps: u32,
    /// Cap on levels compared per side, counted from the guard outwards.
    ///
    /// Pure cost control: Bitstamp's REST returns its entire book, which is
    /// thousands of levels, and the far tail adds no signal the near-deep
    /// levels do not already carry.
    pub max_levels: usize,
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
        guard_bps: 10,
        max_levels: 500,
        consecutive_before_desync: 2,
    };
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

/// How many disagreeing prices one audit will carry forward.
///
/// Bounded because the interesting question is which prices *recur*, and a
/// wholesale disagreement — a genuinely broken book — is answered by the first
/// handful as well as by ten thousand.
pub const MAX_FINDINGS: usize = 64;

/// What one comparison concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    /// Nothing comparable. Either book was too shallow to reach past the
    /// guard, or their price ranges did not overlap — which happens on a
    /// genuinely fast-moving book and is not evidence of anything.
    Inconclusive,
    /// Every compared level agreed.
    Match { compared: usize },
    /// Some levels disagreed.
    ///
    /// **All** of them are carried, up to [`MAX_FINDINGS`], rather than just
    /// the first. That is what lets [`AuditTrail`] ask the question that
    /// matters — whether the *same price* is still wrong next time — instead of
    /// the much weaker "did something disagree again".
    Mismatch {
        findings: Vec<AuditFinding>,
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

    /// The disagreeing prices, as a set.
    fn prices(&self) -> BTreeSet<Price> {
        match self {
            Self::Mismatch { findings, .. } => findings.iter().map(|f| f.price).collect(),
            _ => BTreeSet::new(),
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
    let mut findings = Vec::new();

    for (side, theirs) in [(Side::Bid, snapshot_bids), (Side::Ask, snapshot_asks)] {
        let (mut found, n) = audit_side(book, side, theirs, policy);
        compared += n;
        findings.append(&mut found);
    }
    findings.truncate(MAX_FINDINGS);

    match (findings.is_empty(), compared) {
        (false, _) => AuditOutcome::Mismatch { findings, compared },
        (true, 0) => AuditOutcome::Inconclusive,
        (true, _) => AuditOutcome::Match { compared },
    }
}

/// The price at which the guard band ends, on one side.
///
/// Anchored on **our** best level, so both windows are cut at the same absolute
/// price and the comparison stays apples-to-apples. Anchoring each book on its
/// own touch would shift the two windows relative to one another exactly when
/// the books disagree about the touch — which is when the audit matters most.
fn guard_edge(best: Price, side: Side, guard_bps: u32) -> Price {
    let best = best.as_decimal();
    let offset = best * Decimal::from(guard_bps) / Decimal::from(10_000_u32);
    Price::from_decimal(match side {
        Side::Bid => best - offset,
        Side::Ask => best + offset,
    })
}

fn audit_side(
    book: &Book,
    side: Side,
    theirs: &[Level],
    policy: AuditPolicy,
) -> (Vec<AuditFinding>, usize) {
    let Some(best) = book.best(side) else {
        return (Vec::new(), 0);
    };
    let edge = guard_edge(best.price, side, policy.guard_bps);
    let beyond = |price: Price| match side {
        Side::Bid => price <= edge,
        Side::Ask => price >= edge,
    };

    let ours: BTreeMap<Price, Qty> = book
        .top_levels(side, usize::MAX)
        .into_iter()
        .filter(|l| beyond(l.price))
        .take(policy.max_levels)
        .map(|l| (l.price, l.qty))
        .collect();

    // The venue's own ordering is not guaranteed to be ours, so sort into
    // best-first order before applying the same band.
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
        .filter(|l| beyond(l.price))
        .take(policy.max_levels)
        .map(|l| (l.price, l.qty))
        .collect();

    if ours.is_empty() || theirs.is_empty() {
        return (Vec::new(), 0);
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
        return (Vec::new(), 0);
    }

    let mut compared = 0;
    let mut findings = Vec::new();

    let prices: BTreeSet<Price> = ours
        .range(lo..=hi)
        .map(|(p, _)| *p)
        .chain(theirs.range(lo..=hi).map(|(p, _)| *p))
        .collect();

    for price in prices {
        compared += 1;
        let ours_qty = ours.get(&price).copied();
        let theirs_qty = theirs.get(&price).copied();
        if ours_qty != theirs_qty && findings.len() < MAX_FINDINGS {
            findings.push(AuditFinding {
                side,
                price,
                ours: ours_qty,
                theirs: theirs_qty,
            });
        }
    }

    (findings, compared)
}

/// Tracks consecutive findings so a single race cannot desync a book.
///
/// See the module docs: one disagreement is not evidence, because the fetch
/// races the stream. Two in a row is, because a race would have resolved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditTrail {
    consecutive: u32,
    /// Prices that have disagreed on every audit of the current streak.
    ///
    /// Intersected rather than replaced, which is the whole mechanism: churn
    /// picks a different price each time and empties this set, while a lost
    /// delta keeps the same price in it indefinitely.
    suspect: BTreeSet<Price>,
    /// Lifetime totals, for the metrics that are the audit's primary output.
    pub audits: u64,
    pub mismatches: u64,
}

impl AuditTrail {
    /// Fold in one outcome, returning a desync reason if the evidence is now
    /// strong enough to distrust the book.
    pub fn observe(&mut self, outcome: &AuditOutcome, policy: AuditPolicy) -> Option<DesyncReason> {
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
                self.suspect.clear();
                None
            }
            AuditOutcome::Mismatch { .. } => {
                self.audits += 1;
                self.mismatches += 1;

                let now = outcome.prices();
                if self.consecutive == 0 {
                    self.suspect = now;
                    self.consecutive = 1;
                    return None;
                }

                // Only prices wrong in *both* audits survive. A book whose
                // disagreements move around — which is what the fetch racing
                // the stream produces — empties this set and starts over.
                self.suspect.retain(|p| now.contains(p));
                if self.suspect.is_empty() {
                    self.suspect = now;
                    self.consecutive = 1;
                    return None;
                }

                self.consecutive += 1;
                if self.consecutive < policy.consecutive_before_desync {
                    return None;
                }
                self.suspect
                    .iter()
                    .next()
                    .map(|price| DesyncReason::AuditMismatch {
                        price: *price,
                        consecutive: self.consecutive,
                    })
            }
        }
    }

    /// Forget the streak. Called on reconnect: the book that disagreed no
    /// longer exists, so the evidence against it does not carry over.
    pub fn reset(&mut self) {
        self.consecutive = 0;
        self.suspect.clear();
    }

    /// Prices currently wrong on every audit of the streak. The thing to look
    /// at first when a book is desynced by an audit.
    pub fn suspect_prices(&self) -> impl Iterator<Item = &Price> {
        self.suspect.iter()
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
    ///
    /// One unit is 10 bps of 1000, so with `guard_bps = 20` the first two
    /// levels a side fall inside the guard and the rest are compared. Chosen so
    /// the arithmetic is legible rather than realistic: a real book is far
    /// denser, which is the whole reason the guard is a distance and not a
    /// level count.
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

    /// 20 bps of a 1000 book is 2 units, so levels 1000 and 999 (and 1001,
    /// 1002) sit inside the guard and everything further out is compared.
    fn policy() -> AuditPolicy {
        AuditPolicy {
            guard_bps: 20,
            max_levels: 8,
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

        // 996 is 40 bps below the touch, outside the 20 bps guard.
        let mut theirs = bids.clone();
        theirs[4] = lv("996", "7");

        match audit(&book, &theirs, &asks, policy()) {
            AuditOutcome::Mismatch { findings, .. } => {
                assert_eq!(findings.len(), 1, "{findings:?}");
                assert_eq!(findings[0].side, Side::Bid);
                assert_eq!(findings[0].price, "996".parse().unwrap());
                assert_eq!(findings[0].ours, Some("1".parse().unwrap()));
                assert_eq!(findings[0].theirs, Some("7".parse().unwrap()));
            }
            other => panic!("expected a mismatch, got {other:?}"),
        }
    }

    #[test]
    fn disagreement_inside_the_guard_band_is_deliberately_ignored() {
        // The levels nearest the touch are where the fetch races the stream,
        // and where real damage is overwritten within seconds anyway. Flagging
        // them would make the audit cry wolf on every liquid pair — which is
        // exactly what a level-counted guard did against the live venues.
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
    fn the_guard_is_a_price_distance_not_a_level_count() {
        // The bug live data found. On a dense book a level count is not a
        // distance: fifty levels of Coinbase BTC-USD span 2.4 bps, so a guard
        // of "five levels" sat entirely inside the churn it was meant to
        // exclude and every audit mismatched.
        //
        // A dense book here: 200 levels a penny apart on a 1000 book, so 200
        // levels span only 20 bps. A 10 bps guard must therefore exclude
        // roughly the first hundred *levels* — something no fixed level count
        // could express, because the same guard on the sparse ladder above
        // excludes one.
        let dense_bids: Vec<Level> = (0..200)
            .map(|i| lv(&format!("{:.2}", 1000.0 - f64::from(i) * 0.01), "1"))
            .collect();
        let dense_asks: Vec<Level> = (0..200)
            .map(|i| lv(&format!("{:.2}", 1000.01 + f64::from(i) * 0.01), "1"))
            .collect();
        let book = book_of(&dense_bids, &dense_asks);

        let policy = AuditPolicy {
            guard_bps: 10,
            max_levels: 500,
            consecutive_before_desync: 2,
        };

        // 10 bps of 1000 is 1.00, i.e. 100 penny levels. Corrupting level 50 —
        // well inside that — must be ignored...
        let mut near = dense_bids.clone();
        near[50] = lv(&format!("{:.2}", 1000.0 - 0.50), "99");
        assert!(
            !audit(&book, &near, &dense_asks, policy).is_mismatch(),
            "a difference 5 bps out was flagged despite a 10 bps guard"
        );

        // ...while corrupting level 150, which is 15 bps out, must be caught.
        let mut far = dense_bids.clone();
        far[150] = lv(&format!("{:.2}", 1000.0 - 1.50), "99");
        match audit(&book, &far, &dense_asks, policy) {
            AuditOutcome::Mismatch { findings, .. } => {
                assert_eq!(findings[0].price, "998.50".parse().unwrap());
            }
            other => panic!("a difference 15 bps out went unnoticed: {other:?}"),
        }
    }

    #[test]
    fn the_guard_edge_is_anchored_on_our_own_touch() {
        // Both windows must be cut at the same absolute price. Anchoring each
        // book on its own touch would slide the two windows relative to each
        // other exactly when the books disagree about the touch — which is
        // when the audit matters most.
        let edge = guard_edge("1000".parse().unwrap(), Side::Bid, 10);
        assert_eq!(edge, "999".parse().unwrap());
        let edge = guard_edge("1000".parse().unwrap(), Side::Ask, 10);
        assert_eq!(edge, "1001".parse().unwrap());
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
            AuditOutcome::Mismatch { findings, .. } => {
                assert_eq!(findings[0].price, "996".parse().unwrap());
                assert!(findings[0].ours.is_some());
                assert!(
                    findings[0].theirs.is_none(),
                    "we should hold a phantom level"
                );
            }
            other => panic!("expected a mismatch, got {other:?}"),
        }
    }

    #[test]
    #[allow(clippy::float_arithmetic)]
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
        // first finding alone. An index-based comparison would report every
        // level after the insertion as wrong.
        let ours: BTreeMap<Price, Qty> = book
            .top_levels(Side::Bid, usize::MAX)
            .into_iter()
            .filter(|l| l.price <= guard_edge("1000".parse().unwrap(), Side::Bid, 20))
            .map(|l| (l.price, l.qty))
            .collect();
        let mut sorted = theirs.clone();
        sorted.sort_by(|a, b| b.price.cmp(&a.price));
        let t: BTreeMap<Price, Qty> = sorted
            .into_iter()
            .filter(|l| l.price <= guard_edge("1000".parse().unwrap(), Side::Bid, 20))
            .map(|l| (l.price, l.qty))
            .collect();
        let lo = *ours.keys().next().unwrap().max(t.keys().next().unwrap());
        let hi = *ours
            .keys()
            .next_back()
            .unwrap()
            .min(t.keys().next_back().unwrap());
        let disagreements = ours
            .range(lo..=hi)
            .map(|(p, _)| *p)
            .chain(t.range(lo..=hi).map(|(p, _)| *p))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|p| ours.get(p) != t.get(p))
            .count();
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

    fn mismatch_at(prices: &[&str]) -> AuditOutcome {
        AuditOutcome::Mismatch {
            findings: prices
                .iter()
                .map(|p| AuditFinding {
                    side: Side::Bid,
                    price: p.parse().unwrap(),
                    ours: Some("1".parse().unwrap()),
                    theirs: Some("7".parse().unwrap()),
                })
                .collect(),
            compared: 400,
        }
    }

    #[test]
    fn the_same_price_wrong_twice_running_is_what_desyncs_a_book() {
        // The rule the live venues forced. A lost delta corrupts one price and
        // that price stays wrong; nothing else about "a mismatch happened"
        // carries information, because on a real book something is almost
        // always mid-flight somewhere.
        let mut trail = AuditTrail::default();
        assert_eq!(
            trail.observe(&mismatch_at(&["996"]), policy()),
            None,
            "acted on a single observation"
        );
        match trail.observe(&mismatch_at(&["996", "997"]), policy()) {
            Some(DesyncReason::AuditMismatch { price, consecutive }) => {
                assert_eq!(price, "996".parse().unwrap());
                assert_eq!(consecutive, 2);
            }
            other => panic!("persistent drift at one price went unreported: {other:?}"),
        }
    }

    #[test]
    fn disagreements_that_move_around_never_desync_anything() {
        // This is what the fetch racing the stream actually looks like, and it
        // is why "two mismatching audits in a row" was not a sufficient rule:
        // against the live venues *some* level disagreed almost every time, so
        // that rule desynced every book on schedule. A different price each
        // time is noise, however many times in a row it happens.
        let mut trail = AuditTrail::default();
        for prices in [
            &["996"][..],
            &["1002"][..],
            &["994", "993"][..],
            &["1010"][..],
            &["991"][..],
            &["1004"][..],
        ] {
            assert_eq!(
                trail.observe(&mismatch_at(prices), policy()),
                None,
                "churn at {prices:?} was mistaken for drift"
            );
        }
        assert_eq!(trail.mismatches, 6, "every finding is still counted");
        assert_eq!(
            trail.suspect_prices().count(),
            1,
            "only the newest observation should be retained"
        );
    }

    #[test]
    fn one_persistent_price_is_found_underneath_moving_noise() {
        // The realistic case: a genuinely lost delta at 996, plus a different
        // racing level on each audit. The intersection strips the noise and
        // leaves the real one.
        let mut trail = AuditTrail::default();
        assert_eq!(
            trail.observe(&mismatch_at(&["996", "1002"]), policy()),
            None
        );
        match trail.observe(&mismatch_at(&["996", "1011"]), policy()) {
            Some(DesyncReason::AuditMismatch { price, .. }) => {
                assert_eq!(price, "996".parse().unwrap(), "the wrong price was blamed");
            }
            other => panic!("a persistent price hidden under churn was missed: {other:?}"),
        }
    }

    #[test]
    fn a_clean_audit_clears_the_streak() {
        let mut trail = AuditTrail::default();
        assert_eq!(trail.observe(&mismatch_at(&["996"]), policy()), None);
        assert_eq!(
            trail.observe(&AuditOutcome::Match { compared: 400 }, policy()),
            None
        );
        assert_eq!(trail.consecutive(), 0);
        assert_eq!(trail.suspect_prices().count(), 0);
        assert_eq!(
            trail.observe(&mismatch_at(&["996"]), policy()),
            None,
            "a clean audit in between must break the chain"
        );
        assert_eq!(trail.audits, 3);
        assert_eq!(trail.mismatches, 2);
    }

    #[test]
    fn an_inconclusive_audit_does_not_launder_a_streak() {
        // If it reset the streak, a book could alternate
        // mismatch/inconclusive indefinitely and never accumulate evidence,
        // while every individual reading looked defensible.
        let mut trail = AuditTrail::default();
        assert_eq!(trail.observe(&mismatch_at(&["1005"]), policy()), None);
        assert_eq!(trail.observe(&AuditOutcome::Inconclusive, policy()), None);
        assert!(
            trail.observe(&mismatch_at(&["1005"]), policy()).is_some(),
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
        trail.observe(&mismatch_at(&["996"]), policy());
        trail.reset();
        assert_eq!(trail.consecutive(), 0);
        assert_eq!(
            trail.observe(&mismatch_at(&["996"]), policy()),
            None,
            "a fresh book was desynced by its predecessor's evidence"
        );
    }
}
