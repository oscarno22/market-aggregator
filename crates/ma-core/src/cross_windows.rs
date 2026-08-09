//! Consolidated per-symbol windows: §11's coverage discipline, across venues
//! that may live on different machines.
//!
//! The gateway merges *instants* — a touch is two quotes read now. A window
//! is different in kind: it is a claim about an interval, and merging
//! interval claims across machines is where inventing data becomes easy. The
//! per-venue windows themselves never needed merging — one node owns each
//! stream, so every [`WindowReading`] is whole wherever it was computed and
//! passes through the gateway untouched. What no single process could
//! compute before this module is the *consolidated* reading: "the 60-second
//! high across all venues", when no node holds all venues.
//!
//! # The rule: merge only what has no order
//!
//! `high`, `low`, `samples`, `trades`, `volume` are order-free — a max, a
//! min, three sums. `mean` is a sample-weighted average and `vwap` a
//! volume-weighted one, both order-free. `range_bps` is recomputed from the
//! merged extremes. Every one of those is exactly as true merged as it was
//! per venue.
//!
//! `first`, `last` and `change_bps` are **absent from the type**, not merely
//! `None`. "First across venues" means ordering samples from two machines
//! against each other, which is precisely the wall-clock comparison §7
//! forbids — each node's samples are ordered on its own monotonic clock, and
//! there is no third clock on which both are ordered. A field that existed
//! but lied sometimes would be worse than no field; refusal by construction
//! is the honest shape.
//!
//! # Coverage merges as a floor, lag rides beside it
//!
//! A consolidated "all venues watched" claim holds only as long as the least
//! -watched contributor: `trusted_ms_floor` is a `min`, the same argument as
//! `integrity_floor` one dimension over. Node lag is *not* subtracted from
//! coverage — a 60s window from a node three seconds stale still describes a
//! real 60 seconds that simply ended three seconds ago — so `max_lag_ms` is
//! published beside the floor, never folded into it. On a node every lag is
//! zero and the same function produces the same shape, which is what lets
//! the page render a consolidated window without knowing whether it is
//! looking at a cluster.
//!
//! Like [`consolidate`](crate::consolidate), this is a pure function: no
//! I/O, no clock read, every input handed in by the caller.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::book::Integrity;
use crate::event::VenueId;
use crate::window::{WindowReading, bps};

/// One venue's windows, as delivered — plus how stale the delivery was.
#[derive(Clone, Copy, Debug)]
pub struct WindowLeg<'a> {
    pub venue: VenueId,
    /// Delivery lag for this leg's node: zero on the node itself, the
    /// gateway's measured hop otherwise. An age-shaped number, which is why
    /// it is published as `max_lag_ms` and never subtracted from coverage.
    pub lag_ms: u64,
    pub windows: &'a [WindowReading],
}

/// Why a venue is not part of one consolidated window.
///
/// Serialised as a machine tag rather than the `Display` prose, because the
/// gateway parses node snapshots back and a described string has no inverse.
/// The page maps the tags to words.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowExclusionReason {
    /// The venue publishes no window of this span — in practice, a node
    /// started with a different `--windows` than the rest of the cluster.
    /// The assignment being a pure function of configuration cuts both
    /// ways: nothing inside one process can verify another's flags, so the
    /// mismatch is published instead of silently narrowing the reading.
    SpanNotPublished,
    /// The window exists and holds neither a book sample nor a print.
    /// Excluded by name so a consolidated reading quietly drawn from one
    /// venue cannot look like one drawn from three — the same argument as
    /// [`CrossVenue::excluded`](crate::CrossVenue::excluded).
    NoData,
}

impl fmt::Display for WindowExclusionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpanNotPublished => f.write_str("publishes no window of this span"),
            Self::NoData => f.write_str("no samples or prints in this window"),
        }
    }
}

/// A venue that did not contribute to one consolidated window, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowExclusion {
    pub venue: VenueId,
    pub reason: WindowExclusionReason,
}

/// One consolidated window over every venue tracking a symbol.
///
/// Deliberately has no `first`, `last` or `change_bps` — see the module
/// docs: ordering samples across machines is the comparison this project
/// refuses, and the refusal lives in the type so it cannot be reintroduced
/// field by field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossWindowReading {
    pub span_ms: u64,
    /// The least-watched contributor's coverage. **Read this first**, same
    /// as `WindowReading::trusted_ms`: the merged numbers below describe
    /// all contributing venues only for this much of the span.
    pub trusted_ms_floor: u64,
    /// Weakest guarantee among the legs whose *book samples* contributed.
    /// `None` when the reading is trades-only.
    pub integrity_floor: Option<Integrity>,
    /// The stalest contributing delivery. Zero when computed on a node;
    /// the slowest node's hop when computed on a gateway. Published beside
    /// the coverage, never subtracted from it.
    pub max_lag_ms: u64,
    pub venues_used: usize,
    pub samples: u64,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    /// Sample-weighted across venues, as each leg's `mean` is within one.
    pub mean: Option<Decimal>,
    /// Recomputed from the merged extremes over the merged mean.
    pub range_bps: Option<Decimal>,
    pub trades: u64,
    /// Total quantity printed across venues. `None` when no venue printed.
    pub volume: Option<Decimal>,
    /// Volume-weighted across venues, exactly: each leg's `vwap` is its
    /// `Σ(price·qty)/Σqty`, so weighting leg vwaps by leg volume recovers
    /// the global `Σ(price·qty)/Σqty`.
    pub vwap: Option<Decimal>,
    pub excluded: Vec<WindowExclusion>,
}

/// Consolidate every span any leg publishes, in ascending span order.
///
/// Ascending rather than first-appearance because the legs may genuinely
/// disagree about the span list (that is what [`SpanNotPublished`] reports),
/// so no single leg's ordering is authoritative.
///
/// [`SpanNotPublished`]: WindowExclusionReason::SpanNotPublished
pub fn consolidate_windows(legs: &[WindowLeg<'_>]) -> Vec<CrossWindowReading> {
    let mut spans: Vec<u64> = legs
        .iter()
        .flat_map(|leg| leg.windows.iter().map(|w| w.span_ms))
        .collect();
    spans.sort_unstable();
    spans.dedup();

    spans
        .into_iter()
        .map(|span| consolidate_span(span, legs))
        .collect()
}

fn consolidate_span(span_ms: u64, legs: &[WindowLeg<'_>]) -> CrossWindowReading {
    let mut out = CrossWindowReading {
        span_ms,
        trusted_ms_floor: 0,
        integrity_floor: None,
        max_lag_ms: 0,
        venues_used: 0,
        samples: 0,
        high: None,
        low: None,
        mean: None,
        range_bps: None,
        trades: 0,
        volume: None,
        vwap: None,
        excluded: Vec::new(),
    };

    let mut floor: Option<u64> = None;
    let mut sum_mean_weighted = Decimal::ZERO;
    let mut sum_vwap_weighted = Decimal::ZERO;
    let mut sum_volume = Decimal::ZERO;

    for leg in legs {
        let Some(w) = leg.windows.iter().find(|w| w.span_ms == span_ms) else {
            out.excluded.push(WindowExclusion {
                venue: leg.venue,
                reason: WindowExclusionReason::SpanNotPublished,
            });
            continue;
        };
        if w.samples == 0 && w.trades == 0 {
            out.excluded.push(WindowExclusion {
                venue: leg.venue,
                reason: WindowExclusionReason::NoData,
            });
            continue;
        }

        out.venues_used += 1;
        floor = Some(floor.map_or(w.trusted_ms, |f| f.min(w.trusted_ms)));
        out.max_lag_ms = out.max_lag_ms.max(leg.lag_ms);

        out.samples += w.samples;
        if w.samples > 0 {
            // Book-derived stats come only from legs that sampled a book,
            // and the integrity floor is taken over exactly those legs — a
            // trades-only leg neither strengthens nor weakens a claim it
            // contributed no price to.
            out.integrity_floor = match (out.integrity_floor, w.integrity_floor) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            out.high = max_opt(out.high, w.high);
            out.low = min_opt(out.low, w.low);
            if let Some(mean) = w.mean {
                sum_mean_weighted += mean * Decimal::from(w.samples);
            }
        }

        out.trades += w.trades;
        if let (Some(volume), Some(vwap)) = (w.volume, w.vwap) {
            sum_volume += volume;
            sum_vwap_weighted += vwap * volume;
        }
    }

    out.trusted_ms_floor = floor.unwrap_or(0);

    if out.samples > 0 {
        let mean = (sum_mean_weighted / Decimal::from(out.samples)).round_dp(MID_SCALE);
        out.mean = Some(mean);
        if let (Some(high), Some(low)) = (out.high, out.low)
            && !mean.is_zero()
        {
            out.range_bps = Some(bps(high - low, mean));
        }
    }
    if out.trades > 0 {
        out.volume = Some(sum_volume);
        if !sum_volume.is_zero() {
            out.vwap = Some((sum_vwap_weighted / sum_volume).round_dp(MID_SCALE));
        }
    }
    out
}

/// Same scale as `window`'s mean, for the same reason.
const MID_SCALE: u32 = 8;

fn max_opt<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn min_opt<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn reading(span_ms: u64) -> WindowReading {
        // serde is the one constructor available outside the crate's window
        // internals, and using it here doubles as a check that every field
        // the merge reads survives the wire.
        serde_json::from_value(serde_json::json!({
            "span_ms": span_ms,
            "trusted_ms": span_ms,
            "samples": 0,
            "integrity_floor": null,
            "first": null, "last": null, "high": null, "low": null,
            "mean": null, "change_bps": null, "range_bps": null,
            "mean_spread_bps": null,
        }))
        .unwrap()
    }

    fn sampled(
        span_ms: u64,
        trusted_ms: u64,
        integrity: Integrity,
        samples: u64,
        high: &str,
        low: &str,
        mean: &str,
    ) -> WindowReading {
        WindowReading {
            trusted_ms,
            samples,
            integrity_floor: Some(integrity),
            high: Some(dec(high)),
            low: Some(dec(low)),
            mean: Some(dec(mean)),
            ..reading(span_ms)
        }
    }

    #[test]
    fn merges_only_order_free_statistics_with_floors_and_max_lag() {
        let a = [sampled(
            60_000,
            60_000,
            Integrity::Verified,
            100,
            "110",
            "90",
            "100",
        )];
        let b = [sampled(
            60_000,
            40_000,
            Integrity::OrderOnly,
            300,
            "120",
            "95",
            "104",
        )];
        let legs = [
            WindowLeg {
                venue: VenueId::Kraken,
                lag_ms: 0,
                windows: &a,
            },
            WindowLeg {
                venue: VenueId::Bitstamp,
                lag_ms: 700,
                windows: &b,
            },
        ];

        let out = consolidate_windows(&legs);
        assert_eq!(out.len(), 1);
        let w = &out[0];

        assert_eq!(w.venues_used, 2);
        assert_eq!(w.high, Some(dec("120")), "high is a max across venues");
        assert_eq!(w.low, Some(dec("90")), "low is a min across venues");
        assert_eq!(w.samples, 400);
        // Sample-weighted: (100*100 + 104*300) / 400 = 103.
        assert_eq!(w.mean, Some(dec("103")));
        // Recomputed from the merged extremes: (120-90)/103 in bps.
        assert_eq!(w.range_bps, Some(bps(dec("30"), dec("103"))));

        // Coverage is the least-watched contributor's, not an average — the
        // "all venues watched" claim only holds that long.
        assert_eq!(w.trusted_ms_floor, 40_000);
        // Integrity floors over the contributing legs, weakest first.
        assert_eq!(w.integrity_floor, Some(Integrity::OrderOnly));
        // Lag is the stalest delivery, published beside the floor.
        assert_eq!(w.max_lag_ms, 700);
    }

    #[test]
    fn a_venue_without_the_span_is_excluded_by_name_not_averaged_in() {
        // The misconfigured-cluster case: one node started with a different
        // --windows. Nothing inside a process can verify another's flags, so
        // the mismatch must surface in the reading itself.
        let a = [sampled(
            60_000,
            60_000,
            Integrity::Verified,
            10,
            "110",
            "90",
            "100",
        )];
        let b = [sampled(
            10_000,
            10_000,
            Integrity::GapDetectable,
            10,
            "300",
            "200",
            "250",
        )];
        let legs = [
            WindowLeg {
                venue: VenueId::Kraken,
                lag_ms: 0,
                windows: &a,
            },
            WindowLeg {
                venue: VenueId::Coinbase,
                lag_ms: 0,
                windows: &b,
            },
        ];

        let out = consolidate_windows(&legs);
        assert_eq!(out.len(), 2, "both spans appear; neither absorbs the other");

        let ten = out.iter().find(|w| w.span_ms == 10_000).unwrap();
        assert_eq!(ten.venues_used, 1);
        assert_eq!(
            ten.excluded,
            vec![WindowExclusion {
                venue: VenueId::Kraken,
                reason: WindowExclusionReason::SpanNotPublished,
            }]
        );
        let sixty = out.iter().find(|w| w.span_ms == 60_000).unwrap();
        assert_eq!(
            sixty.high,
            Some(dec("110")),
            "the 10s window's prices leaked in"
        );
        assert_eq!(
            sixty.excluded[0].reason,
            WindowExclusionReason::SpanNotPublished
        );
    }

    #[test]
    fn an_empty_window_is_excluded_and_cannot_drag_the_floor_to_zero() {
        // A venue with no data in the span contributes nothing — including
        // to the floor. Its absence is named instead: a floor dragged to
        // zero by a venue that wasn't there would make every merged number
        // read as worthless when two venues covered the span completely.
        let a = [sampled(
            60_000,
            60_000,
            Integrity::Verified,
            10,
            "110",
            "90",
            "100",
        )];
        let empty = [reading(60_000)];
        let legs = [
            WindowLeg {
                venue: VenueId::Kraken,
                lag_ms: 0,
                windows: &a,
            },
            WindowLeg {
                venue: VenueId::Bitstamp,
                lag_ms: 900,
                windows: &empty,
            },
        ];

        let out = consolidate_windows(&legs);
        let w = &out[0];
        assert_eq!(w.venues_used, 1);
        assert_eq!(w.trusted_ms_floor, 60_000);
        assert_eq!(
            w.max_lag_ms, 0,
            "an excluded venue's lag bounded a reading it contributed nothing to"
        );
        assert_eq!(
            w.excluded,
            vec![WindowExclusion {
                venue: VenueId::Bitstamp,
                reason: WindowExclusionReason::NoData,
            }]
        );
    }

    #[test]
    fn a_trades_only_leg_counts_prints_without_touching_the_price_claims() {
        // A desynced venue still prints. Its trades merge in; its absent
        // book stats neither strengthen nor weaken the integrity floor,
        // which is taken over the legs that contributed a price.
        let a = [sampled(
            60_000,
            60_000,
            Integrity::Verified,
            10,
            "110",
            "90",
            "100",
        )];
        let trades_only = [WindowReading {
            trades: 5,
            volume: Some(dec("2")),
            vwap: Some(dec("101")),
            trusted_ms: 0,
            ..reading(60_000)
        }];
        let legs = [
            WindowLeg {
                venue: VenueId::Kraken,
                lag_ms: 0,
                windows: &a,
            },
            WindowLeg {
                venue: VenueId::Bitstamp,
                lag_ms: 0,
                windows: &trades_only,
            },
        ];

        let out = consolidate_windows(&legs);
        let w = &out[0];
        assert_eq!(w.venues_used, 2);
        assert_eq!(w.trades, 5);
        assert_eq!(w.volume, Some(dec("2")));
        assert_eq!(w.vwap, Some(dec("101")));
        assert_eq!(
            w.integrity_floor,
            Some(Integrity::Verified),
            "a leg that contributed no price weakened the price claim"
        );
        assert_eq!(
            w.trusted_ms_floor, 0,
            "but a used leg's coverage still floors the window — the venue \
             was contributing, and it was untrusted the whole time"
        );
        assert_eq!(w.high, Some(dec("110")));
    }

    #[test]
    fn vwap_merges_volume_weighted_exactly() {
        // Leg vwaps weighted by leg volume recover the global Σ(p·q)/Σq:
        // (100·2 + 106·1) / 3 = 102.
        let a = [WindowReading {
            trades: 4,
            volume: Some(dec("2")),
            vwap: Some(dec("100")),
            ..reading(60_000)
        }];
        let b = [WindowReading {
            trades: 1,
            volume: Some(dec("1")),
            vwap: Some(dec("106")),
            ..reading(60_000)
        }];
        let legs = [
            WindowLeg {
                venue: VenueId::Coinbase,
                lag_ms: 0,
                windows: &a,
            },
            WindowLeg {
                venue: VenueId::Kraken,
                lag_ms: 0,
                windows: &b,
            },
        ];

        let w = &consolidate_windows(&legs)[0];
        assert_eq!(w.trades, 5);
        assert_eq!(w.volume, Some(dec("3")));
        assert_eq!(w.vwap, Some(dec("102")));
        assert_eq!(w.samples, 0);
        assert_eq!(w.mean, None, "no book sampled anywhere; no mean invented");
    }

    #[test]
    fn the_type_refuses_order_dependent_statistics_by_construction() {
        // Not a runtime behaviour — a shape. If someone adds first/last/
        // change_bps back, this test names the argument they need to answer:
        // ordering samples across machines is the wall-clock comparison §7
        // forbids, because there is no third clock both nodes' samples are
        // ordered on.
        let rendered = format!("{:?}", consolidate_windows(&[]));
        assert!(!rendered.contains("first"));
        assert!(!rendered.contains("change_bps"));
    }
}
