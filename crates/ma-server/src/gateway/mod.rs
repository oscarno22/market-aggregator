//! One view across every node, and the timing problem that comes with it.
//!
//! v3 shards streams across nodes. Each node then serves its own page and its
//! own share, and `/cluster` says who has what — but nothing puts the halves
//! back together, so the consolidated touch every node publishes is
//! consolidated over *the venues that node happens to own*. On a two-node
//! cluster that is routinely a single venue wearing a cross-venue label.
//!
//! This is the gateway that merges them. It subscribes to every node's SSE
//! stream, re-consolidates, and serves the result — and the reason it is a
//! separate piece of work rather than a flag is that it inherits the whole of
//! `docs/DESIGN.md` §12 across a **network hop** rather than a socket.
//!
//! # The same problem, one layer out
//!
//! §12's rule is that two venues' books were never observed at the same
//! instant, so a cross-venue reading has to publish how simultaneous it
//! actually is and exclude anything stalled. Here there is a second gap on top
//! of the first: a node's snapshot describes its books *as of that node's
//! tick*, and it then travels a network to get here.
//!
//! So every age in a merged view is **two monotonic durations added
//! together**:
//!
//! ```text
//! effective age = the node's own book age + the gateway's lag since that snapshot arrived
//! ```
//!
//! Both halves are monotonic — the node's from its own `IngestTime`, the
//! gateway's from its own — so no clock step anywhere can shrink one. Nothing
//! compares a wall clock across machines, which is the one thing §7 forbids
//! and the obvious implementation does first: a node's `wall_unix_ms` is right
//! there in the snapshot, and subtracting it from the gateway's wall clock
//! would fold every machine's NTP offset straight into the staleness guard.
//!
//! Omitting the lag is the more dangerous mistake and it looks like nothing.
//! A node that dies mid-tick leaves a last snapshot whose `age_ms` is frozen
//! at a healthy few milliseconds. Merged unadjusted, that node's books stay
//! *fresh forever* and keep contributing legs to a consolidated touch drawn
//! from a market that has since moved — the §12 failure exactly, arrived at
//! through a dead process rather than a quiet socket.
//!
//! Adding the lag can only make a book look *older* than it is, never younger,
//! so the error is in the direction that excludes rather than includes. Same
//! judgement as `Desynced`, and as the sharding rule that prefers a visible
//! gap to a silent duplicate.
//!
//! # A node is excluded by name and reason, or not at all
//!
//! `CrossView::excluded` publishes which venues did not contribute and why,
//! because a touch that quietly narrows to one venue looks exactly like one
//! drawn from three. [`NodeStatus`] is that rule applied to nodes: every
//! configured node appears in the output whether or not it was used, with the
//! reason it was not. A gateway that silently served two nodes' worth of data
//! while three were configured would be publishing the same lie one level up.
//!
//! # The one thing only a gateway can see
//!
//! **At most one node runs a given stream** is v3's safety property, and until
//! now nothing in the system could actually check it. A node knows what it
//! owns; it cannot know what anyone else owns without trusting the same
//! registry that produced the answer. The gateway holds every node's snapshot
//! at once, and a node omits streams it does not own — so two nodes reporting
//! the same `(symbol, venue)` is direct evidence of a doubly-owned stream.
//!
//! [`MergedSnapshot::duplicated`] publishes it, and `/metrics` exposes it as
//! `ma_gateway_duplicated_streams`. That is the alert worth having: the
//! failure it detects is the one v3's whole design exists to prevent, and it
//! is invisible from every other vantage point in the system until a venue
//! starts refusing connections.

pub mod feed;
pub mod http;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::time::Duration;

use ma_core::{
    BookState, CrossPolicy, DesyncReason, IngestTime, Integrity, Symbol, TopOfBook, VenueId,
};
use ma_pipeline::aggregator::{BookStatus, Snapshot, SymbolView, TradeView, VenueView, cross_view};
use ma_pipeline::channel::ChannelMetrics;
use serde::Serialize;

/// Which clock a merged view's durations were measured on.
///
/// Deliberately *not* `ingest_monotonic`, which is what a node publishes: these
/// numbers are a node's monotonic reading plus this process's monotonic lag,
/// and a reader comparing a gateway age against a node age directly would be
/// comparing two different quantities. Naming it is CLAUDE.md §4's rule —
/// any surfaced cross-venue comparison must say which clock it is on — applied
/// to a comparison that now spans machines.
pub const GATEWAY_CLOCK: &str = "node_monotonic+gateway_monotonic";

/// What the gateway is willing to merge.
#[derive(Clone, Copy, Debug)]
pub struct GatewayPolicy {
    /// How long since a node's last snapshot before that node is dropped from
    /// the merge entirely.
    ///
    /// Coarser than [`CrossPolicy::max_age`] and a different question: that one
    /// asks whether a *book* has stalled, this asks whether a *node* is still
    /// there. A node ticking at 250ms that has said nothing for several seconds
    /// is not slow, it is gone — and its last snapshot describes a market that
    /// has moved.
    pub max_node_age: Duration,
    /// Applied to the merged books, after their ages are adjusted for lag.
    pub cross: CrossPolicy,
}

impl Default for GatewayPolicy {
    fn default() -> Self {
        Self {
            // Twelve node ticks at the default 250ms. Generous enough that a
            // garbage-collection pause or a retransmit does not evict a healthy
            // node, tight enough that a dead one stops contributing legs well
            // inside the time a market moves.
            max_node_age: Duration::from_secs(3),
            cross: CrossPolicy::default(),
        }
    }
}

/// The latest thing heard from one node.
#[derive(Clone, Debug)]
pub struct NodeReport {
    /// How this node is labelled in the output. The URL unless the operator
    /// gave it a name — see `feed::parse_nodes`.
    pub node: String,
    pub url: String,
    /// `None` until the first snapshot arrives.
    pub snapshot: Option<Snapshot>,
    /// When that snapshot arrived, on **this process's** monotonic clock.
    pub received_at: Option<IngestTime>,
    /// The most recent connection or parse failure, if any. Retained after a
    /// reconnect rather than cleared, so a node that is flapping shows why
    /// rather than looking healthy between failures.
    pub last_error: Option<String>,
    pub connects: u64,
    pub failures: u64,
}

impl NodeReport {
    pub fn new(node: String, url: String) -> Self {
        Self {
            node,
            url,
            snapshot: None,
            received_at: None,
            last_error: None,
            connects: 0,
            failures: 0,
        }
    }
}

/// Why a configured node contributed nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeExclusion {
    /// Configured, but no snapshot has ever arrived.
    NeverReported,
    /// Reported once and has since gone quiet past
    /// [`GatewayPolicy::max_node_age`].
    Stale { lag: Duration },
}

impl std::fmt::Display for NodeExclusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverReported => f.write_str("no snapshot received yet"),
            Self::Stale { lag } => write!(f, "no snapshot for {}ms", lag.as_millis()),
        }
    }
}

/// One node's standing in the merge, published whether or not it was used.
#[derive(Clone, Debug, Serialize)]
pub struct NodeStatus {
    pub node: String,
    pub url: String,
    pub included: bool,
    /// Set exactly when `included` is false.
    pub excluded_because: Option<String>,
    /// How long ago this node's latest snapshot arrived here, on the gateway's
    /// monotonic clock.
    pub lag_ms: Option<u64>,
    /// Streams this node reported. A node omits what it does not own, so
    /// summed across included nodes this should equal the cluster's stream
    /// count — the same arithmetic `ma_cluster_owned_streams` supports, done
    /// from the other side.
    pub streams: usize,
    /// The node's own snapshot sequence, so a stuck node is visible as a
    /// sequence that stops advancing even while the connection stays up.
    pub seq: Option<u64>,
    pub connects: u64,
    pub failures: u64,
    pub last_error: Option<String>,
}

/// A stream two nodes both claim to be running.
///
/// The violation of v3's safety property, seen from the only place it is
/// visible. See the module docs.
#[derive(Clone, Debug, Serialize)]
pub struct DuplicatedStream {
    pub symbol: String,
    pub venue: VenueId,
    pub nodes: Vec<String>,
}

/// The merged view.
///
/// Serialises as a [`Snapshot`] with two extra fields, deliberately: the
/// gateway then satisfies the *same* wire contract a node does, so the chart
/// page, `/api/snapshot` and any dashboard work against it unchanged — and a
/// gateway can be pointed at another gateway without anything special.
#[derive(Clone, Debug, Serialize)]
pub struct MergedSnapshot {
    #[serde(flatten)]
    pub snapshot: Snapshot,
    /// Every configured node, included or not. See the module docs on why a
    /// silently narrowed merge is the failure worth designing against.
    pub nodes: Vec<NodeStatus>,
    /// Streams claimed by more than one node. Empty is the only healthy value.
    pub duplicated: Vec<DuplicatedStream>,
}

impl MergedSnapshot {
    /// Nodes actually contributing.
    pub fn nodes_used(&self) -> usize {
        self.nodes.iter().filter(|n| n.included).count()
    }
}

/// Merge every node's latest snapshot into one view.
///
/// Pure: no I/O, no clock read, no async. `now` is passed in for the same
/// reason `ma_core::consolidate` takes one — the staleness rules are the part
/// most worth testing and they should not need a sleep to exercise.
pub fn merge(
    reports: &[NodeReport],
    now: IngestTime,
    seq: u64,
    policy: GatewayPolicy,
) -> MergedSnapshot {
    let mut statuses = Vec::with_capacity(reports.len());
    // Symbol -> venue -> the nodes reporting it, freshest first. A `Vec` per
    // venue rather than one entry, because more than one entry is the
    // violation this gateway exists to be able to see.
    let mut books: BTreeMap<Symbol, BTreeMap<VenueId, Vec<(String, VenueView)>>> = BTreeMap::new();
    let mut channel = ChannelMetrics {
        len: 0,
        capacity: 0,
        dropped: 0,
    };

    for report in reports {
        let lag = report.received_at.map(|at| now.since(at));
        let excluded = match (&report.snapshot, lag) {
            (Some(_), Some(lag)) if lag <= policy.max_node_age => None,
            (Some(_), Some(lag)) => Some(NodeExclusion::Stale { lag }),
            // No snapshot, or one with no arrival time — the second cannot
            // happen, and treating it as "never reported" is the conservative
            // reading of a state that should not exist.
            _ => Some(NodeExclusion::NeverReported),
        };

        let streams = report
            .snapshot
            .as_ref()
            .map_or(0, |s| s.symbols.iter().map(|sym| sym.venues.len()).sum());

        statuses.push(NodeStatus {
            node: report.node.clone(),
            url: report.url.clone(),
            included: excluded.is_none(),
            excluded_because: excluded.map(|e| e.to_string()),
            lag_ms: lag.map(millis),
            streams,
            seq: report.snapshot.as_ref().map(|s| s.seq),
            connects: report.connects,
            failures: report.failures,
            last_error: report.last_error.clone(),
        });

        if excluded.is_some() {
            continue;
        }
        let (Some(snapshot), Some(lag)) = (&report.snapshot, lag) else {
            continue;
        };

        // Summed across nodes. `dropped` summing is the number that matters —
        // it is a lifetime counter and a cluster's total losses are the sum of
        // its nodes' — while `len`/`capacity` sum to a cluster-wide occupancy,
        // which is the only reading of them that means anything once the
        // channels are in different processes.
        channel.len += snapshot.channel.len;
        channel.capacity += snapshot.channel.capacity;
        channel.dropped += snapshot.channel.dropped;

        for symbol in &snapshot.symbols {
            for view in &symbol.venues {
                books
                    .entry(Symbol::new(&symbol.symbol))
                    .or_default()
                    .entry(view.venue)
                    .or_default()
                    .push((report.node.clone(), aged(view, lag)));
            }
        }
    }

    let mut duplicated = Vec::new();
    let mut symbols = Vec::with_capacity(books.len());

    for (symbol, by_venue) in books {
        let mut views = Vec::with_capacity(by_venue.len());
        let mut tops = Vec::with_capacity(by_venue.len());

        for (venue, mut claims) in by_venue {
            if claims.len() > 1 {
                duplicated.push(DuplicatedStream {
                    symbol: symbol.to_string(),
                    venue,
                    nodes: claims.iter().map(|(node, _)| node.clone()).collect(),
                });
                // Freshest wins, then node name so the choice is stable rather
                // than flapping between two nodes tick by tick. Picking *a*
                // winner is a rendering decision and not a repair: the
                // violation is published above, and nothing here can fix a
                // stream that two processes are running.
                claims.sort_by(|a, b| {
                    effective_age(&a.1)
                        .cmp(&effective_age(&b.1))
                        .then_with(|| a.0.cmp(&b.0))
                });
            }
            let Some((_, view)) = claims.into_iter().next() else {
                continue;
            };
            tops.push((venue, top_of_book(&view, now)));
            views.push(view);
        }

        let weakest_integrity = views
            .iter()
            .filter(|v| v.status == BookStatus::Live)
            .filter_map(|v| v.integrity)
            .min();

        symbols.push(SymbolView {
            symbol: symbol.to_string(),
            weakest_integrity,
            cross: cross_view(tops, policy.cross, Cow::Borrowed(GATEWAY_CLOCK)),
            venues: views,
        });
    }

    MergedSnapshot {
        snapshot: Snapshot {
            seq,
            wall_unix_ms: unix_millis(now.wall()),
            clock: Cow::Borrowed(GATEWAY_CLOCK),
            symbols,
            channel,
        },
        nodes: statuses,
        duplicated,
    }
}

/// A node's view with every *age* advanced by how long its snapshot took to
/// get here and sit in this process.
///
/// The three fields adjusted are the three that are "time since something",
/// and each would otherwise freeze at whatever it read when the node last
/// spoke:
///
/// - `age_ms` — time since the last applied update. The one the staleness
///   guard reads, and the one that makes a dead node's books look permanently
///   fresh if it is left alone.
/// - `status_for_ms` — how long the book has held its status. A node that dies
///   two seconds into a desync should not still be reporting two seconds.
/// - `last_verified_ms` — age of Kraken's last matching checksum. A `Verified`
///   book whose verification is minutes old is not really verified, and the
///   whole point of publishing this is to let a reader notice.
/// - `last_trade.age_ms` — the print is as stale as the node it came through.
///
/// `desynced_total_ms` is deliberately *not* adjusted: it is a cumulative
/// total, not an age, and adding lag to it would inflate a counter rather than
/// correct a measurement.
///
/// The rolling windows are also left alone, and that is a stated limitation
/// rather than an oversight. A 60s window from a node three seconds stale
/// describes a real 60 seconds — it simply ended three seconds ago. Shifting
/// `span_ms` would be inventing coverage; the honest signal is the node's
/// `lag_ms`, published beside it.
fn aged(view: &VenueView, lag: Duration) -> VenueView {
    let lag_ms = millis(lag);
    VenueView {
        age_ms: Some(view.age_ms.map_or(lag_ms, |a| a.saturating_add(lag_ms))),
        status_for_ms: view.status_for_ms.saturating_add(lag_ms),
        last_verified_ms: view.last_verified_ms.map(|v| v.saturating_add(lag_ms)),
        last_trade: view.last_trade.clone().map(|t| TradeView {
            age_ms: t.age_ms.saturating_add(lag_ms),
            ..t
        }),
        ..view.clone()
    }
}

fn effective_age(view: &VenueView) -> u64 {
    view.age_ms.unwrap_or(u64::MAX)
}

/// Rebuild the shape `ma_core::consolidate` consumes.
///
/// Only three things are read from it — whether the book is `Live`, its
/// `Integrity`, and its age — so `since`, `last_verified` and the desync reason
/// below are placeholders that never reach a caller. They are not fabricated
/// into the published view: the `VenueView` served to clients carries the
/// node's own `desync_reason` string, untouched.
///
/// A `Live` view with no integrity cannot be produced by a node — the two are
/// coupled in `VenueView` precisely so a desynced book cannot be rendered as
/// though its integrity still meant something — so this arm only fires on
/// input from something that is not one of our nodes. It takes the weakest
/// guarantee rather than the strongest, because a number derived from a book
/// of unknown provenance should not claim checksum grade.
fn top_of_book(view: &VenueView, now: IngestTime) -> TopOfBook {
    let state = match view.status {
        BookStatus::Live => BookState::Live {
            integrity: view.integrity.unwrap_or(Integrity::OrderOnly),
            since: now,
            last_verified: None,
        },
        BookStatus::Desynced => BookState::Desynced {
            since: now,
            reason: DesyncReason::ConnectionLost,
        },
        BookStatus::Uninitialized => BookState::Uninitialized,
    };
    TopOfBook {
        bid: view.bid,
        ask: view.ask,
        state,
        age: view.age_ms.map(Duration::from_millis),
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn unix_millis(at: std::time::SystemTime) -> u64 {
    at.duration_since(std::time::UNIX_EPOCH)
        .map(millis)
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Clock, Level, Price, Qty, TestClock};
    use ma_pipeline::aggregator::CrossView;
    use ma_pipeline::metrics::{Rates, VenueCountersSnapshot};
    use std::str::FromStr;

    fn level(price: &str) -> Level {
        Level::new(Price::from_str(price).unwrap(), Qty::from_str("1").unwrap())
    }

    fn view(venue: VenueId, bid: &str, ask: &str, age_ms: u64, integrity: Integrity) -> VenueView {
        VenueView {
            venue,
            status: BookStatus::Live,
            integrity: Some(integrity),
            desync_reason: None,
            bid: Some(level(bid)),
            ask: Some(level(ask)),
            bids: vec![level(bid)],
            asks: vec![level(ask)],
            spread: None,
            mid: None,
            age_ms: Some(age_ms),
            status_for_ms: 1_000,
            desynced_total_ms: 250,
            last_verified_ms: Some(5),
            audits: 0,
            audit_mismatches: 0,
            levels_held: [10, 10],
            windows: Vec::new(),
            last_trade: Some(ma_pipeline::aggregator::TradeView {
                price: "100.5".to_owned(),
                qty: "0.25".to_owned(),
                taker_side: None,
                age_ms: 15,
            }),
            counters: VenueCountersSnapshot::default(),
            rates: Rates::default(),
        }
    }

    fn snapshot(symbol: &str, views: Vec<VenueView>) -> Snapshot {
        Snapshot {
            seq: 7,
            wall_unix_ms: 1_786_247_000_000,
            clock: Cow::Borrowed(ma_pipeline::aggregator::INGEST_MONOTONIC),
            symbols: vec![SymbolView {
                symbol: symbol.to_owned(),
                weakest_integrity: None,
                cross: empty_cross(),
                venues: views,
            }],
            channel: ChannelMetrics {
                len: 1,
                capacity: 1024,
                dropped: 3,
            },
        }
    }

    fn empty_cross() -> CrossView {
        CrossView {
            bid: None,
            ask: None,
            spread: None,
            spread_bps: None,
            mid: None,
            integrity_floor: None,
            oldest_leg_ms: None,
            venues_used: 0,
            crossed: false,
            single_venue: false,
            excluded: Vec::new(),
            clock: Cow::Borrowed("ingest_monotonic"),
        }
    }

    fn report(node: &str, snapshot: Snapshot, at: IngestTime) -> NodeReport {
        NodeReport {
            snapshot: Some(snapshot),
            received_at: Some(at),
            connects: 1,
            ..NodeReport::new(node.to_owned(), format!("http://{node}"))
        }
    }

    #[test]
    fn two_nodes_holding_different_venues_become_one_consolidated_touch() {
        // The whole point. Each node consolidates over the venues it happens to
        // own, so on a two-node cluster a node's own "cross-venue" touch is
        // routinely one venue wearing a cross-venue label. Merged, the touch is
        // drawn from both.
        let clock = TestClock::new();
        let now = clock.now();
        let reports = [
            report(
                "a",
                snapshot(
                    "BTC-USD",
                    vec![view(
                        VenueId::Kraken,
                        "10010",
                        "10020",
                        10,
                        Integrity::Verified,
                    )],
                ),
                now,
            ),
            report(
                "b",
                snapshot(
                    "BTC-USD",
                    vec![view(
                        VenueId::Coinbase,
                        "9990",
                        "10000",
                        10,
                        Integrity::GapDetectable,
                    )],
                ),
                now,
            ),
        ];

        let merged = merge(&reports, now, 1, GatewayPolicy::default());
        let symbol = &merged.snapshot.symbols[0];

        assert_eq!(symbol.venues.len(), 2);
        assert_eq!(merged.nodes_used(), 2);
        assert_eq!(symbol.cross.venues_used, 2);
        assert_eq!(symbol.cross.bid.unwrap().venue, VenueId::Kraken);
        assert_eq!(symbol.cross.ask.unwrap().venue, VenueId::Coinbase);
        assert!(
            !symbol.cross.single_venue,
            "a merged touch drawn from two nodes reported as single-venue"
        );
        // The floor is over the legs used, and one of them is only
        // gap-detectable. Same rule as within a process, now across machines.
        assert_eq!(symbol.cross.integrity_floor, Some(Integrity::GapDetectable));
        assert_eq!(symbol.weakest_integrity, Some(Integrity::GapDetectable));
        assert!(merged.duplicated.is_empty());
    }

    #[test]
    fn a_books_age_carries_the_network_hop_that_delivered_it() {
        // The correction this module exists for. Left alone, a node's `age_ms`
        // is frozen at whatever it read when the node last spoke — so a node
        // that dies mid-tick leaves books that look fresh forever.
        let clock = TestClock::new();
        let arrived = clock.now();
        let reports = [report(
            "a",
            snapshot(
                "BTC-USD",
                vec![view(VenueId::Kraken, "100", "101", 40, Integrity::Verified)],
            ),
            arrived,
        )];

        clock.advance(Duration::from_millis(600));
        let merged = merge(&reports, clock.now(), 1, GatewayPolicy::default());
        let v = &merged.snapshot.symbols[0].venues[0];

        assert_eq!(
            v.age_ms,
            Some(640),
            "the 600ms hop was not added to the age"
        );
        assert_eq!(v.status_for_ms, 1_600);
        assert_eq!(v.last_verified_ms, Some(605));
        assert_eq!(
            v.desynced_total_ms, 250,
            "a cumulative total was inflated by lag as though it were an age"
        );
        assert_eq!(
            v.last_trade.as_ref().map(|t| t.age_ms),
            Some(615),
            "a print is as stale as the node it came through"
        );
        assert_eq!(merged.snapshot.symbols[0].cross.oldest_leg_ms, Some(640));
    }

    #[test]
    fn a_dead_nodes_frozen_book_stops_being_a_leg() {
        // The consequence of the adjustment above, and the failure it prevents.
        // Node b's snapshot says its book is 10ms old and always will; without
        // the lag it would keep contributing an aggressive quote from a market
        // that has since moved, and show a permanent arbitrage against the node
        // still running.
        let clock = TestClock::new();
        let both = clock.now();
        let mut reports = [
            report(
                "a",
                snapshot(
                    "BTC-USD",
                    vec![view(VenueId::Kraken, "100", "102", 10, Integrity::Verified)],
                ),
                both,
            ),
            report(
                "b",
                snapshot(
                    "BTC-USD",
                    vec![view(
                        VenueId::Coinbase,
                        "99999",
                        "100000",
                        10,
                        Integrity::GapDetectable,
                    )],
                ),
                both,
            ),
        ];

        // a keeps reporting; b went away 2.5s ago — inside `max_node_age` so it
        // is still merged, but its books are now well past `CrossPolicy`'s
        // two-second guard.
        clock.advance(Duration::from_millis(2_500));
        reports[0].received_at = Some(clock.now());
        let merged = merge(&reports, clock.now(), 1, GatewayPolicy::default());
        let cross = &merged.snapshot.symbols[0].cross;

        assert_eq!(cross.venues_used, 1);
        assert!(
            !cross.crossed,
            "a dead node's frozen quote manufactured an arbitrage"
        );
        assert_eq!(cross.bid.unwrap().venue, VenueId::Kraken);
        assert_eq!(
            cross.excluded.len(),
            1,
            "the stale venue was dropped without saying so"
        );
        assert!(
            cross.excluded[0].reason.contains("no update for"),
            "{:?}",
            cross.excluded
        );
        // Still merged as a node — the *book* is stale, the node is not.
        assert_eq!(merged.nodes_used(), 2);
    }

    #[test]
    fn a_node_that_has_gone_quiet_is_dropped_by_name_and_reason() {
        let clock = TestClock::new();
        let both = clock.now();
        let reports = [
            report(
                "a",
                snapshot(
                    "BTC-USD",
                    vec![view(VenueId::Kraken, "100", "102", 10, Integrity::Verified)],
                ),
                both,
            ),
            NodeReport::new("b".to_owned(), "http://b".to_owned()),
        ];

        clock.advance(Duration::from_secs(10));
        let merged = merge(&reports, clock.now(), 1, GatewayPolicy::default());

        // Both configured nodes appear. A gateway that served one node's data
        // while two were configured, and said nothing, would be publishing the
        // same lie `CrossView::excluded` exists to prevent.
        assert_eq!(merged.nodes.len(), 2);
        assert_eq!(merged.nodes_used(), 0, "a is 10s stale and must drop too");

        let a = merged.nodes.iter().find(|n| n.node == "a").unwrap();
        assert!(
            a.excluded_because
                .as_deref()
                .is_some_and(|r| r.contains("no snapshot for 10000ms")),
            "{a:?}"
        );
        let b = merged.nodes.iter().find(|n| n.node == "b").unwrap();
        assert_eq!(
            b.excluded_because.as_deref(),
            Some("no snapshot received yet"),
            "a node that never reported must not read the same as one that died"
        );
        assert!(merged.snapshot.symbols.is_empty());
    }

    #[test]
    fn two_nodes_claiming_one_stream_is_published_rather_than_merged_away() {
        // The property no other component in the system can check. A node knows
        // what it owns and cannot know what anyone else owns without trusting
        // the registry that produced the answer; the gateway holds both
        // snapshots at once, and a node omits streams it does not own.
        //
        // Left unpublished this is invisible: taking the freshest of the two
        // produces a perfectly plausible book, and the first symptom is a venue
        // refusing connections some hours later.
        let clock = TestClock::new();
        let now = clock.now();
        let reports = [
            report(
                "a",
                snapshot(
                    "BTC-USD",
                    vec![view(VenueId::Kraken, "100", "102", 50, Integrity::Verified)],
                ),
                now,
            ),
            report(
                "b",
                snapshot(
                    "BTC-USD",
                    vec![view(VenueId::Kraken, "101", "103", 10, Integrity::Verified)],
                ),
                now,
            ),
        ];

        let merged = merge(&reports, now, 1, GatewayPolicy::default());

        assert_eq!(merged.duplicated.len(), 1);
        assert_eq!(merged.duplicated[0].venue, VenueId::Kraken);
        assert_eq!(merged.duplicated[0].symbol, "BTC-USD");
        assert_eq!(merged.duplicated[0].nodes, vec!["a", "b"]);

        // One book is served, not two, and it is the freshest — a rendering
        // decision, not a repair. Nothing here can fix two processes running
        // one stream.
        assert_eq!(merged.snapshot.symbols[0].venues.len(), 1);
        assert_eq!(
            merged.snapshot.symbols[0].venues[0]
                .bid
                .unwrap()
                .price
                .to_string(),
            "101"
        );
    }

    #[test]
    fn the_merged_view_names_its_own_clock_rather_than_a_nodes() {
        // Every age here is a node's monotonic reading plus this process's
        // monotonic lag. A reader comparing a gateway age against a node age as
        // though they were the same quantity would be wrong, and the only thing
        // that stops them is this label.
        let clock = TestClock::new();
        let now = clock.now();
        let reports = [report(
            "a",
            snapshot(
                "BTC-USD",
                vec![view(VenueId::Kraken, "100", "102", 10, Integrity::Verified)],
            ),
            now,
        )];

        let merged = merge(&reports, now, 1, GatewayPolicy::default());
        assert_eq!(merged.snapshot.clock, GATEWAY_CLOCK);
        assert_eq!(merged.snapshot.symbols[0].cross.clock, GATEWAY_CLOCK);
        assert_ne!(
            merged.snapshot.clock,
            ma_pipeline::aggregator::INGEST_MONOTONIC,
            "the gateway claimed a node's clock for a number it computed itself"
        );
    }

    #[test]
    fn channel_counters_sum_across_the_nodes_that_contributed() {
        let clock = TestClock::new();
        let now = clock.now();
        let reports = [
            report(
                "a",
                snapshot(
                    "BTC-USD",
                    vec![view(VenueId::Kraken, "100", "102", 10, Integrity::Verified)],
                ),
                now,
            ),
            report(
                "b",
                snapshot(
                    "BTC-USD",
                    vec![view(
                        VenueId::Coinbase,
                        "99",
                        "103",
                        10,
                        Integrity::GapDetectable,
                    )],
                ),
                now,
            ),
        ];

        let merged = merge(&reports, now, 1, GatewayPolicy::default());
        assert_eq!(merged.snapshot.channel.dropped, 6, "3 dropped on each node");
        assert_eq!(merged.snapshot.channel.capacity, 2048);
    }

    #[test]
    fn an_excluded_nodes_books_are_left_out_of_the_totals_too() {
        // A node dropped for staleness must not contribute its channel
        // counters either, or the drop count keeps climbing from a process
        // nobody can reach.
        let clock = TestClock::new();
        let now = clock.now();
        let mut reports = [report(
            "a",
            snapshot(
                "BTC-USD",
                vec![view(VenueId::Kraken, "100", "102", 10, Integrity::Verified)],
            ),
            now,
        )];
        reports[0].received_at = Some(now);

        clock.advance(Duration::from_secs(30));
        let merged = merge(&reports, clock.now(), 1, GatewayPolicy::default());

        assert_eq!(merged.nodes_used(), 0);
        assert_eq!(merged.snapshot.channel.dropped, 0);
        assert_eq!(merged.snapshot.channel.capacity, 0);
    }
}
