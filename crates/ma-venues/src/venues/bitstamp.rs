//! Bitstamp `diff_order_book_{pair}` channel.
//!
//! Bitstamp is the odd one out among the three venues, and deliberately kept:
//! it does **not** send a snapshot over the websocket at all. A connection
//! gets diffs from the moment it subscribes and nothing else; the only way to
//! get a base state is a separate REST call, buffering whatever arrives on
//! the socket in the meantime and splicing it onto the REST snapshot once it
//! lands. That is [`RecoveryStrategy::RestSnapshot`], and it is the algorithm
//! the original design brief described as though every venue used it.
//!
//! Ordering is a `microtimestamp` — not a counter. That is what makes this
//! venue [`Integrity::OrderOnly`]: strictly increasing timestamps prove
//! nothing arrived out of order, but say nothing at all about whether
//! something arrived at all. See [`crate::sync::RestSnapshot`] for why that
//! also means the splice can discard what the snapshot already covers but
//! cannot verify the result is complete — the "verify no hole" step in the
//! original brief has no equivalent here.
//!
//! This module owns the wire *shapes* for both the websocket diffs and the
//! REST snapshot body; making the actual HTTP request is the ingest task's
//! job in `ma-pipeline`; nothing here performs I/O.

use ma_core::{DesyncReason, EventKind, Integrity, Level, Side, VenueId};
use serde::Deserialize;

use crate::sync::{
    Ingested, RawFrame, RecoveryStrategy, RestSnapshot, SyncAction, VenueError, VenueSync,
};

use super::common;

#[derive(Deserialize)]
struct Envelope {
    event: String,
    channel: String,
    // Deliberately untyped here rather than `Option<DiffData>`: Bitstamp's
    // subscription-ack messages carry `"data": {}` — present, but not `null`
    // and not a valid DiffData (no microtimestamp). `Option<DiffData>` would
    // try to parse that `{}` as a DiffData and fail the whole envelope before
    // we ever get to check `event`. Deferring the typed parse to after that
    // check, like every other venue's ack handling, sidesteps it entirely.
    #[serde(default)]
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct DiffData {
    microtimestamp: String,
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

/// One print on the `live_trades_{pair}` channel.
///
/// Only the `_str` twins are read. Bitstamp also sends `price` and `amount`
/// as JSON floats, and touching those would undo the exact-digits discipline
/// every other number in this project follows — the floats exist on the wire,
/// and this struct's job is to make them unrepresentable here.
#[derive(Deserialize)]
struct TradeData {
    microtimestamp: String,
    price_str: String,
    amount_str: String,
    /// `0` is a buy, `1` is a sell — the taker's direction, per the docs.
    /// Anything else degrades to "side unknown" rather than failing the
    /// frame that carries the print.
    #[serde(rename = "type", default)]
    direction: Option<i64>,
}

#[derive(Deserialize)]
struct RestOrderBook {
    microtimestamp: String,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

/// Parse Bitstamp's `GET /api/v2/order_book/{pair}/` response body.
///
/// The wire shape belongs here with the rest of this venue's knowledge; the
/// HTTP request that produces `body` is made elsewhere, by code that's
/// allowed to touch a network.
pub fn parse_rest_snapshot(body: &str) -> Result<RestSnapshot, VenueError> {
    let parsed: RestOrderBook =
        serde_json::from_str(body).map_err(|e| VenueError::Malformed(e.to_string()))?;
    let as_of = parse_micros(&parsed.microtimestamp)?;
    Ok(RestSnapshot {
        bids: common::levels_from_str_pairs(&parsed.bids)?,
        asks: common::levels_from_str_pairs(&parsed.asks)?,
        as_of,
    })
}

fn parse_micros(raw: &str) -> Result<u64, VenueError> {
    raw.parse()
        .map_err(|_| VenueError::Malformed(format!("bad microtimestamp {raw:?}")))
}

/// Check strictly-increasing order, the one thing an `OrderOnly` venue can
/// prove. Free function rather than a method: it's used identically from live
/// ingest and from the post-snapshot splice, and taking `&mut u64` directly
/// keeps both call sites honest about what state it actually touches.
fn check_order(last_micros: &mut u64, micros: u64) -> Option<DesyncReason> {
    if micros <= *last_micros {
        Some(DesyncReason::TimestampRegression {
            last_micros: *last_micros,
            got_micros: micros,
        })
    } else {
        *last_micros = micros;
        None
    }
}

#[derive(Debug, Clone)]
struct PendingDiff {
    micros: u64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Debug)]
enum Mode {
    /// No REST snapshot applied yet. Every diff seen so far, oldest first —
    /// `ma-pipeline` will fetch a snapshot and hand it to
    /// [`BitstampSync::apply_rest_snapshot`], at which point this buffer gets
    /// spliced in and discarded.
    AwaitingSnapshot { pending: Vec<PendingDiff> },
    /// Snapshot applied; tracking the last-applied microtimestamp so ordering
    /// can still be checked going forward.
    Live {
        last_micros: u64,
        /// The `as_of` of the snapshot this book was built from.
        ///
        /// Kept separately from `last_micros` to tell two different things
        /// apart. A diff older than the snapshot is *redundant* — the
        /// snapshot already contains it by definition — whereas a diff older
        /// than something we have already applied on top of the snapshot is
        /// *reordering*, which is a real desync. Collapsing both into
        /// `last_micros` reports the first as the second.
        ///
        /// This is not hypothetical. The REST fetch runs concurrently with
        /// the read loop (see `ma_pipeline::ingest`), so diffs generated
        /// between the subscribe and the snapshot can arrive on either side
        /// of it. Landing after it must not desync a book that is perfectly
        /// correct.
        spliced_at: u64,
    },
}

/// Sync discipline for one Bitstamp pair (e.g. `btcusd`).
///
/// Holds both full channel names (`diff_order_book_btcusd`,
/// `live_trades_btcusd`) rather than the bare pair, since those are the
/// literal strings the wire envelopes carry and string comparison against
/// the wire is the whole job.
#[derive(Debug)]
pub struct BitstampSync {
    channel: String,
    trades_channel: String,
    mode: Mode,
}

impl BitstampSync {
    /// `pair` is Bitstamp's native spelling, e.g. `btcusd`.
    pub fn new(pair: impl AsRef<str>) -> Self {
        let pair = pair.as_ref();
        Self {
            channel: format!("diff_order_book_{pair}"),
            trades_channel: format!("live_trades_{pair}"),
            mode: Mode::AwaitingSnapshot {
                pending: Vec::new(),
            },
        }
    }
}

impl VenueSync for BitstampSync {
    fn venue(&self) -> VenueId {
        VenueId::Bitstamp
    }

    fn integrity(&self) -> Integrity {
        Integrity::OrderOnly
    }

    fn recovery(&self) -> RecoveryStrategy {
        RecoveryStrategy::RestSnapshot
    }

    fn reset(&mut self) {
        self.mode = Mode::AwaitingSnapshot {
            pending: Vec::new(),
        };
    }

    fn parse_rest_snapshot(&self, body: &str) -> Result<RestSnapshot, VenueError> {
        parse_rest_snapshot(body)
    }

    fn ingest(&mut self, frame: &RawFrame) -> Result<Ingested, VenueError> {
        let envelope: Envelope = serde_json::from_slice(&frame.payload)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        // Prints are handled before the book path and never touch it. In
        // particular a trade's microtimestamp is **not** fed to
        // `check_order`: trades and diffs are separate channels whose
        // timestamps interleave arbitrarily, so a print stamped older than
        // the last diff is normal delivery, and running it through the
        // ordering check would fabricate a `TimestampRegression` against a
        // book that is perfectly correct.
        if envelope.event == "trade" {
            if envelope.channel != self.trades_channel {
                return Err(VenueError::Malformed(format!(
                    "trade for channel {:?}, expected {:?}",
                    envelope.channel, self.trades_channel
                )));
            }
            let data: TradeData = serde_json::from_value(envelope.data)
                .map_err(|e| VenueError::Malformed(e.to_string()))?;
            let micros = parse_micros(&data.microtimestamp)?;
            let level = common::level_from_str_pair(&data.price_str, &data.amount_str)?;
            let actions = vec![SyncAction::Forward(EventKind::Trade {
                price: level.price,
                qty: level.qty,
                taker_side: match data.direction {
                    Some(0) => Some(Side::Bid),
                    Some(1) => Some(Side::Ask),
                    _ => None,
                },
            })];
            return Ok(Ingested::untimed(actions).at(Some(common::system_time_from_micros(micros))));
        }

        if envelope.event != "data" {
            // bts:subscription_succeeded, bts:error, and the rest carry no
            // book content. Out of scope here, same as every other venue's
            // acks — the live ingest task is where a failed subscribe surfaces.
            return Ok(Ingested::ignored());
        }
        if envelope.channel != self.channel {
            return Err(VenueError::Malformed(format!(
                "frame for channel {:?}, expected {:?}",
                envelope.channel, self.channel
            )));
        }
        let data: DiffData = serde_json::from_value(envelope.data)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let micros = parse_micros(&data.microtimestamp)?;
        let bids = common::levels_from_str_pairs(&data.bids)?;
        let asks = common::levels_from_str_pairs(&data.asks)?;

        // The one venue whose ordering field and whose clock are the same
        // number. It is still only ever *reported* as a timestamp: ordering
        // uses it because Bitstamp offers nothing else, and that limitation is
        // exactly what `Integrity::OrderOnly` names.
        let venue_ts = Some(common::system_time_from_micros(micros));

        let actions = match &mut self.mode {
            Mode::AwaitingSnapshot { pending } => {
                // Only the first buffered frame is reported as a state change;
                // the rest are absorbed silently so `Book`'s `since` timestamp
                // reflects when we actually lost trust, not the latest diff
                // that happened to arrive while still waiting.
                let first = pending.is_empty();
                pending.push(PendingDiff { micros, bids, asks });
                vec![if first {
                    SyncAction::Desync(DesyncReason::AwaitingSnapshot)
                } else {
                    SyncAction::Ignore
                }]
            }

            Mode::Live {
                last_micros,
                spliced_at,
            } => {
                if micros <= *spliced_at {
                    // Already in the snapshot. Applying it would be harmless
                    // but pointless; calling it a regression would be a lie.
                    return Ok(Ingested::ignored().at(venue_ts));
                }
                vec![match check_order(last_micros, micros) {
                    Some(reason) => SyncAction::Desync(reason),
                    None => SyncAction::Delta { bids, asks },
                }]
            }
        };

        Ok(Ingested::untimed(actions).at(venue_ts))
    }

    fn apply_rest_snapshot(&mut self, snapshot: RestSnapshot) -> Vec<SyncAction> {
        let snapshot_as_of = snapshot.as_of;
        let pending = match &mut self.mode {
            Mode::AwaitingSnapshot { pending } => std::mem::take(pending),
            // A REST re-snapshot arriving while already live is the v2
            // periodic audit, not a reconnect: there is no buffer to splice,
            // just a fresh anchor point to check future diffs against.
            Mode::Live { .. } => Vec::new(),
        };

        // Discard whatever the snapshot already covers; keep the rest in
        // order. There is no way to additionally verify that nothing is
        // missing between the snapshot and the earliest survivor — see the
        // doc comment on `RestSnapshot` for why a timestamp can't do that.
        let mut surviving: Vec<PendingDiff> = pending
            .into_iter()
            .filter(|d| d.micros > snapshot.as_of)
            .collect();
        surviving.sort_by_key(|d| d.micros);

        let mut last_micros = snapshot.as_of;
        let mut actions = vec![SyncAction::Snapshot {
            bids: snapshot.bids,
            asks: snapshot.asks,
        }];
        for diff in surviving {
            actions.push(match check_order(&mut last_micros, diff.micros) {
                Some(reason) => SyncAction::Desync(reason),
                None => SyncAction::Delta {
                    bids: diff.bids,
                    asks: diff.asks,
                },
            });
        }

        self.mode = Mode::Live {
            last_micros,
            spliced_at: snapshot_as_of,
        };
        actions
    }
}
