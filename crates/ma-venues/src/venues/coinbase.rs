//! Coinbase Advanced Trade `l2_data` channel.
//!
//! The snapshot arrives over the websocket itself (no REST call needed), and
//! every message on the channel carries a `sequence_num` that increases by
//! exactly one. A gap is detected the instant the next message arrives, which
//! is [`Integrity::GapDetectable`] — strictly weaker than Kraken's checksum
//! (which validates the book we actually built), strictly stronger than
//! Bitstamp's bare ordering (which can't see a gap at all).
//!
//! Reference: <https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels>

use ma_core::{DesyncReason, EventKind, Integrity, Level, VenueId};
use serde::Deserialize;

use crate::sync::{RecoveryStrategy, SyncAction, VenueError, VenueSync};

use super::common;

#[derive(Deserialize)]
struct Envelope {
    channel: String,
    sequence_num: u64,
    #[serde(default)]
    events: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum L2Kind {
    Snapshot,
    Update,
}

#[derive(Deserialize)]
struct L2Event {
    #[serde(rename = "type")]
    kind: L2Kind,
    product_id: String,
    updates: Vec<L2Update>,
}

/// Coinbase's spelling of the ask side is `"offer"` in the Advanced Trade
/// `l2_data` payloads, though `"ask"` appears in parts of the documentation
/// and in this crate's hand-authored fixtures. Both are accepted.
///
/// Accepting an alias rather than picking one is the cautious choice for a
/// field whose only failure mode is total: get it wrong and every ask update
/// is a parse error, the book holds bids only, and — because a one-sided book
/// can never cross — nothing detects it. The `parse_errors` counter would
/// climb, which is the signal that matters, but the book would still be
/// served. A tape recorded from the live feed is the authority on which
/// spelling actually arrives; see `tapes/`.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum CbSide {
    Bid,
    #[serde(alias = "offer")]
    Ask,
}

#[derive(Deserialize)]
struct L2Update {
    side: CbSide,
    price_level: String,
    new_quantity: String,
}

#[derive(Deserialize)]
struct HeartbeatEvent {
    heartbeat_counter: u64,
}

/// Sync discipline for one Coinbase product (e.g. `BTC-USD`).
///
/// Coinbase spells symbols with a hyphen; that spelling is what this struct
/// expects on the wire; translating to and from `ma_core::Symbol`'s
/// normalised form is the caller's job when it constructs a [`VenueBook`],
/// same as every other venue.
///
/// [`VenueBook`]: crate::sync::VenueBook
#[derive(Debug)]
pub struct CoinbaseSync {
    product_id: String,
    expected_seq: Option<u64>,
}

impl CoinbaseSync {
    pub fn new(product_id: impl Into<String>) -> Self {
        Self {
            product_id: product_id.into(),
            expected_seq: None,
        }
    }

    /// Contiguity check: exactly the same shape as every other
    /// `GapDetectable` venue, because that's what the guarantee *is*.
    fn check_seq(&mut self, seq: u64) -> Option<DesyncReason> {
        let expected = self.expected_seq.replace(seq + 1);
        match expected {
            Some(expected) if seq != expected => {
                Some(DesyncReason::SequenceGap { expected, got: seq })
            }
            _ => None,
        }
    }

    fn split_updates(
        &self,
        updates: Vec<L2Update>,
    ) -> Result<(Vec<Level>, Vec<Level>), VenueError> {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for u in updates {
            let level = common::level_from_str_pair(&u.price_level, &u.new_quantity)?;
            match u.side {
                CbSide::Bid => bids.push(level),
                CbSide::Ask => asks.push(level),
            }
        }
        Ok((bids, asks))
    }
}

impl VenueSync for CoinbaseSync {
    fn venue(&self) -> VenueId {
        VenueId::Coinbase
    }

    fn integrity(&self) -> Integrity {
        Integrity::GapDetectable
    }

    fn recovery(&self) -> RecoveryStrategy {
        RecoveryStrategy::Resubscribe
    }

    fn reset(&mut self) {
        self.expected_seq = None;
    }

    fn ingest(&mut self, frame: &crate::sync::RawFrame) -> Result<Vec<SyncAction>, VenueError> {
        let envelope: Envelope = serde_json::from_slice(&frame.payload)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        match envelope.channel.as_str() {
            "l2_data" => {
                // The gap check covers the whole message, not each event
                // inside it: sequence_num is a property of the envelope Coinbase
                // sent, not of any individual product's data within it.
                if let Some(reason) = self.check_seq(envelope.sequence_num) {
                    return Ok(vec![SyncAction::Desync(reason)]);
                }

                let mut actions = Vec::with_capacity(envelope.events.len());
                for raw in envelope.events {
                    let event: L2Event = serde_json::from_value(raw)
                        .map_err(|e| VenueError::Malformed(e.to_string()))?;

                    // A shared connection subscribed to more than one product
                    // would interleave other products' events here. We only
                    // own one product's book, so anything else is skipped
                    // rather than treated as an error — it isn't wrong, it's
                    // just not ours.
                    if event.product_id != self.product_id {
                        continue;
                    }

                    let (bids, asks) = self.split_updates(event.updates)?;
                    actions.push(match event.kind {
                        L2Kind::Snapshot => SyncAction::Snapshot { bids, asks },
                        L2Kind::Update => SyncAction::Delta { bids, asks },
                    });
                }
                Ok(actions)
            }

            "heartbeats" => {
                let mut actions = Vec::with_capacity(envelope.events.len());
                for raw in envelope.events {
                    let hb: HeartbeatEvent = serde_json::from_value(raw)
                        .map_err(|e| VenueError::Malformed(e.to_string()))?;
                    actions.push(SyncAction::Forward(EventKind::Heartbeat {
                        counter: Some(hb.heartbeat_counter),
                    }));
                }
                Ok(actions)
            }

            // Subscription acks, error frames, and any channel we haven't
            // wired up carry no book content. Out of scope here; the live
            // ingest task (v1/8) is where a subscribe failure gets surfaced.
            _ => Ok(vec![SyncAction::Ignore]),
        }
    }
}
