//! Kraken WebSocket v2 `book` channel.
//!
//! The snapshot arrives over the websocket itself, like Coinbase. Unlike
//! Coinbase, there is **no sequence number anywhere in the protocol** — the
//! only integrity signal is a CRC32 checksum, sent with *every* book message
//! (snapshot and update alike), computed over the top 10 levels of the book
//! the client actually built. That makes it [`Integrity::Verified`]: strictly
//! stronger than sequence-number gap detection, because it validates the
//! resulting state rather than the path taken to reach it, and so would catch
//! e.g. a delta applied to the wrong side, which no sequence number ever
//! would.
//!
//! References:
//! - <https://docs.kraken.com/api/docs/websocket-v2/book/>
//! - <https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2>

use ma_core::{Book, Integrity, Level, Price, Qty, Side, VenueId};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error as DeError};

use crate::sync::{Ingested, RawFrame, RecoveryStrategy, SyncAction, VenueError, VenueSync};

use super::common;

/// Deserialize a Kraken price/qty field by capturing its exact source text
/// before any numeric interpretation.
///
/// Kraken sends these as bare JSON numbers, not strings. Deserializing a bare
/// JSON number the ordinary way passes it through `f64`, which silently
/// drops trailing zeros: `0.00100000` becomes `0.001`, which after the
/// checksum's own point-removal and leading-zero-stripping becomes `"1"`
/// instead of the `"100000"` Kraken actually hashed — a completely different
/// checksum for a numerically identical value. `RawValue` captures the JSON
/// number's literal text and hands it to `Decimal::from_str` directly, so the
/// digits Kraken sent are the digits we hash.
fn exact_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
    raw.get()
        .parse::<Decimal>()
        .map_err(|e| DeError::custom(format!("bad decimal {:?}: {e}", raw.get())))
}

/// `data` is captured as unparsed source text rather than typed here, and the
/// choice is forced from two directions at once.
///
/// **Correctness:** it must not be a `serde_json::Value`. `Value` stores
/// numbers as `f64`, which discards exactly the trailing zeros Kraken's
/// checksum is computed over — the failure `exact_decimal` exists to prevent
/// would simply move one level up and become invisible again. `RawValue`
/// keeps the literal bytes, so the second parse sees the digits the venue
/// sent.
///
/// **Robustness:** it must not be `Vec<BookData>` either. Kraken sends a
/// `status` message on every connection whose `data` has no `symbol` field,
/// so typing the envelope eagerly failed the whole frame before `channel` was
/// ever looked at — one parse error per connection, on a counter whose entire
/// job is to signal venue schema drift. Found by replaying a live tape;
/// Bitstamp's envelope already defers its `data` parse for the same reason,
/// against its own subscription acks.
#[derive(Deserialize)]
struct Envelope {
    #[serde(default)]
    channel: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    data: Option<Box<serde_json::value::RawValue>>,
}

#[derive(Deserialize)]
struct BookData {
    symbol: String,
    #[serde(default)]
    bids: Vec<WireLevel>,
    #[serde(default)]
    asks: Vec<WireLevel>,
    checksum: Option<u32>,
    /// Kraken's own clock, RFC 3339, present on `update` messages and absent
    /// from `snapshot` ones. Reported as skew, never used for ordering — which
    /// is just as well, since a field that appears on only some messages could
    /// not order anything anyway.
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct WireLevel {
    #[serde(deserialize_with = "exact_decimal")]
    price: Decimal,
    #[serde(deserialize_with = "exact_decimal")]
    qty: Decimal,
}

fn to_levels(wire: &[WireLevel]) -> Result<Vec<Level>, VenueError> {
    wire.iter()
        .map(|w| {
            let qty = Qty::from_decimal(w.qty).map_err(|e| VenueError::Malformed(e.to_string()))?;
            Ok(Level::new(Price::from_decimal(w.price), qty))
        })
        .collect()
}

/// Hash a book the way Kraken hashes it: CRC32 over the top 10 asks
/// (best/lowest first), then the top 10 bids (best/highest first), each level
/// as price-digits-then-qty-digits with the decimal point removed and leading
/// zeros stripped. Trailing zeros are kept — they are digits Kraken sent, not
/// padding we can discard.
///
/// [`Book::top_levels`] already returns each side in exactly this order (see
/// its doc comment), so there is no re-sorting to do here — which is itself
/// a small piece of evidence that ordering belonged on `Book`, not here.
pub fn checksum(book: &Book) -> u32 {
    let mut buf = String::new();
    for level in book.top_levels(Side::Ask, 10) {
        push_digits(&mut buf, &level.price.to_string());
        push_digits(&mut buf, &level.qty.to_string());
    }
    for level in book.top_levels(Side::Bid, 10) {
        push_digits(&mut buf, &level.price.to_string());
        push_digits(&mut buf, &level.qty.to_string());
    }
    crc32fast::hash(buf.as_bytes())
}

fn push_digits(buf: &mut String, rendered: &str) {
    let no_point: String = rendered.chars().filter(|&c| c != '.').collect();
    let trimmed = no_point.trim_start_matches('0');
    buf.push_str(if trimmed.is_empty() { "0" } else { trimmed });
}

/// Sync discipline for one Kraken pair (e.g. `BTC/USD`).
///
/// Kraken spells pairs with a slash; that spelling is what this struct
/// expects on the wire.
#[derive(Debug)]
pub struct KrakenSync {
    symbol: String,
}

impl KrakenSync {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
        }
    }
}

impl VenueSync for KrakenSync {
    fn venue(&self) -> VenueId {
        VenueId::Kraken
    }

    fn integrity(&self) -> Integrity {
        Integrity::Verified
    }

    fn recovery(&self) -> RecoveryStrategy {
        RecoveryStrategy::Resubscribe
    }

    fn checksum(&self, book: &Book) -> Option<u32> {
        Some(checksum(book))
    }

    // Kraken's protocol carries no sequence counter or connection-scoped
    // state at all — the checksum is computed fresh from whatever the book
    // holds, every time. There is nothing here to forget on reconnect.
    fn reset(&mut self) {}

    fn ingest(&mut self, frame: &RawFrame) -> Result<Ingested, VenueError> {
        let envelope: Envelope = serde_json::from_slice(&frame.payload)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        let Some(channel) = envelope.channel.as_deref() else {
            // Kraken's request/response acks (subscribe confirmations, errors)
            // use a "method"/"success" shape with no "channel" field at all.
            // Out of scope here; the live ingest task surfaces a failed
            // subscribe.
            return Ok(Ingested::ignored());
        };

        match channel {
            "book" => {
                let Some(kind) = envelope.kind.as_deref() else {
                    return Err(VenueError::Malformed(
                        "book message carried no \"type\"".to_owned(),
                    ));
                };

                let entries: Vec<BookData> = match &envelope.data {
                    // Re-parsed from the captured source text, so the price
                    // and quantity digits are the ones Kraken actually sent.
                    Some(raw) => serde_json::from_str(raw.get())
                        .map_err(|e| VenueError::Malformed(e.to_string()))?,
                    None => {
                        return Err(VenueError::Malformed(
                            "book message carried no \"data\"".to_owned(),
                        ));
                    }
                };

                let mut actions = Vec::new();
                let mut venue_ts = None;
                for entry in entries {
                    // A shared connection subscribed to more than one pair
                    // would interleave other pairs' data here; skip what
                    // isn't ours rather than treating it as an error.
                    if entry.symbol != self.symbol {
                        continue;
                    }

                    // Last one wins. A frame carrying several entries for our
                    // own symbol is not something Kraken does, and if it ever
                    // did, the newest claim is the least misleading one to
                    // report as skew.
                    venue_ts = entry
                        .timestamp
                        .as_deref()
                        .and_then(common::parse_rfc3339)
                        .or(venue_ts);

                    let bids = to_levels(&entry.bids)?;
                    let asks = to_levels(&entry.asks)?;
                    actions.push(match kind {
                        "snapshot" => SyncAction::Snapshot { bids, asks },
                        "update" => SyncAction::Delta { bids, asks },
                        other => {
                            return Err(VenueError::Malformed(format!(
                                "unknown book message type {other:?}"
                            )));
                        }
                    });

                    // Both snapshot and update messages carry a checksum, so
                    // the book is verified after every single message, not
                    // just after updates.
                    if let Some(checksum) = entry.checksum {
                        actions.push(SyncAction::Verify { checksum });
                    }
                }
                Ok(Ingested::untimed(actions).at(venue_ts))
            }

            "heartbeat" => Ok(Ingested::untimed(vec![SyncAction::Forward(
                ma_core::EventKind::Heartbeat { counter: None },
            )])),

            _ => Ok(Ingested::ignored()),
        }
    }
}
