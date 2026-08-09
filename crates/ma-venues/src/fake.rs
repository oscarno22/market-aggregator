//! A scripted venue, for proving gap-fill correctness offline.
//!
//! The brief is explicit that gap-fill correctness is proven here and not in
//! production, so this module is load-bearing rather than test scaffolding.
//!
//! The working method is **generate correct, then corrupt**. A [`Script`]
//! builds a well-formed stream and keeps a shadow book alongside it so that
//! checksums are genuinely correct for the intended state. The resulting
//! [`Tape`] is then damaged — drop a frame, duplicate one, swap two — which is
//! exactly the division of labour in reality: venues send good data and
//! networks damage it.
//!
//! The fake speaks one wire format but can be run at any [`Integrity`], which
//! is what lets the suite assert the thing that actually matters: that a
//! [`Integrity::OrderOnly`] venue *fails to notice* a dropped message. If that
//! test ever goes green by detecting the loss, the enum is lying.

use ma_core::{
    Book, Clock, DesyncReason, IngestTime, Integrity, Level, Side, StreamId, Symbol, TestClock,
    VenueId,
};
use serde::{Deserialize, Serialize};

use crate::sync::{Ingested, RawFrame, RecoveryStrategy, SyncAction, VenueError, VenueSync};

/// Gap between synthetic frames. Arbitrary, but fixed so replays are
/// bit-identical run to run.
const FRAME_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// The fake wire format.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum FakeMsg {
    Snapshot {
        seq: u64,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
    Delta {
        seq: u64,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
    },
    Checksum {
        seq: u64,
        value: u32,
    },
}

impl FakeMsg {
    fn seq(&self) -> u64 {
        match self {
            Self::Snapshot { seq, .. } | Self::Delta { seq, .. } | Self::Checksum { seq, .. } => {
                *seq
            }
        }
    }
}

fn to_levels(pairs: &[(String, String)]) -> Result<Vec<Level>, VenueError> {
    pairs
        .iter()
        .map(|(p, q)| {
            let price = p
                .parse()
                .map_err(|_| VenueError::Malformed(format!("price {p:?}")))?;
            let qty = q
                .parse()
                .map_err(|_| VenueError::Malformed(format!("qty {q:?}")))?;
            Ok(Level::new(price, qty))
        })
        .collect()
}

fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(p, q)| ((*p).to_owned(), (*q).to_owned()))
        .collect()
}

/// Hash a book the way the fake venue does.
///
/// Modelled on Kraken's scheme — CRC32 over the top ten levels of each side,
/// rendered in the venue's own digits — because the point of the fake is to
/// exercise the code path Kraken will use, not to invent a third one.
pub fn fake_checksum(book: &Book) -> u32 {
    let mut buf = String::new();
    for side in [Side::Bid, Side::Ask] {
        for level in book.top_levels(side, 10) {
            buf.push_str(&level.price.to_string());
            buf.push(':');
            buf.push_str(&level.qty.to_string());
            buf.push('|');
        }
    }
    crc32fast::hash(buf.as_bytes())
}

/// Builds a well-formed frame sequence, tracking a shadow book so that
/// checkpoints carry checksums that are actually right.
#[derive(Debug)]
pub struct Script {
    frames: Vec<RawFrame>,
    seq: u64,
    shadow: Book,
    clock: TestClock,
    symbol: Symbol,
}

impl Default for Script {
    fn default() -> Self {
        Self::new()
    }
}

impl Script {
    pub fn new() -> Self {
        Self::for_symbol(Symbol::new("BTC-USD"))
    }

    /// A script for a named symbol.
    ///
    /// Multi-symbol tests need this: the frames a script produces carry a
    /// [`StreamId`], and a [`VenueBook`](crate::sync::VenueBook) for `ETH-USD`
    /// fed frames stamped `BTC-USD` would be testing the wrong thing.
    pub fn for_symbol(symbol: Symbol) -> Self {
        Self {
            frames: Vec::new(),
            seq: 0,
            shadow: Book::new(VenueId::Fake, symbol.clone()),
            clock: TestClock::new(),
            symbol,
        }
    }

    fn push(&mut self, msg: &FakeMsg) {
        let payload = serde_json::to_vec(msg).unwrap_or_else(|e| {
            // Serialising our own type cannot fail in practice; encode the
            // failure into the tape rather than panicking a test helper.
            format!(r#"{{"type":"malformed","error":"{e}"}}"#).into_bytes()
        });
        self.frames.push(RawFrame::new(
            StreamId::new(VenueId::Fake, self.symbol.clone()),
            payload,
            self.clock.now(),
        ));
        self.clock.advance(FRAME_INTERVAL);
    }

    /// Emit a snapshot and reset the shadow book to it.
    pub fn snapshot(mut self, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Self {
        self.seq += 1;
        let msg = FakeMsg::Snapshot {
            seq: self.seq,
            bids: owned(bids),
            asks: owned(asks),
        };

        if let (Ok(b), Ok(a)) = (to_levels(&owned(bids)), to_levels(&owned(asks))) {
            let _ = self
                .shadow
                .apply_snapshot(&b, &a, Integrity::Verified, self.clock.now());
        }
        self.push(&msg);
        self
    }

    /// Emit an incremental update. Quantity `"0"` deletes a level.
    pub fn delta(mut self, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> Self {
        self.seq += 1;
        let msg = FakeMsg::Delta {
            seq: self.seq,
            bids: owned(bids),
            asks: owned(asks),
        };

        if let (Ok(b), Ok(a)) = (to_levels(&owned(bids)), to_levels(&owned(asks))) {
            let _ = self.shadow.apply_delta(&b, &a, self.clock.now());
        }
        self.push(&msg);
        self
    }

    /// Emit a checksum over the book as it should be at this point.
    ///
    /// This is the frame that catches loss on a venue with no sequence numbers,
    /// so scripts exercising [`Integrity::Verified`] need at least one.
    pub fn checkpoint(mut self) -> Self {
        self.seq += 1;
        let msg = FakeMsg::Checksum {
            seq: self.seq,
            value: fake_checksum(&self.shadow),
        };
        self.push(&msg);
        self
    }

    pub fn build(self) -> Tape {
        Tape {
            frames: self.frames,
            symbol: self.symbol,
        }
    }
}

/// A recorded frame sequence, and the ways a network can ruin one.
#[derive(Clone, Debug)]
pub struct Tape {
    frames: Vec<RawFrame>,
    symbol: Symbol,
}

impl Default for Tape {
    fn default() -> Self {
        Self {
            frames: Vec::new(),
            symbol: Symbol::new("BTC-USD"),
        }
    }
}

impl Tape {
    pub fn frames(&self) -> &[RawFrame] {
        &self.frames
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Lose a message in transit. The defining failure this project exists to
    /// survive.
    pub fn drop_at(mut self, index: usize) -> Self {
        if index < self.frames.len() {
            self.frames.remove(index);
        }
        self
    }

    /// Deliver a message twice. Common on reconnect, when a venue replays.
    pub fn duplicate_at(mut self, index: usize) -> Self {
        if let Some(frame) = self.frames.get(index).cloned() {
            self.frames.insert(index + 1, frame);
        }
        self
    }

    /// Deliver two messages out of order.
    pub fn swap(mut self, a: usize, b: usize) -> Self {
        if a < self.frames.len() && b < self.frames.len() {
            self.frames.swap(a, b);
        }
        self
    }

    /// Cut the stream off, as a dropped connection would.
    pub fn truncate_from(mut self, index: usize) -> Self {
        self.frames.truncate(index);
        self
    }
}

/// A venue that behaves exactly as badly as you tell it to.
///
/// `integrity` is a constructor parameter rather than a constant because the
/// same scripted stream must be runnable under all three disciplines. That is
/// the only way to demonstrate that the weakest one really is weaker.
#[derive(Debug)]
pub struct FakeSync {
    integrity: Integrity,
    recovery: RecoveryStrategy,
    /// Next sequence number expected. `None` until the first message lands.
    expected_seq: Option<u64>,
}

impl FakeSync {
    pub fn new(integrity: Integrity) -> Self {
        Self {
            integrity,
            recovery: match integrity {
                // Mirrors the real mapping: the venues that resend snapshots
                // unprompted recover by resubscribing.
                Integrity::OrderOnly => RecoveryStrategy::RestSnapshot,
                Integrity::GapDetectable | Integrity::Verified => RecoveryStrategy::Resubscribe,
            },
            expected_seq: None,
        }
    }

    /// Check the ordering field according to this venue's discipline.
    ///
    /// The three arms are the whole point of the type:
    ///
    /// - `Verified` ignores sequence entirely, because Kraken has none. Loss is
    ///   caught later, by the checksum, on the state rather than the path.
    /// - `GapDetectable` demands contiguity and reports the hole immediately.
    /// - `OrderOnly` can only tell that time moved forwards. A hole passes
    ///   through here undetected, by construction, because that is the truth
    ///   about Bitstamp.
    fn check_seq(&mut self, seq: u64) -> Option<DesyncReason> {
        let expected = self.expected_seq.replace(seq + 1);

        match self.integrity {
            Integrity::Verified => None,

            Integrity::GapDetectable => match expected {
                Some(expected) if seq != expected => {
                    Some(DesyncReason::SequenceGap { expected, got: seq })
                }
                _ => None,
            },

            Integrity::OrderOnly => match expected {
                // The ordering field must strictly increase, so replays and
                // reordering are caught. A *gap* is not: it looks exactly like
                // normal forward progress, and no amount of care here can
                // change that. This arm is the honest limit of what Bitstamp
                // makes knowable.
                Some(expected) if seq < expected => Some(DesyncReason::TimestampRegression {
                    last_micros: expected.saturating_sub(1),
                    got_micros: seq,
                }),
                _ => None,
            },
        }
    }
}

impl VenueSync for FakeSync {
    fn venue(&self) -> VenueId {
        VenueId::Fake
    }

    fn integrity(&self) -> Integrity {
        self.integrity
    }

    fn recovery(&self) -> RecoveryStrategy {
        self.recovery
    }

    /// Only a [`Integrity::Verified`] venue can hash our book, because only a
    /// venue that publishes checksums has told us how. This is the coupling
    /// that stops `Verified` from being a label anyone can apply to themselves.
    fn checksum(&self, book: &Book) -> Option<u32> {
        match self.integrity {
            Integrity::Verified => Some(fake_checksum(book)),
            Integrity::OrderOnly | Integrity::GapDetectable => None,
        }
    }

    fn reset(&mut self) {
        self.expected_seq = None;
    }

    fn ingest(&mut self, frame: &RawFrame) -> Result<Ingested, VenueError> {
        if frame.venue() != VenueId::Fake {
            return Err(VenueError::WrongVenue {
                expected: VenueId::Fake,
                got: frame.venue(),
            });
        }

        let msg: FakeMsg = serde_json::from_slice(&frame.payload)
            .map_err(|e| VenueError::Malformed(e.to_string()))?;

        // A snapshot re-establishes ground truth, so it resets the sequence
        // expectation instead of being checked against it.
        if let FakeMsg::Snapshot { seq, bids, asks } = &msg {
            self.expected_seq = Some(seq + 1);
            return Ok(Ingested::untimed(vec![SyncAction::Snapshot {
                bids: to_levels(bids)?,
                asks: to_levels(asks)?,
            }]));
        }

        if let Some(reason) = self.check_seq(msg.seq()) {
            return Ok(Ingested::untimed(vec![SyncAction::Desync(reason)]));
        }

        Ok(Ingested::untimed(match msg {
            FakeMsg::Delta { bids, asks, .. } => vec![SyncAction::Delta {
                bids: to_levels(&bids)?,
                asks: to_levels(&asks)?,
            }],
            // A tape may carry checksums even when the sync under test runs at
            // a weaker integrity — that is how the same script gets replayed
            // under all three disciplines. A venue that does not publish
            // checksums must ignore them rather than quietly gaining a
            // guarantee its protocol does not offer.
            FakeMsg::Checksum { value, .. } => match self.integrity {
                Integrity::Verified => vec![SyncAction::Verify { checksum: value }],
                Integrity::OrderOnly | Integrity::GapDetectable => vec![SyncAction::Ignore],
            },
            FakeMsg::Snapshot { .. } => unreachable!("handled above"),
        }))
    }
}

/// Convenience for tests: run a whole tape through a fresh book.
pub fn run(tape: &Tape, integrity: Integrity) -> crate::sync::VenueBook {
    let mut vb =
        crate::sync::VenueBook::new(Box::new(FakeSync::new(integrity)), tape.symbol().clone());
    for frame in tape.frames() {
        let _ = vb.feed(frame);
    }
    vb
}

/// The instant a tape ends, for age assertions after a run.
pub fn tape_end(tape: &Tape) -> Option<IngestTime> {
    tape.frames().last().map(|f| f.ingest_ts)
}
