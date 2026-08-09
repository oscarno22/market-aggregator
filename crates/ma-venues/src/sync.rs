//! The venue boundary: raw frames in, book instructions out.
//!
//! Everything venue-specific lives behind [`VenueSync`]. The three real venues
//! disagree about almost everything — where the snapshot comes from, what an
//! ordering field even is, whether loss is detectable — and this trait is the
//! seam that keeps that disagreement from leaking downstream.
//!
//! A [`VenueSync`] is a pure state machine. It owns no socket and no book, and
//! it never sleeps. That is what lets the scripted fake in [`crate::fake`] drive
//! the exact same code path a live connection does.

use std::fmt;

use ma_core::{
    Book, DesyncReason, EventKind, IngestTime, Integrity, Level, MarketEvent, Symbol, VenueId,
};

/// Bytes as they came off the wire, before anything was parsed.
///
/// This is deliberately the unit the tape recorder writes. Recording *after*
/// parsing would mean a recorded session could never reproduce a parser bug or
/// a venue schema change — the two failures most likely to happen unattended.
#[derive(Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub venue: VenueId,
    pub payload: Vec<u8>,
    pub ingest_ts: IngestTime,
}

impl RawFrame {
    pub fn new(venue: VenueId, payload: impl Into<Vec<u8>>, ingest_ts: IngestTime) -> Self {
        Self {
            venue,
            payload: payload.into(),
            ingest_ts,
        }
    }

    pub fn as_str(&self) -> Result<&str, VenueError> {
        std::str::from_utf8(&self.payload).map_err(|_| VenueError::NotUtf8)
    }
}

impl fmt::Debug for RawFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Show the payload as text where possible; a hex dump of JSON helps
        // nobody reading a failing test.
        match std::str::from_utf8(&self.payload) {
            Ok(s) => write!(f, "RawFrame({}, {s:?})", self.venue),
            Err(_) => write!(f, "RawFrame({}, {} bytes)", self.venue, self.payload.len()),
        }
    }
}

/// What the venue layer tells the book to do.
#[derive(Clone, Debug, PartialEq)]
pub enum SyncAction {
    /// Subscription acks, pongs, and anything else with no book meaning.
    Ignore,
    /// Replace the book wholesale. Ends a resync.
    Snapshot { bids: Vec<Level>, asks: Vec<Level> },
    /// Incremental update. Zero quantity deletes a level.
    Delta { bids: Vec<Level>, asks: Vec<Level> },
    /// Check the book we built against the venue's own hash of it.
    Verify { checksum: u32 },
    /// Non-book content to forward downstream (trades, heartbeats).
    Forward(EventKind),
    /// The stream is broken. Recover per [`VenueSync::recovery`].
    Desync(DesyncReason),
}

/// How a venue gets back to a good book after loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryStrategy {
    /// Drop the socket, reconnect, resubscribe; the venue resends a snapshot
    /// unprompted. **Coinbase and Kraken.**
    ///
    /// Simpler than the alternative, and the reason neither venue needs a REST
    /// client at all.
    Resubscribe,
    /// Keep the socket, buffer arriving deltas, fetch a REST snapshot, discard
    /// the buffered deltas the snapshot already contains, splice on the rest.
    /// **Bitstamp.**
    ///
    /// This is the algorithm the original design brief described as though it
    /// were universal. It applies to exactly one of our three venues.
    RestSnapshot,
}

#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    #[error("frame was not valid UTF-8")]
    NotUtf8,
    #[error("could not parse frame: {0}")]
    Malformed(String),
    #[error("frame was for {got}, but this is a {expected} sync")]
    WrongVenue { expected: VenueId, got: VenueId },
}

/// A venue's wire protocol and its integrity discipline.
pub trait VenueSync: fmt::Debug + Send {
    fn venue(&self) -> VenueId;

    /// What this venue can actually prove about a synced book.
    ///
    /// This is not a description of how carefully the code was written. It is a
    /// property of the venue's protocol, and it caps what any consumer
    /// downstream is entitled to believe.
    fn integrity(&self) -> Integrity;

    fn recovery(&self) -> RecoveryStrategy;

    /// Feed one raw frame.
    ///
    /// Returns several actions rather than one because a resync completes in a
    /// burst: the snapshot arrives and the buffered deltas that survived the
    /// splice must be applied immediately after it, atomically from the book's
    /// point of view. Buffering lives here, in the venue, because only the
    /// venue knows how to decide which buffered deltas the snapshot subsumed.
    fn ingest(&mut self, frame: &RawFrame) -> Result<Vec<SyncAction>, VenueError>;

    /// Hash the book the way this venue hashes it, for [`SyncAction::Verify`].
    ///
    /// `None` for every venue that publishes no checksum — which is all of them
    /// except Kraken. A venue returning `None` here can never reach
    /// [`Integrity::Verified`], and that is the intended coupling.
    fn checksum(&self, _book: &Book) -> Option<u32> {
        None
    }

    /// Return to the pre-subscription state after a reconnect.
    fn reset(&mut self);
}

/// A venue's sync state machine paired with the book it maintains.
///
/// This is the unit the aggregator owns one of, per (venue, symbol). It is
/// deliberately thin: all the interesting decisions were made by the
/// [`VenueSync`] before the actions got here.
#[derive(Debug)]
pub struct VenueBook {
    sync: Box<dyn VenueSync>,
    book: Book,
}

/// Something worth telling the rest of the system about.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// The book's trust level changed. Always reported — a state change that
    /// nobody hears about is the silent-failure mode this project exists to
    /// avoid.
    StateChanged {
        from: ma_core::BookState,
        to: ma_core::BookState,
    },
    /// A non-book event to forward downstream.
    Event(MarketEvent),
}

impl VenueBook {
    pub fn new(sync: Box<dyn VenueSync>, symbol: Symbol) -> Self {
        let book = Book::new(sync.venue(), symbol);
        Self { sync, book }
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn integrity(&self) -> Integrity {
        self.sync.integrity()
    }

    pub fn recovery(&self) -> RecoveryStrategy {
        self.sync.recovery()
    }

    /// Called after a reconnect: forget the stream position and distrust the
    /// book until a fresh snapshot lands.
    pub fn reset(&mut self, at: IngestTime) {
        self.sync.reset();
        self.book.mark_desynced(DesyncReason::ConnectionLost, at);
    }

    /// Feed one frame; apply whatever it implies.
    pub fn feed(&mut self, frame: &RawFrame) -> Result<Vec<Outcome>, VenueError> {
        let before = self.book.state();
        let actions = self.sync.ingest(frame)?;
        let at = frame.ingest_ts;
        let mut outcomes = Vec::new();

        for action in actions {
            match action {
                SyncAction::Ignore => {}

                SyncAction::Snapshot { bids, asks } => {
                    // A crossed snapshot desyncs the book from inside
                    // `apply_snapshot`; the error is the same information as
                    // the state change we report below.
                    let _ = self
                        .book
                        .apply_snapshot(&bids, &asks, self.sync.integrity(), at);
                }

                SyncAction::Delta { bids, asks } => {
                    if self.book.apply_delta(&bids, &asks, at).is_err() {
                        // Deltas arriving while desynced are expected during a
                        // resync — the venue is still buffering. Dropping them
                        // here is correct; applying them is how books go
                        // silently wrong.
                        continue;
                    }
                }

                SyncAction::Verify { checksum } => self.verify(checksum, at),

                SyncAction::Forward(kind) => outcomes.push(Outcome::Event(MarketEvent {
                    venue: self.book.venue(),
                    symbol: self.book.symbol().clone(),
                    venue_ts: None,
                    ingest_ts: at,
                    kind,
                })),

                SyncAction::Desync(reason) => self.book.mark_desynced(reason, at),
            }
        }

        let after = self.book.state();
        if before != after {
            outcomes.push(Outcome::StateChanged {
                from: before,
                to: after,
            });
        }
        Ok(outcomes)
    }

    fn verify(&mut self, expected: u32, at: IngestTime) {
        let Some(computed) = self.sync.checksum(&self.book) else {
            // A venue that sends a checksum but has no way to compute one over
            // our book is a bug in that venue's implementation, not a desync.
            debug_assert!(false, "venue sent a checksum but implements no checksum fn");
            return;
        };

        if computed == expected {
            self.book.mark_verified(at);
        } else {
            self.book
                .mark_desynced(DesyncReason::ChecksumMismatch { expected, computed }, at);
        }
    }
}
