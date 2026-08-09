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
use serde::{Deserialize, Serialize};

/// Which network call produced a frame.
///
/// Only Bitstamp ever produces the second variant, but it is on the shared
/// frame type rather than hidden inside that venue for one specific reason:
/// the tape recorder writes [`RawFrame`]s, so a REST snapshot that is a frame
/// gets recorded like any other. Fetching it out-of-band instead would leave
/// Bitstamp tapes permanently unreplayable — the diffs are all there, but the
/// base state they splice onto never happened, so the book could never leave
/// `AwaitingSnapshot`. A tape that cannot reproduce a synced book is not a
/// tape of anything useful.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameSource {
    /// Read off the venue's websocket.
    #[default]
    WebSocket,
    /// The body of an out-of-band REST depth request, for a
    /// [`RecoveryStrategy::RestSnapshot`] venue.
    RestSnapshot,
}

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
    pub source: FrameSource,
}

impl RawFrame {
    /// A frame read off the websocket — the overwhelmingly common case, which
    /// is why it gets the short constructor.
    pub fn new(venue: VenueId, payload: impl Into<Vec<u8>>, ingest_ts: IngestTime) -> Self {
        Self {
            venue,
            payload: payload.into(),
            ingest_ts,
            source: FrameSource::WebSocket,
        }
    }

    /// A REST depth response, to be spliced rather than parsed as a diff.
    pub fn rest_snapshot(
        venue: VenueId,
        payload: impl Into<Vec<u8>>,
        ingest_ts: IngestTime,
    ) -> Self {
        Self {
            source: FrameSource::RestSnapshot,
            ..Self::new(venue, payload, ingest_ts)
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
        let source = match self.source {
            FrameSource::WebSocket => "",
            FrameSource::RestSnapshot => ", rest",
        };
        match std::str::from_utf8(&self.payload) {
            Ok(s) => write!(f, "RawFrame({}{source}, {s:?})", self.venue),
            Err(_) => write!(
                f,
                "RawFrame({}{source}, {} bytes)",
                self.venue,
                self.payload.len()
            ),
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
    /// were universal. It applies to exactly one of our three venues, and even
    /// there it is weaker than the brief assumed — see [`RestSnapshot`].
    RestSnapshot,
}

/// A snapshot fetched out-of-band over REST, for a venue whose
/// [`RecoveryStrategy`] is [`RecoveryStrategy::RestSnapshot`].
///
/// `ma-venues` has no HTTP client and performs no I/O; the ingest task in
/// `ma-pipeline` is what actually issues the request. This type is how the
/// result crosses back into the network-free sync layer, via
/// [`VenueSync::apply_rest_snapshot`].
///
/// **On `as_of` and the hole check the original design brief assumed:** the
/// brief's reconnect algorithm calls for verifying that the buffered deltas
/// surviving the splice start at exactly `snapshot_sequence + 1`, and
/// discarding everything and restarting if there's a hole. That check
/// requires a dense integer counter. Bitstamp gives us a microtimestamp
/// instead, and time does not work as a hole detector — an hour can pass
/// between two adjacent, entirely legitimate diffs. `as_of` is used to
/// discard deltas the snapshot already contains; there is no corresponding
/// way to *prove* the ones that survive are complete. That gap is
/// `Integrity::OrderOnly`'s cost, and it is why the v2 plan calls for a
/// periodic re-snapshot audit rather than trusting the splice indefinitely.
#[derive(Clone, Debug, PartialEq)]
pub struct RestSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    /// The venue's own ordering marker for this snapshot, in the same units
    /// as the ordering field on that venue's deltas. Bitstamp: microseconds.
    pub as_of: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum VenueError {
    #[error("frame was not valid UTF-8")]
    NotUtf8,
    #[error("could not parse frame: {0}")]
    Malformed(String),
    #[error("frame was for {got}, but this is a {expected} sync")]
    WrongVenue { expected: VenueId, got: VenueId },
    #[error("{venue} has no network endpoint; it is driven by a script or a tape")]
    NoEndpoint { venue: VenueId },
    #[error("{venue} recovers by resubscribing and has no REST snapshot to parse")]
    NoRestSnapshot { venue: VenueId },
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

    /// Parse this venue's REST depth response body.
    ///
    /// Split from [`Self::apply_rest_snapshot`] so that the wire format and
    /// the splice remain separately testable, and so the ingest task can hand
    /// over a body it fetched without knowing anything about its shape. The
    /// default refuses, because a venue whose recovery is
    /// [`RecoveryStrategy::Resubscribe`] has no REST endpoint to have fetched
    /// this from — reaching here means a frame was mislabelled upstream.
    fn parse_rest_snapshot(&self, body: &str) -> Result<RestSnapshot, VenueError> {
        let _ = body;
        Err(VenueError::NoRestSnapshot {
            venue: self.venue(),
        })
    }

    /// Splice in a REST-fetched snapshot, for [`RecoveryStrategy::RestSnapshot`]
    /// venues. Never called for a [`RecoveryStrategy::Resubscribe`] venue,
    /// which gets its snapshot over the websocket instead — the default
    /// implementation is a no-op precisely so that forgetting to override it
    /// fails loudly in debug builds rather than silently doing nothing useful.
    fn apply_rest_snapshot(&mut self, snapshot: RestSnapshot) -> Vec<SyncAction> {
        debug_assert!(
            self.recovery() == RecoveryStrategy::RestSnapshot,
            "apply_rest_snapshot called on a venue whose recovery is {:?}, not RestSnapshot",
            self.recovery()
        );
        let _ = snapshot;
        Vec::new()
    }
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

    /// Cap retained depth, mirroring [`Book::with_max_depth`].
    ///
    /// Only correct for a venue that is itself publishing a depth-limited
    /// feed — see the hazard on `Book::with_max_depth` and the reasoning on
    /// [`VenueSpec::max_depth`](crate::venues::endpoint::VenueSpec::max_depth),
    /// which is where the per-venue decision actually lives.
    pub fn with_max_depth(mut self, depth: usize) -> Self {
        self.book = self.book.with_max_depth(depth);
        self
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
    ///
    /// Dispatches on [`RawFrame::source`], so a REST snapshot recorded onto a
    /// tape replays through this same entry point and drives the splice
    /// exactly as it did live. That is the whole reason the discriminator sits
    /// on the frame — see [`FrameSource`].
    pub fn feed(&mut self, frame: &RawFrame) -> Result<Vec<Outcome>, VenueError> {
        let actions = match frame.source {
            FrameSource::WebSocket => self.sync.ingest(frame)?,
            FrameSource::RestSnapshot => {
                let snapshot = self.sync.parse_rest_snapshot(frame.as_str()?)?;
                self.sync.apply_rest_snapshot(snapshot)
            }
        };
        Ok(self.apply_actions(actions, frame.ingest_ts))
    }

    /// Splice in a REST-fetched snapshot. See [`VenueSync::apply_rest_snapshot`]
    /// and the caveat on [`RestSnapshot`] about what this can and cannot prove
    /// for an [`Integrity::OrderOnly`] venue.
    pub fn apply_rest_snapshot(&mut self, snapshot: RestSnapshot, at: IngestTime) -> Vec<Outcome> {
        let actions = self.sync.apply_rest_snapshot(snapshot);
        self.apply_actions(actions, at)
    }

    /// Apply the actions a [`VenueSync`] returned, and report whether trust in
    /// the book changed as a result. Shared by [`Self::feed`] and
    /// [`Self::apply_rest_snapshot`] so the two entry points can't drift.
    fn apply_actions(&mut self, actions: Vec<SyncAction>, at: IngestTime) -> Vec<Outcome> {
        let before = self.book.state();
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
        outcomes
    }

    fn verify(&mut self, expected: u32, at: IngestTime) {
        // A checksum can only confirm or refute a book we actually have. On a
        // book that is not live it would hash an empty or distrusted set of
        // levels, always mismatch, and report `ChecksumMismatch` — which reads
        // as "the venue and we disagree about the book" when the truth is "we
        // have no book". Found against a real Kraken tape whose snapshot was
        // dropped by the channel: every subsequent update produced a fresh
        // mismatch against `computed: 0`, burying the actual problem under
        // hundreds of misleading ones.
        //
        // Staying `Uninitialized` here is the honest answer, and it is exactly
        // the distinction `BookState` exists to preserve: no data, rather than
        // data I do not trust.
        if !self.book.state().is_live() {
            return;
        }

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
