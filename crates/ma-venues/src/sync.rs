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
use std::time::SystemTime;

use ma_core::{
    AuditPolicy, AuditTrail, Book, DesyncReason, EventKind, IngestTime, Integrity, Level,
    MarketEvent, StreamId, Symbol, VenueId,
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
    /// The body of a **periodic** REST depth request, fetched to check a book
    /// we already trust rather than to build one.
    ///
    /// Distinct from [`Self::RestSnapshot`] because the two do opposite
    /// things with the same bytes. A snapshot *replaces* the book and is how
    /// Bitstamp recovers; an audit *compares* against it and leaves the book
    /// alone. Collapsing them would mean every audit silently repaired the
    /// drift it was supposed to detect — the book would look healthy forever
    /// and the one question the audit exists to answer, "did we drift?", could
    /// never be asked. See [`ma_core::audit`].
    RestAudit,
}

/// Bytes as they came off the wire, before anything was parsed.
///
/// This is deliberately the unit the tape recorder writes. Recording *after*
/// parsing would mean a recorded session could never reproduce a parser bug or
/// a venue schema change — the two failures most likely to happen unattended.
///
/// # Why the symbol rides alongside the bytes it is already inside
///
/// Every venue names the symbol somewhere in the payload, so carrying it
/// separately looks redundant. It is not, for one reason: **routing happens
/// before parsing.** The aggregator has to pick which book this frame belongs
/// to, and the only way to learn that from the payload is to parse it — which
/// requires already knowing which venue's parser to use *and* which book's
/// sync state to feed. The [`StreamId`] is what the ingest task subscribed
/// with, so it is knowledge the task already has and the reader would
/// otherwise have to re-derive.
///
/// It also makes a multi-symbol tape self-describing: a line says which
/// subscription produced it, rather than leaving a reader to infer it from the
/// bytes with a venue-specific parser.
#[derive(Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub stream: StreamId,
    pub payload: Vec<u8>,
    pub ingest_ts: IngestTime,
    pub source: FrameSource,
}

impl RawFrame {
    /// A frame read off the websocket — the overwhelmingly common case, which
    /// is why it gets the short constructor.
    pub fn new(stream: StreamId, payload: impl Into<Vec<u8>>, ingest_ts: IngestTime) -> Self {
        Self {
            stream,
            payload: payload.into(),
            ingest_ts,
            source: FrameSource::WebSocket,
        }
    }

    /// A periodic REST depth response, to be compared against the book rather
    /// than applied to it.
    pub fn rest_audit(
        stream: StreamId,
        payload: impl Into<Vec<u8>>,
        ingest_ts: IngestTime,
    ) -> Self {
        Self {
            source: FrameSource::RestAudit,
            ..Self::new(stream, payload, ingest_ts)
        }
    }

    /// A REST depth response, to be spliced rather than parsed as a diff.
    pub fn rest_snapshot(
        stream: StreamId,
        payload: impl Into<Vec<u8>>,
        ingest_ts: IngestTime,
    ) -> Self {
        Self {
            source: FrameSource::RestSnapshot,
            ..Self::new(stream, payload, ingest_ts)
        }
    }

    pub fn venue(&self) -> VenueId {
        self.stream.venue
    }

    pub fn symbol(&self) -> &Symbol {
        &self.stream.symbol
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
            FrameSource::RestAudit => ", audit",
        };
        match std::str::from_utf8(&self.payload) {
            Ok(s) => write!(f, "RawFrame({}{source}, {s:?})", self.stream),
            Err(_) => write!(
                f,
                "RawFrame({}{source}, {} bytes)",
                self.stream,
                self.payload.len()
            ),
        }
    }
}

/// What one frame turned into: instructions for the book, plus the venue's own
/// claim about when it happened.
///
/// # Why the timestamp is returned separately rather than on each action
///
/// `venue_ts` is a property of the *frame*, not of any one instruction inside
/// it — a single Kraken book message yields a delta and a checksum verification
/// that share one timestamp. Hanging it off each action would duplicate it and
/// invite the two copies to disagree.
///
/// It exists to be **measured, never trusted**. `ma_core::MarketEvent`'s docs
/// and `docs/DESIGN.md` §6 are emphatic: venues disagree by seconds and some
/// are simply wrong, so nothing orders or windows by this. It is carried so
/// clock skew is observable and so the persistence layer can write a column
/// that says what the venue claimed, next to the column that says what we
/// observed.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ingested {
    pub actions: Vec<SyncAction>,
    /// The venue's own timestamp for this frame, if it sent one at all.
    pub venue_ts: Option<SystemTime>,
}

impl Ingested {
    /// Actions with no venue timestamp — acks, and venues that send none.
    pub fn untimed(actions: Vec<SyncAction>) -> Self {
        Self {
            actions,
            venue_ts: None,
        }
    }

    /// Nothing to do. The common case for subscription acks and pongs.
    pub fn ignored() -> Self {
        Self::untimed(vec![SyncAction::Ignore])
    }

    #[must_use]
    pub fn at(mut self, venue_ts: Option<SystemTime>) -> Self {
        self.venue_ts = venue_ts;
        self
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
    fn ingest(&mut self, frame: &RawFrame) -> Result<Ingested, VenueError>;

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
    audit_policy: AuditPolicy,
    audit_trail: AuditTrail,
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
    /// A normalised event, in the order it was applied.
    ///
    /// **Every** frame with content produces one of these, including the
    /// snapshots and deltas that changed the book — not merely the trades and
    /// heartbeats that pass through. v1 emitted only the latter, because the
    /// only consumer was a heartbeat counter.
    ///
    /// v2's persistence layer is the reason that changed: a Parquet file
    /// written from a stream that omits snapshots and deltas records the
    /// commentary and discards the market. Emitting the applied events here
    /// means the normalised history and the live book are derived from one
    /// sequence, so a replay of the history cannot diverge from what the live
    /// run believed — they are the same events.
    Event(MarketEvent),
}

impl VenueBook {
    pub fn new(sync: Box<dyn VenueSync>, symbol: Symbol) -> Self {
        let book = Book::new(sync.venue(), symbol);
        Self {
            sync,
            book,
            audit_policy: AuditPolicy::DEFAULT,
            audit_trail: AuditTrail::default(),
        }
    }

    #[must_use]
    pub fn with_audit_policy(mut self, policy: AuditPolicy) -> Self {
        self.audit_policy = policy;
        self
    }

    /// Lifetime audit totals: how many comparisons ran, and how many
    /// disagreed. The audit's primary output is this pair, not the desync —
    /// see [`ma_core::audit`] on why a single finding is not evidence.
    pub fn audit_trail(&self) -> &AuditTrail {
        &self.audit_trail
    }

    /// This book's subscription identity.
    pub fn stream(&self) -> StreamId {
        StreamId::new(self.book.venue(), self.book.symbol().clone())
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
        // The book that failed an audit is gone; the case against it does not
        // carry over to its replacement. Lifetime totals survive, because they
        // are a record of what happened rather than evidence about the book
        // that exists now.
        self.audit_trail.reset();
        self.book.mark_desynced(DesyncReason::ConnectionLost, at);
    }

    /// Compare the book against a periodically-fetched REST snapshot.
    ///
    /// This is the **only** independent evidence available for Coinbase and
    /// Bitstamp: Kraken hashes our book on every message, and the other two
    /// hash nothing at all. See [`ma_core::audit`] for why a single
    /// disagreement is treated as noise and a repeated one as proof.
    ///
    /// Auditing a book that is not live is skipped rather than reported. A
    /// desynced book is *expected* to disagree — it is mid-recovery — and
    /// counting that as a finding would manufacture the evidence the audit is
    /// supposed to be gathering. Same argument as [`Self::verify`] refusing to
    /// checksum a book that does not exist.
    pub fn audit(&mut self, snapshot: &RestSnapshot, at: IngestTime) -> Vec<Outcome> {
        if !self.book.state().is_live() {
            return Vec::new();
        }

        let before = self.book.state();
        let outcome = ma_core::audit(
            &self.book,
            &snapshot.bids,
            &snapshot.asks,
            self.audit_policy,
        );

        if let Some(reason) = self.audit_trail.observe(&outcome, self.audit_policy) {
            self.book.mark_desynced(reason, at);
        }

        let after = self.book.state();
        if before == after {
            return Vec::new();
        }
        vec![Outcome::StateChanged {
            from: before,
            to: after,
        }]
    }

    /// Feed one frame; apply whatever it implies.
    ///
    /// Dispatches on [`RawFrame::source`], so a REST snapshot recorded onto a
    /// tape replays through this same entry point and drives the splice
    /// exactly as it did live. That is the whole reason the discriminator sits
    /// on the frame — see [`FrameSource`].
    pub fn feed(&mut self, frame: &RawFrame) -> Result<Vec<Outcome>, VenueError> {
        let ingested = match frame.source {
            FrameSource::WebSocket => self.sync.ingest(frame)?,
            FrameSource::RestSnapshot => {
                let snapshot = self.sync.parse_rest_snapshot(frame.as_str()?)?;
                Ingested::untimed(self.sync.apply_rest_snapshot(snapshot))
            }
            FrameSource::RestAudit => {
                let snapshot = self.sync.parse_rest_snapshot(frame.as_str()?)?;
                return Ok(self.audit(&snapshot, frame.ingest_ts));
            }
        };
        Ok(self.apply_ingested(ingested, frame.ingest_ts))
    }

    /// Splice in a REST-fetched snapshot. See [`VenueSync::apply_rest_snapshot`]
    /// and the caveat on [`RestSnapshot`] about what this can and cannot prove
    /// for an [`Integrity::OrderOnly`] venue.
    pub fn apply_rest_snapshot(&mut self, snapshot: RestSnapshot, at: IngestTime) -> Vec<Outcome> {
        let actions = self.sync.apply_rest_snapshot(snapshot);
        self.apply_ingested(Ingested::untimed(actions), at)
    }

    /// Apply an already-normalised event, bypassing the wire parser entirely.
    ///
    /// This is the entry point for replaying **normalised** history — v2's
    /// Parquet layer — as opposed to the raw-frame tape, which goes through
    /// [`Self::feed`] and the venue's parser like a live socket.
    ///
    /// The two replay layers are deliberately not interchangeable, and this
    /// method is where the difference becomes concrete. A raw-frame tape can
    /// reproduce a parser bug because the bytes are still bytes. This path
    /// cannot: parsing already happened, once, when the event was recorded.
    /// What it *can* do is reproduce the book, including Kraken's checksum
    /// verification, because [`EventKind::Checksum`] is part of the normalised
    /// stream — so a replayed book is still checked against what the venue said
    /// it should be, rather than merely against itself.
    pub fn apply_event(&mut self, event: MarketEvent, at: IngestTime) -> Vec<Outcome> {
        let action = match event.kind {
            EventKind::Snapshot { bids, asks } => SyncAction::Snapshot { bids, asks },
            EventKind::Delta { bids, asks } => SyncAction::Delta { bids, asks },
            EventKind::Checksum { value } => SyncAction::Verify { checksum: value },
            kind @ (EventKind::Trade { .. } | EventKind::Heartbeat { .. }) => {
                SyncAction::Forward(kind)
            }
        };
        self.apply_ingested(
            Ingested {
                actions: vec![action],
                venue_ts: event.venue_ts,
            },
            at,
        )
    }

    /// Apply what a [`VenueSync`] returned, and report whether trust in the
    /// book changed as a result. Shared by every entry point so they can't
    /// drift.
    ///
    /// Each action becomes a [`MarketEvent`] *before* it is applied, and the
    /// book is then updated from that event's own payload. The order matters
    /// for a boring but load-bearing reason: it moves the level vectors into
    /// the event rather than cloning them out of it. Coinbase's opening
    /// snapshot is tens of thousands of levels, and a design that emitted a
    /// copy for the persistence layer would double that allocation on every
    /// resync.
    fn apply_ingested(&mut self, ingested: Ingested, at: IngestTime) -> Vec<Outcome> {
        let before = self.book.state();
        let mut outcomes = Vec::new();
        let integrity = self.sync.integrity();

        for action in ingested.actions {
            let kind = match action {
                SyncAction::Ignore => continue,

                SyncAction::Desync(reason) => {
                    self.book.mark_desynced(reason, at);
                    continue;
                }

                SyncAction::Verify { checksum } => {
                    self.verify(checksum, at);
                    EventKind::Checksum { value: checksum }
                }

                SyncAction::Snapshot { bids, asks } => {
                    let kind = EventKind::Snapshot { bids, asks };
                    if let EventKind::Snapshot { bids, asks } = &kind {
                        // A crossed snapshot desyncs the book from inside
                        // `apply_snapshot`; the error is the same information
                        // as the state change reported below.
                        let _ = self.book.apply_snapshot(bids, asks, integrity, at);
                    }
                    kind
                }

                SyncAction::Delta { bids, asks } => {
                    let kind = EventKind::Delta { bids, asks };
                    if let EventKind::Delta { bids, asks } = &kind
                        && self.book.apply_delta(bids, asks, at).is_err()
                    {
                        // Deltas arriving while desynced are expected during a
                        // resync — the venue is still buffering. Dropping them
                        // here is correct; applying them is how books go
                        // silently wrong. It must not reach the persistence
                        // layer either: a recorded delta that was never
                        // applied would replay into a book the live run never
                        // had.
                        continue;
                    }
                    kind
                }

                SyncAction::Forward(kind) => kind,
            };

            outcomes.push(Outcome::Event(MarketEvent {
                venue: self.book.venue(),
                symbol: self.book.symbol().clone(),
                venue_ts: ingested.venue_ts,
                ingest_ts: at,
                kind,
            }));
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
