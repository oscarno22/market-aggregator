//! The single task that owns every book.
//!
//! One task, exclusive ownership of all [`VenueBook`]s, no locks anywhere. Not
//! because locks are slow at this scale — three venues publishing top-of-book
//! would not trouble a `Mutex` — but because a lock would make it *possible*
//! to read one venue's book while another is mid-update, and then someone
//! would. Single ownership makes a torn cross-venue read unrepresentable
//! rather than merely unlikely.
//!
//! # Two clocks, and only one of them is ever compared
//!
//! Every duration published here — book age, time in `Desynced`, the rate
//! interval — is measured on [`IngestTime`]'s monotonic reading. The wall
//! clock appears exactly once per snapshot, as `wall_unix_ms`, so a human or
//! a chart has something to anchor to. CLAUDE.md requires that anything
//! surfaced in the UI say which clock it used; [`Snapshot::clock`] is that
//! label, emitted with every snapshot rather than documented somewhere and
//! hoped for.
//!
//! Venue timestamps are deliberately absent. They disagree by seconds, and
//! some venues are simply wrong, so they are worth measuring (v2) and never
//! worth ordering by.
//!
//! # Prices leave here as strings
//!
//! [`Price`](ma_core::Price) serialises as a JSON string, and that is load
//! bearing rather than incidental. JSON numbers in a browser are `f64`, which
//! is the exact representation this project refuses everywhere else — a book
//! built with `Decimal` precision, checksum-verified against Kraken, would be
//! silently rounded the moment it reached the chart. There is a test below
//! pinning it, because the change that would break it is a one-word feature
//! flag on `rust_decimal`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ma_core::{
    BookState, Clock, DesyncReason, EventKind, IngestTime, Integrity, Level, Symbol, VenueId,
};
use ma_venues::{Outcome, VenueBook, VenueSpec};
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::channel::{ChannelMetrics, Receiver};
use crate::ingest::{IngestMessage, Shutdown};
use crate::metrics::{Metrics, Rates, VenueCounters, VenueCountersSnapshot};

/// How often the aggregator publishes, when not told otherwise.
///
/// Fast enough that a chart looks live, slow enough that a browser is not
/// re-rendering per tick of a busy book. The venues send far faster than this;
/// publishing every update would make the SSE stream the bottleneck and would
/// not tell a reader anything a 4Hz view does not.
pub const DEFAULT_TICK: Duration = Duration::from_millis(250);

/// How many snapshots the broadcast channel holds for a slow subscriber.
///
/// At [`DEFAULT_TICK`] this is eight seconds of grace before a client starts
/// missing snapshots — and missing them is fine, by design: see
/// [`Snapshot::seq`] and the SSE handler's `Lagged` behaviour.
pub const BROADCAST_CAPACITY: usize = 32;

/// What a book is, in one word, for a consumer that does not want to match on
/// [`BookState`]'s payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookStatus {
    /// No data. Different from bad data.
    Uninitialized,
    /// Data, trusted to the degree `integrity` states.
    Live,
    /// Data we do not trust.
    Desynced,
}

/// One venue's view at snapshot time.
#[derive(Clone, Debug, Serialize)]
pub struct VenueView {
    pub venue: VenueId,
    pub status: BookStatus,
    /// How strong the `Live` claim is. `None` unless `status` is `live` —
    /// the coupling that stops a `Desynced` book from being rendered as
    /// though its integrity still meant something.
    pub integrity: Option<Integrity>,
    /// Why the book is untrusted, in words, when it is.
    pub desync_reason: Option<String>,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
    /// Exact, as a string. Never a float — see the module docs.
    pub spread: Option<String>,
    pub mid: Option<String>,
    /// Time since the last applied update. CLAUDE.md's "book age".
    pub age_ms: Option<u64>,
    /// How long the book has held its current status.
    pub status_for_ms: u64,
    /// Cumulative time this venue has spent `Desynced` since startup,
    /// including any desync still in progress. The metric CLAUDE.md's v1 list
    /// calls "time-in-Desynced": a book that flaps in and out of sync every
    /// few seconds has excellent instantaneous availability and is useless,
    /// and only a cumulative number shows that.
    pub desynced_total_ms: u64,
    /// Age of the last matching checksum. Only ever set for Kraken; a
    /// `Verified` book whose last check is minutes old is not really verified.
    pub last_verified_ms: Option<u64>,
    /// Levels held per side, `[bids, asks]`.
    pub depth: [usize; 2],
    pub counters: VenueCountersSnapshot,
    pub rates: Rates,
}

/// Everything the fan-out publishes, once per tick.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub symbol: String,
    /// Monotonic per process, so a client that skipped ahead after a
    /// `Lagged` can say how far it jumped instead of pretending it did not.
    pub seq: u64,
    /// Wall clock, for display only. The only wall-clock value here.
    pub wall_unix_ms: u64,
    /// Which clock every `_ms` duration in this snapshot was measured on.
    /// Published rather than documented, per CLAUDE.md's rule that any
    /// surfaced comparison must name its clock.
    pub clock: &'static str,
    /// The weakest integrity among live books, or `None` if none are live.
    ///
    /// `Integrity` is ordered weakest-first precisely so this can be a `min`.
    /// It is what a future cross-venue spread view must display next to any
    /// number it derives from more than one venue — otherwise a Kraken book
    /// verified by checksum and a Bitstamp book that may have quietly lost a
    /// message get averaged into a figure that looks equally trustworthy than
    /// either.
    pub weakest_integrity: Option<Integrity>,
    pub venues: Vec<VenueView>,
    /// The ingest channel's occupancy and lifetime drop count.
    pub channel: ChannelMetrics,
}

const INGEST_MONOTONIC: &str = "ingest_monotonic";

/// Per-venue bookkeeping the aggregator keeps alongside the book itself.
#[derive(Debug)]
struct VenueState {
    book: VenueBook,
    counters: Arc<VenueCounters>,
    /// Counters as of the previous tick, for deriving rates.
    previous: VenueCountersSnapshot,
    /// Accumulated time spent `Desynced`, excluding any in progress.
    desynced_total: Duration,
    /// Start of the desync currently in progress, if any.
    desynced_since: Option<IngestTime>,
    /// When the current status began. `BookState` carries this for `Live` and
    /// `Desynced` but not for `Uninitialized`, and a view that reported
    /// "uninitialized for 0ms" forever would be worse than useless.
    status_since: IngestTime,
}

impl VenueState {
    fn status(&self) -> BookStatus {
        match self.book.book().state() {
            BookState::Uninitialized => BookStatus::Uninitialized,
            BookState::Live { .. } => BookStatus::Live,
            BookState::Desynced { .. } => BookStatus::Desynced,
        }
    }

    /// Fold a state transition into the desync accounting.
    fn note_transition(&mut self, from: BookState, to: BookState, at: IngestTime) {
        let was_desynced = matches!(from, BookState::Desynced { .. });
        let is_desynced = matches!(to, BookState::Desynced { .. });

        match (was_desynced, is_desynced) {
            (false, true) => {
                self.desynced_since = Some(at);
                self.counters.record_desync();
            }
            (true, false) => {
                if let Some(since) = self.desynced_since.take() {
                    self.desynced_total += at.since(since);
                }
            }
            // Desynced -> Desynced with a different reason keeps the clock
            // running; the book never regained trust in between.
            _ => {}
        }
        self.status_since = at;
    }

    fn desynced_total(&self, now: IngestTime) -> Duration {
        match self.desynced_since {
            Some(since) => self.desynced_total + now.since(since),
            None => self.desynced_total,
        }
    }
}

/// Owns every book; reads the ingest channel; publishes snapshots.
#[derive(Debug)]
pub struct Aggregator {
    symbol: Symbol,
    venues: BTreeMap<VenueId, VenueState>,
    clock: Arc<dyn Clock>,
    tick: Duration,
    seq: u64,
    tx: broadcast::Sender<Arc<Snapshot>>,
}

impl Aggregator {
    /// Build from one [`VenueSpec`] per venue, sharing `metrics`' counters
    /// with the ingest tasks.
    pub fn new(
        symbol: Symbol,
        specs: Vec<VenueSpec>,
        clock: Arc<dyn Clock>,
        metrics: &Metrics,
    ) -> Self {
        let now = clock.now();
        let venues = specs
            .into_iter()
            .map(|spec| {
                let venue = spec.sync.venue();
                let mut book = VenueBook::new(spec.sync, symbol.clone());
                if let Some(depth) = spec.max_depth {
                    book = book.with_max_depth(depth);
                }
                let counters = metrics.venue(venue).unwrap_or_default();
                (
                    venue,
                    VenueState {
                        book,
                        counters,
                        previous: VenueCountersSnapshot::default(),
                        desynced_total: Duration::ZERO,
                        desynced_since: None,
                        status_since: now,
                    },
                )
            })
            .collect();

        Self {
            symbol,
            venues,
            clock,
            tick: DEFAULT_TICK,
            seq: 0,
            tx: broadcast::channel(BROADCAST_CAPACITY).0,
        }
    }

    #[must_use]
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    /// Subscribe to the snapshot stream. Every SSE client gets one of these.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Snapshot>> {
        self.tx.subscribe()
    }

    /// A handle that can hand out subscriptions after `self` has been moved
    /// into [`Self::run`].
    pub fn publisher(&self) -> broadcast::Sender<Arc<Snapshot>> {
        self.tx.clone()
    }

    /// Drain the channel, apply, publish on the tick. Returns when the ingest
    /// channel closes or shutdown fires.
    pub async fn run(mut self, rx: Receiver<IngestMessage>, mut shutdown: Shutdown) {
        let mut ticker = tokio::time::interval(self.tick);
        // A tick missed because the aggregator was busy applying a burst
        // should not be made up for by firing several in a row — the point of
        // the tick is a steady publish cadence, not a quota.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!(
            symbol = %self.symbol,
            venues = self.venues.len(),
            tick = ?self.tick,
            "aggregator started"
        );

        loop {
            // Apply everything already queued in one batch, so a backlog costs
            // one loop turn rather than one per message.
            while let Some(message) = rx.try_recv() {
                self.apply(message);
            }

            tokio::select! {
                biased;

                () = shutdown.wait() => break,

                _ = ticker.tick() => {
                    let snapshot = self.snapshot(rx.metrics());
                    // An error means nobody is subscribed. Normal — the
                    // aggregator keeps books whether or not a browser is open.
                    let _ = self.tx.send(Arc::new(snapshot));
                }

                message = rx.recv() => {
                    match message {
                        Some(message) => self.apply(message),
                        None => {
                            debug!("ingest channel closed; aggregator stopping");
                            break;
                        }
                    }
                }
            }
        }

        // One last snapshot so a client sees the final state rather than the
        // stream simply stopping mid-tick.
        let final_snapshot = Arc::new(self.snapshot(rx.metrics()));
        let _ = self.tx.send(final_snapshot);
    }

    /// Apply one message. Synchronous and infallible from the caller's point
    /// of view: everything that can go wrong with a frame is a property of
    /// that frame, recorded on that venue, and must not stop the others.
    pub fn apply(&mut self, message: IngestMessage) {
        let venue = message.venue();
        let Some(state) = self.venues.get_mut(&venue) else {
            // A frame for a venue this process does not track. Possible from a
            // tape recorded with a different venue set — worth saying once,
            // not worth stopping for.
            debug!(%venue, "message for an untracked venue, ignored");
            return;
        };

        match message {
            IngestMessage::Frame(frame) => {
                let before = state.book.book().state();
                match state.book.feed(&frame) {
                    Ok(outcomes) => {
                        let after = state.book.book().state();
                        if before != after {
                            state.note_transition(before, after, frame.ingest_ts);
                            log_transition(venue, before, after);
                        }
                        for outcome in outcomes {
                            if let Outcome::Event(event) = outcome
                                && matches!(event.kind, EventKind::Heartbeat { .. })
                            {
                                state.counters.record_heartbeat();
                            }
                        }
                    }
                    Err(e) => {
                        // A frame we cannot parse does not desync the book: we
                        // learned nothing, which is different from learning
                        // something wrong. It does get counted, because a
                        // climbing parse_errors is how a venue's schema change
                        // announces itself.
                        state.counters.record_parse_error();
                        warn!(%venue, error = %e, "could not parse frame");
                    }
                }
            }

            IngestMessage::SessionEnded { at, end, .. } => {
                let before = state.book.book().state();
                state.book.reset(at);
                let after = state.book.book().state();
                if before != after {
                    state.note_transition(before, after, at);
                }
                info!(%venue, ?end, "session ended; book reset and marked desynced");
            }
        }
    }

    /// Build the snapshot for this tick.
    pub fn snapshot(&mut self, channel: ChannelMetrics) -> Snapshot {
        let now = self.clock.now();
        self.seq += 1;
        let tick = self.tick;

        let venues: Vec<VenueView> = self
            .venues
            .iter_mut()
            .map(|(venue, state)| {
                let counters = state.counters.snapshot();
                let rates = Rates::between(state.previous, counters, tick);
                state.previous = counters;

                let book = state.book.book();
                let top = book.top_of_book(now);
                let (bids, asks) = book.depth();

                let (integrity, last_verified_ms) = match top.state {
                    BookState::Live {
                        integrity,
                        last_verified,
                        ..
                    } => (
                        Some(integrity),
                        last_verified.map(|at| millis(now.since(at))),
                    ),
                    _ => (None, None),
                };

                VenueView {
                    venue: *venue,
                    status: state.status(),
                    integrity,
                    desync_reason: match top.state {
                        BookState::Desynced { reason, .. } => Some(describe(reason)),
                        _ => None,
                    },
                    bid: top.bid,
                    ask: top.ask,
                    spread: top.spread().map(|d| d.to_string()),
                    mid: top.mid().map(|d| d.to_string()),
                    age_ms: top.age.map(millis),
                    status_for_ms: millis(now.since(state.status_since)),
                    desynced_total_ms: millis(state.desynced_total(now)),
                    last_verified_ms,
                    depth: [bids, asks],
                    counters,
                    rates,
                }
            })
            .collect();

        Snapshot {
            symbol: self.symbol.to_string(),
            seq: self.seq,
            wall_unix_ms: unix_millis(now.wall()),
            clock: INGEST_MONOTONIC,
            weakest_integrity: venues.iter().filter_map(|v| v.integrity).min(),
            venues,
            channel,
        }
    }
}

fn log_transition(venue: VenueId, from: BookState, to: BookState) {
    match to {
        BookState::Desynced { reason, .. } => {
            warn!(%venue, ?from, ?reason, "book lost trust");
        }
        BookState::Live { integrity, .. } => {
            info!(%venue, ?from, ?integrity, "book is live");
        }
        BookState::Uninitialized => {}
    }
}

/// Human-readable desync reason for the UI.
///
/// A string rather than a serialised enum because these are read by a person
/// deciding what to do, and `{"SequenceGap":{"expected":41,"got":57}}` is not
/// what that person needs to see.
fn describe(reason: DesyncReason) -> String {
    match reason {
        DesyncReason::SequenceGap { expected, got } => {
            format!("sequence gap: expected {expected}, got {got}")
        }
        DesyncReason::ChecksumMismatch { expected, computed } => {
            format!("checksum mismatch: venue said {expected}, we computed {computed}")
        }
        DesyncReason::TimestampRegression {
            last_micros,
            got_micros,
        } => format!("timestamps went backwards: after {last_micros}, got {got_micros}"),
        DesyncReason::CrossedBook { best_bid, best_ask } => {
            format!("crossed book: bid {best_bid} >= ask {best_ask}")
        }
        DesyncReason::ConnectionLost => "connection lost".to_owned(),
        DesyncReason::AwaitingSnapshot => "awaiting a REST depth snapshot".to_owned(),
        DesyncReason::SnapshotGap => "buffered deltas did not join onto the snapshot".to_owned(),
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn unix_millis(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::channel::bounded;
    use crate::ingest::SessionEnd;
    use ma_core::{SystemClock, TestClock};
    use ma_venues::{RawFrame, spec_for};

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD")
    }

    fn aggregator(venues: &[VenueId]) -> (Aggregator, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::new(venues.iter().copied()));
        let specs = venues
            .iter()
            .map(|v| spec_for(*v, &symbol()).expect("spec"))
            .collect();
        let agg = Aggregator::new(symbol(), specs, Arc::new(SystemClock), &metrics);
        (agg, metrics)
    }

    fn frame(venue: VenueId, json: &str) -> IngestMessage {
        IngestMessage::Frame(RawFrame::new(
            venue,
            json.as_bytes().to_vec(),
            SystemClock.now(),
        ))
    }

    fn empty_channel() -> ChannelMetrics {
        ChannelMetrics {
            len: 0,
            capacity: 1,
            dropped: 0,
        }
    }

    /// A Coinbase l2 message at a given sequence number.
    fn coinbase(seq: u64, kind: &str, bid: &str, ask: &str) -> IngestMessage {
        frame(
            VenueId::Coinbase,
            &format!(
                r#"{{"channel":"l2_data","sequence_num":{seq},"events":[{{"type":"{kind}",
                   "product_id":"BTC-USD","updates":[
                     {{"side":"bid","price_level":"{bid}","new_quantity":"1"}},
                     {{"side":"offer","price_level":"{ask}","new_quantity":"2"}}]}}]}}"#
            ),
        )
    }

    fn view(snapshot: &Snapshot, venue: VenueId) -> VenueView {
        snapshot
            .venues
            .iter()
            .find(|v| v.venue == venue)
            .expect("venue in snapshot")
            .clone()
    }

    #[test]
    fn a_fresh_aggregator_reports_no_data_rather_than_nothing() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp]);
        let snap = agg.snapshot(empty_channel());

        assert_eq!(snap.venues.len(), 3);
        assert_eq!(snap.weakest_integrity, None);
        for v in &snap.venues {
            assert_eq!(v.status, BookStatus::Uninitialized);
            assert!(v.integrity.is_none(), "an unsynced book claimed integrity");
            assert!(v.bid.is_none() && v.ask.is_none());
        }
    }

    #[test]
    fn a_snapshot_frame_brings_a_book_live_with_its_integrity() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live);
        assert_eq!(v.integrity, Some(Integrity::GapDetectable));
        assert_eq!(v.spread.as_deref(), Some("1"));
        assert_eq!(v.depth, [1, 1]);
    }

    #[test]
    fn prices_are_json_strings_not_numbers() {
        // Load bearing. JSON numbers are f64 in every browser, so serialising
        // a price as a number would undo the exact-decimal discipline the rest
        // of this project is built on — silently, and only at the last step.
        // Turning on rust_decimal's `serde-float` feature is all it would take.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(0, "snapshot", "45000.10", "45000.20"));

        let json = serde_json::to_string(&agg.snapshot(empty_channel())).unwrap();
        assert!(
            json.contains(r#""price":"45000.10""#),
            "price was not a string, or lost its trailing zero: {json}"
        );
        assert!(
            !json.contains("45000.1,") && !json.contains(":45000.1"),
            "a bare numeric price reached the wire: {json}"
        );
    }

    #[test]
    fn a_sequence_gap_desyncs_and_says_why_in_words() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(coinbase(7, "update", "100", "101")); // expected 1

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Desynced);
        assert!(v.integrity.is_none(), "a desynced book still claimed trust");
        assert_eq!(
            v.desync_reason.as_deref(),
            Some("sequence gap: expected 1, got 7")
        );
        assert_eq!(v.counters.desyncs, 1);
    }

    #[test]
    fn a_session_boundary_resets_the_venue_so_it_can_resync() {
        // The failure this exists to prevent: a reconnected Coinbase restarts
        // sequence_num from a fresh base. Without the reset the sync sees an
        // enormous backwards jump, discards the snapshot riding in that frame
        // along with it, and the book never recovers — permanently Desynced on
        // a perfectly healthy socket.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(500, "snapshot", "100", "101"));
        agg.apply(coinbase(501, "update", "100", "101"));
        assert_eq!(
            view(&agg.snapshot(empty_channel()), VenueId::Coinbase).status,
            BookStatus::Live
        );

        agg.apply(IngestMessage::SessionEnded {
            venue: VenueId::Coinbase,
            at: SystemClock.now(),
            end: SessionEnd::Idle,
        });
        assert_eq!(
            view(&agg.snapshot(empty_channel()), VenueId::Coinbase).status,
            BookStatus::Desynced,
            "a dead connection must make the book untrusted immediately"
        );

        // The new stream starts over at 0, as Coinbase actually does.
        agg.apply(coinbase(0, "snapshot", "200", "201"));
        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live, "the book never resynced");
        assert_eq!(v.bid.map(|l| l.price.to_string()).as_deref(), Some("200"));
    }

    #[test]
    fn without_the_reset_a_resubscribed_book_would_stay_broken() {
        // The counterfactual for the test above, asserted directly so the
        // reason the SessionEnded message exists is written down as a fact
        // about the venue rather than as a claim in a comment.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(500, "snapshot", "100", "101"));
        agg.apply(coinbase(0, "snapshot", "200", "201")); // reconnect, no reset

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Desynced);
        assert_eq!(v.bid.map(|l| l.price.to_string()).as_deref(), Some("100"));
    }

    #[test]
    fn time_in_desynced_accumulates_across_separate_outages() {
        // Instantaneous availability hides flapping: a book that desyncs and
        // recovers every few seconds looks healthy in every sample and is
        // useless. Only the cumulative number shows it.
        let clock = Arc::new(TestClock::new());
        let metrics = Arc::new(Metrics::new([VenueId::Coinbase]));
        let mut agg = Aggregator::new(
            symbol(),
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            clock.clone(),
            &metrics,
        );

        let at = |c: &TestClock| c.now();
        agg.apply(IngestMessage::Frame(RawFrame::new(
            VenueId::Coinbase,
            br#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]}]}"#.to_vec(),
            at(&clock),
        )));
        assert_eq!(
            view(&agg.snapshot(empty_channel()), VenueId::Coinbase).desynced_total_ms,
            0
        );

        // Outage one: 3s.
        agg.apply(IngestMessage::SessionEnded {
            venue: VenueId::Coinbase,
            at: at(&clock),
            end: SessionEnd::Errored,
        });
        clock.advance(Duration::from_secs(3));
        agg.apply(IngestMessage::Frame(RawFrame::new(
            VenueId::Coinbase,
            br#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]}]}"#.to_vec(),
            at(&clock),
        )));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live);
        assert_eq!(v.desynced_total_ms, 3_000);

        // Outage two, still in progress: 2s so far, and it must be counted.
        agg.apply(IngestMessage::SessionEnded {
            venue: VenueId::Coinbase,
            at: at(&clock),
            end: SessionEnd::Errored,
        });
        clock.advance(Duration::from_secs(2));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Desynced);
        assert_eq!(
            v.desynced_total_ms, 5_000,
            "an in-progress desync must be included, or a book stuck forever \
             reports the same total as one that recovered"
        );
    }

    #[test]
    fn the_weakest_integrity_is_what_a_combined_view_reports() {
        // The property Integrity's Ord derive exists for. A verified Kraken
        // book beside a Bitstamp book that may have silently lost a message
        // must report OrderOnly, not Verified.
        let (mut agg, _m) = aggregator(&[VenueId::Kraken, VenueId::Bitstamp]);
        agg.apply(frame(
            VenueId::Kraken,
            r#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":100,"qty":1}],"asks":[{"price":101,"qty":1}],"checksum":0}]}"#,
        ));
        // Kraken's checksum will not match a one-level book, so it desyncs —
        // which is correct, and leaves Kraken out of the minimum entirely.
        agg.apply(IngestMessage::Frame(RawFrame::rest_snapshot(
            VenueId::Bitstamp,
            br#"{"microtimestamp":"1700000000000000","bids":[["100","1"]],"asks":[["101","1"]]}"#
                .to_vec(),
            SystemClock.now(),
        )));

        let snap = agg.snapshot(empty_channel());
        assert_eq!(snap.weakest_integrity, Some(Integrity::OrderOnly));
    }

    #[test]
    fn an_unparseable_frame_is_counted_but_does_not_desync() {
        // We learned nothing, which is different from learning something
        // wrong. Desyncing on garbage would let one malformed frame throw away
        // a book that is still perfectly correct.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(frame(VenueId::Coinbase, "{not json"));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live);
        assert_eq!(v.counters.parse_errors, 1);
    }

    #[test]
    fn one_venues_bad_frame_does_not_disturb_another() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase, VenueId::Kraken]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(frame(VenueId::Kraken, "{not json at all"));

        let snap = agg.snapshot(empty_channel());
        assert_eq!(view(&snap, VenueId::Coinbase).status, BookStatus::Live);
        assert_eq!(view(&snap, VenueId::Kraken).counters.parse_errors, 1);
        assert_eq!(view(&snap, VenueId::Coinbase).counters.parse_errors, 0);
    }

    #[test]
    fn a_frame_for_an_untracked_venue_is_ignored_not_fatal() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(frame(VenueId::Kraken, r#"{"channel":"heartbeat"}"#));
        assert_eq!(agg.snapshot(empty_channel()).venues.len(), 1);
    }

    #[test]
    fn sequence_numbers_increase_so_a_lagged_client_can_measure_its_jump() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        let a = agg.snapshot(empty_channel()).seq;
        let b = agg.snapshot(empty_channel()).seq;
        assert_eq!(b, a + 1);
    }

    #[test]
    fn every_snapshot_names_the_clock_its_durations_use() {
        // CLAUDE.md requires any surfaced comparison to say which clock it is
        // on. Emitting the label with the data means a consumer cannot read
        // the numbers without it.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        let json = serde_json::to_string(&agg.snapshot(empty_channel())).unwrap();
        assert!(json.contains(r#""clock":"ingest_monotonic""#));
    }

    #[tokio::test(start_paused = true)]
    async fn the_run_loop_publishes_on_the_tick_and_stops_on_shutdown() {
        let (tx, rx) = bounded::<IngestMessage>(16);
        let (trigger, shut) = crate::ingest::shutdown();
        let (agg, _m) = aggregator(&[VenueId::Coinbase]);
        let mut sub = agg.subscribe();
        let handle = tokio::spawn(agg.with_tick(Duration::from_millis(50)).run(rx, shut));

        tx.send(coinbase(0, "snapshot", "100", "101"));

        // interval fires immediately, so the first snapshot may predate the
        // frame; take snapshots until the book is live.
        let mut live = false;
        for _ in 0..10 {
            let snap = sub.recv().await.expect("snapshot");
            if view(&snap, VenueId::Coinbase).status == BookStatus::Live {
                live = true;
                break;
            }
        }
        assert!(live, "the aggregator never published a live book");

        trigger.stop();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("aggregator ignored shutdown")
            .expect("aggregator panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_ingest_channel_stops_the_aggregator() {
        let (tx, rx) = bounded::<IngestMessage>(4);
        let (_trigger, shut) = crate::ingest::shutdown();
        let (agg, _m) = aggregator(&[VenueId::Coinbase]);
        let handle = tokio::spawn(agg.with_tick(Duration::from_millis(10)).run(rx, shut));

        tx.close();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("aggregator kept running with no producers")
            .expect("aggregator panicked");
    }
}
