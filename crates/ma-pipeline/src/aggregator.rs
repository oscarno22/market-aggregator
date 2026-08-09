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
    BookState, Clock, CrossLeg, CrossPolicy, DesyncReason, EventKind, IngestTime, Integrity, Level,
    MarketEvent, RollingWindows, Side, StreamId, Symbol, TopOfBook, VenueId, WindowReading,
    WindowSpec,
};
use ma_venues::{Outcome, VenueBook, VenueSpec};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info, warn};

use crate::channel::{ChannelMetrics, Receiver};
use crate::ingest::{IngestMessage, Shutdown};
use crate::metrics::{Metrics, Rates, VenueCounters, VenueCountersSnapshot};
use crate::resync::ResyncRequests;

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

/// How many price levels per side each snapshot carries.
///
/// This is a **view** limit, not a retention limit, and the distinction is the
/// whole of v2's depth story. The books hold everything the venue sends (except
/// Kraken, which publishes a depth-limited feed and is pruned to match — see
/// `VenueSpec::max_depth`). Serving fewer levels than we hold costs nothing and
/// risks nothing.
///
/// Serving *more* than a browser can draw is the real hazard, and it is a
/// throughput one: Coinbase's BTC-USD book runs to tens of thousands of levels,
/// and publishing all of them four times a second would push megabytes per
/// second down an SSE connection to render pixels nobody can distinguish. Ten
/// is what fits on the page and what Kraken checksums.
pub const DEFAULT_DEPTH_LEVELS: usize = 10;

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
    /// Best bid and ask. Always equal to the first entry of `bids`/`asks`;
    /// kept as their own fields because the touch is what `spread` and `mid`
    /// are derived from, and a consumer that only wants the top should not
    /// have to index into a ladder. `top_of_book_is_the_head_of_the_ladder`
    /// below pins the two together so they cannot drift.
    pub bid: Option<Level>,
    pub ask: Option<Level>,
    /// The L2 ladder, best first, up to [`DEFAULT_DEPTH_LEVELS`] per side.
    ///
    /// v1 served the touch only. These are what make it an order book rather
    /// than a price ticker, and they are a projection of the full book the
    /// aggregator holds — see [`DEFAULT_DEPTH_LEVELS`] on why the served depth
    /// and the retained depth are different numbers.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
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
    /// Periodic depth audits run against this book, and how many disagreed.
    ///
    /// The audit's primary output. A desync only follows repeated findings —
    /// see [`ma_core::audit`] — so a climbing `audit_mismatches` with a still-
    /// `live` book is the interesting reading: the book is drifting in a way
    /// no single comparison has been able to prove.
    pub audits: u64,
    pub audit_mismatches: u64,
    /// Levels *held* per side, `[bids, asks]` — the full book, not the
    /// truncated ladder above. On Coinbase this is routinely five figures
    /// while `bids.len()` is ten, and the gap between the two numbers is the
    /// point: it is how a reader can tell a depth-limited *view* from a
    /// depth-limited *book*.
    pub levels_held: [usize; 2],
    /// Rolling indicators, one per configured span, in the order they were
    /// configured.
    ///
    /// Each carries its own `trusted_ms`/`span_ms` pair rather than inheriting
    /// this view's `status`, and the two answer different questions: `status`
    /// is the book *now*, coverage is how much of the window we were entitled
    /// to speak about. A book that is `live` this instant and was `desynced`
    /// for the previous forty seconds has a perfectly healthy `status` and a
    /// 60s window that means almost nothing.
    pub windows: Vec<WindowReading>,
    pub counters: VenueCountersSnapshot,
    pub rates: Rates,
}

/// Every venue's view of one symbol.
///
/// The grouping exists because the cross-venue comparisons below are only
/// meaningful *within* a symbol: the weakest integrity across BTC-USD says
/// nothing about ETH-USD, and a UI that mixed them would invite exactly the
/// wrong reading.
#[derive(Clone, Debug, Serialize)]
pub struct SymbolView {
    pub symbol: String,
    /// The weakest integrity among this symbol's live books, or `None` if none
    /// are live.
    ///
    /// `Integrity` is ordered weakest-first precisely so this can be a `min`.
    /// It is what a cross-venue spread view must display next to any number it
    /// derives from more than one venue — otherwise a Kraken book verified by
    /// checksum and a Bitstamp book that may have quietly lost a message get
    /// combined into a figure that looks more trustworthy than either.
    pub weakest_integrity: Option<Integrity>,
    /// The best bid and best ask across venues, and what they were derived
    /// from. See [`ma_core::cross`] for why this is the most misreadable
    /// number the process publishes and what stops it being noise.
    pub cross: CrossView,
    pub venues: Vec<VenueView>,
}

/// [`ma_core::CrossVenue`] rendered for the wire.
///
/// A separate type rather than serialising the core one, for the same reason
/// [`describe`] exists: `Decimal` fields leave here as strings (see the module
/// docs on why a JSON number would undo the exact-decimal discipline), and an
/// exclusion reason is read by a person deciding what to do, not by a machine
/// matching on a tag.
#[derive(Clone, Debug, Serialize)]
pub struct CrossView {
    pub bid: Option<CrossLeg>,
    pub ask: Option<CrossLeg>,
    /// Signed. Negative means the venues' books are crossed.
    pub spread: Option<String>,
    pub spread_bps: Option<String>,
    pub mid: Option<String>,
    /// Weakest guarantee among the legs used — not among the venues present.
    pub integrity_floor: Option<Integrity>,
    /// Age of the older leg: how simultaneous this reading actually is.
    pub oldest_leg_ms: Option<u64>,
    pub venues_used: usize,
    /// Best bid at or above best ask. The apparent-arbitrage flag, and it is
    /// *apparent* — gross of fees, latency and transfer time, and derived from
    /// two quotes that were never observed at the same instant.
    pub crossed: bool,
    /// Both legs came from one venue, so this is that venue's own spread and
    /// cannot show an arbitrage.
    pub single_venue: bool,
    pub excluded: Vec<ExclusionView>,
    /// Which clock `oldest_leg_ms` was measured on.
    ///
    /// Repeated here even though [`Snapshot::clock`] already carries it, and
    /// deliberately: CLAUDE.md requires that *any cross-venue comparison
    /// surfaced in the UI* name its clock, and this object is the one thing in
    /// the snapshot most likely to be pulled out and rendered on its own.
    pub clock: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExclusionView {
    pub venue: VenueId,
    pub reason: String,
}

/// Everything the fan-out publishes, once per tick.
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    /// Monotonic per process, so a client that skipped ahead after a
    /// `Lagged` can say how far it jumped instead of pretending it did not.
    pub seq: u64,
    /// Wall clock, for display only. The only wall-clock value here.
    pub wall_unix_ms: u64,
    /// Which clock every `_ms` duration in this snapshot was measured on.
    /// Published rather than documented, per CLAUDE.md's rule that any
    /// surfaced comparison must name its clock.
    pub clock: &'static str,
    /// One entry per symbol this process tracks, in a stable order.
    pub symbols: Vec<SymbolView>,
    /// The ingest channel's occupancy and lifetime drop count. Process-wide:
    /// every stream shares one channel, which is what makes a single
    /// `dropped` count meaningful.
    pub channel: ChannelMetrics,
}

impl Snapshot {
    /// One symbol's view, by name.
    pub fn symbol(&self, symbol: &str) -> Option<&SymbolView> {
        self.symbols.iter().find(|s| s.symbol == symbol)
    }

    /// Every venue view across every symbol, for consumers like `/metrics`
    /// that label rather than group.
    pub fn views(&self) -> impl Iterator<Item = (&str, &VenueView)> {
        self.symbols
            .iter()
            .flat_map(|s| s.venues.iter().map(move |v| (s.symbol.as_str(), v)))
    }
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
    /// Rolling indicators over this stream's mid. Sampled per applied message,
    /// not per publish tick — see [`RollingWindows::observe`].
    windows: RollingWindows,
}

impl VenueState {
    /// Fold the book's current state into the rolling windows.
    ///
    /// Called on *every* applied message, including the ones that change
    /// nothing — a parse error, a heartbeat, a session ending. The windows
    /// need the calls that change nothing as much as the ones that do: their
    /// coverage accounting is interval-based, so a stretch during which the
    /// book was untrusted is only charged when something reports the time
    /// passing.
    fn sample_windows(&mut self, at: IngestTime) {
        let top = self.book.book().top_of_book(at);
        self.windows.observe(&top, at);
    }

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
    streams: BTreeMap<StreamId, VenueState>,
    clock: Arc<dyn Clock>,
    tick: Duration,
    depth_levels: usize,
    seq: u64,
    tx: broadcast::Sender<Arc<Snapshot>>,
    resync: ResyncRequests,
    events: Option<mpsc::UnboundedSender<MarketEvent>>,
    window_spec: WindowSpec,
    cross_policy: CrossPolicy,
    /// Streams this node is responsible for, when running in a cluster.
    ///
    /// `None` — the default and the single-node case — means every configured
    /// stream. When set, the aggregator keeps state for every stream it was
    /// built with but *publishes* only the owned ones, so a released stream
    /// disappears from the view rather than lingering with the last prices
    /// this node happened to see. Another node is watching it now, and a card
    /// showing our stale copy beside their live one is two answers to one
    /// question.
    owned: Option<watch::Receiver<std::collections::BTreeSet<StreamId>>>,
}

impl Aggregator {
    /// Build from one [`VenueSpec`] per stream, sharing `metrics`' counters
    /// with the ingest tasks.
    ///
    /// A spec carries the symbol it was built for, so the caller passes one
    /// spec per (venue, symbol) pair and this constructor does not need to know
    /// how the two lists were crossed.
    pub fn new(specs: Vec<VenueSpec>, clock: Arc<dyn Clock>, metrics: &Metrics) -> Self {
        Self::with_window_spec(specs, clock, metrics, WindowSpec::default())
    }

    /// As [`Aggregator::new`], with the rolling windows configured.
    ///
    /// Taken at construction rather than through a `with_` builder because the
    /// windows have to exist before the first message is applied. A builder
    /// that replaced them afterwards would silently discard whatever coverage
    /// had already been accounted, which is precisely the kind of quiet hole
    /// `trusted_ms` exists to make visible.
    pub fn with_window_spec(
        specs: Vec<VenueSpec>,
        clock: Arc<dyn Clock>,
        metrics: &Metrics,
        window_spec: WindowSpec,
    ) -> Self {
        let now = clock.now();
        let streams = specs
            .into_iter()
            .map(|spec| {
                let stream = StreamId::new(spec.sync.venue(), spec.symbol.clone());
                let mut book = VenueBook::new(spec.sync, spec.symbol);
                if let Some(depth) = spec.max_depth {
                    book = book.with_max_depth(depth);
                }
                let counters = metrics.stream(&stream).unwrap_or_default();
                (
                    stream,
                    VenueState {
                        book,
                        counters,
                        previous: VenueCountersSnapshot::default(),
                        desynced_total: Duration::ZERO,
                        desynced_since: None,
                        status_since: now,
                        windows: RollingWindows::new(window_spec.clone(), now),
                    },
                )
            })
            .collect();

        Self {
            streams,
            clock,
            tick: DEFAULT_TICK,
            depth_levels: DEFAULT_DEPTH_LEVELS,
            seq: 0,
            tx: broadcast::channel(BROADCAST_CAPACITY).0,
            resync: ResyncRequests::default(),
            events: None,
            window_spec,
            cross_policy: CrossPolicy::default(),
            owned: None,
        }
    }

    /// Publish only the streams this node owns. See [`Aggregator::owned`].
    #[must_use]
    pub fn restricted_to(
        mut self,
        owned: watch::Receiver<std::collections::BTreeSet<StreamId>>,
    ) -> Self {
        self.owned = Some(owned);
        self
    }

    /// How stale a book may be and still be a leg of the consolidated touch.
    #[must_use]
    pub fn with_cross_policy(mut self, policy: CrossPolicy) -> Self {
        self.cross_policy = policy;
        self
    }

    /// The window spans this aggregator publishes, for a consumer that needs
    /// to label them — `/metrics` does, since a Prometheus series name cannot
    /// carry a position in a list.
    pub fn window_spec(&self) -> &WindowSpec {
        &self.window_spec
    }

    /// Tee every normalised event to a persistence sink.
    ///
    /// # Why the aggregator is the only place this can come from
    ///
    /// Normalising is the venue layer's job, and the venue layer's state
    /// machines live here, inside the single task that owns the books. A
    /// second consumer reading raw frames off the channel could not produce
    /// the same events without duplicating every `VenueSync` — two copies of
    /// the sequence-gap logic, two REST splice buffers, and eventually two
    /// different opinions about what the market did.
    ///
    /// Emitting from here instead means the recorded history is *the same
    /// sequence* the live books were built from, not a second derivation of
    /// it. That is what makes the round-trip property in `ma-persist` — replay
    /// the Parquet, get the same books — a real check rather than a
    /// tautology about two parsers agreeing.
    ///
    /// **The sink is unbounded, and that is the tape tee's policy, not the
    /// ingest channel's.** Same argument as `ingest::Ingest::recording_to`:
    /// durable history with a hole in it silently invalidates everything built
    /// on it, so persistence gets the claims-processing policy rather than the
    /// market-data one. Unlike the tape tee, this one runs in steady state, so
    /// the writer is responsible for keeping up — `ma-persist` batches to
    /// Parquet row groups rather than fsyncing per event.
    #[must_use]
    pub fn publishing_events_to(mut self, events: mpsc::UnboundedSender<MarketEvent>) -> Self {
        self.events = Some(events);
        self
    }

    /// Levels per side to publish in each snapshot. See
    /// [`DEFAULT_DEPTH_LEVELS`].
    #[must_use]
    pub fn with_depth_levels(mut self, levels: usize) -> Self {
        self.depth_levels = levels;
        self
    }

    /// Wire up the channel that asks an ingest task to reconnect.
    ///
    /// Without it the aggregator can *detect* a desync and do nothing about
    /// it. Every venue here recovers by getting a fresh snapshot, and every
    /// venue only sends one on a new subscription, so a book broken by bad
    /// data rather than a dead socket stays broken: the connection is healthy,
    /// the idle watchdog never fires, and updates keep arriving that the book
    /// correctly refuses to apply. See [`crate::resync`].
    #[must_use]
    pub fn requesting_resync_through(mut self, resync: ResyncRequests) -> Self {
        self.resync = resync;
        self
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
            streams = self.streams.len(),
            tick = ?self.tick,
            depth = self.depth_levels,
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
        let stream = message.stream().clone();
        let Some(state) = self.streams.get_mut(&stream) else {
            // A message for a stream this process does not track. Possible
            // from a tape recorded with a different venue or symbol set —
            // worth saying once, not worth stopping for.
            debug!(%stream, "message for an untracked stream, ignored");
            return;
        };

        state.counters.record_applied();

        let (before, after, outcomes, at) = match message {
            IngestMessage::Frame(frame) => {
                let before = state.book.book().state();
                match state.book.feed(&frame) {
                    Ok(outcomes) => (before, state.book.book().state(), outcomes, frame.ingest_ts),
                    Err(e) => {
                        // A frame we cannot parse does not desync the book: we
                        // learned nothing, which is different from learning
                        // something wrong. It does get counted, because a
                        // climbing parse_errors is how a venue's schema change
                        // announces itself.
                        state.counters.record_parse_error();
                        warn!(%stream, error = %e, "could not parse frame");
                        state.sample_windows(frame.ingest_ts);
                        return;
                    }
                }
            }

            IngestMessage::Event { event, .. } => {
                let before = state.book.book().state();
                let at = event.ingest_ts;
                let outcomes = state.book.apply_event(event, at);
                (before, state.book.book().state(), outcomes, at)
            }

            IngestMessage::SessionEnded { at, end, .. } => {
                let before = state.book.book().state();
                state.book.reset(at);
                let after = state.book.book().state();
                if !before.same_status(after) {
                    state.note_transition(before, after, at);
                }
                info!(%stream, ?end, "session ended; book reset and marked desynced");
                state.sample_windows(at);
                return;
            }
        };

        // `same_status` rather than `!=`: a Kraken book's `last_verified`
        // advances on every matching checksum, and comparing whole states
        // would read each of those as a transition — resetting the "live for"
        // clock and logging a line per message on the one venue that verifies
        // continuously. See `BookState::same_status`.
        if !before.same_status(after) {
            state.note_transition(before, after, at);
            log_transition(&stream, before, after);

            // A desync caused by the *data* — a gap, a failed checksum, a
            // crossed book — needs a new subscription to repair, and only the
            // ingest task can get one. A desync caused by our own reset after
            // a disconnect must not land here, or the reconnect that just
            // happened would immediately request another; that case returns
            // above from the SessionEnded arm, which deliberately does not
            // ask.
            if let BookState::Desynced { reason, .. } = after
                && !matches!(before, BookState::Desynced { .. })
                && reason.needs_fresh_stream()
            {
                let heard = self.resync.request(&stream);
                warn!(
                    %stream, ?reason, heard,
                    "requesting a resync; the book cannot repair \
                     itself without a fresh snapshot"
                );
            }
        }

        for outcome in outcomes {
            let Outcome::Event(event) = outcome else {
                continue;
            };
            if matches!(event.kind, EventKind::Heartbeat { .. }) {
                state.counters.record_heartbeat();
            }
            if let Some(sink) = &self.events {
                // A closed sink means the writer finished or was never
                // started. Not a reason to stop aggregating — the live book is
                // the primary product and persistence is downstream of it.
                let _ = sink.send(event);
            }
        }

        // Sample after the book has been updated and after any transition has
        // been recorded, so the window sees the state this message produced
        // rather than the one it replaced.
        state.sample_windows(at);
    }

    /// Build the snapshot for this tick.
    pub fn snapshot(&mut self, channel: ChannelMetrics) -> Snapshot {
        let now = self.clock.now();
        self.seq += 1;
        let tick = self.tick;
        let depth_levels = self.depth_levels;

        // Grouped by symbol, preserving the BTreeMap's (venue, symbol) order
        // within each group. `symbols` ends up ordered by first appearance,
        // which for a BTreeMap keyed venue-then-symbol is stable run to run —
        // the property that keeps UI cards from shuffling between ticks.
        let mut by_symbol: BTreeMap<Symbol, Vec<VenueView>> = BTreeMap::new();
        // Kept beside the views because the consolidated touch is computed
        // from the same `TopOfBook` values the per-venue cards report. Reading
        // the books a second time would let the two disagree by a tick, and a
        // cross-venue spread that does not add up against the cards beside it
        // is worse than none.
        let mut tops: BTreeMap<Symbol, Vec<(VenueId, TopOfBook)>> = BTreeMap::new();

        // Cloned rather than held as a borrow guard across the loop: it is a
        // handful of `StreamId`s four times a second, and the alternative is a
        // guard alive while the books are borrowed mutably.
        let owned = self.owned.as_ref().map(|rx| rx.borrow().clone());

        for (stream, state) in &mut self.streams {
            if owned.as_ref().is_some_and(|owned| !owned.contains(stream)) {
                continue;
            }
            let counters = state.counters.snapshot();
            let rates = Rates::between(state.previous, counters, tick);
            state.previous = counters;

            let status = state.status();
            let trail = state.book.audit_trail();
            // Read before borrowing the book: `read` charges trust time up to
            // `now`, which is what keeps a stream that has gone silent from
            // reporting the coverage it had at its last message forever.
            let windows = state.windows.read(now);
            let book = state.book.book();
            let top = book.top_of_book(now);
            let (held_bids, held_asks) = book.depth();

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

            tops.entry(stream.symbol.clone())
                .or_default()
                .push((stream.venue, top));

            by_symbol
                .entry(stream.symbol.clone())
                .or_default()
                .push(VenueView {
                    venue: stream.venue,
                    status,
                    integrity,
                    desync_reason: match top.state {
                        BookState::Desynced { reason, .. } => Some(describe(reason)),
                        _ => None,
                    },
                    bid: top.bid,
                    ask: top.ask,
                    bids: book.top_levels(Side::Bid, depth_levels),
                    asks: book.top_levels(Side::Ask, depth_levels),
                    spread: top.spread().map(|d| d.to_string()),
                    mid: top.mid().map(|d| d.to_string()),
                    audits: trail.audits,
                    audit_mismatches: trail.mismatches,
                    age_ms: top.age.map(millis),
                    status_for_ms: millis(now.since(state.status_since)),
                    desynced_total_ms: millis(state.desynced_total(now)),
                    last_verified_ms,
                    levels_held: [held_bids, held_asks],
                    windows,
                    counters,
                    rates,
                });
        }

        Snapshot {
            seq: self.seq,
            wall_unix_ms: unix_millis(now.wall()),
            clock: INGEST_MONOTONIC,
            symbols: by_symbol
                .into_iter()
                .map(|(symbol, venues)| SymbolView {
                    weakest_integrity: venues.iter().filter_map(|v| v.integrity).min(),
                    cross: cross_view(tops.remove(&symbol).unwrap_or_default(), self.cross_policy),
                    symbol: symbol.to_string(),
                    venues,
                })
                .collect(),
            channel,
        }
    }
}

/// Consolidate one symbol's books and render the result for the wire.
fn cross_view(tops: Vec<(VenueId, TopOfBook)>, policy: CrossPolicy) -> CrossView {
    let cross = ma_core::consolidate(tops, policy);
    CrossView {
        bid: cross.bid,
        ask: cross.ask,
        spread: cross.spread.map(|d| d.to_string()),
        spread_bps: cross.spread_bps.map(|d| d.to_string()),
        mid: cross.mid.map(|d| d.to_string()),
        integrity_floor: cross.integrity_floor,
        oldest_leg_ms: cross.oldest_leg_ms,
        venues_used: cross.venues_used,
        crossed: cross.is_crossed(),
        single_venue: cross.is_single_venue(),
        excluded: cross
            .excluded
            .iter()
            .map(|e| ExclusionView {
                venue: e.venue,
                reason: e.reason.to_string(),
            })
            .collect(),
        clock: INGEST_MONOTONIC,
    }
}

fn log_transition(stream: &StreamId, from: BookState, to: BookState) {
    match to {
        BookState::Desynced { reason, .. } => {
            warn!(%stream, ?from, ?reason, "book lost trust");
        }
        BookState::Live { integrity, .. } => {
            info!(%stream, ?from, ?integrity, "book is live");
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
        DesyncReason::AuditMismatch { price, consecutive } => {
            format!("depth audit disagreed at {price}, {consecutive} audits running")
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
    use crate::resync::ResyncRequests;
    use ma_core::{SystemClock, TestClock};
    use ma_venues::{RawFrame, spec_for};

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD")
    }

    fn aggregator(venues: &[VenueId]) -> (Aggregator, Arc<Metrics>) {
        aggregator_over(venues, &[symbol()])
    }

    /// Every venue crossed with every symbol — the shape a multi-symbol
    /// process actually runs in.
    fn aggregator_over(venues: &[VenueId], symbols: &[Symbol]) -> (Aggregator, Arc<Metrics>) {
        let streams: Vec<StreamId> = venues
            .iter()
            .flat_map(|v| symbols.iter().map(|s| StreamId::new(*v, s.clone())))
            .collect();
        let metrics = Arc::new(Metrics::new(streams));
        let specs = venues
            .iter()
            .flat_map(|v| symbols.iter().map(|s| spec_for(*v, s).expect("spec")))
            .collect();
        let agg = Aggregator::new(specs, Arc::new(SystemClock), &metrics);
        (agg, metrics)
    }

    fn frame(venue: VenueId, json: &str) -> IngestMessage {
        frame_for(venue, &symbol(), json)
    }

    fn frame_for(venue: VenueId, symbol: &Symbol, json: &str) -> IngestMessage {
        IngestMessage::Frame(RawFrame::new(
            StreamId::new(venue, symbol.clone()),
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
        view_of(snapshot, "BTC-USD", venue)
    }

    fn view_of(snapshot: &Snapshot, symbol: &str, venue: VenueId) -> VenueView {
        snapshot
            .symbol(symbol)
            .unwrap_or_else(|| panic!("{symbol} in snapshot"))
            .venues
            .iter()
            .find(|v| v.venue == venue)
            .expect("venue in snapshot")
            .clone()
    }

    /// The single symbol group, for the many tests that only use one.
    fn only_symbol(snapshot: &Snapshot) -> &SymbolView {
        assert_eq!(snapshot.symbols.len(), 1, "expected exactly one symbol");
        &snapshot.symbols[0]
    }

    #[test]
    fn a_fresh_aggregator_reports_no_data_rather_than_nothing() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp]);
        let snap = agg.snapshot(empty_channel());

        let group = only_symbol(&snap);
        assert_eq!(group.venues.len(), 3);
        assert_eq!(group.weakest_integrity, None);
        for v in &group.venues {
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
        assert_eq!(v.levels_held, [1, 1]);
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
            stream: StreamId::new(VenueId::Coinbase, symbol()),
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
        let metrics = Arc::new(Metrics::new([StreamId::new(VenueId::Coinbase, symbol())]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            clock.clone(),
            &metrics,
        );

        let at = |c: &TestClock| c.now();
        agg.apply(IngestMessage::Frame(RawFrame::new(
            StreamId::new(VenueId::Coinbase, symbol()),
            br#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]}]}"#.to_vec(),
            at(&clock),
        )));
        assert_eq!(
            view(&agg.snapshot(empty_channel()), VenueId::Coinbase).desynced_total_ms,
            0
        );

        // Outage one: 3s.
        agg.apply(IngestMessage::SessionEnded {
            stream: StreamId::new(VenueId::Coinbase, symbol()),
            at: at(&clock),
            end: SessionEnd::Errored,
        });
        clock.advance(Duration::from_secs(3));
        agg.apply(IngestMessage::Frame(RawFrame::new(
            StreamId::new(VenueId::Coinbase, symbol()),
            br#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]}]}"#.to_vec(),
            at(&clock),
        )));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live);
        assert_eq!(v.desynced_total_ms, 3_000);

        // Outage two, still in progress: 2s so far, and it must be counted.
        agg.apply(IngestMessage::SessionEnded {
            stream: StreamId::new(VenueId::Coinbase, symbol()),
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
    fn a_matching_checksum_is_not_a_state_transition() {
        // Found by replaying a live tape: Kraken's `last_verified` advances on
        // every matching checksum, so comparing whole `BookState`s read each
        // verified message as a transition. Two symptoms, both invisible on
        // the other two venues because neither publishes a checksum — and
        // invisible to every fixture, because it takes a stream of *matching*
        // checksums rather than one.
        let clock = Arc::new(TestClock::new());
        let stream = StreamId::new(VenueId::Kraken, symbol());
        let metrics = Arc::new(Metrics::new([stream.clone()]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Kraken, &symbol()).unwrap()],
            clock.clone(),
            &metrics,
        );

        // Checksum 0 over an empty book is the degenerate case that verifies,
        // which is all this needs: the point is a *stream* of matching
        // checksums, not any particular book. A snapshot would legitimately
        // reset the clock — it re-establishes the book — so the repeats below
        // are updates.
        let frame = |kind: &str, clock: &TestClock| {
            IngestMessage::Frame(RawFrame::new(
                stream.clone(),
                format!(
                    r#"{{"channel":"book","type":"{kind}","data":[{{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":0}}]}}"#
                )
                .into_bytes(),
                clock.now(),
            ))
        };
        let verified = |clock: &TestClock| frame("update", clock);

        agg.apply(frame("snapshot", &clock));
        let v = view(&agg.snapshot(empty_channel()), VenueId::Kraken);
        assert_eq!(v.status, BookStatus::Live);
        assert_eq!(v.integrity, Some(Integrity::Verified));

        clock.advance(Duration::from_secs(30));
        agg.apply(verified(&clock));
        clock.advance(Duration::from_secs(30));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Kraken);
        assert_eq!(
            v.status_for_ms, 60_000,
            "a matching checksum reset the 'live for' clock, so a book healthy \
             for a minute reports only the time since its last message"
        );
        assert_eq!(
            v.desynced_total_ms, 0,
            "a verified book accrued untrusted time"
        );
    }

    #[test]
    fn the_consolidated_touch_spans_venues_and_names_its_clock() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase, VenueId::Bitstamp]);
        agg.apply(coinbase(0, "snapshot", "100", "103"));
        agg.apply(IngestMessage::Frame(RawFrame::rest_snapshot(
            StreamId::new(VenueId::Bitstamp, symbol()),
            br#"{"microtimestamp":"1700000000000000","bids":[["101","1"]],"asks":[["104","1"]]}"#
                .to_vec(),
            SystemClock.now(),
        )));

        let snap = agg.snapshot(empty_channel());
        let cross = &only_symbol(&snap).cross;

        assert_eq!(cross.bid.unwrap().venue, VenueId::Bitstamp, "best bid 101");
        assert_eq!(cross.ask.unwrap().venue, VenueId::Coinbase, "best ask 103");
        assert_eq!(cross.spread.as_deref(), Some("2"));
        assert_eq!(cross.venues_used, 2);
        assert!(!cross.crossed);
        assert!(!cross.single_venue);
        // A spread whose weaker leg cannot detect a lost message is an
        // order-only number, whatever the other leg proves.
        assert_eq!(cross.integrity_floor, Some(Integrity::OrderOnly));
        // CLAUDE.md: any cross-venue comparison surfaced must name its clock.
        assert_eq!(cross.clock, "ingest_monotonic");
    }

    #[test]
    fn a_desynced_venue_is_excluded_from_the_consolidated_touch_in_words() {
        // The pipeline-level version of ma_core::cross's rule. A desynced book
        // keeps its contents on purpose; if those contents reached the
        // consolidation, a stuck aggressive bid would show a standing
        // arbitrage against every healthy venue.
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase, VenueId::Kraken]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(frame(
            VenueId::Kraken,
            r#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":99999,"qty":1}],"asks":[{"price":100000,"qty":1}],"checksum":0}]}"#,
        ));

        let snap = agg.snapshot(empty_channel());
        let cross = &only_symbol(&snap).cross;

        // Kraken's checksum cannot match a one-level book, so it desyncs.
        assert_eq!(view(&snap, VenueId::Kraken).status, BookStatus::Desynced);
        assert_eq!(cross.venues_used, 1);
        assert!(!cross.crossed, "a desynced book manufactured an arbitrage");
        assert_eq!(cross.excluded.len(), 1);
        assert_eq!(cross.excluded[0].venue, VenueId::Kraken);
        assert_eq!(cross.excluded[0].reason, "book is not trusted");
    }

    #[test]
    fn a_window_spanning_an_outage_reports_the_hole_it_has() {
        // The end-to-end version of ma_core::window's coverage test, through
        // the real parser and the real session-boundary path. The reading that
        // matters is the one a naive implementation gets wrong: after the
        // reconnect the book is `live` and its 4s window looks like any other,
        // but a full second of that window is a book we were not entitled to
        // speak about.
        let clock = Arc::new(TestClock::new());
        let stream = StreamId::new(VenueId::Coinbase, symbol());
        let metrics = Arc::new(Metrics::new([stream.clone()]));
        let mut agg = Aggregator::with_window_spec(
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            clock.clone(),
            &metrics,
            WindowSpec::new(Duration::from_millis(250), [Duration::from_secs(4)]),
        );

        let snapshot_frame = |clock: &TestClock| {
            IngestMessage::Frame(RawFrame::new(
                stream.clone(),
                br#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"},{"side":"offer","price_level":"101","new_quantity":"1"}]}]}"#.to_vec(),
                clock.now(),
            ))
        };

        agg.apply(snapshot_frame(&clock));
        clock.advance(Duration::from_secs(1));

        agg.apply(IngestMessage::SessionEnded {
            stream: stream.clone(),
            at: clock.now(),
            end: SessionEnd::Errored,
        });
        clock.advance(Duration::from_secs(1));

        agg.apply(snapshot_frame(&clock));
        clock.advance(Duration::from_secs(1));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        assert_eq!(v.status, BookStatus::Live, "the book recovered");

        let w = &v.windows[0];
        assert_eq!(w.span_ms, 4_000);
        assert_eq!(
            w.trusted_ms, 2_000,
            "the outage did not show up as missing coverage"
        );
        assert!(w.is_partial());
        assert_eq!(w.samples, 2);
        assert_eq!(w.integrity_floor, Some(Integrity::GapDetectable));
        // `normalize` because a mid is *derived*: its scale is an artefact of
        // the division, not digits a venue sent and a checksum covers. The
        // trailing-zero discipline in `Price` applies to the latter.
        assert_eq!(
            w.high.map(|d| d.normalize().to_string()).as_deref(),
            Some("100.5")
        );
    }

    #[test]
    fn window_readings_are_published_for_every_configured_span() {
        let (mut agg, _m) = aggregator(&[VenueId::Coinbase]);
        agg.apply(coinbase(0, "snapshot", "100", "101"));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Coinbase);
        let spans: Vec<u64> = v.windows.iter().map(|w| w.span_ms).collect();
        assert_eq!(
            spans,
            vec![1_000, 10_000, 60_000],
            "the default spans are not what the snapshot published"
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
            StreamId::new(VenueId::Bitstamp, symbol()),
            br#"{"microtimestamp":"1700000000000000","bids":[["100","1"]],"asks":[["101","1"]]}"#
                .to_vec(),
            SystemClock.now(),
        )));

        let snap = agg.snapshot(empty_channel());
        assert_eq!(
            only_symbol(&snap).weakest_integrity,
            Some(Integrity::OrderOnly)
        );
    }

    #[test]
    fn a_data_caused_desync_asks_the_ingest_task_to_reconnect() {
        // Detection without recovery is the failure this closes. A sequence
        // gap leaves a healthy socket delivering updates the book correctly
        // refuses to apply; nothing else in the system would ever ask for the
        // snapshot that repairs it.
        let requests = ResyncRequests::new([StreamId::new(VenueId::Coinbase, symbol())]);
        let metrics = Arc::new(Metrics::new([StreamId::new(VenueId::Coinbase, symbol())]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            Arc::new(SystemClock),
            &metrics,
        )
        .requesting_resync_through(requests.clone());

        agg.apply(coinbase(0, "snapshot", "100", "101"));
        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Coinbase, symbol())),
            0,
            "healthy book"
        );

        agg.apply(coinbase(9, "update", "100", "101")); // expected 1
        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Coinbase, symbol())),
            1,
            "a sequence gap must ask for a resync"
        );
    }

    #[test]
    fn bitstamps_normal_startup_does_not_trigger_a_reconnect() {
        // Caught against the live venue, not in a test: Bitstamp opens in
        // Desynced{AwaitingSnapshot} on every connection, because it sends no
        // snapshot over the socket and the REST fetch is still in flight. That
        // is the protocol working, not a fault. Treating it as one reconnected
        // on every single startup — discarding a healthy socket and restarting
        // a handshake that was about to succeed, against a venue that can
        // rate-limit for it.
        let requests = ResyncRequests::new([StreamId::new(VenueId::Bitstamp, symbol())]);
        let metrics = Arc::new(Metrics::new([StreamId::new(VenueId::Bitstamp, symbol())]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Bitstamp, &symbol()).unwrap()],
            Arc::new(SystemClock),
            &metrics,
        )
        .requesting_resync_through(requests.clone());

        agg.apply(frame(
            VenueId::Bitstamp,
            r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"microtimestamp":"1700000000000001","bids":[["100","1"]],"asks":[]}}"#,
        ));

        let v = view(&agg.snapshot(empty_channel()), VenueId::Bitstamp);
        assert_eq!(v.status, BookStatus::Desynced, "should await a snapshot");
        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Bitstamp, symbol())),
            0,
            "awaiting a REST snapshot means recovery is already in flight"
        );

        // ...but a genuine ordering fault, after the splice, still asks.
        //
        // The regressing timestamp has to sit *above* the splice point and
        // below the last applied diff. Anything at or below the splice point
        // is data the snapshot already contains, which is ignored rather than
        // called a regression — see BitstampSync's `spliced_at`.
        agg.apply(IngestMessage::Frame(RawFrame::rest_snapshot(
            StreamId::new(VenueId::Bitstamp, symbol()),
            br#"{"microtimestamp":"1700000000000000","bids":[["100","1"]],"asks":[["101","1"]]}"#
                .to_vec(),
            SystemClock.now(),
        )));
        assert_eq!(
            view(&agg.snapshot(empty_channel()), VenueId::Bitstamp).status,
            BookStatus::Live
        );
        agg.apply(frame(
            VenueId::Bitstamp,
            r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"microtimestamp":"1700000000000020","bids":[["100","2"]],"asks":[]}}"#,
        ));
        agg.apply(frame(
            VenueId::Bitstamp,
            r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"microtimestamp":"1700000000000010","bids":[["100","3"]],"asks":[]}}"#,
        ));
        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Bitstamp, symbol())),
            1,
            "a timestamp regression is a real fault and must ask"
        );
    }

    #[test]
    fn our_own_reset_after_a_disconnect_does_not_ask_for_another_reconnect() {
        // SessionEnded already means the ingest task is reconnecting. Treating
        // the Desynced state it produces as a fresh problem would have the
        // aggregator request a reconnect for every reconnect — a self-inflicted
        // storm against a venue that may already be rate-limiting us.
        let requests = ResyncRequests::new([StreamId::new(VenueId::Coinbase, symbol())]);
        let metrics = Arc::new(Metrics::new([StreamId::new(VenueId::Coinbase, symbol())]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            Arc::new(SystemClock),
            &metrics,
        )
        .requesting_resync_through(requests.clone());

        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(IngestMessage::SessionEnded {
            stream: StreamId::new(VenueId::Coinbase, symbol()),
            at: SystemClock.now(),
            end: SessionEnd::Errored,
        });

        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Coinbase, symbol())),
            0
        );
    }

    #[test]
    fn a_book_already_desynced_does_not_re_request_on_every_frame() {
        // A venue sending a hundred updates a second into a desynced book must
        // not produce a hundred reconnect requests. The transition is what is
        // interesting, not the state.
        let requests = ResyncRequests::new([StreamId::new(VenueId::Coinbase, symbol())]);
        let metrics = Arc::new(Metrics::new([StreamId::new(VenueId::Coinbase, symbol())]));
        let mut agg = Aggregator::new(
            vec![spec_for(VenueId::Coinbase, &symbol()).unwrap()],
            Arc::new(SystemClock),
            &metrics,
        )
        .requesting_resync_through(requests.clone());

        agg.apply(coinbase(0, "snapshot", "100", "101"));
        agg.apply(coinbase(9, "update", "100", "101")); // gap -> 1 request
        for seq in 10..40 {
            agg.apply(coinbase(seq, "update", "100", "101"));
        }

        assert_eq!(
            requests.requested(&StreamId::new(VenueId::Coinbase, symbol())),
            1,
            "only the transition into Desynced should ask"
        );
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
        assert_eq!(only_symbol(&agg.snapshot(empty_channel())).venues.len(), 1);
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
