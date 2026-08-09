//! Counters.
//!
//! CLAUDE.md's line about the drop policy — "a silent drop policy is a bug" —
//! generalises to everything in this module. A reconnect nobody counted, a
//! book that spent four minutes `Desynced` while the UI showed prices, a REST
//! snapshot that 503'd on every attempt: each of those is a system that is
//! wrong and looks fine. The counters here are what make them visible.
//!
//! # Monotonic counters, rates derived at the edge
//!
//! Everything here only ever increases. Nothing computes an average, and
//! nothing holds a window. "Events per second" is produced by the aggregator
//! diffing two snapshots a tick apart ([`Rates::between`]), which means the
//! rate is always attributable to a stated interval rather than to a decaying
//! average whose time constant nobody remembers. It is also what a Prometheus
//! scrape would want if this grows one.
//!
//! # Why atomics rather than a channel
//!
//! Ingest tasks are on the hot path, and the point of the bounded channel is
//! that ingest never waits for a consumer. Making ingest publish metrics
//! through a channel would reintroduce exactly the coupling the channel
//! removes. A relaxed atomic increment is a few nanoseconds and cannot block
//! anybody. Relaxed ordering is correct here because no counter guards access
//! to other memory — they are read for display, never to make a decision.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ma_core::StreamId;
use serde::Serialize;

/// Live counters for one venue's ingest task.
#[derive(Debug, Default)]
pub struct VenueCounters {
    frames: AtomicU64,
    bytes: AtomicU64,
    connects: AtomicU64,
    disconnects: AtomicU64,
    connect_failures: AtomicU64,
    idle_timeouts: AtomicU64,
    dropped: AtomicU64,
    rest_fetches: AtomicU64,
    rest_failures: AtomicU64,
    parse_errors: AtomicU64,
    heartbeats: AtomicU64,
    desyncs: AtomicU64,
    applied: AtomicU64,
}

macro_rules! counter {
    ($( $field:ident => $bump:ident ),* $(,)?) => {
        impl VenueCounters {
            $(
                pub fn $bump(&self) {
                    self.$field.fetch_add(1, Ordering::Relaxed);
                }
            )*
        }
    };
}

counter! {
    connects => record_connect,
    disconnects => record_disconnect,
    connect_failures => record_connect_failure,
    idle_timeouts => record_idle_timeout,
    dropped => record_drop,
    rest_fetches => record_rest_fetch,
    rest_failures => record_rest_failure,
    parse_errors => record_parse_error,
    heartbeats => record_heartbeat,
    desyncs => record_desync,
    applied => record_applied,
}

impl VenueCounters {
    /// One frame arrived. Byte count travels with it so that "the venue is
    /// sending, but sending nothing useful" is distinguishable from silence.
    pub fn record_frame(&self, bytes: usize) {
        self.frames.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(bytes.try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> VenueCountersSnapshot {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        VenueCountersSnapshot {
            frames: load(&self.frames),
            bytes: load(&self.bytes),
            connects: load(&self.connects),
            disconnects: load(&self.disconnects),
            connect_failures: load(&self.connect_failures),
            idle_timeouts: load(&self.idle_timeouts),
            dropped: load(&self.dropped),
            rest_fetches: load(&self.rest_fetches),
            rest_failures: load(&self.rest_failures),
            parse_errors: load(&self.parse_errors),
            heartbeats: load(&self.heartbeats),
            desyncs: load(&self.desyncs),
            applied: load(&self.applied),
        }
    }
}

/// A reading of [`VenueCounters`] at one instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct VenueCountersSnapshot {
    pub frames: u64,
    pub bytes: u64,
    /// Successful connections. Also the reconnect count, less one, which is
    /// the number CLAUDE.md's metric list asks for.
    pub connects: u64,
    pub disconnects: u64,
    /// Attempts that never opened a socket. Separate from `disconnects`
    /// because "the venue refuses us" and "the venue keeps hanging up" call
    /// for different responses: the first is often a ban, the second is not.
    pub connect_failures: u64,
    /// Sessions killed by the idle watchdog. A climbing count with no
    /// `connect_failures` is the signature of a venue that accepts
    /// connections and then stops talking — the failure a liveness check that
    /// only watched the socket would never see.
    pub idle_timeouts: u64,
    /// Frames the bounded channel evicted before the aggregator read them.
    /// See `channel`'s module docs for why dropping is the policy and why
    /// this counter is what keeps it from being a silent one.
    pub dropped: u64,
    pub rest_fetches: u64,
    pub rest_failures: u64,
    /// Frames the venue's parser rejected. Non-zero means the venue changed
    /// its wire format, or we misread it — the drift that risk register #2
    /// says tape fixtures exist to catch.
    pub parse_errors: u64,
    pub heartbeats: u64,
    /// Times this venue's book went from trusted to untrusted. Distinct from
    /// `disconnects`: a checksum mismatch or a sequence gap desyncs a book on
    /// a connection that never dropped.
    pub desyncs: u64,
    /// Messages the aggregator actually processed.
    ///
    /// Deliberately not the same number as `frames`, which counts what ingest
    /// *received*. The gap between them is `dropped` — what the bounded
    /// channel discarded on the way — so publishing both makes backpressure
    /// legible instead of inferable.
    ///
    /// It is also what makes replay honest: a replayed tape has no ingest
    /// task, so `frames` is genuinely zero while `applied` climbs. Folding the
    /// two together would have meant either lying about what was received or
    /// showing a page full of zeroes beside a visibly updating book.
    pub applied: u64,
}

impl VenueCountersSnapshot {
    /// Reconnects, i.e. connections after the first.
    pub const fn reconnects(&self) -> u64 {
        self.connects.saturating_sub(1)
    }
}

/// Per-second rates over a stated interval, derived from two snapshots.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Rates {
    pub frames_per_sec: f64,
    pub bytes_per_sec: f64,
    pub drops_per_sec: f64,
    /// The interval these rates were measured over. Published alongside them
    /// deliberately: a rate without its window is not a number anyone can
    /// check.
    pub over_secs: f64,
}

impl Rates {
    /// Rates between an earlier and a later reading.
    ///
    /// Returns zeroes for a non-positive interval rather than dividing — two
    /// snapshots taken inside the same tick is a caller mistake, not a reason
    /// to publish an infinity into a chart.
    #[allow(clippy::float_arithmetic)]
    pub fn between(
        earlier: VenueCountersSnapshot,
        later: VenueCountersSnapshot,
        interval: Duration,
    ) -> Self {
        let secs = interval.as_secs_f64();
        if secs <= 0.0 {
            return Self::default();
        }
        let per_sec = |a: u64, b: u64| {
            // Counters only increase, but a restarted process or a
            // freshly-registered venue can make `later` smaller. Saturating
            // keeps a negative rate off the chart.
            let delta = b.saturating_sub(a);
            // Lossy past 2^53 frames, which at any plausible venue rate is
            // several million years of uptime.
            delta as f64 / secs
        };
        Self {
            frames_per_sec: per_sec(earlier.frames, later.frames),
            bytes_per_sec: per_sec(earlier.bytes, later.bytes),
            drops_per_sec: per_sec(earlier.dropped, later.dropped),
            over_secs: secs,
        }
    }
}

/// Every stream's counters, fixed at startup.
///
/// The stream set is known when the process starts and never changes, so the
/// map is built once and then only read. That is why there is no lock here
/// and no `RwLock<HashMap>`: registration is not a runtime operation.
///
/// # Keyed by stream, not by venue
///
/// v1 keyed this by [`VenueId`], which was correct while there was one symbol.
/// With several, a venue-keyed counter holds the *sum* across symbols, and a
/// sum is the worst possible answer here: `parse_errors{venue="kraken"} = 3`
/// tells an operator nothing about which feed is drifting, and a single
/// desynced symbol is invisible inside a healthy venue total. Every counter is
/// therefore per [`StreamId`], and `/metrics` emits `venue` and `symbol` as
/// two labels so a query can still aggregate over either.
#[derive(Debug, Default)]
pub struct Metrics {
    streams: BTreeMap<StreamId, Arc<VenueCounters>>,
}

impl Metrics {
    pub fn new(streams: impl IntoIterator<Item = StreamId>) -> Self {
        Self {
            streams: streams
                .into_iter()
                .map(|s| (s, Arc::new(VenueCounters::default())))
                .collect(),
        }
    }

    /// Counters for a stream, or `None` if it was not registered at startup.
    pub fn stream(&self, stream: &StreamId) -> Option<Arc<VenueCounters>> {
        self.streams.get(stream).cloned()
    }

    pub fn snapshot(&self) -> BTreeMap<StreamId, VenueCountersSnapshot> {
        self.streams
            .iter()
            .map(|(stream, counters)| (stream.clone(), counters.snapshot()))
            .collect()
    }

    /// Every registered stream, in a stable order.
    pub fn streams(&self) -> impl Iterator<Item = &StreamId> {
        self.streams.keys()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::VenueId;

    #[test]
    fn counters_start_at_zero_and_count_up() {
        let c = VenueCounters::default();
        assert_eq!(c.snapshot(), VenueCountersSnapshot::default());

        c.record_frame(120);
        c.record_frame(80);
        c.record_connect();
        c.record_drop();

        let s = c.snapshot();
        assert_eq!(s.frames, 2);
        assert_eq!(s.bytes, 200);
        assert_eq!(s.connects, 1);
        assert_eq!(s.dropped, 1);
    }

    #[test]
    fn the_first_connection_is_not_a_reconnect() {
        let c = VenueCounters::default();
        assert_eq!(c.snapshot().reconnects(), 0, "never connected");
        c.record_connect();
        assert_eq!(c.snapshot().reconnects(), 0, "first connect is not a re-");
        c.record_connect();
        assert_eq!(c.snapshot().reconnects(), 1);
    }

    #[test]
    fn rates_are_a_delta_over_a_stated_interval() {
        let earlier = VenueCountersSnapshot {
            frames: 100,
            bytes: 1_000,
            dropped: 2,
            ..Default::default()
        };
        let later = VenueCountersSnapshot {
            frames: 160,
            bytes: 4_000,
            dropped: 8,
            ..Default::default()
        };
        let r = Rates::between(earlier, later, Duration::from_secs(2));
        assert_eq!(r.frames_per_sec, 30.0);
        assert_eq!(r.bytes_per_sec, 1_500.0);
        assert_eq!(r.drops_per_sec, 3.0);
        assert_eq!(r.over_secs, 2.0);
    }

    #[test]
    fn a_zero_interval_yields_zero_rather_than_infinity() {
        let s = VenueCountersSnapshot {
            frames: 5,
            ..Default::default()
        };
        let r = Rates::between(VenueCountersSnapshot::default(), s, Duration::ZERO);
        assert_eq!(r, Rates::default());
        assert!(r.frames_per_sec.is_finite(), "an infinity reached a chart");
    }

    #[test]
    fn a_counter_going_backwards_does_not_produce_a_negative_rate() {
        let earlier = VenueCountersSnapshot {
            frames: 500,
            ..Default::default()
        };
        let later = VenueCountersSnapshot {
            frames: 3,
            ..Default::default()
        };
        let r = Rates::between(earlier, later, Duration::from_secs(1));
        assert_eq!(r.frames_per_sec, 0.0);
    }

    fn stream(venue: VenueId, symbol: &str) -> StreamId {
        StreamId::new(venue, ma_core::Symbol::new(symbol))
    }

    #[test]
    fn counters_are_shared_not_copied() {
        // The ingest task holds one Arc and the metrics endpoint reads
        // another. If `stream()` handed out a fresh counter set, every metric
        // would read zero forever while looking perfectly plausible.
        let metrics = Metrics::new([
            stream(VenueId::Coinbase, "BTC-USD"),
            stream(VenueId::Kraken, "BTC-USD"),
        ]);
        let held = metrics
            .stream(&stream(VenueId::Coinbase, "BTC-USD"))
            .expect("registered");
        held.record_frame(10);

        assert_eq!(
            metrics.snapshot()[&stream(VenueId::Coinbase, "BTC-USD")].frames,
            1
        );
        assert_eq!(
            metrics.snapshot()[&stream(VenueId::Kraken, "BTC-USD")].frames,
            0
        );
        assert!(
            metrics
                .stream(&stream(VenueId::Bitstamp, "BTC-USD"))
                .is_none()
        );
    }

    #[test]
    fn two_symbols_on_one_venue_do_not_share_counters() {
        // The whole reason this map is keyed by stream. Summing them would
        // hide a single desynced symbol inside a healthy venue total, and
        // would look entirely plausible while doing it.
        let btc = stream(VenueId::Coinbase, "BTC-USD");
        let eth = stream(VenueId::Coinbase, "ETH-USD");
        let metrics = Metrics::new([btc.clone(), eth.clone()]);

        metrics.stream(&btc).expect("registered").record_frame(10);
        metrics.stream(&btc).expect("registered").record_frame(10);
        metrics
            .stream(&eth)
            .expect("registered")
            .record_parse_error();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot[&btc].frames, 2);
        assert_eq!(snapshot[&btc].parse_errors, 0);
        assert_eq!(snapshot[&eth].frames, 0);
        assert_eq!(snapshot[&eth].parse_errors, 1);
    }
}
