//! Time as *we* observed it, which is the only time we trust.
//!
//! Venue timestamps disagree with each other, sometimes by seconds, and some
//! venues are simply wrong. Every rule in this module exists because of that.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

/// A moment observed locally, carrying both clocks on purpose.
///
/// The two are not interchangeable and neither one alone is sufficient:
///
/// - [`mono`](Self::mono) is a [`Instant`], and is the **only** clock used for
///   windowing, book age, and ordering. It cannot run backwards when NTP steps
///   the system clock mid-session.
/// - [`wall`](Self::wall) is a [`SystemTime`], and is the **only** clock that
///   can be written to Parquet or shown to a human, because an `Instant` has no
///   meaning outside the process that created it.
///
/// Carrying one and deriving the other looks like a simplification and is not.
/// Wall-only makes every window wrong across an NTP correction. Monotonic-only
/// makes recorded data unreadable on the next run.
///
/// Note the deliberate absence of `Serialize`: serialising an `Instant` is
/// meaningless, so the persistence layer is forced to reach for `wall()`
/// explicitly and state what it is doing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct IngestTime {
    mono: Instant,
    wall: SystemTime,
}

impl IngestTime {
    /// Construct from both clocks. Prefer a [`Clock`] in library code; this is
    /// for replay, where timestamps come off a tape rather than the OS.
    pub fn new(mono: Instant, wall: SystemTime) -> Self {
        Self { mono, wall }
    }

    /// The monotonic reading. Use this for anything comparative.
    pub fn mono(&self) -> Instant {
        self.mono
    }

    /// The wall reading. Use this only for output: logs, Parquet, the UI.
    pub fn wall(&self) -> SystemTime {
        self.wall
    }

    /// Elapsed time since an earlier observation, measured monotonically.
    ///
    /// Saturates at zero rather than panicking if `earlier` is actually later,
    /// which can happen when two ingest tasks stamp events concurrently.
    pub fn since(&self, earlier: IngestTime) -> Duration {
        self.mono.saturating_duration_since(earlier.mono)
    }

    /// Both clocks advanced by the same amount. This is how replay synthesises
    /// timestamps: take a base observation and step it by the deltas recorded
    /// on the tape, so the monotonic ordering of a replay matches the original.
    pub fn advanced_by(&self, delta: Duration) -> Self {
        Self {
            // Saturating rather than panicking. Overflow needs a delta of
            // ~584 years, so this arm is unreachable in practice; it exists so
            // a corrupt tape degrades instead of taking down the process.
            mono: self.mono.checked_add(delta).unwrap_or(self.mono),
            wall: self.wall.checked_add(delta).unwrap_or(self.wall),
        }
    }
}

impl fmt::Debug for IngestTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `Instant`'s own Debug is an opaque platform value and tells a reader
        // nothing, so show the wall clock and note that it is not the one used
        // for comparisons.
        match self.wall.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => write!(
                f,
                "IngestTime(wall={}.{:09}s)",
                d.as_secs(),
                d.subsec_nanos()
            ),
            Err(_) => write!(f, "IngestTime(wall=pre-epoch)"),
        }
    }
}

/// Source of [`IngestTime`].
///
/// Exists so that no logic in this crate calls `Instant::now()` directly. Every
/// timing rule — book age, stale-data thresholds, backoff schedules — is then
/// testable by advancing a [`TestClock`] instead of sleeping, which is what
/// keeps the offline suite fast and deterministic.
pub trait Clock: Send + Sync + fmt::Debug {
    fn now(&self) -> IngestTime;
}

/// The real clock. Used everywhere outside tests and replay.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> IngestTime {
        IngestTime::new(Instant::now(), SystemTime::now())
    }
}

/// A clock that only moves when told to.
///
/// Lets a test assert "the book was stale for 30 seconds" without a test that
/// takes 30 seconds, and lets backoff schedules be checked by their shape
/// rather than by waiting them out.
#[derive(Debug)]
pub struct TestClock {
    base: IngestTime,
    offset_nanos: AtomicU64,
}

impl TestClock {
    pub fn new() -> Self {
        Self {
            base: SystemClock.now(),
            offset_nanos: AtomicU64::new(0),
        }
    }

    /// Move both clocks forward by `delta`.
    pub fn advance(&self, delta: Duration) {
        let nanos = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        self.offset_nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Default for TestClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TestClock {
    fn now(&self) -> IngestTime {
        self.base.advanced_by(Duration::from_nanos(
            self.offset_nanos.load(Ordering::Relaxed),
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_moves_only_when_advanced() {
        let clock = TestClock::new();
        let t0 = clock.now();
        let t1 = clock.now();
        assert_eq!(t0.since(t1), Duration::ZERO, "clock moved on its own");

        clock.advance(Duration::from_secs(30));
        let t2 = clock.now();
        assert_eq!(t2.since(t0), Duration::from_secs(30));
    }

    #[test]
    fn both_clocks_advance_together() {
        let clock = TestClock::new();
        let t0 = clock.now();
        clock.advance(Duration::from_millis(1500));
        let t1 = clock.now();

        let mono_delta = t1.since(t0);
        let wall_delta = t1
            .wall()
            .duration_since(t0.wall())
            .expect("wall clock went backwards under a monotonic advance");

        assert_eq!(mono_delta, wall_delta);
    }

    #[test]
    fn since_saturates_instead_of_panicking_on_reordering() {
        // Two ingest tasks can stamp events on different cores and arrive out of
        // order. That must not be able to panic the aggregator.
        let clock = TestClock::new();
        let earlier = clock.now();
        clock.advance(Duration::from_secs(5));
        let later = clock.now();

        assert_eq!(earlier.since(later), Duration::ZERO);
        assert_eq!(later.since(earlier), Duration::from_secs(5));
    }
}
