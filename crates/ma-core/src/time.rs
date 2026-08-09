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

/// A clock that runs at a multiple of the real one, from a fixed base.
///
/// # Why replay needs its own clock at all
///
/// Replay reconstructs each frame's [`IngestTime`] as `base + recorded_offset`
/// — see `ma_pipeline::tape`. At a speed multiplier of `n`, those offsets
/// advance `n` times faster than the wall clock does, so an aggregator reading
/// [`SystemClock`] is comparing timestamps from *tape time* against a `now`
/// from *wall time*, and the two diverge without limit.
///
/// Every duration the aggregator derives is then wrong, and wrong in the
/// direction that hides it: `now` is behind the events, so
/// `now.since(last_update)` saturates to zero and book age reads a healthy
/// `0ms` while every rolling window, whose buckets are indexed off the event
/// clock, reads *empty*. A `--speed 5` demo showed full books, zero ages, and
/// no window data at all — three symptoms of one mismatch, none of which looks
/// like a clock problem.
///
/// A `ScaledClock` closes it by construction: the aggregator's `now` advances
/// at exactly the rate the tape's offsets do, so a "10-second window" over a
/// 5× replay means ten seconds *of market*, which is the only reading that
/// means anything.
///
/// At `speed == 1.0` this is [`SystemClock`] with an offset, which is why a
/// realtime replay was correct before this existed and a fast one was not.
///
/// **Full-speed replay (`Pacing::Faithful`) has no meaningful wall-clock
/// semantics at all** — it consumes a three-minute tape in about a second —
/// and deliberately does not use this. Its purpose is to prove that the same
/// tape produces the same *books*, which is a claim about the event sequence
/// and not about time.
#[derive(Debug)]
pub struct ScaledClock {
    base: IngestTime,
    origin: Instant,
    speed: f64,
}

impl ScaledClock {
    /// `base` should be the same reading replay uses to anchor its offsets.
    ///
    /// A non-positive speed is clamped rather than rejected: it arrives from a
    /// command line, and the clock stopping dead is a far more confusing
    /// failure than a run that goes at real time and says so.
    pub fn new(base: IngestTime, speed: f64) -> Self {
        Self {
            base,
            origin: Instant::now(),
            speed: if speed > 0.0 { speed } else { 1.0 },
        }
    }

    pub fn speed(&self) -> f64 {
        self.speed
    }
}

impl Clock for ScaledClock {
    fn now(&self) -> IngestTime {
        self.base
            .advanced_by(self.origin.elapsed().mul_f64(self.speed))
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
    fn a_scaled_clock_runs_at_its_multiple_of_real_time() {
        let base = SystemClock.now();
        let clock = ScaledClock::new(base, 10.0);
        std::thread::sleep(Duration::from_millis(20));
        let elapsed = clock.now().since(base);

        // Bounds rather than an equality: this is the one clock in the project
        // that cannot be driven deterministically, because scaling a real
        // clock is the whole of its job. Ten times a 20ms sleep is 200ms, and
        // the window below is wide enough for a loaded machine and far too
        // narrow to admit an unscaled clock, which would read ~20ms.
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed < Duration::from_secs(2),
            "a 10x clock reported {elapsed:?} over a 20ms sleep"
        );
    }

    #[test]
    fn a_non_positive_speed_falls_back_to_real_time_rather_than_stopping() {
        // Arrives from a command line. A clock that stopped dead would make
        // every book age zero and every window empty — the exact symptom this
        // type exists to prevent.
        for speed in [0.0, -1.0] {
            assert_eq!(ScaledClock::new(SystemClock.now(), speed).speed(), 1.0);
        }
    }

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
