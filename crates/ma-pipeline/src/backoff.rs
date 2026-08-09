//! Reconnect backoff.
//!
//! This module exists before any code that opens a real socket, and that
//! ordering is deliberate: the plan's risk register puts "IP ban from
//! reconnect storms" first, with the mitigation "backoff tested before first
//! live connect." A venue that drops us and then sees a thousand reconnects a
//! second does not conclude that we are eager; it concludes that we are a
//! problem, and the ban outlasts the bug.
//!
//! # Nothing here sleeps
//!
//! [`Backoff::next_delay`] returns a [`Duration`]. Awaiting it is the caller's
//! job. That split is what makes the schedule assertable: the tests below
//! check the exact sequence of delays a venue that refuses ten connections in
//! a row would experience, and they run in microseconds because no test ever
//! waits out a delay it is checking.
//!
//! # Equal jitter, not full jitter
//!
//! With `attempt` failures behind us the ceiling is `base * 2^attempt`, capped.
//! The delay actually returned is a random point in the **upper half** of that
//! range: `[ceiling/2, ceiling]`.
//!
//! The better-known alternative is AWS's "full jitter" — a random point in
//! `[0, ceiling]` — which minimises total contention when many clients race
//! for one resource, and which this module deliberately does not use. Full
//! jitter's best case is retrying immediately, and immediately is exactly the
//! wrong thing to do to a venue that just rate-limited us: what gets an IP
//! banned is the number of attempts inside a window, and full jitter puts no
//! floor on that at all. Equal jitter keeps the decorrelation that matters
//! (three venues, or three deploys of this process, do not resynchronise into
//! a thundering herd after a shared outage) while guaranteeing each successive
//! attempt waits at least half the ceiling, which is what actually bounds the
//! attempt rate.
//!
//! # Why a connection succeeding is not enough to reset
//!
//! The obvious reset rule — "connected, so clear the counter" — has a failure
//! mode that shows up only against a venue that is unhealthy rather than down.
//! If the venue accepts the TCP connection and closes it half a second later,
//! every attempt is a "success", the counter resets every time, and the
//! backoff flatlines at `base` forever: a reconnect storm built entirely out
//! of successful connections. [`Backoff::note_session`] takes how long the
//! session actually lasted and resets only past
//! [`BackoffPolicy::min_stable`], so flapping keeps escalating.

use std::fmt;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::time::Duration;

/// Picks the actual delay inside a computed ceiling.
///
/// A trait rather than a hardcoded RNG so that tests can assert an exact
/// schedule ([`NoJitter`]) and separately assert that the randomised version
/// stays inside its bounds ([`EqualJitter`]).
pub trait Jitter: Send + fmt::Debug {
    /// Choose a delay for this attempt, given the ceiling the schedule
    /// computed. Implementations must return a value in `[0, ceiling]`.
    fn sample(&mut self, ceiling: Duration) -> Duration;
}

/// Returns the ceiling unjittered — the bare exponential schedule.
///
/// For tests, and for anywhere a reproducible schedule is worth more than
/// decorrelation. Not what a live connection should use: see the module docs
/// on why every client retrying on the same boundary is the problem jitter
/// solves.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoJitter;

impl Jitter for NoJitter {
    fn sample(&mut self, ceiling: Duration) -> Duration {
        ceiling
    }
}

/// Uniform in the upper half of the ceiling: `[ceiling/2, ceiling]`.
///
/// Backed by SplitMix64 rather than the `rand` crate. Jitter has no
/// cryptographic requirement — the only property needed is that two processes
/// starting from different seeds do not produce the same sequence — and a
/// dozen lines with an explicit seed makes a failing schedule test
/// reproducible by pasting the seed back in, which is worth more here than a
/// dependency.
#[derive(Debug, Clone)]
pub struct EqualJitter {
    state: u64,
}

impl EqualJitter {
    /// Seed from the OS, via the same entropy `HashMap` uses for its DoS
    /// resistance. Different every process.
    pub fn from_entropy() -> Self {
        Self::seeded(RandomState::new().build_hasher().finish())
    }

    /// Seed explicitly, for a reproducible sequence.
    pub const fn seeded(seed: u64) -> Self {
        Self { state: seed }
    }

    /// SplitMix64. Constants are the published ones; this is a fixed
    /// algorithm, not a tuned one.
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl Default for EqualJitter {
    fn default() -> Self {
        Self::from_entropy()
    }
}

impl Jitter for EqualJitter {
    fn sample(&mut self, ceiling: Duration) -> Duration {
        let floor = ceiling / 2;
        let span = nanos_u64(ceiling.saturating_sub(floor));
        if span == 0 {
            return ceiling;
        }
        // `% (span + 1)` so the top of the range is reachable; the modulo bias
        // over a span this size is far below the resolution anyone can observe
        // in a reconnect delay.
        floor + Duration::from_nanos(self.next_u64() % (span + 1))
    }
}

fn nanos_u64(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// The shape of the schedule, separate from where it currently is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    /// Ceiling for the first retry. Doubles per consecutive failure.
    pub base: Duration,
    /// Hard ceiling. A venue in a maintenance window can be down for an hour;
    /// the retry interval should flatten out rather than grow until the
    /// process is effectively no longer trying.
    pub cap: Duration,
    /// How long a session must last before it counts as a real recovery. See
    /// the module docs: without this, a venue that accepts and instantly
    /// closes pins the delay at `base` forever.
    pub min_stable: Duration,
}

impl BackoffPolicy {
    /// 500ms → 60s, needing 30s of uptime to count as recovered.
    ///
    /// The numbers are chosen against the venues rather than in the abstract:
    /// 500ms is comfortably above any venue's per-connection rate limit for a
    /// single retry, 60s means a venue in a maintenance window sees one
    /// attempt a minute from us, and 30s is longer than Coinbase's 60–90s idle
    /// disconnect is fast, so a connection that dies from a missed heartbeat
    /// still registers as unstable rather than as a healthy session.
    pub const DEFAULT: Self = Self {
        base: Duration::from_millis(500),
        cap: Duration::from_secs(60),
        min_stable: Duration::from_secs(30),
    };
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A policy plus the current position in it.
#[derive(Debug)]
pub struct Backoff {
    policy: BackoffPolicy,
    jitter: Box<dyn Jitter>,
    /// Consecutive failures so far. Drives the exponent.
    attempt: u32,
}

impl Backoff {
    pub fn new(policy: BackoffPolicy, jitter: impl Jitter + 'static) -> Self {
        Self {
            policy,
            jitter: Box::new(jitter),
            attempt: 0,
        }
    }

    /// The default policy with real jitter — what a live ingest task uses.
    pub fn live() -> Self {
        Self::new(BackoffPolicy::DEFAULT, EqualJitter::from_entropy())
    }

    /// Consecutive failures recorded so far.
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The un-jittered ceiling for the next call to [`Self::next_delay`].
    ///
    /// Exposed so a metric or a log line can report the schedule position
    /// without consuming an attempt.
    pub fn ceiling(&self) -> Duration {
        // 2^attempt, saturating. `checked_shl` returns `None` past 31 shifts,
        // by which point the cap has bound the result for a long time — a
        // saturating factor changes nothing observable.
        let factor = 1u32.checked_shl(self.attempt).unwrap_or(u32::MAX);
        self.policy
            .base
            .checked_mul(factor)
            .unwrap_or(self.policy.cap)
            .min(self.policy.cap)
    }

    /// How long to wait before the next connection attempt, advancing the
    /// schedule by one step.
    ///
    /// Returns rather than sleeps, so the schedule is testable without
    /// waiting — see the module docs.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.jitter.sample(self.ceiling());
        self.attempt = self.attempt.saturating_add(1);
        delay
    }

    /// Report how long a just-ended session lasted, resetting the schedule
    /// only if it lasted long enough to count as a recovery.
    ///
    /// Returns whether it reset, which is what a caller logs — "reconnected
    /// after 4 attempts" is only true if this said so.
    pub fn note_session(&mut self, lasted: Duration) -> bool {
        let stable = lasted >= self.policy.min_stable;
        if stable {
            self.reset();
        }
        stable
    }

    /// Clear the schedule unconditionally. Prefer [`Self::note_session`] from
    /// a reconnect loop; this is for a caller that knows something stronger
    /// than elapsed time, such as a successful resubscribe handshake.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn policy() -> BackoffPolicy {
        BackoffPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(2),
            min_stable: Duration::from_secs(30),
        }
    }

    fn schedule(backoff: &mut Backoff, steps: usize) -> Vec<Duration> {
        (0..steps).map(|_| backoff.next_delay()).collect()
    }

    #[test]
    fn delays_double_until_the_cap_then_flatten() {
        // The whole point of the return-a-Duration design: this asserts the
        // shape of a nine-attempt outage and takes no measurable time.
        let mut b = Backoff::new(policy(), NoJitter);
        let ms: Vec<u128> = schedule(&mut b, 8)
            .iter()
            .map(Duration::as_millis)
            .collect();
        assert_eq!(ms, [100, 200, 400, 800, 1600, 2000, 2000, 2000]);
    }

    #[test]
    fn a_long_outage_never_overflows_the_exponent() {
        // 2^attempt overflows u32 at 32 attempts; a venue down for a day would
        // reach that. The delay must stay pinned at the cap, not wrap to zero
        // and turn into the reconnect storm this module exists to prevent.
        let mut b = Backoff::new(policy(), NoJitter);
        for _ in 0..200 {
            let _ = b.next_delay();
        }
        assert_eq!(b.next_delay(), Duration::from_secs(2));
        assert_eq!(b.ceiling(), Duration::from_secs(2));
    }

    #[test]
    fn a_stable_session_resets_the_schedule() {
        let mut b = Backoff::new(policy(), NoJitter);
        schedule(&mut b, 4);
        assert_eq!(b.attempt(), 4);

        assert!(b.note_session(Duration::from_secs(60)));
        assert_eq!(b.attempt(), 0);
        assert_eq!(b.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn a_flapping_connection_keeps_escalating() {
        // The bug this prevents: a venue that accepts the connection and drops
        // it immediately is a "success" on every attempt. Resetting on connect
        // rather than on stability would hold the delay at `base` forever and
        // produce a reconnect storm made entirely of successful connections.
        let mut b = Backoff::new(policy(), NoJitter);
        let mut delays = Vec::new();
        for _ in 0..4 {
            delays.push(b.next_delay());
            assert!(
                !b.note_session(Duration::from_millis(400)),
                "a 400ms session should not count as a recovery"
            );
        }
        let ms: Vec<u128> = delays.iter().map(Duration::as_millis).collect();
        assert_eq!(ms, [100, 200, 400, 800], "flapping flatlined the backoff");
    }

    #[test]
    fn min_stable_is_inclusive_at_the_boundary() {
        let mut b = Backoff::new(policy(), NoJitter);
        b.next_delay();
        assert!(b.note_session(Duration::from_secs(30)));
    }

    #[test]
    fn jitter_stays_in_the_upper_half_of_the_ceiling() {
        // The floor is the property that bounds the attempt rate, and it is
        // what full jitter would give up. Checked across the whole schedule,
        // not just one step, because the ceiling changes under it.
        let mut b = Backoff::new(policy(), EqualJitter::seeded(0xDECAF));
        for _ in 0..40 {
            let ceiling = b.ceiling();
            let delay = b.next_delay();
            assert!(
                delay >= ceiling / 2 && delay <= ceiling,
                "delay {delay:?} escaped [{:?}, {ceiling:?}]",
                ceiling / 2
            );
        }
    }

    #[test]
    fn jitter_actually_varies() {
        // A "jitter" that returned the same value every time would pass the
        // bounds test above and provide no decorrelation at all.
        let mut b = Backoff::new(policy(), EqualJitter::seeded(7));
        b.attempt = 5; // pinned at the cap, so the ceiling stops moving
        let samples: std::collections::HashSet<Duration> =
            (0..20).map(|_| b.jitter.sample(b.policy.cap)).collect();
        assert!(
            samples.len() > 15,
            "only {} distinct delays out of 20",
            samples.len()
        );
    }

    #[test]
    fn different_seeds_diverge() {
        let ceiling = Duration::from_secs(2);
        let mut a = EqualJitter::seeded(1);
        let mut b = EqualJitter::seeded(2);
        let sa: Vec<Duration> = (0..10).map(|_| a.sample(ceiling)).collect();
        let sb: Vec<Duration> = (0..10).map(|_| b.sample(ceiling)).collect();
        assert_ne!(sa, sb, "two seeds produced an identical schedule");
    }

    #[test]
    fn a_seed_reproduces_its_schedule() {
        // What makes a failing schedule test debuggable: paste the seed back.
        let ceiling = Duration::from_secs(2);
        let run = || {
            let mut j = EqualJitter::seeded(0xA11CE);
            (0..10).map(|_| j.sample(ceiling)).collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn a_zero_width_ceiling_is_handled() {
        // `base` of zero, or a cap rounding to nothing, must not divide by
        // zero or spin.
        let mut j = EqualJitter::seeded(1);
        assert_eq!(j.sample(Duration::ZERO), Duration::ZERO);
        assert_eq!(j.sample(Duration::from_nanos(1)), Duration::from_nanos(1));
    }
}
