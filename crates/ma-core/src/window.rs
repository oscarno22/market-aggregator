//! Rolling indicators over configurable windows.
//!
//! v1 and v2 published *instantaneous* readings: the touch, the spread, the
//! book age at this tick. Those answer "what is the market now". They cannot
//! answer "how much has it moved in the last minute", which is the question
//! every consumer of this data asks second.
//!
//! # The claim a window makes, and why it is usually false
//!
//! A "60-second high" claims to be the highest mid **over the last sixty
//! seconds**. If the book spent twenty of those seconds `Desynced`, the number
//! is the highest mid over the forty seconds we were watching, published under
//! a label that says sixty. That is the same class of lie as a cross-venue
//! spread that does not name its clock, or a `Live` book that does not say how
//! strongly it is live: a number that looks like it covers more than it does.
//!
//! It is also the *likely* case rather than the exotic one. Every reconnect,
//! every sequence gap, every REST splice at startup puts a hole in a window,
//! and a Bitstamp book at startup is `Desynced` for the first REST round trip
//! by construction.
//!
//! So every [`WindowReading`] carries two fields it would be cheaper to omit:
//!
//! - [`WindowReading::trusted_ms`] out of [`WindowReading::span_ms`] — how much
//!   of the window the book was actually trusted for. A reader can divide;
//!   this type will not do it for them, because a single "coverage: 0.67"
//!   float hides which of the two numbers is unusual.
//! - [`WindowReading::integrity_floor`] — the weakest [`Integrity`] observed
//!   while sampling. The same argument as `SymbolView::weakest_integrity`, one
//!   dimension over: a window that spans a reconnect can contain samples from
//!   two different guarantees, and reporting the stronger one is a lie the
//!   `Ord` on `Integrity` exists to prevent.
//!
//! A window with `trusted_ms == 0` is not "flat". It is *nothing*, and the
//! `Option` fields are all `None` so it cannot be read as zero.
//!
//! # One sample store, many windows
//!
//! The obvious implementation gives every configured span its own buffer of
//! samples. Three spans then keep three copies of the same mids, and the cost
//! of adding a fourth window is another copy of a busy book's entire update
//! stream.
//!
//! Instead there is one ring of fixed-width **buckets** per stream, sized to
//! the longest configured span, and a window of span `S` is a suffix of that
//! ring `ceil(S / resolution)` buckets long. Adding a span costs nothing at
//! ingest time and nothing in memory unless it is longer than the current
//! longest. Each bucket holds an aggregate — count, sum, first, last, high,
//! low — so memory is `O(spans_max / resolution)` rather than
//! `O(updates_per_second × spans_max)`, which on Coinbase BTC-USD is the
//! difference between a few kilobytes and a few megabytes per stream.
//!
//! The price is granularity, and it is worth stating precisely rather than
//! hand-waving: **a reading covers whole buckets only.** The in-progress
//! bucket is deliberately excluded, so `span_ms` is exactly
//! `ceil(S / resolution) × resolution` rather than "about `S`, plus however
//! far into the current bucket we happen to be". The cost is that a reading
//! lags by up to one `resolution`. Given the aggregator publishes at 250ms and
//! the default resolution matches it, that lag is one publish tick, and in
//! exchange every window boundary is exact and reproducible in a test.
//!
//! # Every duration here is monotonic
//!
//! Bucket boundaries, the trust accounting, and `span_ms` are all measured on
//! [`IngestTime::mono`]. A window keyed on the wall clock would silently
//! double-count or skip an entire bucket the first time NTP stepped the clock
//! mid-session, and would do it without any signal that it had happened.
//!
//! # What is deliberately not computed here
//!
//! Realised volatility, in the usual sense — the standard deviation of log
//! returns — is absent. It needs a logarithm and a square root, which means
//! `f64`, which the workspace lints against everywhere for the reasons in
//! [`crate::price`]. [`WindowReading::range_bps`] is the substitute: the
//! high-low range as a fraction of the window's own mean, in basis points. It
//! is a cruder statistic and it is exact, needs no distributional assumption,
//! and cannot disagree with the prices the checksum verified.

use std::time::Duration;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::{BookState, Integrity, TopOfBook};
use crate::time::IngestTime;

/// Bucket width when none is given. Matches the aggregator's publish tick, so
/// a reading is at most one publish stale — see the module docs on why the
/// in-progress bucket is excluded.
pub const DEFAULT_RESOLUTION: Duration = Duration::from_millis(250);

/// Spans published when none are configured: a second, ten seconds, a minute.
///
/// Three rather than one because the interesting reading is usually the
/// *disagreement* between them — a 1s range far wider than the 60s range is a
/// burst, the reverse is a trend — and because the marginal cost of a span is
/// a loop over buckets already in cache.
pub const DEFAULT_SPANS: [Duration; 3] = [
    Duration::from_secs(1),
    Duration::from_secs(10),
    Duration::from_secs(60),
];

/// Which windows to compute, and how finely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowSpec {
    /// Bucket width. Every reported `span_ms` is a whole multiple of this, and
    /// every reading lags by at most this much.
    pub resolution: Duration,
    /// Window lengths to publish, in the order they should be published.
    pub spans: Vec<Duration>,
}

impl WindowSpec {
    /// Build from arbitrary spans, normalising the two ways this can be
    /// nonsense: a zero resolution (infinite buckets) and a span shorter than
    /// one bucket (a window that could never contain a completed bucket).
    ///
    /// Clamping rather than erroring because these arrive from a command line,
    /// and a process that refuses to start over `--window 100ms` when the
    /// resolution is 250ms is less useful than one that rounds it up and says
    /// what it did in the reading's own `span_ms`.
    pub fn new(resolution: Duration, spans: impl IntoIterator<Item = Duration>) -> Self {
        let resolution = resolution.max(Duration::from_millis(1));
        let spans = spans.into_iter().map(|s| s.max(resolution)).collect();
        Self { resolution, spans }
    }

    /// The longest configured span, which sizes the ring.
    fn longest(&self) -> Duration {
        self.spans.iter().copied().max().unwrap_or(Duration::ZERO)
    }
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self::new(DEFAULT_RESOLUTION, DEFAULT_SPANS)
    }
}

/// One window's indicators, as of the last completed bucket.
///
/// Every price-derived field is `Option`, and they go `None` under exactly
/// two conditions, one per source. The book-derived fields (`first` through
/// `mean_spread_bps`) are `None` when no trusted two-sided sample landed in
/// the window; the trade-derived ones (`volume`, `vwap`) are `None` when no
/// print did. The two conditions are independent — a desynced book still
/// prints trades — and either way a caller cannot accidentally read "no
/// data" as zero, which is the mistake this whole project is organised
/// against.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowReading {
    /// The window actually examined: the configured span rounded up to a whole
    /// number of buckets. Reported rather than the configured value, because
    /// the two can differ and the one that matters is the one measured.
    pub span_ms: u64,
    /// How much of `span_ms` the book was `Live` for. **Read this before any
    /// other field.** `trusted_ms < span_ms` means every number below covers
    /// less time than its label claims.
    pub trusted_ms: u64,
    /// Book updates that produced a two-sided mid inside the window.
    ///
    /// Distinguishes a genuinely quiet market (`trusted_ms == span_ms`,
    /// `samples` small) from an absent one (`trusted_ms` small), which the
    /// price fields alone cannot.
    pub samples: u64,
    /// Weakest [`Integrity`] any sample in the window was taken under.
    ///
    /// A window spanning a reconnect can hold samples from more than one
    /// guarantee. `Integrity` is ordered weakest-first so this is a `min`, for
    /// the same reason `SymbolView::weakest_integrity` is.
    pub integrity_floor: Option<Integrity>,
    /// Mid at the start of the window, and at the end.
    pub first: Option<Decimal>,
    pub last: Option<Decimal>,
    pub high: Option<Decimal>,
    pub low: Option<Decimal>,
    /// Arithmetic mean of every sampled mid. Sample-weighted, not
    /// time-weighted: a second with a thousand updates counts a thousand
    /// times. That is the right weighting for "where did the book sit while it
    /// was moving", and the wrong one for "what was the average price" —
    /// that one is `vwap` below, which weights by what actually traded.
    pub mean: Option<Decimal>,
    /// `(last - first) / first`, in basis points. Signed: negative is down.
    pub change_bps: Option<Decimal>,
    /// `(high - low) / mean`, in basis points. A non-parametric volatility
    /// proxy — see the module docs on why realised volatility is absent.
    pub range_bps: Option<Decimal>,
    /// Mean quoted spread over the window, in basis points of mid.
    ///
    /// Computed as `Σspread / Σmid`, one division per read, rather than
    /// averaging a per-sample ratio, which would be one division per book
    /// update on the hot path. The two agree to within the variation of mid
    /// across the window, which is the `range_bps` printed beside it — single
    /// digit basis points in every normal regime, and in an abnormal one
    /// `range_bps` is already the number being read.
    pub mean_spread_bps: Option<Decimal>,
    /// Prints observed in the window. Counted whatever the book's state —
    /// a print is the venue's fact about its own matches, not a claim about
    /// our book — so a desynced stretch still counts its trades while
    /// `trusted_ms` honestly labels how much of the *book*-derived numbers
    /// to believe.
    ///
    /// `#[serde(default)]` on all three trade fields because the gateway
    /// deserialises other nodes' snapshots, and a node one release behind
    /// simply has no trades to report — which is exactly what zero and
    /// `None` say.
    #[serde(default)]
    pub trades: u64,
    /// Total quantity printed in the window. `None` when `trades` is zero —
    /// no data, not zero volume.
    #[serde(default)]
    pub volume: Option<Decimal>,
    /// Volume-weighted average price of the window's prints. The average
    /// `mean` is deliberately not: weighted by what traded, not by how often
    /// the book moved.
    #[serde(default)]
    pub vwap: Option<Decimal>,
}

impl WindowReading {
    /// An empty window of the given span: the shape returned before any
    /// bucket has completed.
    fn empty(span_ms: u64) -> Self {
        Self {
            span_ms,
            trusted_ms: 0,
            samples: 0,
            integrity_floor: None,
            first: None,
            last: None,
            high: None,
            low: None,
            mean: None,
            change_bps: None,
            range_bps: None,
            mean_spread_bps: None,
            trades: 0,
            volume: None,
            vwap: None,
        }
    }

    /// True when the window covers less time than its label claims — because
    /// the book was untrusted, or because the process has not been running
    /// long enough to fill it.
    ///
    /// The condition a UI should render differently, rather than making every
    /// reader re-derive it.
    pub const fn is_partial(&self) -> bool {
        self.trusted_ms < self.span_ms
    }
}

/// One resolution's worth of samples, aggregated.
///
/// `index` is the absolute bucket number rather than a ring slot, which is
/// what lets a stale slot be recognised and cleared lazily instead of needing
/// an explicit rotation pass — and, more importantly, makes "this bucket never
/// existed" a distinguishable state from "this bucket was empty".
#[derive(Clone, Debug, Default)]
struct Bucket {
    index: Option<u64>,
    samples: u64,
    sum_mid: Decimal,
    sum_spread: Decimal,
    first: Option<Decimal>,
    last: Option<Decimal>,
    high: Option<Decimal>,
    low: Option<Decimal>,
    integrity: Option<Integrity>,
    /// Prints, in the same bucket as the book samples rather than a ring of
    /// their own: a second ring would need its own coverage accounting, and
    /// two coverage accounts over one stream will eventually disagree.
    trades: u64,
    sum_qty: Decimal,
    sum_price_qty: Decimal,
    /// Time inside this bucket during which the book was not `Live`.
    untrusted: Duration,
}

impl Bucket {
    fn reset(&mut self, index: u64) {
        *self = Self {
            index: Some(index),
            ..Self::default()
        };
    }

    fn record(&mut self, mid: Decimal, spread: Decimal, integrity: Integrity) {
        self.samples += 1;
        self.sum_mid += mid;
        self.sum_spread += spread;
        if self.first.is_none() {
            self.first = Some(mid);
        }
        self.last = Some(mid);
        self.high = Some(self.high.map_or(mid, |h| h.max(mid)));
        self.low = Some(self.low.map_or(mid, |l| l.min(mid)));
        self.integrity = Some(self.integrity.map_or(integrity, |i| i.min(integrity)));
    }

    fn record_trade(&mut self, price: Decimal, qty: Decimal) {
        self.trades += 1;
        self.sum_qty += qty;
        self.sum_price_qty += price * qty;
    }
}

/// Every configured window over one stream, backed by a single bucket ring.
///
/// Owned by the aggregator alongside the book it samples, so it inherits the
/// same single-ownership guarantee: no locks, and no possibility of reading a
/// window mid-update.
#[derive(Clone, Debug)]
pub struct RollingWindows {
    spec: WindowSpec,
    buckets: Vec<Bucket>,
    /// Bucket zero starts here. Never moves, so bucket indices are stable for
    /// the life of the process.
    origin: IngestTime,
    /// Time up to which trust has been accounted. Everything between here and
    /// `now` is charged at the *current* trust state on the next call.
    charged_to: IngestTime,
    /// Whether the book was `Live` as of `charged_to`.
    trusted: bool,
}

impl RollingWindows {
    pub fn new(spec: WindowSpec, now: IngestTime) -> Self {
        // Exactly enough slots for the in-progress bucket plus every completed
        // bucket the longest window can reach: indices `end - k_max ..= end`
        // are `k_max + 1` consecutive values, so they never alias modulo
        // `k_max + 1`. One more would be dead memory; one fewer would let the
        // oldest bucket in a full window be overwritten by the newest.
        let k_max = buckets_for(spec.longest(), spec.resolution);
        let len = usize::try_from(k_max)
            .unwrap_or(usize::MAX)
            .saturating_add(1);

        Self {
            spec,
            buckets: vec![Bucket::default(); len],
            origin: now,
            charged_to: now,
            trusted: false,
        }
    }

    pub fn spec(&self) -> &WindowSpec {
        &self.spec
    }

    /// Fold one book observation in.
    ///
    /// Called on every applied message rather than once per publish tick, so
    /// that `high` and `low` are the real extremes rather than whatever the
    /// book happened to show when the ticker fired. A tick-sampled high is a
    /// different and much weaker statistic, and on a book updating hundreds of
    /// times a second it would miss most of what it claims to measure.
    ///
    /// Trust is taken from `top.state` rather than passed separately, because
    /// the two must not be able to disagree: the whole value of `trusted_ms`
    /// rests on it being the same `BookState` the rest of the system reports.
    pub fn observe(&mut self, top: &TopOfBook, at: IngestTime) {
        // Charge the interval *before* the trust state changes, or a book that
        // has just gone Desynced would retroactively mark the healthy seconds
        // leading up to it as untrusted.
        self.charge(at);

        let integrity = match top.state {
            BookState::Live { integrity, .. } => {
                self.trusted = true;
                integrity
            }
            _ => {
                self.trusted = false;
                return;
            }
        };

        // A one-sided book has no mid. That is not a sample worth inventing:
        // it happens mid-splice and briefly after a snapshot, and filling it
        // with the last known mid would fabricate a flat spot in the range.
        let (Some(bid), Some(ask)) = (top.bid, top.ask) else {
            return;
        };

        let bid = bid.price.as_decimal();
        let ask = ask.price.as_decimal();
        let idx = self.index_at(at);
        self.slot_mut(idx)
            .record((bid + ask) / Decimal::TWO, ask - bid, integrity);
    }

    /// Fold one print in.
    ///
    /// Deliberately independent of book state: a print is the venue's fact
    /// about its own matches, not a claim about the book we built, so a
    /// desynced stretch still counts its trades. It does not touch the trust
    /// clock either — `trusted_ms` keeps describing the *book*-derived
    /// numbers, which is the reading it was designed to guard.
    pub fn observe_trade(&mut self, price: Decimal, qty: Decimal, at: IngestTime) {
        self.charge(at);
        let idx = self.index_at(at);
        self.slot_mut(idx).record_trade(price, qty);
    }

    /// Every configured window, in `spec.spans` order.
    ///
    /// Takes `&mut self` because reading advances the trust accounting to
    /// `now` — a book that has been silently `Live` (or silently `Desynced`)
    /// since the last observation has still spent that time in that state, and
    /// a read that did not charge it would report the last update's coverage
    /// forever on a stalled stream.
    pub fn read(&mut self, now: IngestTime) -> Vec<WindowReading> {
        self.charge(now);
        let end = self.index_at(now);
        self.spec
            .spans
            .iter()
            .map(|span| self.read_span(*span, end))
            .collect()
    }

    /// One window: the `k` completed buckets ending immediately before the
    /// in-progress bucket `end`.
    fn read_span(&self, span: Duration, end: u64) -> WindowReading {
        let k = buckets_for(span, self.spec.resolution);
        let span_ms = millis(self.spec.resolution.saturating_mul(saturating_u32(k)));

        // `end` is in progress and deliberately excluded, so the newest
        // readable bucket is `end - 1`. Before the first bucket completes
        // there is nothing to read and the reading is empty rather than
        // fabricated from a partial bucket.
        let Some(newest) = end.checked_sub(1) else {
            return WindowReading::empty(span_ms);
        };
        let oldest = newest.saturating_sub(k.saturating_sub(1));

        let mut out = WindowReading::empty(span_ms);
        let mut sum_mid = Decimal::ZERO;
        let mut sum_spread = Decimal::ZERO;
        let mut sum_qty = Decimal::ZERO;
        let mut sum_price_qty = Decimal::ZERO;

        for idx in oldest..=newest {
            // A slot whose index does not match never existed — the process
            // had not started yet. It contributes no samples and, crucially,
            // no trusted time: a 60s window read 5s after startup reports 5s
            // of coverage, not 60s of a book we were not watching.
            let Some(bucket) = self.slot(idx) else {
                continue;
            };

            out.trusted_ms += millis(self.spec.resolution.saturating_sub(bucket.untrusted));

            // Trades accumulate before the samples check: a bucket can hold
            // prints and no book samples — a desynced book still trades —
            // and gating one source's data on the other's presence would
            // quietly drop it.
            out.trades += bucket.trades;
            sum_qty += bucket.sum_qty;
            sum_price_qty += bucket.sum_price_qty;

            if bucket.samples == 0 {
                continue;
            }
            out.samples += bucket.samples;
            sum_mid += bucket.sum_mid;
            sum_spread += bucket.sum_spread;
            out.first = out.first.or(bucket.first);
            out.last = bucket.last.or(out.last);
            out.high = max_opt(out.high, bucket.high);
            out.low = min_opt(out.low, bucket.low);
            out.integrity_floor = min_opt(out.integrity_floor, bucket.integrity);
        }

        if out.trades > 0 {
            out.volume = Some(sum_qty);
            if !sum_qty.is_zero() {
                out.vwap = Some((sum_price_qty / sum_qty).round_dp(MID_SCALE));
            }
        }

        if out.samples == 0 {
            return out;
        }

        let n = Decimal::from(out.samples);
        let mean = (sum_mid / n).round_dp(MID_SCALE);
        out.mean = Some(mean);

        if let (Some(first), Some(last)) = (out.first, out.last)
            && !first.is_zero()
        {
            out.change_bps = Some(bps(last - first, first));
        }
        if let (Some(high), Some(low)) = (out.high, out.low)
            && !mean.is_zero()
        {
            out.range_bps = Some(bps(high - low, mean));
        }
        if !sum_mid.is_zero() {
            out.mean_spread_bps = Some(bps(sum_spread, sum_mid));
        }
        out
    }

    /// Attribute elapsed time to buckets at the trust state that was in force
    /// while it elapsed.
    ///
    /// This is the whole of the coverage accounting, and it is interval-based
    /// rather than sampled on purpose: a book that is `Desynced` for 900ms
    /// between two updates has 900ms of untrusted time whether or not anything
    /// was sampled during it, and a counter incremented per observation would
    /// report a silent desynced book as perfectly covered.
    fn charge(&mut self, now: IngestTime) {
        // Two ingest tasks can stamp events on different cores, so `now` is
        // not guaranteed to be later than the last one seen. Charging a
        // negative interval is not meaningful, and moving `charged_to`
        // backwards would charge the same interval twice.
        if now.mono() <= self.charged_to.mono() {
            return;
        }

        let mut cursor = self.charged_to;
        let end = self.index_at(now);
        let ring = u64::try_from(self.buckets.len()).unwrap_or(u64::MAX);

        // A stall longer than the whole ring — a suspended laptop, a stopped
        // debugger — cannot be represented, and every bucket it covered has
        // already aged out of every window. Skip to the oldest bucket still
        // readable rather than looping over history that no reading can see.
        // The skipped buckets keep stale indices, so they read as "never
        // existed" and contribute no coverage, which is the conservative
        // answer: we genuinely were not watching.
        if end.saturating_sub(self.index_at(cursor)) >= ring {
            cursor = self.bucket_start(end.saturating_sub(ring).saturating_add(1));
        }

        while cursor.mono() < now.mono() {
            let idx = self.index_at(cursor);
            let boundary = self.bucket_start(idx.saturating_add(1));
            let slice_end = if boundary.mono() < now.mono() {
                boundary
            } else {
                now
            };
            let elapsed = slice_end.since(cursor);

            if !self.trusted {
                self.slot_mut(idx).untrusted += elapsed;
            } else {
                // Still touch the slot, so that a bucket in which the book was
                // trusted but silent is distinguishable from one that never
                // existed. Without this a quiet minute would read as zero
                // coverage rather than full coverage of a quiet market.
                self.slot_mut(idx);
            }

            // `bucket_start` saturates, so an overflowing boundary would spin
            // here forever rather than terminating. Guard by construction:
            // stop as soon as the cursor stops advancing.
            if slice_end.mono() <= cursor.mono() {
                break;
            }
            cursor = slice_end;
        }

        self.charged_to = now;
    }

    fn index_at(&self, t: IngestTime) -> u64 {
        let elapsed = t.since(self.origin).as_nanos();
        let resolution = self.spec.resolution.as_nanos().max(1);
        u64::try_from(elapsed / resolution).unwrap_or(u64::MAX)
    }

    fn bucket_start(&self, index: u64) -> IngestTime {
        let offset = u128::from(index) * self.spec.resolution.as_nanos();
        self.origin.advanced_by(Duration::from_nanos(
            u64::try_from(offset).unwrap_or(u64::MAX),
        ))
    }

    fn slot(&self, index: u64) -> Option<&Bucket> {
        let len = u64::try_from(self.buckets.len()).unwrap_or(u64::MAX).max(1);
        let slot = usize::try_from(index % len).unwrap_or(0);
        self.buckets
            .get(slot)
            .filter(|bucket| bucket.index == Some(index))
    }

    /// The bucket for `index`, cleared first if the slot still holds an older
    /// one. Lazy clearing rather than an explicit rotation pass: rotation has
    /// to be driven by something, and anything that can fail to run it turns a
    /// stale bucket into silently wrong history.
    fn slot_mut(&mut self, index: u64) -> &mut Bucket {
        let len = u64::try_from(self.buckets.len()).unwrap_or(u64::MAX).max(1);
        let slot = usize::try_from(index % len).unwrap_or(0);
        let bucket = &mut self.buckets[slot];
        if bucket.index != Some(index) {
            bucket.reset(index);
        }
        bucket
    }
}

/// Scale for a mean mid. Wider than any venue's tick so that averaging a
/// half-cent book does not round the answer back onto the tick grid and hide
/// the sub-tick drift the mean exists to show.
const MID_SCALE: u32 = 8;

/// Scale for every basis-point figure. Four places is a hundredth of a basis
/// point, which is finer than any of these numbers is meaningful to, and stops
/// a `Decimal` division from publishing twenty-eight digits of noise.
const BPS_SCALE: u32 = 4;

pub(crate) fn bps(numerator: Decimal, denominator: Decimal) -> Decimal {
    (numerator * Decimal::from(10_000_u32) / denominator).round_dp(BPS_SCALE)
}

/// Buckets needed to cover `span`, rounding up: a window is never shorter than
/// asked for, only rounded out to a whole bucket.
fn buckets_for(span: Duration, resolution: Duration) -> u64 {
    let resolution = resolution.as_nanos().max(1);
    let span = span.as_nanos();
    u64::try_from(span.div_ceil(resolution)).unwrap_or(u64::MAX)
}

fn saturating_u32(v: u64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn max_opt<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

fn min_opt<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::book::DesyncReason;
    use crate::event::Level;
    use crate::price::{Price, Qty};
    use crate::time::{Clock, TestClock};
    use std::str::FromStr;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn level(price: &str) -> Level {
        Level::new(Price::from_str(price).unwrap(), Qty::from_str("1").unwrap())
    }

    fn live(bid: &str, ask: &str, at: IngestTime) -> TopOfBook {
        TopOfBook {
            bid: Some(level(bid)),
            ask: Some(level(ask)),
            state: BookState::Live {
                integrity: Integrity::Verified,
                since: at,
                last_verified: None,
            },
            age: None,
        }
    }

    fn desynced(at: IngestTime) -> TopOfBook {
        TopOfBook {
            bid: None,
            ask: None,
            state: BookState::Desynced {
                since: at,
                reason: DesyncReason::ConnectionLost,
            },
            age: None,
        }
    }

    /// 250ms buckets, one 1s window: four buckets, and every test below can
    /// name exact bucket boundaries.
    fn windows(clock: &TestClock) -> RollingWindows {
        RollingWindows::new(
            WindowSpec::new(Duration::from_millis(250), [Duration::from_secs(1)]),
            clock.now(),
        )
    }

    fn only(readings: Vec<WindowReading>) -> WindowReading {
        assert_eq!(readings.len(), 1);
        readings.into_iter().next().unwrap()
    }

    #[test]
    fn trades_are_counted_summed_and_vwap_weighted() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        // Two prints in the first bucket: 2 @ 100 and 1 @ 130.
        w.observe_trade(dec("100"), dec("2"), clock.now());
        w.observe_trade(dec("130"), dec("1"), clock.now());

        // Let the bucket complete, then read.
        clock.advance(Duration::from_millis(500));
        let r = only(w.read(clock.now()));

        assert_eq!(r.trades, 2);
        assert_eq!(r.volume, Some(dec("3")));
        // (100*2 + 130*1) / 3 = 110, weighted by quantity — the mid-based
        // `mean` could never produce this number from these inputs.
        assert_eq!(r.vwap, Some(dec("110").round_dp(8)));
    }

    #[test]
    fn a_window_with_no_trades_has_no_volume_not_zero_volume() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe(&live("100", "101", clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));

        let r = only(w.read(clock.now()));
        assert!(r.samples > 0, "the book sample must have landed");
        assert_eq!(r.trades, 0);
        assert_eq!(r.volume, None, "no prints is no data, not zero volume");
        assert_eq!(r.vwap, None);
    }

    #[test]
    fn trades_expire_with_the_ring_like_everything_else() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe_trade(dec("100"), dec("1"), clock.now());

        // Two seconds later the 1s window has aged the print out entirely.
        clock.advance(Duration::from_secs(2));
        let r = only(w.read(clock.now()));
        assert_eq!(r.trades, 0, "a print outlived the window that held it");
        assert_eq!(r.volume, None);
    }

    #[test]
    fn trades_during_untrusted_time_still_count_while_coverage_stays_low() {
        // The design point: a print is the venue's fact, not a claim about
        // our book. A desynced stretch counts its trades, and `trusted_ms`
        // keeps honestly labelling the book-derived numbers.
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe(&desynced(clock.now()), clock.now());
        clock.advance(Duration::from_millis(250));
        w.observe_trade(dec("100"), dec("1"), clock.now());
        clock.advance(Duration::from_millis(750));

        let r = only(w.read(clock.now()));
        assert_eq!(r.trades, 1, "a desynced book still prints trades");
        assert_eq!(r.volume, Some(dec("1")));
        assert_eq!(r.trusted_ms, 0, "counting the print must not invent trust");
        assert_eq!(
            r.high, None,
            "no book-derived number without a trusted sample"
        );
    }

    #[test]
    fn a_window_with_no_data_is_none_everywhere_not_zero() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        clock.advance(Duration::from_secs(2));

        let r = only(w.read(clock.now()));
        assert_eq!(r.samples, 0);
        assert_eq!(r.trusted_ms, 0, "an unstarted book reported coverage");
        assert_eq!(r.high, None);
        assert_eq!(r.low, None);
        assert_eq!(r.mean, None);
        // The reading a naive implementation gets wrong: 0.0 for a range it
        // has no basis to report at all.
        assert_eq!(r.range_bps, None);
    }

    #[test]
    fn high_and_low_come_from_every_update_not_the_tick() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        // Three updates inside one bucket. A tick-sampled window would see
        // only the last and report a range of zero.
        w.observe(&live("100", "102", clock.now()), clock.now());
        w.observe(&live("110", "112", clock.now()), clock.now());
        w.observe(&live("100", "102", clock.now()), clock.now());

        clock.advance(Duration::from_millis(300));
        let r = only(w.read(clock.now()));

        assert_eq!(r.samples, 3);
        assert_eq!(r.high, Some(dec("111")));
        assert_eq!(r.low, Some(dec("101")));
        assert_eq!(r.first, Some(dec("101")));
        assert_eq!(r.last, Some(dec("101")));
        assert_eq!(r.change_bps, Some(Decimal::ZERO));
    }

    #[test]
    fn change_is_signed_and_measured_end_to_end() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        w.observe(&live("999", "1001", clock.now()), clock.now());
        clock.advance(Duration::from_millis(300));
        w.observe(&live("989", "991", clock.now()), clock.now());
        clock.advance(Duration::from_millis(300));

        let r = only(w.read(clock.now()));
        assert_eq!(r.first, Some(dec("1000")));
        assert_eq!(r.last, Some(dec("990")));
        // -10 on 1000 is -100 bps.
        assert_eq!(r.change_bps, Some(dec("-100")));
        assert_eq!(r.range_bps, Some(dec("100.5025")));
    }

    #[test]
    fn a_desynced_stretch_shows_up_as_missing_coverage() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        // Trusted for 500ms...
        w.observe(&live("100", "102", clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));
        // ...then the book loses trust for the next 500ms.
        w.observe(&desynced(clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));

        let r = only(w.read(clock.now()));
        assert_eq!(r.span_ms, 1000);
        assert_eq!(
            r.trusted_ms, 500,
            "a window spanning a desync claimed full coverage"
        );
        assert!(r.is_partial());
        // The samples that did land are still reported — the window is not
        // void, it is *partial*, and those are different.
        assert_eq!(r.samples, 1);
        assert_eq!(r.high, Some(dec("101")));
    }

    #[test]
    fn a_quiet_but_trusted_book_is_fully_covered() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        // One update, then silence. Nothing has invalidated the book, so the
        // window is fully covered — the distinction between "quiet" and
        // "absent" that `samples` beside `trusted_ms` exists to draw.
        w.observe(&live("100", "102", clock.now()), clock.now());
        clock.advance(Duration::from_millis(2000));

        let r = only(w.read(clock.now()));
        assert_eq!(r.trusted_ms, 1000);
        assert!(!r.is_partial());
        assert_eq!(r.samples, 0, "a sample survived past its window");
        assert_eq!(r.high, None);
    }

    #[test]
    fn coverage_is_bounded_by_how_long_the_process_has_run() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe(&live("100", "102", clock.now()), clock.now());

        // 500ms into a 1s window: two completed buckets, not four.
        clock.advance(Duration::from_millis(500));
        let r = only(w.read(clock.now()));
        assert_eq!(r.span_ms, 1000);
        assert_eq!(
            r.trusted_ms, 500,
            "a young process claimed a full window of coverage"
        );
    }

    #[test]
    fn samples_leave_the_window_when_it_slides_past_them() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        w.observe(&live("100", "102", clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));
        w.observe(&live("200", "202", clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));

        // Both still inside the 1s window.
        let r = only(w.read(clock.now()));
        assert_eq!(r.samples, 2);
        assert_eq!(r.low, Some(dec("101")));

        // Slide past the first one. 500ms rather than the 1s that would
        // "obviously" expire it: a reading covers the completed buckets
        // *before* the one in progress, so the window at t=1500ms is
        // [500ms, 1500ms) and the sample at t=0 left it half a second ago.
        clock.advance(Duration::from_millis(500));
        let r = only(w.read(clock.now()));
        assert_eq!(r.samples, 1);
        assert_eq!(r.low, Some(dec("201")), "an expired sample still counted");
    }

    #[test]
    fn the_integrity_floor_is_the_weakest_sample_in_the_window() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        let strong = live("100", "102", clock.now());
        let mut weak = live("100", "102", clock.now());
        weak.state = BookState::Live {
            integrity: Integrity::OrderOnly,
            since: clock.now(),
            last_verified: None,
        };

        w.observe(&strong, clock.now());
        clock.advance(Duration::from_millis(300));
        w.observe(&weak, clock.now());
        clock.advance(Duration::from_millis(300));

        let r = only(w.read(clock.now()));
        assert_eq!(
            r.integrity_floor,
            Some(Integrity::OrderOnly),
            "a window mixing guarantees reported the stronger one"
        );
    }

    #[test]
    fn several_spans_share_one_sample_store() {
        let clock = TestClock::new();
        let mut w = RollingWindows::new(
            WindowSpec::new(
                Duration::from_millis(250),
                [Duration::from_secs(1), Duration::from_secs(4)],
            ),
            clock.now(),
        );

        w.observe(&live("100", "102", clock.now()), clock.now());
        clock.advance(Duration::from_secs(2));
        w.observe(&live("200", "202", clock.now()), clock.now());
        clock.advance(Duration::from_millis(500));

        let readings = w.read(clock.now());
        assert_eq!(readings.len(), 2);

        // The 1s window sees only the recent sample; the 4s window sees both,
        // from the same buckets.
        assert_eq!(readings[0].span_ms, 1000);
        assert_eq!(readings[0].samples, 1);
        assert_eq!(readings[0].low, Some(dec("201")));

        assert_eq!(readings[1].span_ms, 4000);
        assert_eq!(readings[1].samples, 2);
        assert_eq!(readings[1].low, Some(dec("101")));
        assert_eq!(readings[1].high, Some(dec("201")));
    }

    #[test]
    fn the_ring_holds_exactly_the_longest_window_plus_the_live_bucket() {
        let clock = TestClock::new();
        let w = RollingWindows::new(
            WindowSpec::new(Duration::from_millis(250), [Duration::from_secs(1)]),
            clock.now(),
        );
        // Four completed buckets for the window, one in progress. A slot fewer
        // and the oldest bucket of a full window would alias onto the newest.
        assert_eq!(w.buckets.len(), 5);
    }

    #[test]
    fn a_span_shorter_than_the_resolution_is_rounded_up_and_says_so() {
        let clock = TestClock::new();
        let mut w = RollingWindows::new(
            WindowSpec::new(Duration::from_millis(250), [Duration::from_millis(10)]),
            clock.now(),
        );
        clock.advance(Duration::from_secs(1));

        // The reading reports the span it actually examined, not the one asked
        // for. Silently honouring an impossible span would be the worse
        // failure: a "10ms high" that is really a 250ms high.
        assert_eq!(only(w.read(clock.now())).span_ms, 250);
    }

    #[test]
    fn a_stall_longer_than_the_ring_does_not_spin_or_lie() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe(&live("100", "102", clock.now()), clock.now());

        // A suspended process. The charge loop must not walk every bucket of
        // the missing hour, and must not claim coverage for it either.
        clock.advance(Duration::from_secs(3600));
        let r = only(w.read(clock.now()));

        assert_eq!(r.samples, 0);
        assert_eq!(
            r.trusted_ms, 1000,
            "an untracked stall was reported as a gap"
        );
    }

    #[test]
    fn mean_spread_is_reported_in_basis_points_of_mid() {
        let clock = TestClock::new();
        let mut w = windows(&clock);

        // Mid 1000, spread 2 => 20 bps. Twice, so the aggregate ratio is
        // exercised rather than a single division.
        w.observe(&live("999", "1001", clock.now()), clock.now());
        clock.advance(Duration::from_millis(250));
        w.observe(&live("999", "1001", clock.now()), clock.now());
        clock.advance(Duration::from_millis(250));

        assert_eq!(only(w.read(clock.now())).mean_spread_bps, Some(dec("20")));
    }

    #[test]
    fn readings_serialise_prices_as_strings() {
        let clock = TestClock::new();
        let mut w = windows(&clock);
        w.observe(&live("45000.05", "45000.15", clock.now()), clock.now());
        clock.advance(Duration::from_millis(300));

        let json = serde_json::to_string(&only(w.read(clock.now()))).unwrap();
        assert!(
            json.contains(r#""high":"45000.10""#),
            "a window price reached the wire as something other than an exact string: {json}"
        );
    }
}
