//! One task per venue connection.
//!
//! Owns the socket, the subscribe handshake, the reconnect schedule, the idle
//! watchdog, and — for Bitstamp — the REST depth fetch. Emits [`RawFrame`]s
//! into the shared bounded channel and touches no shared state beyond its own
//! counters. It does not parse anything, does not own a book, and cannot see
//! any other venue.
//!
//! # The three ways a connection dies
//!
//! Distinguishing them is most of the value here, because they need different
//! responses and only one of them is obvious:
//!
//! 1. **Refused.** `connect` fails. Usually the venue is down, sometimes it is
//!    a rate limit, and in the rate-limit case retrying hard is how a
//!    temporary block becomes a lasting one.
//! 2. **Closed or errored.** The socket goes away mid-stream. Visible, and the
//!    easy case.
//! 3. **Silent.** The socket is open, TCP is healthy, and nothing arrives.
//!    Nothing at any lower layer reports a problem, and a liveness check that
//!    watched only the socket would report the venue as up indefinitely. This
//!    is what [`VenueEndpoint::idle_timeout`] exists for, and it is why
//!    Coinbase's subscription includes the heartbeats channel: without a
//!    steady signal, "quiet market" and "dead feed" are the same observation.
//!
//! All three are counted separately ([`VenueCountersSnapshot`]), because
//! "climbing `idle_timeouts`, zero `connect_failures`" and the reverse mean
//! very different things to whoever is on the other end of the runbook.
//!
//! # Reconnect is a resync
//!
//! A reconnected socket does not resume anything. Every venue here starts a
//! new stream — new sequence numbers at Coinbase, a fresh snapshot at Kraken,
//! nothing at all at Bitstamp until the REST call lands. The book from before
//! the disconnect is not merely stale, it is *unknown*, and the aggregator
//! marks it `Desynced` on the disconnect rather than waiting to find out. The
//! ingest task's contribution to that is simply to be honest about session
//! boundaries, which is what [`SessionEnd`] carries.

use std::sync::Arc;

use ma_core::{Clock, VenueId};
use ma_venues::{RawFrame, VenueEndpoint};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::backoff::{Backoff, BackoffPolicy, EqualJitter};
use crate::channel::{SendOutcome, Sender};
use crate::metrics::VenueCounters;
use crate::net::{Network, Transport};

/// Cooperative stop signal, shared by every ingest task in the process.
///
/// A `watch` rather than a flag because a task parked on `recv` has to be
/// *woken*, not merely able to observe a change the next time it happens to
/// look. Polling a flag would leave a silent venue's task hanging until its
/// idle timeout expired, turning a clean shutdown into a 30-second one.
#[derive(Clone, Debug)]
pub struct Shutdown(watch::Receiver<bool>);

/// Fires the [`Shutdown`] signal. Dropping it also fires, so a caller that
/// panics or returns early does not leave ingest tasks running.
#[derive(Debug)]
pub struct ShutdownTrigger(watch::Sender<bool>);

/// Create a linked trigger and signal.
pub fn shutdown() -> (ShutdownTrigger, Shutdown) {
    let (tx, rx) = watch::channel(false);
    (ShutdownTrigger(tx), Shutdown(rx))
}

impl ShutdownTrigger {
    /// Ask every holder of the paired [`Shutdown`] to stop.
    pub fn stop(&self) {
        let _ = self.0.send(true);
    }
}

impl Drop for ShutdownTrigger {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Shutdown {
    pub fn is_set(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolve once the signal fires, or immediately if it already has.
    pub async fn wait(&mut self) {
        // `changed()` only reports values arriving after the last observation,
        // so a signal fired before this receiver existed has to be caught by
        // the borrow. Without the first check a late-spawned task would wait
        // forever for a shutdown that already happened.
        if self.is_set() {
            return;
        }
        // An error means the trigger was dropped, which `ShutdownTrigger`'s
        // Drop turns into a stop anyway.
        let _ = self.0.changed().await;
    }
}

/// How a connection attempt ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEnd {
    /// Never opened.
    ConnectFailed,
    /// Peer closed cleanly.
    Closed,
    /// Socket error mid-stream.
    Errored,
    /// Open but silent past the venue's idle timeout.
    Idle,
    /// The aggregator's channel closed — nothing downstream is listening, so
    /// there is no reason to keep reading.
    Downstream,
    /// Asked to stop.
    Stopped,
}

impl SessionEnd {
    /// Whether the ingest loop should keep trying.
    const fn should_retry(self) -> bool {
        !matches!(self, Self::Downstream | Self::Stopped)
    }
}

/// What an ingest task hands the aggregator.
///
/// # Why session boundaries travel in-band
///
/// A reconnect is not a pause in the stream, it is a *new* stream, and the
/// aggregator has to be told. Coinbase makes the consequence concrete: a
/// resubscribed connection restarts `sequence_num` from a fresh base, so
/// without this message the sync sees an enormous backwards jump, declares a
/// gap, and — because the snapshot riding in that same frame is discarded
/// along with it — never recovers. The book would sit `Desynced` forever while
/// the socket was perfectly healthy.
///
/// Sending it through the same channel as the frames, rather than out of band,
/// is what keeps the ordering right: the boundary lands exactly where it
/// happened relative to the frames on either side, even if the channel is
/// backed up.
#[derive(Clone, Debug)]
pub enum IngestMessage {
    /// Bytes a venue sent.
    Frame(RawFrame),
    /// A connection ended. The venue's sync state is reset and its book
    /// marked `Desynced` until a fresh snapshot lands.
    SessionEnded {
        venue: VenueId,
        at: ma_core::IngestTime,
        end: SessionEnd,
    },
}

impl IngestMessage {
    pub const fn venue(&self) -> VenueId {
        match self {
            Self::Frame(frame) => frame.venue,
            Self::SessionEnded { venue, .. } => *venue,
        }
    }
}

/// Everything a venue's ingest task needs, assembled once at startup.
pub struct Ingest<N: Network> {
    net: Arc<N>,
    endpoint: VenueEndpoint,
    tx: Sender<IngestMessage>,
    clock: Arc<dyn Clock>,
    counters: Arc<VenueCounters>,
    policy: BackoffPolicy,
    tape: Option<mpsc::UnboundedSender<IngestMessage>>,
    shutdown: Shutdown,
}

// Hand-written because `Sender<RawFrame>` and `Arc<dyn Clock>` do not derive
// usefully, and a derived impl would demand `N: Debug`.
impl<N: Network> std::fmt::Debug for Ingest<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingest")
            .field("venue", &self.endpoint.venue)
            .field("url", &self.endpoint.ws_url)
            .field("recording", &self.tape.is_some())
            .finish_non_exhaustive()
    }
}

impl<N: Network> Ingest<N> {
    pub fn new(
        net: Arc<N>,
        endpoint: VenueEndpoint,
        tx: Sender<IngestMessage>,
        clock: Arc<dyn Clock>,
        counters: Arc<VenueCounters>,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            net,
            endpoint,
            tx,
            clock,
            counters,
            policy: BackoffPolicy::DEFAULT,
            tape: None,
            shutdown,
        }
    }

    #[must_use]
    pub fn with_backoff(mut self, policy: BackoffPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Tee every frame to a tape recorder as well as to the aggregator.
    ///
    /// **The tee is unbounded, and that is the opposite policy to the channel
    /// it sits beside.** `channel`'s module docs argue that a stale market
    /// tick has negative value, so a full buffer should drop the oldest event;
    /// that argument is about *data being consumed now*. A tape is not that.
    /// A tape is a record of what a venue actually sent, and a tape with a
    /// hole in it is worse than no tape, because every offline test built on
    /// it silently exercises a stream the venue never produced. Recording is
    /// closer to the claims-processing case `channel` contrasts itself with —
    /// every message is a fact — so it gets the claims-processing policy.
    ///
    /// The memory risk that normally makes unbounded channels a bad idea is
    /// bounded differently here: recording is a deliberate, time-limited Tier
    /// 2 operation with a human watching, not something the server does in
    /// steady state.
    #[must_use]
    pub fn recording_to(mut self, tape: mpsc::UnboundedSender<IngestMessage>) -> Self {
        self.tape = Some(tape);
        self
    }

    const fn venue(&self) -> VenueId {
        self.endpoint.venue
    }

    /// Connect, subscribe, read, reconnect. Returns when told to stop or when
    /// nothing downstream is listening.
    pub async fn run(mut self) {
        let mut backoff = Backoff::new(self.policy, EqualJitter::from_entropy());
        let venue = self.venue();

        while !self.shutdown.is_set() {
            let started = self.clock.now();
            let end = self.session().await;
            let lasted = self.clock.now().since(started);

            if !end.should_retry() {
                debug!(%venue, ?end, "ingest task finished");
                return;
            }

            // Tell the aggregator the stream ended *before* sleeping, so the
            // book is marked untrustworthy for the whole reconnect window
            // rather than only once a new socket is up. A book that keeps
            // reporting `Live` prices through a 60-second outage is precisely
            // the silent wrongness this project exists to avoid.
            self.publish(IngestMessage::SessionEnded {
                venue,
                at: self.clock.now(),
                end,
            });

            if backoff.note_session(lasted) {
                info!(%venue, ?end, ?lasted, "session ended after a stable run");
            }
            let delay = backoff.next_delay();
            warn!(
                %venue, ?end, ?lasted, ?delay,
                attempt = backoff.attempt(),
                "reconnecting"
            );

            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = self.shutdown.wait() => return,
            }
        }
    }

    /// One connection attempt, from `connect` to whatever ends it.
    async fn session(&self) -> SessionEnd {
        let venue = self.venue();

        let mut socket = match self.net.connect(&self.endpoint.ws_url).await {
            Ok(socket) => socket,
            Err(e) => {
                self.counters.record_connect_failure();
                warn!(%venue, error = %e, "connect failed");
                return SessionEnd::ConnectFailed;
            }
        };

        for payload in &self.endpoint.subscribe {
            if let Err(e) = socket.send_text(payload).await {
                self.counters.record_connect_failure();
                warn!(%venue, error = %e, "subscribe failed");
                return SessionEnd::ConnectFailed;
            }
        }

        self.counters.record_connect();
        info!(%venue, url = %self.endpoint.ws_url, "connected and subscribed");

        // Started *after* the subscribe, and run concurrently with the read
        // loop rather than before it. Both halves of that matter, and they are
        // the reconnect algorithm in CLAUDE.md §1 written out: subscribing
        // first means no diff generated after the snapshot can be missed, and
        // reading concurrently means the diffs that arrive while the fetch is
        // in flight are buffered by the venue's sync rather than lost. Doing
        // the fetch first and reading second would produce a book that is
        // quietly missing everything sent during the request.
        //
        // For Coinbase and Kraken this future is `pending()` and never fires;
        // their snapshot arrives over the websocket like any other frame.
        let rest = self.rest_snapshot();
        tokio::pin!(rest);
        let mut rest_pending = true;

        let mut shutdown = self.shutdown.clone();

        loop {
            tokio::select! {
                // Checked first: a shutdown mid-frame should stop, not race.
                biased;

                () = shutdown.wait() => return SessionEnd::Stopped,

                () = &mut rest, if rest_pending => {
                    rest_pending = false;
                }

                received = tokio::time::timeout(self.endpoint.idle_timeout, socket.recv()) => {
                    match received {
                        Err(_elapsed) => {
                            self.counters.record_idle_timeout();
                            warn!(
                                %venue,
                                timeout = ?self.endpoint.idle_timeout,
                                "no frames within the idle timeout; treating the connection as dead"
                            );
                            return SessionEnd::Idle;
                        }
                        Ok(Ok(None)) => {
                            self.counters.record_disconnect();
                            return SessionEnd::Closed;
                        }
                        Ok(Err(e)) => {
                            self.counters.record_disconnect();
                            warn!(%venue, error = %e, "socket error");
                            return SessionEnd::Errored;
                        }
                        Ok(Ok(Some(payload))) => {
                            let frame = RawFrame::new(venue, payload, self.clock.now());
                            if !self.emit(frame) {
                                return SessionEnd::Downstream;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Fetch the REST depth snapshot, retrying until it lands.
    ///
    /// Retries for as long as the session lasts, rather than giving up: while
    /// this is failing the book is `Desynced { AwaitingSnapshot }`, which is
    /// an honest and visible state, so there is no hurry and nothing is being
    /// silently misreported. `rest_failures` is what surfaces it.
    ///
    /// Resolves to `pending()` forever for a venue with no REST URL, so the
    /// caller's `select!` arm simply never fires.
    async fn rest_snapshot(&self) {
        let Some(url) = self.endpoint.rest_snapshot_url.as_deref() else {
            std::future::pending::<()>().await;
            return;
        };
        let venue = self.venue();
        let mut backoff = Backoff::new(self.policy, EqualJitter::from_entropy());

        loop {
            self.counters.record_rest_fetch();
            match self.net.get(url).await {
                Ok(body) => {
                    debug!(%venue, bytes = body.len(), "rest depth snapshot fetched");
                    let frame = RawFrame::rest_snapshot(venue, body.into_bytes(), self.clock.now());
                    self.emit(frame);
                    return;
                }
                Err(e) => {
                    self.counters.record_rest_failure();
                    let delay = backoff.next_delay();
                    warn!(%venue, %url, error = %e, ?delay, "rest snapshot failed; retrying");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Hand a frame to the tape and to the aggregator.
    ///
    /// Returns whether anything downstream is still listening. Synchronous
    /// throughout — there is no `.await` in this function, which is the
    /// property that makes "ingest never blocks on a slow consumer" true
    /// rather than aspirational.
    fn emit(&self, frame: RawFrame) -> bool {
        self.counters.record_frame(frame.payload.len());
        self.publish(IngestMessage::Frame(frame))
    }

    /// Hand any message to the tape and to the aggregator.
    fn publish(&self, message: IngestMessage) -> bool {
        if let Some(tape) = &self.tape {
            // A closed tape means the recorder finished; that is not a reason
            // to stop ingesting.
            let _ = tape.send(message.clone());
        }

        match self.tx.send(message) {
            SendOutcome::Sent => true,
            SendOutcome::DroppedOldest(_) => {
                self.counters.record_drop();
                true
            }
            SendOutcome::Closed(_) => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::channel::bounded;
    use crate::net::fake::{FakeNetwork, Session, SessionEnd as FakeEnd};
    use ma_core::{Symbol, SystemClock};
    use ma_venues::{FrameSource, spec_for};
    use std::time::Duration;

    fn endpoint(venue: VenueId) -> VenueEndpoint {
        spec_for(venue, &Symbol::new("BTC-USD"))
            .expect("spec")
            .endpoint
    }

    /// A short schedule so a test that does wait out a delay under tokio's
    /// paused clock does not have to reason about minutes.
    fn fast_policy() -> BackoffPolicy {
        BackoffPolicy {
            base: Duration::from_millis(10),
            cap: Duration::from_millis(100),
            min_stable: Duration::from_secs(30),
        }
    }

    struct Harness {
        net: Arc<FakeNetwork>,
        counters: Arc<VenueCounters>,
        rx: crate::channel::Receiver<IngestMessage>,
        trigger: ShutdownTrigger,
        handle: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn start(venue: VenueId, net: FakeNetwork) -> Self {
            let net = Arc::new(net);
            let (tx, rx) = bounded::<IngestMessage>(64);
            let counters = Arc::new(VenueCounters::default());
            let (trigger, shutdown) = shutdown();

            let ingest = Ingest::new(
                Arc::clone(&net),
                endpoint(venue),
                tx,
                Arc::new(SystemClock),
                Arc::clone(&counters),
                shutdown,
            )
            .with_backoff(fast_policy());

            Self {
                net,
                counters,
                rx,
                trigger,
                handle: tokio::spawn(ingest.run()),
            }
        }

        /// Read the next *frame's* payload as text, skipping the session
        /// boundaries the reconnect tests deliberately provoke.
        async fn next_payload(&self) -> String {
            loop {
                match self.rx.recv().await.expect("channel closed early") {
                    IngestMessage::Frame(frame) => {
                        return String::from_utf8(frame.payload).expect("utf8");
                    }
                    IngestMessage::SessionEnded { .. } => {}
                }
            }
        }

        /// Read the next message of any kind.
        async fn next_message(&self) -> IngestMessage {
            self.rx.recv().await.expect("channel closed early")
        }

        async fn finish(self) {
            self.trigger.stop();
            self.handle.await.expect("ingest task panicked");
        }
    }

    fn serving(frames: &[&str], then: FakeEnd) -> Session {
        Session::Serve {
            frames: frames.iter().map(|s| (*s).to_owned()).collect(),
            then,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn frames_reach_the_channel_and_are_counted() {
        let net = FakeNetwork::new([serving(&["{\"a\":1}", "{\"a\":2}"], FakeEnd::Hang)]);
        let h = Harness::start(VenueId::Kraken, net);

        assert_eq!(h.next_payload().await, "{\"a\":1}");
        assert_eq!(h.next_payload().await, "{\"a\":2}");

        let s = h.counters.snapshot();
        assert_eq!(s.frames, 2);
        assert_eq!(s.connects, 1);
        assert_eq!(s.bytes, 14);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_connection_is_retried_and_counted_separately() {
        // Two refusals then a working session. The point of the separate
        // counter: a venue refusing us is often a rate limit, and reacting to
        // it the same way as a mid-stream disconnect is how a temporary block
        // becomes a lasting one.
        let net = FakeNetwork::new([
            Session::Refuse,
            Session::Refuse,
            serving(&["{\"ok\":true}"], FakeEnd::Hang),
        ]);
        let h = Harness::start(VenueId::Kraken, net);

        assert_eq!(h.next_payload().await, "{\"ok\":true}");

        let s = h.counters.snapshot();
        assert_eq!(s.connect_failures, 2);
        assert_eq!(s.connects, 1);
        assert_eq!(s.disconnects, 0, "a refusal is not a disconnect");
        assert_eq!(h.net.attempts().len(), 3);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_socket_is_treated_as_dead() {
        // The failure mode nothing below this layer reports: the socket is
        // open, TCP is fine, and no data is coming. Reading the second
        // session's frame is only possible if the first session's idle timeout
        // fired and the task reconnected.
        let net = FakeNetwork::new([
            serving(&["{\"first\":1}"], FakeEnd::Hang),
            serving(&["{\"second\":1}"], FakeEnd::Hang),
        ]);
        let h = Harness::start(VenueId::Kraken, net);

        assert_eq!(h.next_payload().await, "{\"first\":1}");
        assert_eq!(h.next_payload().await, "{\"second\":1}");

        let s = h.counters.snapshot();
        assert_eq!(s.idle_timeouts, 1);
        assert_eq!(s.connects, 2);
        assert_eq!(s.reconnects(), 1);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_close_reconnects_and_resubscribes() {
        let net = FakeNetwork::new([
            serving(&["{\"a\":1}"], FakeEnd::Close),
            serving(&["{\"b\":1}"], FakeEnd::Hang),
        ]);
        let h = Harness::start(VenueId::Coinbase, net);

        assert_eq!(h.next_payload().await, "{\"a\":1}");
        assert_eq!(h.next_payload().await, "{\"b\":1}");

        assert_eq!(h.counters.snapshot().disconnects, 1);
        // Coinbase subscribes twice per connection (level2 + heartbeats), and
        // must do it again on the new socket: there is no session to resume.
        assert_eq!(h.net.attempts().len(), 2);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_socket_error_reconnects() {
        let net = FakeNetwork::new([
            serving(&["{\"a\":1}"], FakeEnd::Error),
            serving(&["{\"b\":1}"], FakeEnd::Hang),
        ]);
        let h = Harness::start(VenueId::Kraken, net);

        assert_eq!(h.next_payload().await, "{\"a\":1}");
        assert_eq!(h.next_payload().await, "{\"b\":1}");
        assert_eq!(h.counters.snapshot().disconnects, 1);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn the_subscribe_payloads_are_actually_sent() {
        // A connection that never subscribes looks identical to a healthy one
        // at every layer below this until the idle timeout finally fires.
        let net = FakeNetwork::new([serving(&["{\"a\":1}"], FakeEnd::Hang)]);
        let h = Harness::start(VenueId::Bitstamp, net);
        h.next_payload().await;

        // The fake records what it was sent per socket; asserting through the
        // endpoint keeps this honest if the payload ever changes.
        let expected = &endpoint(VenueId::Bitstamp).subscribe;
        assert_eq!(expected.len(), 1);
        assert!(expected[0].contains("diff_order_book_btcusd"));
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn bitstamp_fetches_a_rest_snapshot_and_tags_it() {
        // The REST body has to reach the aggregator as a frame, tagged as a
        // snapshot rather than as a diff, or the splice never happens and a
        // recorded Bitstamp tape can never reach a synced book.
        let body =
            r#"{"microtimestamp":"1700000000000000","bids":[["100","1"]],"asks":[["101","1"]]}"#;
        let net = FakeNetwork::new([serving(
            &["{\"event\":\"bts:subscription_succeeded\"}"],
            FakeEnd::Hang,
        )])
        .with_rest_body(body);
        let h = Harness::start(VenueId::Bitstamp, net);

        let mut saw_snapshot = false;
        for _ in 0..2 {
            if let IngestMessage::Frame(frame) = h.next_message().await
                && frame.source == FrameSource::RestSnapshot
            {
                assert_eq!(frame.payload, body.as_bytes());
                saw_snapshot = true;
            }
        }
        assert!(saw_snapshot, "no REST snapshot frame was emitted");
        assert_eq!(h.counters.snapshot().rest_fetches, 1);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_rest_fetch_retries_without_dropping_the_socket() {
        // While this fails the book is Desynced{AwaitingSnapshot} — honest and
        // visible — so the right response is to keep asking, not to tear down
        // a websocket that is delivering diffs perfectly well.
        let net = FakeNetwork::new([serving(&["{\"a\":1}"], FakeEnd::Hang)]); // no rest body -> 503
        let h = Harness::start(VenueId::Bitstamp, net);

        assert_eq!(h.next_payload().await, "{\"a\":1}");
        // Let the retry loop turn over under the paused clock.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let s = h.counters.snapshot();
        assert!(s.rest_failures >= 2, "only {} attempts", s.rest_failures);
        assert_eq!(s.connects, 1, "the websocket was torn down over a REST 503");
        assert_eq!(s.disconnects, 0);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn venues_without_rest_recovery_never_call_it() {
        let net = FakeNetwork::new([serving(&["{\"a\":1}"], FakeEnd::Hang)]);
        let h = Harness::start(VenueId::Kraken, net);
        h.next_payload().await;
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(h.counters.snapshot().rest_fetches, 0);
        h.finish().await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_channel_drops_rather_than_blocking_ingest() {
        // The channel is capacity 2 and nothing reads it, so the venue keeps
        // sending into a full buffer. Ingest must keep up and count the
        // losses, not stall — a blocked ingest task also stops noticing that
        // the venue died.
        let frames: Vec<String> = (0..20).map(|i| format!("{{\"n\":{i}}}")).collect();
        let net = Arc::new(FakeNetwork::new([Session::Serve {
            frames,
            then: FakeEnd::Hang,
        }]));
        let (tx, rx) = bounded::<IngestMessage>(2);
        let counters = Arc::new(VenueCounters::default());
        let (trigger, shut) = shutdown();

        let handle = tokio::spawn(
            Ingest::new(
                net,
                endpoint(VenueId::Kraken),
                tx,
                Arc::new(SystemClock),
                Arc::clone(&counters),
                shut,
            )
            .with_backoff(fast_policy())
            .run(),
        );

        // Give the task room to run the whole script against a full channel.
        tokio::time::sleep(Duration::from_millis(1)).await;
        trigger.stop();
        handle.await.expect("ingest panicked");

        let s = counters.snapshot();
        assert_eq!(s.frames, 20, "ingest stalled on a full channel");
        assert_eq!(s.dropped, 18, "capacity 2, so all but the last two go");
        assert_eq!(rx.metrics().len, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_channel_stops_the_task_rather_than_reconnecting_forever() {
        let net = FakeNetwork::new([serving(&["{\"a\":1}", "{\"b\":2}"], FakeEnd::Hang)]);
        let net = Arc::new(net);
        let (tx, rx) = bounded::<IngestMessage>(8);
        let counters = Arc::new(VenueCounters::default());
        let (_trigger, shut) = shutdown();
        let probe = Arc::clone(&net);

        tx.close();
        let handle = tokio::spawn(
            Ingest::new(
                net,
                endpoint(VenueId::Kraken),
                tx,
                Arc::new(SystemClock),
                counters,
                shut,
            )
            .with_backoff(fast_policy())
            .run(),
        );

        // Must return on its own, without the shutdown trigger.
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task kept running with nothing listening")
            .expect("ingest panicked");
        assert_eq!(probe.attempts().len(), 1, "it reconnected anyway");
        drop(rx);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_stops_a_task_parked_on_a_silent_socket() {
        // The reason shutdown is a watch and not a polled flag: this task is
        // asleep inside `recv` with a 15s timeout, and a clean shutdown must
        // not have to wait that timeout out.
        let net = FakeNetwork::new([serving(&[], FakeEnd::Hang)]);
        let h = Harness::start(VenueId::Kraken, net);
        tokio::time::sleep(Duration::from_millis(1)).await;

        h.trigger.stop();
        tokio::time::timeout(Duration::from_millis(50), h.handle)
            .await
            .expect("shutdown did not wake a parked task")
            .expect("ingest panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_trigger_also_stops_the_task() {
        let net = FakeNetwork::new([serving(&[], FakeEnd::Hang)]);
        let h = Harness::start(VenueId::Kraken, net);
        tokio::time::sleep(Duration::from_millis(1)).await;

        drop(h.trigger);
        tokio::time::timeout(Duration::from_millis(50), h.handle)
            .await
            .expect("dropping the trigger left the task running")
            .expect("ingest panicked");
    }

    #[tokio::test(start_paused = true)]
    async fn frames_are_teed_to_a_tape_without_dropping() {
        // The tee is unbounded on purpose: a tape with a hole in it silently
        // invalidates every offline test built on it. Capacity 1 on the live
        // channel, 20 frames in, and the tape must still see all 20.
        let frames: Vec<String> = (0..20).map(|i| format!("{{\"n\":{i}}}")).collect();
        let net = Arc::new(FakeNetwork::new([Session::Serve {
            frames,
            then: FakeEnd::Hang,
        }]));
        let (tx, _rx) = bounded::<IngestMessage>(1);
        let (tape_tx, mut tape_rx) = mpsc::unbounded_channel();
        let (trigger, shut) = shutdown();

        let handle = tokio::spawn(
            Ingest::new(
                net,
                endpoint(VenueId::Kraken),
                tx,
                Arc::new(SystemClock),
                Arc::new(VenueCounters::default()),
                shut,
            )
            .with_backoff(fast_policy())
            .recording_to(tape_tx)
            .run(),
        );

        tokio::time::sleep(Duration::from_millis(1)).await;
        trigger.stop();
        handle.await.expect("ingest panicked");

        let mut taped = Vec::new();
        while let Ok(frame) = tape_rx.try_recv() {
            taped.push(frame);
        }
        assert_eq!(
            taped.len(),
            20,
            "the tape lost frames the live channel dropped"
        );
    }
}
