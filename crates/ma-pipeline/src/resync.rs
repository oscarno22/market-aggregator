//! Asking a venue's connection to start over.
//!
//! # The gap this closes
//!
//! Detecting a desync and recovering from one happen in different tasks, and
//! until this module existed only the first half worked. The aggregator owns
//! the books, so it is the only thing that can notice a sequence gap or a
//! checksum mismatch. The ingest task owns the socket, so it is the only thing
//! that can do anything about it. Every venue here recovers by getting a fresh
//! snapshot, and every venue only sends one on a new subscription.
//!
//! Without a signal between them, a book that desynced from *bad data* rather
//! than from a *dead socket* stayed broken indefinitely: the connection is
//! healthy, so the idle watchdog never fires; the venue keeps sending updates,
//! which the book correctly refuses to apply; and nothing ever asks for the
//! snapshot that would fix it. A disconnect recovered fine and a genuine gap
//! did not — the opposite of the priority, since a gap is the case the whole
//! `Desynced` apparatus exists to catch.
//!
//! # Why a `watch` counter rather than a flag or a channel
//!
//! A request must not be lost if it arrives while the ingest task is already
//! mid-reconnect, and it must not queue up either — three requests during one
//! outage should still mean one reconnect. A monotonically increasing counter
//! gives both: the receiver compares against what it last acted on, so
//! coalescing is automatic and nothing is dropped.

use std::collections::BTreeMap;
use std::sync::Arc;

use ma_core::StreamId;
use tokio::sync::watch;

/// The requesting half. Held by the aggregator.
///
/// Keyed by [`StreamId`] rather than by venue, and that is the same decision
/// as `ma_core::stream`'s "one connection per stream" — seen from the other
/// end. A resync **is** a disconnect, so a venue-keyed request would tear down
/// every symbol on that venue to repair one of them. Per-stream signals keep
/// the blast radius of a gap to the book that had the gap.
#[derive(Clone, Debug, Default)]
pub struct ResyncRequests {
    streams: Arc<BTreeMap<StreamId, watch::Sender<u64>>>,
}

impl ResyncRequests {
    /// Registered once at startup, for a fixed stream set — same reasoning as
    /// [`crate::metrics::Metrics`]: registration is not a runtime operation,
    /// so nothing here needs a lock.
    pub fn new(streams: impl IntoIterator<Item = StreamId>) -> Self {
        Self {
            streams: Arc::new(
                streams
                    .into_iter()
                    .map(|s| (s, watch::channel(0_u64).0))
                    .collect(),
            ),
        }
    }

    /// Ask one stream's ingest task to tear down its connection and resync.
    ///
    /// Returns whether anyone was listening. A `false` is not an error — the
    /// replay path has no ingest tasks at all, and a replayed desync has
    /// nothing to reconnect.
    pub fn request(&self, stream: &StreamId) -> bool {
        let Some(tx) = self.streams.get(stream) else {
            return false;
        };
        tx.send_modify(|n| *n = n.saturating_add(1));
        tx.receiver_count() > 0
    }

    /// The listening half for one stream.
    pub fn subscribe(&self, stream: &StreamId) -> Option<ResyncSignal> {
        self.streams
            .get(stream)
            .map(|tx| ResyncSignal(tx.subscribe()))
    }

    /// How many resyncs have been requested for `stream`. For tests and logs.
    pub fn requested(&self, stream: &StreamId) -> u64 {
        self.streams.get(stream).map_or(0, |tx| *tx.borrow())
    }
}

/// The listening half. Held by one ingest task.
#[derive(Debug)]
pub struct ResyncSignal(watch::Receiver<u64>);

impl ResyncSignal {
    /// Resolve when a resync is requested.
    ///
    /// Only requests made *after* this signal was last observed count. That is
    /// what stops a reconnect loop: the request that caused the current
    /// reconnect must not immediately trigger the next one.
    pub async fn requested(&mut self) {
        // An error means the aggregator is gone, in which case there is
        // nothing left to serve and the task is shutting down anyway; never
        // resolving is the right behaviour and lets the shutdown arm win.
        if self.0.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Symbol, VenueId};
    use std::time::Duration;

    fn stream(venue: VenueId) -> StreamId {
        StreamId::new(venue, Symbol::new("BTC-USD"))
    }

    #[tokio::test]
    async fn a_request_wakes_a_waiting_ingest_task() {
        let requests = ResyncRequests::new([stream(VenueId::Coinbase)]);
        let mut signal = requests
            .subscribe(&stream(VenueId::Coinbase))
            .expect("registered");

        let waiter = tokio::spawn(async move {
            signal.requested().await;
            signal
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(requests.request(&stream(VenueId::Coinbase)));

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the signal did not wake the waiter")
            .expect("panicked");
    }

    #[tokio::test]
    async fn a_request_made_before_anyone_waits_is_not_lost() {
        // The ordering that actually happens: the aggregator notices the gap
        // while the ingest task is still blocked reading the socket.
        let requests = ResyncRequests::new([stream(VenueId::Kraken)]);
        let mut signal = requests
            .subscribe(&stream(VenueId::Kraken))
            .expect("registered");
        requests.request(&stream(VenueId::Kraken));

        tokio::time::timeout(Duration::from_millis(50), signal.requested())
            .await
            .expect("a request made before the wait was dropped");
    }

    #[tokio::test]
    async fn repeated_requests_during_one_outage_coalesce() {
        // A book that desyncs on every frame for a second would otherwise
        // queue hundreds of reconnects — a self-inflicted reconnect storm,
        // against a venue that may already be rate-limiting us.
        let requests = ResyncRequests::new([stream(VenueId::Coinbase)]);
        let mut signal = requests
            .subscribe(&stream(VenueId::Coinbase))
            .expect("registered");
        for _ in 0..50 {
            requests.request(&stream(VenueId::Coinbase));
        }

        signal.requested().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), signal.requested())
                .await
                .is_err(),
            "50 requests should have collapsed into one observation"
        );
        assert_eq!(requests.requested(&stream(VenueId::Coinbase)), 50);
    }

    #[tokio::test]
    async fn one_symbols_resync_does_not_reconnect_another() {
        // The reason this is keyed by stream. On a venue-keyed signal — or on
        // a multiplexed connection — repairing BTC-USD would drop ETH-USD's
        // perfectly healthy subscription with it, turning one book's gap into
        // a venue-wide outage.
        let btc = StreamId::new(VenueId::Coinbase, Symbol::new("BTC-USD"));
        let eth = StreamId::new(VenueId::Coinbase, Symbol::new("ETH-USD"));
        let requests = ResyncRequests::new([btc.clone(), eth.clone()]);
        let mut eth_signal = requests.subscribe(&eth).expect("registered");

        requests.request(&btc);

        assert_eq!(requests.requested(&btc), 1);
        assert_eq!(requests.requested(&eth), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), eth_signal.requested())
                .await
                .is_err(),
            "ETH-USD's ingest task was woken by BTC-USD's desync"
        );
    }

    #[tokio::test]
    async fn an_unregistered_venue_is_a_no_op_not_a_panic() {
        // Replay registers no ingest tasks, and a replayed desync must not
        // bring the process down trying to reconnect a socket that does not
        // exist.
        let requests = ResyncRequests::new([stream(VenueId::Coinbase)]);
        assert!(!requests.request(&stream(VenueId::Bitstamp)));
        assert!(requests.subscribe(&stream(VenueId::Bitstamp)).is_none());
    }

    #[tokio::test]
    async fn a_venue_with_no_listener_reports_that_nobody_heard() {
        let requests = ResyncRequests::new([stream(VenueId::Coinbase)]);
        assert!(
            !requests.request(&stream(VenueId::Coinbase)),
            "registered but unsubscribed should report no listener"
        );
    }
}
