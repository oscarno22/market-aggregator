//! Turning an ownership set into running sockets, and back.
//!
//! [`ma_coord`] decides *which* streams this node should run. This is the part
//! that makes that decision true of the process: it starts an ingest task when
//! a stream is acquired, and stops one when a stream is released.
//!
//! # Releasing has to be prompt, and it has to be visible
//!
//! The lease argument in `ma_coord::lease` bounds when another node may take a
//! stream, and the bound is only worth anything if this node has actually
//! dropped the socket by then. `guard` (2s by default) is the budget, and
//! dropping a websocket is milliseconds — but the release path must not be
//! *waiting* on anything with a longer timeout, which is why it drops the
//! per-stream [`ShutdownTrigger`] rather than asking the task to finish a
//! reconnect first.
//!
//! Releasing also tells the aggregator, by publishing a `SessionEnded`
//! **through the ingest channel** — the same message a real disconnect
//! produces, for the same reason it exists there. The book for a released
//! stream is not stale, it is *unknown*: this node has stopped watching, and
//! another node's book is the live one now. Leaving the last known prices
//! sitting in the snapshot marked `live` would be exactly the silent wrongness
//! the whole project is organised against — and it would feed the cross-venue
//! view a frozen quote, which `ma_core::cross` would then have to exclude on
//! staleness seconds later rather than never seeing at all.
//!
//! # One shutdown per stream, plus the global one
//!
//! Each stream gets its own [`ShutdownTrigger`], held by the supervisor. The
//! process-wide trigger still stops everything, because the supervisor itself
//! waits on it and drops the whole map on the way out.

use std::collections::BTreeMap;

use ma_core::{Clock, StreamId};
use ma_pipeline::channel::Sender;
use ma_pipeline::ingest::{IngestMessage, SessionEnd, Shutdown, ShutdownTrigger, shutdown};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// How a supervisor starts one stream.
///
/// A trait rather than a bare closure bound so the signature can be named in
/// return position without spelling it out three times. `None` means the
/// stream could not be built at all — a venue spec that no longer resolves —
/// which is worth a log line and is not worth failing the whole node for,
/// since every other stream it owns is still serviceable.
pub trait SpawnStream: FnMut(&StreamId, Shutdown) -> Option<JoinHandle<()>> {}

impl<F> SpawnStream for F where F: FnMut(&StreamId, Shutdown) -> Option<JoinHandle<()>> {}

/// A running stream: the trigger that stops it, and its task.
#[derive(Debug)]
struct Running {
    trigger: ShutdownTrigger,
    task: JoinHandle<()>,
}

/// Reconciles the set of running streams against the set this node owns.
///
/// Generic over the spawn function so the offline suite can drive it with
/// counters instead of sockets — the same seam `Network` provides one layer
/// down, for the same reason: a supervisor that can only be tested by opening
/// connections to three exchanges is a supervisor nobody tests.
pub struct Supervisor<F> {
    spawn: F,
    running: BTreeMap<StreamId, Running>,
    tx: Sender<IngestMessage>,
    clock: std::sync::Arc<dyn Clock>,
}

impl<F> std::fmt::Debug for Supervisor<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor")
            .field("running", &self.running.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<F: SpawnStream> Supervisor<F> {
    pub fn new(spawn: F, tx: Sender<IngestMessage>, clock: std::sync::Arc<dyn Clock>) -> Self {
        Self {
            spawn,
            running: BTreeMap::new(),
            tx,
            clock,
        }
    }

    /// Streams currently running, in a stable order.
    pub fn running(&self) -> impl Iterator<Item = &StreamId> {
        self.running.keys()
    }

    /// Start what is newly owned and stop what is no longer owned.
    pub fn reconcile(&mut self, owned: &std::collections::BTreeSet<StreamId>) {
        // Release first. If a rebalance both takes and gives — which it does
        // whenever membership changes — letting go before taking on keeps the
        // peak socket count at what the node was already carrying.
        let released: Vec<StreamId> = self
            .running
            .keys()
            .filter(|s| !owned.contains(*s))
            .cloned()
            .collect();

        for stream in released {
            if let Some(running) = self.running.remove(&stream) {
                info!(%stream, "releasing stream to the cluster");
                // Dropping the trigger signals the task. Deliberately not
                // awaited: the release budget is the lease guard, and a task
                // mid-reconnect could otherwise hold it for a backoff.
                drop(running.trigger);
                running.task.abort();
            }
            // Through the channel, not around it, so it lands in the right
            // place relative to any frames this stream already queued.
            let _ = self.tx.send(IngestMessage::SessionEnded {
                stream: stream.clone(),
                at: self.clock.now(),
                end: SessionEnd::Errored,
            });
        }

        for stream in owned {
            if self.running.contains_key(stream) {
                continue;
            }
            let (trigger, signal) = shutdown();
            match (self.spawn)(stream, signal) {
                Some(task) => {
                    info!(%stream, "acquired stream from the cluster");
                    self.running
                        .insert(stream.clone(), Running { trigger, task });
                }
                None => warn!(%stream, "could not start an acquired stream"),
            }
        }
    }

    /// Watch `owned` and reconcile on every change, until `stop` fires.
    pub async fn run(
        mut self,
        mut owned: watch::Receiver<std::collections::BTreeSet<StreamId>>,
        mut stop: Shutdown,
    ) {
        loop {
            let current = owned.borrow_and_update().clone();
            self.reconcile(&current);

            tokio::select! {
                () = stop.wait() => break,
                changed = owned.changed() => {
                    if changed.is_err() {
                        // The coordinator stopped. Its last act is to publish
                        // an empty set, which the pass above has already
                        // applied, so there is nothing left to run.
                        break;
                    }
                }
            }
        }

        info!(
            running = self.running.len(),
            "supervisor stopping; releasing every stream"
        );
        self.reconcile(&std::collections::BTreeSet::new());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Symbol, SystemClock, VenueId};
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn stream(venue: VenueId, symbol: &str) -> StreamId {
        StreamId::new(venue, Symbol::new(symbol))
    }

    fn set(streams: &[StreamId]) -> BTreeSet<StreamId> {
        streams.iter().cloned().collect()
    }

    /// A supervisor whose "ingest task" is a future that parks until its own
    /// shutdown fires, and a counter of how many are alive.
    fn supervisor(
        alive: Arc<AtomicUsize>,
    ) -> (
        Supervisor<impl SpawnStream>,
        ma_pipeline::channel::Receiver<IngestMessage>,
    ) {
        let (tx, rx) = ma_pipeline::channel::bounded(64);
        let spawn = move |_: &StreamId, mut signal: Shutdown| {
            let alive = Arc::clone(&alive);
            alive.fetch_add(1, Ordering::SeqCst);
            Some(tokio::spawn(async move {
                signal.wait().await;
                alive.fetch_sub(1, Ordering::SeqCst);
            }))
        };
        (Supervisor::new(spawn, tx, Arc::new(SystemClock)), rx)
    }

    #[tokio::test]
    async fn acquiring_starts_exactly_the_streams_owned() {
        let alive = Arc::new(AtomicUsize::new(0));
        let (mut sup, _rx) = supervisor(Arc::clone(&alive));

        let owned = set(&[
            stream(VenueId::Coinbase, "BTC-USD"),
            stream(VenueId::Kraken, "BTC-USD"),
        ]);
        sup.reconcile(&owned);
        assert_eq!(sup.running().count(), 2);
        assert_eq!(alive.load(Ordering::SeqCst), 2);

        // Reconciling to the same set must not restart anything. A rebalance
        // that reconnected healthy streams would be a self-inflicted reconnect
        // storm against venues that ban for exactly that.
        sup.reconcile(&owned);
        assert_eq!(alive.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn releasing_stops_the_task_and_tells_the_aggregator() {
        let alive = Arc::new(AtomicUsize::new(0));
        let (mut sup, rx) = supervisor(Arc::clone(&alive));

        let cb = stream(VenueId::Coinbase, "BTC-USD");
        let kr = stream(VenueId::Kraken, "BTC-USD");
        sup.reconcile(&set(&[cb.clone(), kr.clone()]));
        sup.reconcile(&set(std::slice::from_ref(&kr)));

        assert_eq!(sup.running().count(), 1);
        assert!(sup.running().any(|s| *s == kr));

        // The released book must be told it is no longer being watched.
        // Without this its last prices sit in the snapshot marked `live`
        // while another node is the one actually receiving updates.
        let mut saw_boundary = false;
        while let Some(message) = rx.try_recv() {
            if let IngestMessage::SessionEnded { stream, .. } = message
                && stream == cb
            {
                saw_boundary = true;
            }
        }
        assert!(
            saw_boundary,
            "a released stream left its book claiming to be live"
        );
    }

    #[tokio::test]
    async fn a_rebalance_releases_before_it_acquires() {
        // Both directions in one pass is the normal case when membership
        // changes. Releasing first keeps the peak socket count at what the
        // node was already carrying, rather than briefly holding both sets.
        let alive = Arc::new(AtomicUsize::new(0));
        let (mut sup, _rx) = supervisor(Arc::clone(&alive));

        sup.reconcile(&set(&[
            stream(VenueId::Coinbase, "BTC-USD"),
            stream(VenueId::Kraken, "BTC-USD"),
        ]));
        sup.reconcile(&set(&[
            stream(VenueId::Kraken, "BTC-USD"),
            stream(VenueId::Bitstamp, "ETH-USD"),
        ]));

        assert_eq!(sup.running().count(), 2);
        let names: Vec<String> = sup.running().map(StreamId::key).collect();
        assert_eq!(names, vec!["kraken:BTC-USD", "bitstamp:ETH-USD"]);
    }

    #[tokio::test]
    async fn standing_down_stops_everything() {
        let alive = Arc::new(AtomicUsize::new(0));
        let (mut sup, _rx) = supervisor(Arc::clone(&alive));

        sup.reconcile(&set(&[
            stream(VenueId::Coinbase, "BTC-USD"),
            stream(VenueId::Kraken, "BTC-USD"),
        ]));
        // What a partitioned node publishes: own nothing, immediately.
        sup.reconcile(&BTreeSet::new());
        assert_eq!(sup.running().count(), 0);
    }
}
