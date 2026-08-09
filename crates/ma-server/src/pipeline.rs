//! Assembling the pipeline, once, for every mode it runs in.
//!
//! CLAUDE.md asks that replay feed "the *same* aggregator through the *same*
//! channel" as a live run. That is easy to claim and easy to quietly break —
//! it takes one convenience constructor for tests, or one shortcut where
//! replay pushes normalised events instead of raw frames, and from then on the
//! offline suite is exercising a pipeline that does not exist in production.
//!
//! [`Pipeline`] is the one place either mode is built. `serve` and `replay`
//! both call [`Pipeline::new`] and both write into [`Pipeline::channel`]; the
//! difference is only whether the thing writing is a websocket or a file.

use std::sync::Arc;
use std::time::Duration;

use ma_core::{Clock, Symbol, SystemClock, VenueId};
use ma_pipeline::aggregator::{Aggregator, Snapshot};
use ma_pipeline::channel::{Receiver, Sender, bounded};
use ma_pipeline::ingest::{Ingest, IngestMessage, Shutdown, ShutdownTrigger, shutdown};
use ma_pipeline::metrics::Metrics;
use ma_pipeline::net::LiveNetwork;
use ma_pipeline::resync::ResyncRequests;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::info;

/// How many raw frames may queue between ingest and the aggregator.
///
/// Deliberately modest. A large buffer does not make this system more correct,
/// it makes it slower to notice that it is behind — and since the drop policy
/// holds that a stale tick has negative value, a buffer big enough to hold
/// seconds of backlog is a buffer full of data nobody should act on. Small
/// enough that `dropped` starts climbing promptly if the aggregator falls
/// behind, which is the signal actually worth having.
pub const CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("venue configuration: {0}")]
    Venue(#[from] ma_venues::VenueError),
    #[error("network: {0}")]
    Net(#[from] ma_pipeline::net::NetError),
    #[error("the aggregator has already been started; there can only be one")]
    AggregatorAlreadyRunning,
}

/// Everything a running pipeline exposes to the outside.
#[derive(Clone, Debug)]
pub struct PipelineHandle {
    /// Subscribe here for the snapshot stream. Every SSE client does.
    pub snapshots: broadcast::Sender<Arc<Snapshot>>,
    pub metrics: Arc<Metrics>,
    pub symbol: Symbol,
    pub venues: Vec<VenueId>,
}

impl PipelineHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Snapshot>> {
        self.snapshots.subscribe()
    }
}

/// A configured but not-yet-running pipeline.
#[derive(Debug)]
pub struct Pipeline {
    symbol: Symbol,
    venues: Vec<VenueId>,
    clock: Arc<dyn Clock>,
    tick: Duration,
    metrics: Arc<Metrics>,
    /// Lets the aggregator ask an ingest task to reconnect after a desync
    /// that a healthy socket cannot repair on its own.
    resync: ResyncRequests,
    tx: Sender<IngestMessage>,
    /// Taken by [`Self::spawn_aggregator`]. There is exactly one receiver, and
    /// moving it out is what makes "the aggregator is the single owner of
    /// every book" a fact the type system holds rather than a convention.
    rx: Option<Receiver<IngestMessage>>,
    trigger: ShutdownTrigger,
    shutdown: Shutdown,
}

impl Pipeline {
    /// # Errors
    /// If any venue has no endpoint, or the symbol is not in normalised
    /// `BASE-QUOTE` form. Checked here so a typo stops the process at startup
    /// rather than at the first reconnect.
    pub fn new(symbol: Symbol, venues: Vec<VenueId>) -> Result<Self, PipelineError> {
        for venue in &venues {
            ma_venues::spec_for(*venue, &symbol)?;
        }
        let (tx, rx) = bounded(CHANNEL_CAPACITY);
        let (trigger, shutdown) = shutdown();
        Ok(Self {
            metrics: Arc::new(Metrics::new(venues.iter().copied())),
            resync: ResyncRequests::new(venues.iter().copied()),
            symbol,
            venues,
            clock: Arc::new(SystemClock),
            tick: ma_pipeline::aggregator::DEFAULT_TICK,
            tx,
            rx: Some(rx),
            trigger,
            shutdown,
        })
    }

    #[must_use]
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    /// The sender every producer writes into — a live ingest task, or replay.
    pub fn channel(&self) -> Sender<IngestMessage> {
        self.tx.clone()
    }

    pub fn shutdown(&self) -> Shutdown {
        self.shutdown.clone()
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    pub fn venues(&self) -> &[VenueId] {
        &self.venues
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Spawn the aggregator task.
    ///
    /// # Errors
    /// If a venue spec cannot be built, or this was already called.
    pub fn spawn_aggregator(&mut self) -> Result<(PipelineHandle, JoinHandle<()>), PipelineError> {
        let rx = self
            .rx
            .take()
            .ok_or(PipelineError::AggregatorAlreadyRunning)?;

        let specs = self
            .venues
            .iter()
            .map(|v| ma_venues::spec_for(*v, &self.symbol))
            .collect::<Result<Vec<_>, _>>()?;

        let aggregator = Aggregator::new(
            self.symbol.clone(),
            specs,
            Arc::clone(&self.clock),
            &self.metrics,
        )
        .with_tick(self.tick)
        .requesting_resync_through(self.resync.clone());

        let handle = PipelineHandle {
            snapshots: aggregator.publisher(),
            metrics: Arc::clone(&self.metrics),
            symbol: self.symbol.clone(),
            venues: self.venues.clone(),
        };

        let task = tokio::spawn(aggregator.run(rx, self.shutdown.clone()));
        Ok((handle, task))
    }

    /// Spawn one live ingest task per venue, optionally teeing to a tape.
    ///
    /// # Errors
    /// If a venue spec cannot be built, or the HTTP client cannot be created.
    pub fn spawn_ingest(
        &self,
        tape: Option<tokio::sync::mpsc::UnboundedSender<IngestMessage>>,
    ) -> Result<Vec<JoinHandle<()>>, PipelineError> {
        let net = Arc::new(LiveNetwork::new()?);
        let mut tasks = Vec::with_capacity(self.venues.len());

        for venue in &self.venues {
            let spec = ma_venues::spec_for(*venue, &self.symbol)?;
            let counters = self.metrics.venue(*venue).unwrap_or_default();
            info!(%venue, url = %spec.endpoint.ws_url, "starting ingest");

            let mut ingest = Ingest::new(
                Arc::clone(&net),
                spec.endpoint,
                self.tx.clone(),
                Arc::clone(&self.clock),
                counters,
                self.shutdown.clone(),
            );
            if let Some(tape) = tape.clone() {
                ingest = ingest.recording_to(tape);
            }
            if let Some(signal) = self.resync.subscribe(*venue) {
                ingest = ingest.listening_for_resync(signal);
            }
            tasks.push(tokio::spawn(ingest.run()));
        }
        Ok(tasks)
    }

    /// Stop every task this pipeline started.
    pub fn stop(&self) {
        self.trigger.stop();
    }

    /// Consume the pipeline, keeping the shutdown trigger alive.
    ///
    /// [`ShutdownTrigger`] stops everything when dropped, so a caller that
    /// lets a `Pipeline` fall out of scope while its tasks are still running
    /// would shut them all down immediately. Holding the returned trigger is
    /// how a binary says "keep running"; this method exists so that
    /// requirement is visible in the call rather than buried in a doc comment.
    #[must_use]
    pub fn into_trigger(self) -> ShutdownTrigger {
        self.trigger
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::DEFAULT_VENUES;

    #[test]
    fn a_bad_symbol_is_refused_at_construction() {
        // Not at the first reconnect, three hours into a soak.
        let err = Pipeline::new(Symbol::new("BTCUSD"), DEFAULT_VENUES.to_vec()).unwrap_err();
        assert!(matches!(err, PipelineError::Venue(_)), "{err}");
    }

    #[test]
    fn the_fake_venue_cannot_be_served() {
        let err = Pipeline::new(Symbol::new("BTC-USD"), vec![VenueId::Fake]).unwrap_err();
        assert!(matches!(err, PipelineError::Venue(_)), "{err}");
    }

    #[tokio::test]
    async fn there_can_only_be_one_aggregator() {
        // Two aggregators would each own a partial view of the same stream and
        // both would look plausible. The receiver moves, so the second attempt
        // cannot compile its way around this.
        let mut p = Pipeline::new(Symbol::new("BTC-USD"), DEFAULT_VENUES.to_vec()).unwrap();
        assert!(p.spawn_aggregator().is_ok());
        assert!(matches!(
            p.spawn_aggregator().unwrap_err(),
            PipelineError::AggregatorAlreadyRunning
        ));
        p.stop();
    }

    #[tokio::test]
    async fn ingest_and_the_aggregator_share_one_set_of_counters() {
        // If they did not, every metric on the page would read zero forever
        // while looking entirely plausible.
        let mut p = Pipeline::new(Symbol::new("BTC-USD"), vec![VenueId::Kraken]).unwrap();
        let (handle, _task) = p.spawn_aggregator().unwrap();

        p.metrics()
            .venue(VenueId::Kraken)
            .expect("registered")
            .record_frame(42);
        assert_eq!(handle.metrics.snapshot()[&VenueId::Kraken].frames, 1);
        p.stop();
    }
}
