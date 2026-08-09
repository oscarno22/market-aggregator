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

use std::collections::BTreeSet;

use ma_coord::{ClusterView, Coordinator, LeaseConfig, NodeId, Registry};
use ma_core::{Clock, CrossPolicy, StreamId, Symbol, SystemClock, VenueId, WindowSpec};
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
    pub symbols: Vec<Symbol>,
    pub venues: Vec<VenueId>,
    /// The rolling-window spans this process publishes. Carried on the handle
    /// because `/metrics` has to name each series after its span — a
    /// Prometheus series cannot say "the second one in the list".
    pub windows: WindowSpec,
    /// This node's view of the cluster, when clustering is on. `None` for a
    /// single-node run, which is the default — and the distinction is worth
    /// keeping in the type rather than reporting a one-member cluster, because
    /// "no coordination configured" and "a cluster of one" behave differently
    /// on startup: only the second serves a settling period.
    pub cluster: Option<tokio::sync::watch::Receiver<ClusterView>>,
}

impl PipelineHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Snapshot>> {
        self.snapshots.subscribe()
    }
}

/// A configured but not-yet-running pipeline.
#[derive(Debug)]
pub struct Pipeline {
    symbols: Vec<Symbol>,
    venues: Vec<VenueId>,
    clock: Arc<dyn Clock>,
    tick: Duration,
    windows: WindowSpec,
    cross: CrossPolicy,
    /// Set by [`Pipeline::clustered`]. When present, the aggregator publishes
    /// only owned streams and a supervisor starts and stops sockets as
    /// ownership moves.
    owned: Option<tokio::sync::watch::Receiver<BTreeSet<StreamId>>>,
    cluster: Option<tokio::sync::watch::Receiver<ClusterView>>,
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
    /// Where normalised events go, if a persistence sink was attached. Taken
    /// by `spawn_aggregator`, which is the only thing that can supply them.
    events: Option<tokio::sync::mpsc::UnboundedSender<ma_core::MarketEvent>>,
}

impl Pipeline {
    /// One symbol, the common case.
    ///
    /// # Errors
    /// As [`Self::new`].
    pub fn single(symbol: Symbol, venues: Vec<VenueId>) -> Result<Self, PipelineError> {
        Self::new(vec![symbol], venues)
    }

    /// Every venue crossed with every symbol: one stream, one connection, one
    /// book, one set of counters per pair. See `ma_core::stream` for why the
    /// cross is per-connection rather than multiplexed.
    ///
    /// # Errors
    /// If any venue has no endpoint, or a symbol is not in normalised
    /// `BASE-QUOTE` form. Checked here so a typo stops the process at startup
    /// rather than at the first reconnect.
    pub fn new(symbols: Vec<Symbol>, venues: Vec<VenueId>) -> Result<Self, PipelineError> {
        for venue in &venues {
            for symbol in &symbols {
                ma_venues::spec_for(*venue, symbol)?;
            }
        }
        let streams: Vec<StreamId> = venues
            .iter()
            .flat_map(|v| symbols.iter().map(|s| StreamId::new(*v, s.clone())))
            .collect();
        let (tx, rx) = bounded(CHANNEL_CAPACITY);
        let (trigger, shutdown) = shutdown();
        Ok(Self {
            metrics: Arc::new(Metrics::new(streams.clone())),
            resync: ResyncRequests::new(streams),
            symbols,
            venues,
            clock: Arc::new(SystemClock),
            tick: ma_pipeline::aggregator::DEFAULT_TICK,
            windows: WindowSpec::default(),
            cross: CrossPolicy::default(),
            owned: None,
            cluster: None,
            tx,
            rx: Some(rx),
            trigger,
            shutdown,
            events: None,
        })
    }

    #[must_use]
    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    /// Rolling-window spans, at the publish tick's resolution.
    ///
    /// The bucket resolution is tied to the tick rather than configured
    /// separately: a window bucket finer than the publish interval buys
    /// nothing a client can observe, and one coarser would make `span_ms`
    /// quantise in a way an operator reading the page has no way to see. Call
    /// after [`Self::with_tick`], which is where the resolution comes from.
    #[must_use]
    pub fn with_windows(mut self, spans: Vec<Duration>) -> Self {
        self.windows = WindowSpec::new(self.tick, spans);
        self
    }

    /// How stale a book may be and still be a leg of the consolidated touch.
    #[must_use]
    pub fn with_cross_max_age(mut self, max_age: Duration) -> Self {
        self.cross = CrossPolicy { max_age };
        self
    }

    /// Tee every normalised event to a persistence sink — `ma-persist`'s
    /// Parquet writer. Must be called before [`Self::spawn_aggregator`], which
    /// is what consumes it.
    #[must_use]
    pub fn recording_events_to(
        mut self,
        events: tokio::sync::mpsc::UnboundedSender<ma_core::MarketEvent>,
    ) -> Self {
        self.events = Some(events);
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

    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    /// Every (venue, symbol) pair this pipeline runs, in a stable order.
    pub fn streams(&self) -> impl Iterator<Item = StreamId> + '_ {
        self.venues
            .iter()
            .flat_map(|v| self.symbols.iter().map(|s| StreamId::new(*v, s.clone())))
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
            .flat_map(|v| self.symbols.iter().map(move |s| ma_venues::spec_for(*v, s)))
            .collect::<Result<Vec<_>, _>>()?;

        let mut aggregator = Aggregator::with_window_spec(
            specs,
            Arc::clone(&self.clock),
            &self.metrics,
            self.windows.clone(),
        )
        .with_tick(self.tick)
        .with_cross_policy(self.cross)
        .requesting_resync_through(self.resync.clone());
        if let Some(events) = self.events.take() {
            aggregator = aggregator.publishing_events_to(events);
        }

        if let Some(owned) = &self.owned {
            aggregator = aggregator.restricted_to(owned.clone());
        }

        let handle = PipelineHandle {
            snapshots: aggregator.publisher(),
            metrics: Arc::clone(&self.metrics),
            symbols: self.symbols.clone(),
            venues: self.venues.clone(),
            windows: self.windows.clone(),
            cluster: self.cluster.clone(),
        };

        let task = tokio::spawn(aggregator.run(rx, self.shutdown.clone()));
        Ok((handle, task))
    }

    /// Join a cluster: run the lease loop, and start and stop streams as
    /// ownership moves.
    ///
    /// Must be called **before** [`Self::spawn_aggregator`], which is what
    /// reads the ownership channel. Returns the coordinator's task; the
    /// supervisor's is spawned by [`Self::spawn_ingest`], because that is
    /// where the network and the venue specs already are.
    ///
    /// # Errors
    /// Never today. Fallible because a future registry — an object store —
    /// will validate its target here rather than at the first renewal.
    pub fn clustered(
        &mut self,
        node: NodeId,
        registry: Box<dyn Registry>,
        config: LeaseConfig,
    ) -> Result<JoinHandle<()>, PipelineError> {
        // The stream list is the *cluster's*, not this node's, and must be
        // identical on every node: the assignment is a pure function of it, so
        // a node configured with a different `--symbols` computes a different
        // answer and the disjointness argument no longer holds. Nothing here
        // can check that, which is why it is stated in the startup log and in
        // docs/DESIGN.md rather than assumed.
        let streams: Vec<StreamId> = self.streams().collect();

        let (owned_tx, owned_rx) = tokio::sync::watch::channel(BTreeSet::new());
        let (view_tx, view_rx) = tokio::sync::watch::channel(ClusterView {
            node: node.clone(),
            members: Vec::new(),
            owned: Vec::new(),
            elsewhere: streams.iter().map(StreamId::key).collect(),
            settling: true,
            stood_down: true,
            last_contact_ms: None,
        });

        self.owned = Some(owned_rx);
        self.cluster = Some(view_rx);

        let coordinator = Coordinator::new(node, registry, config, streams);
        let clock = Arc::clone(&self.clock);
        let mut stop = self.shutdown.clone();
        Ok(tokio::spawn(async move {
            coordinator
                .run(
                    &*clock,
                    owned_tx,
                    view_tx,
                    Box::pin(async move { stop.wait().await }),
                )
                .await;
        }))
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

        // Clustered: which streams run is decided by the coordinator and
        // changes while the process is up, so the tasks are owned by a
        // supervisor rather than spawned once here.
        if let Some(owned) = self.owned.clone() {
            let supervisor = self.supervisor(net, tape)?;
            return Ok(vec![tokio::spawn(
                supervisor.run(owned, self.shutdown.clone()),
            )]);
        }

        let mut tasks = Vec::new();
        for stream in self.streams() {
            let spec = ma_venues::spec_for(stream.venue, &stream.symbol)?;
            let counters = self.metrics.stream(&stream).unwrap_or_default();
            info!(%stream, url = %spec.endpoint.ws_url, "starting ingest");

            let mut ingest = Ingest::new(
                Arc::clone(&net),
                stream.clone(),
                spec.endpoint,
                self.tx.clone(),
                Arc::clone(&self.clock),
                counters,
                self.shutdown.clone(),
            );
            if let Some(tape) = tape.clone() {
                ingest = ingest.recording_to(tape);
            }
            if let Some(signal) = self.resync.subscribe(&stream) {
                ingest = ingest.listening_for_resync(signal);
            }
            tasks.push(tokio::spawn(ingest.run()));
        }
        Ok(tasks)
    }

    /// A supervisor whose spawn function builds the same [`Ingest`] the
    /// single-node path does — deliberately the same construction, so a
    /// clustered stream is not a second, subtly different kind of stream.
    fn supervisor(
        &self,
        net: Arc<LiveNetwork>,
        tape: Option<tokio::sync::mpsc::UnboundedSender<IngestMessage>>,
        // `use<>` because the closure captures only owned clones, so the
        // supervisor outlives the `&self` it was built from. Without it,
        // edition 2024's default capture ties the opaque type to that borrow
        // and the task cannot be spawned.
    ) -> Result<crate::cluster::Supervisor<impl crate::cluster::SpawnStream + use<>>, PipelineError>
    {
        let tx = self.tx.clone();
        let clock = Arc::clone(&self.clock);
        let metrics = Arc::clone(&self.metrics);
        let resync = self.resync.clone();
        let channel = self.tx.clone();

        let spawn = move |stream: &StreamId, signal: ma_pipeline::ingest::Shutdown| {
            let spec = ma_venues::spec_for(stream.venue, &stream.symbol).ok()?;
            let counters = metrics.stream(stream).unwrap_or_default();
            info!(%stream, url = %spec.endpoint.ws_url, "starting ingest");

            let mut ingest = Ingest::new(
                Arc::clone(&net),
                stream.clone(),
                spec.endpoint,
                channel.clone(),
                Arc::clone(&clock),
                counters,
                signal,
            );
            if let Some(tape) = tape.clone() {
                ingest = ingest.recording_to(tape);
            }
            if let Some(signal) = resync.subscribe(stream) {
                ingest = ingest.listening_for_resync(signal);
            }
            Some(tokio::spawn(ingest.run()))
        };

        Ok(crate::cluster::Supervisor::new(
            spawn,
            tx,
            Arc::clone(&self.clock),
        ))
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
        let err = Pipeline::single(Symbol::new("BTCUSD"), DEFAULT_VENUES.to_vec()).unwrap_err();
        assert!(matches!(err, PipelineError::Venue(_)), "{err}");
    }

    #[test]
    fn the_fake_venue_cannot_be_served() {
        let err = Pipeline::single(Symbol::new("BTC-USD"), vec![VenueId::Fake]).unwrap_err();
        assert!(matches!(err, PipelineError::Venue(_)), "{err}");
    }

    #[test]
    fn every_venue_is_crossed_with_every_symbol() {
        // Three venues and two symbols is six connections, six books, six sets
        // of counters — not three of anything. See `ma_core::stream` for why
        // these are separate sockets rather than one multiplexed subscription
        // per venue.
        let p = Pipeline::new(
            vec![Symbol::new("BTC-USD"), Symbol::new("ETH-USD")],
            DEFAULT_VENUES.to_vec(),
        )
        .unwrap();

        let streams: Vec<String> = p.streams().map(|s| s.key()).collect();
        assert_eq!(streams.len(), 6);
        assert!(streams.contains(&"coinbase:BTC-USD".to_owned()));
        assert!(streams.contains(&"bitstamp:ETH-USD".to_owned()));
        assert_eq!(p.metrics().streams().count(), 6);
    }

    #[test]
    fn one_bad_symbol_in_a_list_stops_the_process_at_startup() {
        let err = Pipeline::new(
            vec![Symbol::new("BTC-USD"), Symbol::new("ETHUSD")],
            DEFAULT_VENUES.to_vec(),
        )
        .unwrap_err();
        assert!(matches!(err, PipelineError::Venue(_)), "{err}");
    }

    #[tokio::test]
    async fn there_can_only_be_one_aggregator() {
        // Two aggregators would each own a partial view of the same stream and
        // both would look plausible. The receiver moves, so the second attempt
        // cannot compile its way around this.
        let mut p = Pipeline::single(Symbol::new("BTC-USD"), DEFAULT_VENUES.to_vec()).unwrap();
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
        let mut p = Pipeline::single(Symbol::new("BTC-USD"), vec![VenueId::Kraken]).unwrap();
        let (handle, _task) = p.spawn_aggregator().unwrap();

        let stream = StreamId::new(VenueId::Kraken, Symbol::new("BTC-USD"));
        p.metrics()
            .stream(&stream)
            .expect("registered")
            .record_frame(42);
        assert_eq!(handle.metrics.snapshot()[&stream].frames, 1);
        p.stop();
    }
}
