//! Membership by lease, and the rule that makes sharding safe without
//! consensus.
//!
//! # The property that has to hold
//!
//! **At most one node runs a given stream at a time.** Two nodes on one stream
//! means two websocket connections to a venue that rate-limits and bans for
//! exactly that, two books, and two sets of metrics carrying identical labels
//! that a dashboard will silently add together.
//!
//! The dual property — that *every* stream is running somewhere — is weaker in
//! consequence, and the asymmetry is the whole design. An unowned stream shows
//! up as `uninitialized` in the snapshot and `ma_book_live 0` in the metrics:
//! obviously, loudly absent. A doubly-owned stream looks *fine* from every
//! angle until the venue starts refusing connections. So when the two cannot
//! both be guaranteed, **this prefers the visible gap to the silent
//! duplicate** — the same judgement `Desynced` makes about a book, one layer
//! up.
//!
//! # Why there is no consensus here, and no compare-and-swap either
//!
//! Each node writes exactly one key: **its own**. Membership is the set of
//! records that have not expired, and the assignment is a pure function of
//! that set ([`crate::assign`]). No node ever writes a key another node might
//! write, so there is nothing to serialise and no lock to acquire — which is
//! why a shared directory is a complete implementation, and why an object
//! store would be too, without needing conditional writes.
//!
//! What replaces agreement is a lease argument, and it has two halves that are
//! easy to get half-right.
//!
//! ## Half one: expiry is enforced by the holder, not by the granter
//!
//! The natural implementation has readers decide who is alive: a record older
//! than `ttl` is dead, and its streams are up for grabs. That is safe only if
//! the dead node agrees it is dead — and a node partitioned from the registry
//! does not know anything has happened. Its sockets are fine, its books are
//! live, and it keeps publishing.
//!
//! So a node stops its own streams by its own clock:
//! [`LeaseConfig::guard`] before its lease could possibly expire elsewhere. If
//! a registry round trip has not succeeded within `ttl - guard`, the node
//! releases **everything** — while another node cannot observe expiry until
//! `ttl`. The `guard` is the margin covering clock-rate differences and the
//! time it takes to actually drop the sockets.
//!
//! This is also why the holder's deadline is monotonic while the record's is
//! wall-clock. Other nodes have to compare timestamps across machines, so the
//! record carries a wall reading; the holder is only comparing against itself,
//! so it uses [`IngestTime::mono`](ma_core::IngestTime::mono) and cannot have
//! its lease extended by an NTP step. Same split as everywhere else in this
//! project — see `docs/DESIGN.md` §7.
//!
//! ## Half two: a joining node waits before it takes anything
//!
//! Holder-side expiry covers a node *leaving*. It does not cover one
//! *arriving*, and the arriving case is the one that looks safe and is not:
//!
//! > Node B starts and writes its record. B reads membership, sees `{A, B}`,
//! > computes the assignment, and finds it now owns a stream A is running. A's
//! > registry reads happen to be failing, so A has not seen B. Both run it.
//!
//! The fix is a settling period: **a node acquires nothing until `ttl + guard`
//! after its own first successful announcement.** That is sufficient, and the
//! argument is short enough to check:
//!
//! - B's record is durable from `t_write`, so *any* successful membership read
//!   after `t_write` returns B.
//! - So if A never releases the stream by recomputing its assignment, A had no
//!   successful round trip after `t_write` — meaning A's own hold deadline is
//!   `t_ok + ttl - guard` for some `t_ok < t_write`.
//! - A therefore releases strictly before `t_write + ttl - guard`, and B
//!   acquires no earlier than `t_write + ttl + guard`. Disjoint, by `2 ×
//!   guard`. ∎
//!
//! The argument depends on the hold deadline being extended only when a
//! **complete** round trip succeeds — announce *and* membership read. A node
//! that can write but cannot read would otherwise keep renewing its right to
//! hold streams it can no longer be told to release.
//!
//! The cost is that joining a cluster takes `ttl + guard` before the new node
//! carries anything. That is deliberate and it is why coordination is off
//! unless configured: a single-node run is the default and pays none of it.
//!
//! # What this is not
//!
//! Not a general cluster manager. There is no leader, no failure detector
//! beyond a lease timing out, no rebalancing policy beyond the hash, and no
//! attempt to move a stream faster than the lease allows. Those are the things
//! `etcd` or a real scheduler is for, and reaching for one is the right answer
//! at a scale this project explicitly does not target.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use ma_core::{Clock, IngestTime, StreamId};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::assign::{NodeId, assigned_to};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("i/o error in the cluster registry: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed lease record for {node}: {source}")]
    Malformed {
        node: String,
        #[source]
        source: serde_json::Error,
    },
}

/// One node's claim to be alive, as other nodes read it.
///
/// The wall clock appears here and nowhere else in this module, because this
/// is the one value compared *between* machines. Everything a node decides
/// about itself is monotonic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub node: NodeId,
    /// When the holder wrote this record, by the holder's wall clock.
    pub written_at_unix_ms: u64,
    /// How long after `written_at` the holder claims it is alive for.
    ///
    /// Carried in the record rather than assumed by the reader, so a cluster
    /// mid-rolling-restart — where one node has a new `ttl` and the others do
    /// not — still has every reader agree about when any given record dies.
    pub ttl_ms: u64,
}

impl Lease {
    /// Whether this record has expired, as of `now`.
    ///
    /// A record from the *future* is treated as live. That is a clock-skew
    /// artefact, and the safe reading of "I cannot tell how old this is" is
    /// "someone may still be holding it": believing it dead is what starts a
    /// double subscription.
    pub fn expired_at(&self, now: SystemTime) -> bool {
        let Ok(now_ms) = now.duration_since(SystemTime::UNIX_EPOCH) else {
            return false;
        };
        let now_ms = u64::try_from(now_ms.as_millis()).unwrap_or(u64::MAX);
        now_ms > self.written_at_unix_ms.saturating_add(self.ttl_ms)
    }
}

/// Where lease records live.
///
/// Deliberately three operations. Note what is *absent*: there is no
/// compare-and-swap, no conditional put, no lock. Each node writes only its
/// own key, so an implementation needs nothing an ordinary filesystem or an
/// ordinary object store does not already give.
pub trait Registry: std::fmt::Debug + Send + Sync {
    /// A description for the startup line that tells an operator what they
    /// just pointed at.
    fn describe(&self) -> String;

    /// Write (or overwrite) this node's own record.
    fn announce(&self, lease: &Lease) -> BoxFuture<'_, Result<(), RegistryError>>;

    /// Every record currently present, expired or not. Filtering is the
    /// caller's job because the caller owns the clock.
    fn members(&self) -> BoxFuture<'_, Result<Vec<Lease>, RegistryError>>;

    /// Remove this node's own record. Best-effort courtesy on a clean
    /// shutdown: it lets the rest of the cluster rebalance in a round trip
    /// instead of waiting out `ttl`. Nothing is allowed to depend on it —
    /// a node that is killed cannot run it, and that case must behave
    /// identically, only slower.
    fn withdraw(&self, node: &NodeId) -> BoxFuture<'_, Result<(), RegistryError>>;
}

/// Lease timings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseConfig {
    /// How long a record stays valid after it is written.
    pub ttl: Duration,
    /// How often to rewrite it. Must be comfortably shorter than `ttl - guard`
    /// or a single slow round trip drops every stream on the node.
    pub renew: Duration,
    /// Safety margin between a holder releasing and any reader being able to
    /// observe expiry. Covers clock-rate differences between machines and the
    /// time it takes to actually stop the sockets.
    pub guard: Duration,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        // 15s / 3s / 2s: five renewal attempts inside one lease, so a node
        // survives four consecutive failed round trips before releasing
        // anything, and a dead node's streams move within 15s. The guard is
        // deliberately larger than any plausible stop latency — dropping a
        // websocket is milliseconds — because the quantity it really has to
        // cover is clock-rate disagreement between two machines over one ttl.
        Self {
            ttl: Duration::from_secs(15),
            renew: Duration::from_secs(3),
            guard: Duration::from_secs(2),
        }
    }
}

impl LeaseConfig {
    /// How long a node holds streams after its last complete round trip.
    fn hold_for(&self) -> Duration {
        self.ttl.saturating_sub(self.guard)
    }

    /// How long after its first announcement a node waits before acquiring.
    /// See the module docs — this is the half of the argument that covers a
    /// node *joining*.
    fn settle_for(&self) -> Duration {
        self.ttl.saturating_add(self.guard)
    }
}

/// What this node currently believes about the cluster, for `/cluster` and
/// `/metrics`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterView {
    pub node: NodeId,
    /// Live members, including this node, in a stable order.
    pub members: Vec<NodeId>,
    /// Streams this node is running right now.
    pub owned: Vec<String>,
    /// Streams configured for the cluster that this node is not running.
    pub elsewhere: Vec<String>,
    /// True while this node is inside its settling period and deliberately
    /// owns nothing yet.
    pub settling: bool,
    /// True when the node has released everything because it could not reach
    /// the registry. The state that distinguishes "this node is idle because
    /// the cluster gave its work to someone else" from "this node is idle
    /// because it went blind and stood down".
    pub stood_down: bool,
    /// How long since the last complete registry round trip.
    pub last_contact_ms: Option<u64>,
}

/// Runs the lease loop and publishes the set of streams this node should own.
#[derive(Debug)]
pub struct Coordinator {
    node: NodeId,
    registry: Box<dyn Registry>,
    config: LeaseConfig,
    /// Every stream the *cluster* runs. Identical on every node — it is the
    /// input to a pure assignment, so a node configured with a different list
    /// would compute a different answer.
    streams: Vec<StreamId>,
}

impl Coordinator {
    pub fn new(
        node: NodeId,
        registry: Box<dyn Registry>,
        config: LeaseConfig,
        streams: Vec<StreamId>,
    ) -> Self {
        Self {
            node,
            registry,
            config,
            streams,
        }
    }

    /// One pass of the loop, exposed so the offline suite can drive several
    /// simulated nodes through interleaved passes against a shared registry
    /// and assert what a real cluster would take minutes to show.
    pub async fn step(&self, state: &mut LeaseState, now: IngestTime) -> ClusterView {
        let lease = Lease {
            node: self.node.clone(),
            written_at_unix_ms: unix_millis(now.wall()),
            ttl_ms: millis(self.config.ttl),
        };

        // A complete round trip is announce *and* read. Extending the hold on
        // a successful write alone would let a node that can publish but not
        // listen keep renewing its right to hold streams it can no longer be
        // told to give up — see the module docs.
        let round_trip = match self.registry.announce(&lease).await {
            Ok(()) => match self.registry.members().await {
                Ok(members) => Some(members),
                Err(e) => {
                    warn!(node = %self.node, error = %e, "could not read cluster membership");
                    None
                }
            },
            Err(e) => {
                warn!(node = %self.node, error = %e, "could not renew this node's lease");
                None
            }
        };

        if let Some(members) = &round_trip {
            state.note_contact(now);
            state.members = live_members(members, now.wall());
        }

        state.reconcile(now, self.config, &self.node, &self.streams);
        state.view(&self.node, &self.streams, now)
    }

    /// Drive the loop until `stop` resolves, publishing ownership on `owned`.
    pub async fn run(
        self,
        clock: &dyn Clock,
        owned: watch::Sender<BTreeSet<StreamId>>,
        view: watch::Sender<ClusterView>,
        mut stop: impl Future<Output = ()> + Unpin,
    ) {
        let mut state = LeaseState::new(clock.now());
        info!(
            node = %self.node,
            registry = %self.registry.describe(),
            ttl = ?self.config.ttl,
            settling_for = ?self.config.settle_for(),
            streams = self.streams.len(),
            "joining the cluster; acquiring nothing until the settling period ends"
        );

        loop {
            let snapshot = self.step(&mut state, clock.now()).await;
            let _ = owned.send(state.owned.clone());
            let _ = view.send(snapshot);

            tokio::select! {
                () = tokio::time::sleep(self.config.renew) => {}
                () = &mut stop => break,
            }
        }

        // Release everything before the process goes away, so the rest of the
        // cluster rebalances now rather than after `ttl`. Best-effort by
        // design: a hard kill cannot run this, and the lease timing out has to
        // produce the same outcome — only later.
        let _ = owned.send(BTreeSet::new());
        if let Err(e) = self.registry.withdraw(&self.node).await {
            debug!(node = %self.node, error = %e, "could not withdraw cleanly; the lease will expire");
        }
    }
}

/// The part of the coordinator that changes, separated so a test can drive it
/// deterministically against a [`TestClock`](ma_core::TestClock).
#[derive(Debug)]
pub struct LeaseState {
    /// Live members as of the last successful read.
    members: Vec<NodeId>,
    /// Last complete registry round trip.
    last_contact: Option<IngestTime>,
    /// When this node's settling period ends. Reset whenever the node stands
    /// down, so a node that goes blind and comes back re-serves its notice
    /// rather than resuming instantly.
    settled_at: IngestTime,
    /// Whether the node is currently holding anything at all.
    stood_down: bool,
    /// Whether the node has a lease but is still serving out its notice.
    settling: bool,
    owned: BTreeSet<StreamId>,
}

impl LeaseState {
    pub fn new(now: IngestTime) -> Self {
        Self {
            members: Vec::new(),
            last_contact: None,
            // Set properly on the first successful contact; until then the
            // node has no lease at all and owns nothing regardless.
            settled_at: now,
            stood_down: true,
            settling: false,
            owned: BTreeSet::new(),
        }
    }

    pub fn owned(&self) -> &BTreeSet<StreamId> {
        &self.owned
    }

    fn note_contact(&mut self, now: IngestTime) {
        if self.last_contact.is_none() {
            // First contact: the settling clock starts here, not at process
            // start. A node that spends a minute failing to reach the registry
            // has not been visible to anyone during it, so its record is only
            // durable from now.
            self.settled_at = now;
        }
        self.last_contact = Some(now);
    }

    /// Decide what this node holds, given how long since it last had contact.
    fn reconcile(
        &mut self,
        now: IngestTime,
        config: LeaseConfig,
        node: &NodeId,
        streams: &[StreamId],
    ) {
        let held = self
            .last_contact
            .is_some_and(|at| now.since(at) < config.hold_for());

        if !held {
            if !self.stood_down {
                warn!(
                    %node,
                    since = ?self.last_contact.map(|at| now.since(at)),
                    "no complete registry round trip within ttl - guard; releasing every \
                     stream. Another node may take them, and must not find us still here."
                );
            }
            self.stand_down(now);
            return;
        }

        // Settling. The half of the safety argument that covers *joining*: the
        // incumbent gets a full ttl to notice this node exists and let go.
        if now.since(self.settled_at) < config.settle_for() {
            self.settling = true;
            self.owned.clear();
            return;
        }
        self.settling = false;

        if self.stood_down {
            info!(%node, members = self.members.len(), "settled; taking assigned streams");
            self.stood_down = false;
        }

        let assigned = assigned_to(node, streams, &self.members);
        if assigned != self.owned {
            info!(
                %node,
                was = self.owned.len(),
                now = assigned.len(),
                "cluster assignment changed"
            );
            self.owned = assigned;
        }
    }

    fn stand_down(&mut self, now: IngestTime) {
        self.owned.clear();
        self.stood_down = true;
        self.settling = false;
        // Re-serve the settling notice. A node that lost contact cannot know
        // what happened while it was blind, so coming back is joining.
        self.last_contact = None;
        self.settled_at = now;
    }

    fn view(&self, node: &NodeId, streams: &[StreamId], now: IngestTime) -> ClusterView {
        ClusterView {
            node: node.clone(),
            members: self.members.clone(),
            owned: self.owned.iter().map(StreamId::key).collect(),
            elsewhere: streams
                .iter()
                .filter(|s| !self.owned.contains(s))
                .map(StreamId::key)
                .collect(),
            settling: self.settling,
            stood_down: self.stood_down,
            last_contact_ms: self.last_contact.map(|at| millis(now.since(at))),
        }
    }
}

/// Unexpired records, deduplicated and sorted so every node computes the same
/// assignment from the same registry regardless of read order.
fn live_members(leases: &[Lease], now: SystemTime) -> Vec<NodeId> {
    let mut live: Vec<NodeId> = leases
        .iter()
        .filter(|l| !l.expired_at(now))
        .map(|l| l.node.clone())
        .collect();
    live.sort();
    live.dedup();
    live
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn unix_millis(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
