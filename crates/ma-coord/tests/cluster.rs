//! Several nodes against one registry, stepped by hand.
//!
//! The safety property — *at most one node runs a given stream* — is a claim
//! about every instant, not about a steady state, and the instants that break
//! it are the handovers: a node joining, a node dying, and a node that is
//! perfectly healthy but cannot reach the registry. A real cluster produces
//! those rarely and at inconvenient times.
//!
//! So the coordinator's per-pass logic is exposed as `Coordinator::step`, and
//! these tests drive two of them through a shared in-process registry against
//! a `TestClock`, asserting disjointness after **every** pass rather than at
//! the end. A violation lasting one 250ms tick fails here; in production it
//! would be a venue ban and a fortnight of wondering why.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::time::Duration;

use ma_coord::lease::{BoxFuture, LeaseState};
use ma_coord::{Coordinator, Lease, LeaseConfig, MemoryRegistry, NodeId, Registry, RegistryError};
use ma_core::{Clock, StreamId, Symbol, TestClock, VenueId};

/// The cluster's stream set: three venues over four symbols.
fn streams() -> Vec<StreamId> {
    let venues = [VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp];
    ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]
        .into_iter()
        .flat_map(|s| {
            let symbol = Symbol::new(s);
            venues
                .iter()
                .map(move |v| StreamId::new(*v, symbol.clone()))
        })
        .collect()
}

fn config() -> LeaseConfig {
    // The defaults, named here because every timing assertion below is
    // arithmetic on them: hold_for = 13s, settle_for = 17s.
    LeaseConfig {
        ttl: Duration::from_secs(15),
        renew: Duration::from_secs(3),
        guard: Duration::from_secs(2),
    }
}

/// One simulated process: a coordinator, its lease state, and its own handle
/// onto the shared registry so it can be partitioned independently.
struct Node {
    name: &'static str,
    registry: MemoryRegistry,
    coord: Coordinator,
    state: LeaseState,
}

impl Node {
    fn new(name: &'static str, shared: &MemoryRegistry, clock: &TestClock) -> Self {
        let registry = shared.handle();
        Self {
            name,
            registry: registry.clone(),
            coord: Coordinator::new(NodeId::new(name), Box::new(registry), config(), streams()),
            state: LeaseState::new(clock.now()),
        }
    }

    async fn step(&mut self, clock: &TestClock) -> BTreeSet<StreamId> {
        self.coord.step(&mut self.state, clock.now()).await;
        self.state.owned().clone()
    }
}

/// Step every node once and fail if any two hold the same stream.
async fn tick(nodes: &mut [&mut Node], clock: &TestClock, at: Duration) {
    let mut held: Vec<(&'static str, BTreeSet<StreamId>)> = Vec::new();
    for node in nodes.iter_mut() {
        let owned = node.step(clock).await;
        held.push((node.name, owned));
    }

    for i in 0..held.len() {
        for j in (i + 1)..held.len() {
            let overlap: Vec<String> = held[i]
                .1
                .intersection(&held[j].1)
                .map(StreamId::key)
                .collect();
            assert!(
                overlap.is_empty(),
                "at t={:?}, {} and {} both held {overlap:?} — two connections to one \
                 venue subscription is the one thing this design exists to prevent",
                at,
                held[i].0,
                held[j].0,
            );
        }
    }
}

#[tokio::test]
async fn one_node_owns_everything_but_only_after_it_settles() {
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);

    // Settling is not a formality even for the first node: it cannot tell an
    // empty cluster from one whose other members it has not yet read.
    let owned = a.step(&clock).await;
    assert!(owned.is_empty(), "a node took streams before settling");

    for _ in 0..5 {
        clock.advance(Duration::from_secs(3));
        let owned = a.step(&clock).await;
        assert!(
            owned.is_empty(),
            "a node took streams during its settling period"
        );
    }

    // 15s + 2s = 17s after first contact.
    clock.advance(Duration::from_secs(3));
    assert_eq!(a.step(&clock).await.len(), streams().len());
}

#[tokio::test]
async fn a_joining_node_never_overlaps_the_incumbent() {
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);
    let mut b = Node::new("b", &shared, &clock);

    // A settles alone and takes everything.
    let mut elapsed = Duration::ZERO;
    for _ in 0..8 {
        tick(&mut [&mut a], &clock, elapsed).await;
        clock.advance(Duration::from_secs(3));
        elapsed += Duration::from_secs(3);
    }
    assert_eq!(a.state.owned().len(), streams().len());

    // B joins. From here both are stepped together, and every pass is checked.
    for _ in 0..40 {
        tick(&mut [&mut a, &mut b], &clock, elapsed).await;
        clock.advance(Duration::from_millis(500));
        elapsed += Duration::from_millis(500);
    }

    let (a_owned, b_owned) = (a.state.owned(), b.state.owned());
    assert!(!b_owned.is_empty(), "the joining node never took any work");
    assert!(!a_owned.is_empty(), "the incumbent gave up everything");
    assert_eq!(
        a_owned.len() + b_owned.len(),
        streams().len(),
        "some stream ended up owned by nobody"
    );
}

#[tokio::test]
async fn a_partitioned_node_stands_down_before_anyone_can_take_its_streams() {
    // The case the whole holder-side-expiry rule exists for, and the one no
    // amount of registry-side checking can cover: node A's sockets are fine,
    // its books are live, and it simply cannot see the registry any more. If A
    // waits to be told, it never is.
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);

    let mut elapsed = Duration::ZERO;
    let step = |d: Duration, elapsed: &mut Duration| {
        clock.advance(d);
        *elapsed += d;
    };

    for _ in 0..8 {
        tick(&mut [&mut a], &clock, elapsed).await;
        step(Duration::from_secs(3), &mut elapsed);
    }
    assert_eq!(
        a.state.owned().len(),
        streams().len(),
        "A should be running everything"
    );

    // A goes blind. B starts at the same instant and can see the registry.
    a.registry.set_offline(true);
    let mut b = Node::new("b", &shared, &clock);
    let partitioned_at = elapsed;

    let mut a_released_at = None;
    let mut b_acquired_at = None;

    for _ in 0..120 {
        tick(&mut [&mut a, &mut b], &clock, elapsed).await;
        if a_released_at.is_none() && a.state.owned().is_empty() {
            a_released_at = Some(elapsed - partitioned_at);
        }
        if b_acquired_at.is_none() && !b.state.owned().is_empty() {
            b_acquired_at = Some(elapsed - partitioned_at);
        }
        step(Duration::from_millis(500), &mut elapsed);
    }

    let released = a_released_at.expect("A never released its streams while partitioned");
    let acquired = b_acquired_at.expect("B never picked up the partitioned node's streams");

    // A must let go at ttl - guard = 13s, strictly before B can observe A's
    // record expire at ttl = 15s. B additionally serves its own settling
    // period, so the real separation is larger still — but the guarantee that
    // matters is the ordering, and it is asserted rather than assumed.
    assert!(
        released <= Duration::from_secs(13) + Duration::from_millis(500),
        "A held on for {released:?}; its lease could have been declared dead at 15s"
    );
    assert!(
        acquired > released,
        "B acquired at {acquired:?} but A did not release until {released:?}"
    );
    assert_eq!(
        b.state.owned().len(),
        streams().len(),
        "B should have ended up running the whole cluster"
    );
}

#[tokio::test]
async fn a_node_that_regains_contact_serves_its_notice_again() {
    // Coming back from a partition is joining: the node has no idea what
    // happened while it was blind, so resuming instantly would be exactly the
    // unsafe case the settling period covers.
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);

    for _ in 0..8 {
        a.step(&clock).await;
        clock.advance(Duration::from_secs(3));
    }
    assert!(!a.state.owned().is_empty());

    a.registry.set_offline(true);
    for _ in 0..6 {
        a.step(&clock).await;
        clock.advance(Duration::from_secs(3));
    }
    assert!(a.state.owned().is_empty(), "A did not stand down");

    a.registry.set_offline(false);
    a.step(&clock).await;
    assert!(
        a.state.owned().is_empty(),
        "A resumed the instant it regained contact, without re-serving its notice"
    );

    for _ in 0..7 {
        clock.advance(Duration::from_secs(3));
        a.step(&clock).await;
    }
    assert_eq!(
        a.state.owned().len(),
        streams().len(),
        "A never resumed after re-settling"
    );
}

#[tokio::test]
async fn a_clean_withdrawal_hands_over_faster_than_a_timeout() {
    // `withdraw` is a courtesy and nothing depends on it, but it should
    // actually save the wait — otherwise a rolling deploy costs a full ttl of
    // unowned streams per node.
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);
    let mut b = Node::new("b", &shared, &clock);

    let mut elapsed = Duration::ZERO;
    for _ in 0..14 {
        tick(&mut [&mut a, &mut b], &clock, elapsed).await;
        clock.advance(Duration::from_secs(3));
        elapsed += Duration::from_secs(3);
    }
    assert!(!a.state.owned().is_empty() && !b.state.owned().is_empty());

    // A leaves cleanly. B is already settled, so it takes over on the next
    // pass rather than after A's lease times out.
    a.registry.withdraw(&NodeId::new("a")).await.unwrap();
    tick(&mut [&mut b], &clock, elapsed).await;
    assert_eq!(
        b.state.owned().len(),
        streams().len(),
        "a clean withdrawal did not hand over immediately"
    );
}

#[tokio::test]
async fn every_node_agrees_about_who_owns_what() {
    // Three nodes, all settled, all reading the same registry. The assignment
    // is a pure function of membership, so their views must partition the
    // stream set exactly — no overlap and no orphan.
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();
    let mut a = Node::new("a", &shared, &clock);
    let mut b = Node::new("b", &shared, &clock);
    let mut c = Node::new("c", &shared, &clock);

    let mut elapsed = Duration::ZERO;
    for _ in 0..14 {
        tick(&mut [&mut a, &mut b, &mut c], &clock, elapsed).await;
        clock.advance(Duration::from_secs(3));
        elapsed += Duration::from_secs(3);
    }

    let union: BTreeSet<StreamId> = a
        .state
        .owned()
        .union(b.state.owned())
        .chain(c.state.owned())
        .cloned()
        .collect();
    assert_eq!(union.len(), streams().len(), "a stream was owned by nobody");
    for node in [&a, &b, &c] {
        assert!(!node.state.owned().is_empty(), "{} idled", node.name);
    }
}

/// A registry that can write and read but not delete.
///
/// Not hypothetical: this is what an S3 bucket looks like under an IAM policy
/// granting `PutObject`, `GetObject` and `ListBucket` and nothing else — which
/// is exactly the policy this project's own scoped user has, and which the
/// first live S3-backed cluster run therefore exercised on 2026-08-09.
#[derive(Clone, Debug)]
struct NoDelete(MemoryRegistry);

impl Registry for NoDelete {
    fn describe(&self) -> String {
        format!("{} (delete denied)", self.0.describe())
    }

    fn announce(&self, lease: &Lease) -> BoxFuture<'_, Result<(), RegistryError>> {
        self.0.announce(lease)
    }

    fn members(&self) -> BoxFuture<'_, Result<Vec<Lease>, RegistryError>> {
        self.0.members()
    }

    fn withdraw(&self, _node: &NodeId) -> BoxFuture<'_, Result<(), RegistryError>> {
        Box::pin(async {
            Err(RegistryError::Io(std::io::Error::other(
                "AccessDenied: s3:DeleteObject",
            )))
        })
    }
}

#[tokio::test]
async fn a_registry_that_cannot_delete_still_hands_over_by_timeout() {
    // The degradation an object store makes easy to reach, and the reason
    // `withdraw` is best-effort rather than load-bearing.
    //
    // A node that cannot delete its own record is indistinguishable, to
    // everyone else, from a node that was `kill -9`'d — and the lease argument
    // already has to cover that case, because a hard kill cannot run a
    // shutdown path. So the safety property is untouched and the only thing
    // lost is the speed of a clean handover.
    //
    // Worth a test rather than a comment because the tempting "fix" is to make
    // a failed withdrawal an error the caller reacts to, and every reaction
    // available is worse than waiting: retrying hammers a registry that has
    // already said no, and treating it as fatal turns a permissions gap into a
    // crash loop.
    let clock = TestClock::new();
    let shared = MemoryRegistry::new();

    let undeletable = NoDelete(shared.handle());
    let mut a = Node::new("a", &shared, &clock);
    a.coord = Coordinator::new(
        NodeId::new("a"),
        Box::new(undeletable.clone()),
        config(),
        streams(),
    );
    let mut b = Node::new("b", &shared, &clock);

    let mut elapsed = Duration::ZERO;
    for _ in 0..14 {
        tick(&mut [&mut a, &mut b], &clock, elapsed).await;
        clock.advance(Duration::from_secs(3));
        elapsed += Duration::from_secs(3);
    }
    let a_held = a.state.owned().clone();
    assert!(!a_held.is_empty() && !b.state.owned().is_empty());

    // A shuts down. Its withdrawal is refused, so its record stays behind with
    // a timestamp that stops advancing — which is precisely the state a hard
    // kill leaves.
    assert!(
        undeletable.withdraw(&NodeId::new("a")).await.is_err(),
        "the harness registry was expected to refuse a delete"
    );
    drop(a);

    // Before the lease expires, B must *not* take A's streams: the stale
    // record is still live, and treating a failed withdrawal as a licence to
    // grab early would be the doubly-owned stream this design exists to
    // prevent.
    clock.advance(Duration::from_secs(9));
    elapsed += Duration::from_secs(9);
    tick(&mut [&mut b], &clock, elapsed).await;
    assert!(
        b.state.owned().is_disjoint(&a_held),
        "B took a stream while A's record was still within its ttl"
    );

    // After it expires, B takes everything — the same outcome a clean
    // withdrawal produces, just one lease later.
    clock.advance(Duration::from_secs(9));
    elapsed += Duration::from_secs(9);
    tick(&mut [&mut b], &clock, elapsed).await;
    assert_eq!(
        b.state.owned().len(),
        streams().len(),
        "an undeletable record left streams unowned forever"
    );
}
