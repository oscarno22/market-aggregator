//! Which node owns which stream.
//!
//! Pure, deterministic, and deliberately free of I/O: given the same set of
//! live nodes, every node in the cluster computes the same answer without
//! talking to any of the others. That is what removes the need for a leader,
//! and with it the need for consensus.
//!
//! # Rendezvous hashing, not modulo
//!
//! The obvious assignment is `nodes[hash(stream) % nodes.len()]`. It is
//! deterministic and it is wrong here, because changing `nodes.len()` changes
//! the answer for *almost every* stream: adding a third node to a two-node
//! cluster reshuffles about two thirds of the assignments rather than the
//! third that has to move.
//!
//! On most systems that is merely wasteful. Here every reassignment is a
//! **disconnect and a resync** — the stream is torn down on one node and
//! rebuilt from a fresh snapshot on another, against venues that rate-limit
//! and ban for reconnect storms. A rebalance that moves three times more
//! streams than necessary is three times the venue traffic and three times the
//! window in which books are `Desynced`.
//!
//! [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
//! (highest random weight) moves only the streams that must move: each stream
//! independently scores every node and picks the highest, so removing a node
//! redistributes exactly that node's streams and disturbs nothing else, and
//! adding one takes exactly a `1/n` share from the others.
//!
//! # The hash must be stable across processes, and `DefaultHasher` is not
//!
//! `std::collections::hash_map::DefaultHasher` is SipHash with keys that are
//! *not* specified to be stable — across a rebuild, across a Rust version, and
//! in `RandomState` across a process. Reaching for it here would be an
//! innocuous-looking choice that breaks the one property this module exists to
//! provide: two nodes would compute *different* assignments from the *same*
//! membership, and both would believe they owned the same stream. That is
//! precisely the failure the lease design in [`crate::lease`] is built to make
//! impossible, reintroduced above it.
//!
//! So the hash is written out here, explicitly and boringly — FNV-1a to mix
//! the bytes, then SplitMix64's finaliser for avalanche. Same reasoning as the
//! backoff jitter using SplitMix64 rather than the `rand` crate: no
//! cryptographic requirement, and a value anyone can reproduce by hand beats a
//! value that depends on a dependency's internals.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use ma_core::StreamId;
use serde::{Deserialize, Serialize};

/// The name of one process in the cluster.
///
/// Must be stable across restarts of that process and unique within the
/// cluster. Stable because a node that renames itself on every restart looks
/// like a new node joining and the old one dying, which triggers a rebalance
/// per deploy; unique because two processes sharing a name share a lease, and
/// each would extend the other's — the exact overlap the lease exists to
/// prevent.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(s: impl AsRef<str>) -> Self {
        Self(Arc::from(s.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

/// The node that should own `stream`, given the live membership.
///
/// `None` only when `nodes` is empty — a cluster with no live member owns
/// nothing, which is the correct and visible answer rather than a panic or an
/// arbitrary fallback.
pub fn owner<'a>(stream: &StreamId, nodes: &'a [NodeId]) -> Option<&'a NodeId> {
    nodes
        .iter()
        // Ties break on the node id, so the answer does not depend on the
        // order the caller happened to list its members in. A tie is
        // vanishingly unlikely and the cost of handling it is one comparison;
        // the cost of *not* handling it is two nodes disagreeing about a
        // stream because they read the registry in different orders.
        .max_by(|a, b| {
            weight(stream, a)
                .cmp(&weight(stream, b))
                .then_with(|| b.cmp(a))
        })
}

/// Every stream `node` should own, in a stable order.
pub fn assigned_to(node: &NodeId, streams: &[StreamId], nodes: &[NodeId]) -> BTreeSet<StreamId> {
    streams
        .iter()
        .filter(|stream| owner(stream, nodes) == Some(node))
        .cloned()
        .collect()
}

/// The score node `n` gives stream `s`. Highest wins.
fn weight(stream: &StreamId, node: &NodeId) -> u64 {
    // A separator between the two fields, so that ("ab", "c") and ("a", "bc")
    // cannot hash alike. Byte 0 cannot occur in a venue name, a symbol, or a
    // node id read from a command line.
    let mut h = FNV_OFFSET;
    h = fnv(h, node.as_str().as_bytes());
    h = fnv(h, &[0]);
    h = fnv(h, stream.venue.as_str().as_bytes());
    h = fnv(h, &[0]);
    h = fnv(h, stream.symbol.as_str().as_bytes());
    // FNV-1a alone has poor avalanche in the high bits, which matters here
    // because the comparison is a `max` over the whole word: without mixing,
    // similar node names produce similar weights and the distribution skews.
    splitmix64_finalise(h)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

fn splitmix64_finalise(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Symbol, VenueId};

    fn nodes(names: &[&str]) -> Vec<NodeId> {
        names.iter().map(NodeId::new).collect()
    }

    /// Every venue crossed with `n` symbols — the shape a real cluster shards.
    fn streams(n: usize) -> Vec<StreamId> {
        let venues = [VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp];
        (0..n)
            .flat_map(|i| {
                let symbol = Symbol::new(format!("SYM{i}-USD"));
                venues
                    .iter()
                    .map(move |v| StreamId::new(*v, symbol.clone()))
            })
            .collect()
    }

    #[test]
    fn every_stream_has_exactly_one_owner() {
        let nodes = nodes(&["a", "b", "c"]);
        let streams = streams(40);

        let mut seen = BTreeSet::new();
        for node in &nodes {
            for stream in assigned_to(node, &streams, &nodes) {
                assert!(seen.insert(stream.clone()), "{stream} owned twice");
            }
        }
        assert_eq!(seen.len(), streams.len(), "some stream was owned by nobody");
    }

    #[test]
    fn the_assignment_does_not_depend_on_the_order_members_were_read_in() {
        // Two nodes reading the same registry can list its records in
        // different orders. If that changed the answer, both would believe
        // they owned the same stream — the failure the whole lease design
        // exists to prevent, reintroduced by a sort order.
        let forward = nodes(&["alpha", "beta", "gamma"]);
        let backward = nodes(&["gamma", "beta", "alpha"]);
        let streams = streams(50);

        for node in &forward {
            assert_eq!(
                assigned_to(node, &streams, &forward),
                assigned_to(node, &streams, &backward),
            );
        }
    }

    #[test]
    fn removing_a_node_moves_only_that_nodes_streams() {
        // The property rendezvous hashing is chosen for. Every reassignment is
        // a disconnect and a resync against a venue that rate-limits, so a
        // rebalance that disturbs a stream it did not have to is real traffic
        // and a real window of Desynced books.
        let before = nodes(&["a", "b", "c"]);
        let after = nodes(&["a", "b"]);
        let streams = streams(60);

        let lost = assigned_to(&NodeId::new("c"), &streams, &before);
        assert!(!lost.is_empty(), "c owned nothing; the test proves nothing");

        for name in ["a", "b"] {
            let node = NodeId::new(name);
            let kept = assigned_to(&node, &streams, &before);
            let now = assigned_to(&node, &streams, &after);
            assert!(
                kept.is_subset(&now),
                "{name} lost a stream when an unrelated node left"
            );
            // Everything gained came from the departed node, and nowhere else.
            assert!(now.difference(&kept).all(|s| lost.contains(s)));
        }
    }

    #[test]
    fn adding_a_node_takes_only_its_share_and_disturbs_nothing_else() {
        let before = nodes(&["a", "b"]);
        let after = nodes(&["a", "b", "c"]);
        let streams = streams(60);

        let gained = assigned_to(&NodeId::new("c"), &streams, &after);
        // Modulo hashing would move roughly two thirds here. Rendezvous moves
        // a third, and the assertion is deliberately loose about the exact
        // count and strict about the property: nothing moves between the two
        // incumbents.
        assert!(
            !gained.is_empty() && gained.len() < streams.len() / 2,
            "a joining node took {} of {} streams",
            gained.len(),
            streams.len()
        );

        for name in ["a", "b"] {
            let node = NodeId::new(name);
            let was = assigned_to(&node, &streams, &before);
            let now = assigned_to(&node, &streams, &after);
            assert!(
                now.is_subset(&was),
                "{name} gained a stream when a new node joined"
            );
            assert!(
                was.difference(&now).all(|s| gained.contains(s)),
                "{name} lost a stream to somewhere other than the new node"
            );
        }
    }

    #[test]
    fn the_split_is_roughly_even() {
        // Not a guarantee rendezvous hashing makes exactly, but a badly skewed
        // hash would show here — and a skewed assignment means one node
        // holding most of the sockets, which is the thing sharding exists to
        // stop.
        let nodes = nodes(&["node-1", "node-2", "node-3", "node-4"]);
        let streams = streams(100); // 300 streams
        let ideal = streams.len() / nodes.len();

        for node in &nodes {
            let n = assigned_to(node, &streams, &nodes).len();
            assert!(
                n > ideal / 2 && n < ideal * 2,
                "{node} got {n} streams against an ideal of {ideal}"
            );
        }
    }

    #[test]
    fn an_empty_cluster_owns_nothing_rather_than_guessing() {
        let stream = StreamId::new(VenueId::Kraken, Symbol::new("BTC-USD"));
        assert_eq!(owner(&stream, &[]), None);
    }

    #[test]
    fn one_node_owns_everything() {
        let nodes = nodes(&["only"]);
        let streams = streams(10);
        assert_eq!(
            assigned_to(&nodes[0], &streams, &nodes).len(),
            streams.len()
        );
    }

    #[test]
    fn weights_are_pinned_so_a_rebuild_cannot_silently_reshuffle_a_cluster() {
        // The hash is part of the wire contract between nodes, not an
        // implementation detail: two nodes running builds that disagree about
        // it would compute different assignments from the same membership and
        // both claim the same stream. Changing these constants is a protocol
        // change and needs a rolling-restart plan, which is why they are
        // pinned here rather than left to whatever `DefaultHasher` does today.
        //
        // The values come from an independent reimplementation of the
        // documented algorithm (FNV-1a over `node \0 venue \0 symbol`, then
        // SplitMix64's finaliser) rather than from this code's own output, so
        // they check the implementation against the specification rather than
        // against itself.
        let stream = StreamId::new(VenueId::Coinbase, Symbol::new("BTC-USD"));
        assert_eq!(weight(&stream, &NodeId::new("a")), 658_005_282_954_361);
        assert_eq!(
            weight(&stream, &NodeId::new("b")),
            4_719_291_369_574_919_490
        );
    }
}
