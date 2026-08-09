//! Sharding streams across nodes, and the coordination that makes it safe.
//!
//! v1 and v2 run every `(venue, symbol)` stream in one process. That is the
//! right answer at a handful of symbols and stops being one at fifty, where
//! three venues means 150 sockets on one node — and `docs/DESIGN.md` §2 has
//! always named sharding as the honest fix rather than multiplexing away the
//! per-stream isolation the resync path depends on.
//!
//! Two pieces, deliberately separable:
//!
//! - [`assign`] — which node owns which stream. Pure, deterministic, no I/O,
//!   no async. Every node computes the same answer from the same membership
//!   without talking to any other node, which is what removes the need for a
//!   leader.
//! - [`lease`] — who is a member. A lease per node, enforced by its **holder**
//!   rather than by its readers, plus a settling period on join. Together
//!   those give "at most one node runs a stream" without consensus and without
//!   a compare-and-swap.
//!
//! [`registry`] holds the two places a lease record can live.
//!
//! The safety argument is written out in [`lease`]'s module docs. It is short,
//! and it is the part worth reading before changing any of the timings.
//!
//! # No Kafka, and now no etcd either
//!
//! CLAUDE.md's non-goals kept Kafka out at single-node scale and said to
//! revisit at v3 sharding. Revisited: the coordination problem here is not a
//! log, it is membership, and membership by lease needs a clock and somewhere
//! to write one key per node. A directory does that. Adding a consensus system
//! would be the largest operational dependency in the project, bought to solve
//! a problem the lease argument already closes.

pub mod assign;
pub mod lease;
pub mod registry;

#[cfg(feature = "s3")]
pub mod s3;

pub use assign::{NodeId, assigned_to, owner};
pub use lease::{
    ClusterView, Coordinator, Lease, LeaseConfig, LeaseState, Registry, RegistryError,
};
pub use registry::{DirRegistry, MemoryRegistry, record_name, registry_from_uri};

#[cfg(feature = "s3")]
pub use s3::S3Registry;
