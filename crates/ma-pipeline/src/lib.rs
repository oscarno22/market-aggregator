//! Async plumbing: the bounded channel, ingest tasks, the aggregator, and
//! replay. Everything here may use `tokio`; the domain logic it calls into
//! (`ma-core`, `ma-venues`) may not.

pub mod aggregator;
pub mod backoff;
pub mod channel;
pub mod ingest;
pub mod metrics;
pub mod net;
pub mod tape;

pub use aggregator::{Aggregator, BookStatus, Snapshot, VenueView};
pub use backoff::{Backoff, BackoffPolicy, EqualJitter, Jitter, NoJitter};
pub use channel::{ChannelMetrics, Receiver, SendOutcome, Sender, bounded};
pub use ingest::{Ingest, IngestMessage, SessionEnd, Shutdown, ShutdownTrigger, shutdown};
pub use metrics::{Metrics, Rates, VenueCounters, VenueCountersSnapshot};
pub use net::{LiveNetwork, NetError, Network, Transport};
pub use tape::{Pacing, ReplayStats, TapeError, TapeReader, TapeWriter, TapedFrame, replay};
