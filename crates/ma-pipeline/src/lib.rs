//! Async plumbing: the bounded channel, ingest tasks, the aggregator, and
//! replay. Everything here may use `tokio`; the domain logic it calls into
//! (`ma-core`, `ma-venues`) may not.

pub mod channel;

pub use channel::{ChannelMetrics, Receiver, SendOutcome, Sender, bounded};
