//! Async plumbing: the bounded channel, ingest tasks, the aggregator, and
//! replay. Everything here may use `tokio`; the domain logic it calls into
//! (`ma-core`, `ma-venues`) may not.

pub mod channel;
pub mod tape;

pub use channel::{ChannelMetrics, Receiver, SendOutcome, Sender, bounded};
pub use tape::{ReplayStats, TapeError, TapeReader, TapeWriter, TapedFrame, replay};
