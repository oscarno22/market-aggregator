//! Core domain types for the market aggregator.
//!
//! This crate holds the order book, the normalised event type, and the clock
//! discipline. It performs no I/O and has no async runtime — see the note in
//! `Cargo.toml` and the `manifest` integration test, which enforce that.
//!
//! # The three states a consumer must be able to distinguish
//!
//! ```text
//!   Uninitialized  ->  no data at all
//!   Desynced       ->  data I do not trust
//!   Live           ->  data I trust, to a degree the venue determines
//! ```
//!
//! The degree is [`Integrity`], and it differs per venue because the venues
//! differ in what they can actually prove:
//!
//! | Venue    | Mechanism                    | [`Integrity`]              |
//! |----------|------------------------------|----------------------------|
//! | Bitstamp | microtimestamp ordering only | [`Integrity::OrderOnly`]     |
//! | Coinbase | contiguous `sequence_num`    | [`Integrity::GapDetectable`] |
//! | Kraken   | CRC32 over the built book    | [`Integrity::Verified`]      |
//!
//! [`Integrity`] is `Ord`, weakest first, so a combined view can take the
//! minimum rather than quietly presenting the strongest.

pub mod audit;
pub mod book;
pub mod cross;
pub mod cross_windows;
pub mod event;
pub mod price;
pub mod stream;
pub mod time;
pub mod window;

pub use audit::{AuditFinding, AuditOutcome, AuditPolicy, AuditTrail, audit};
pub use book::{Book, BookError, BookState, BookStateKind, DesyncReason, Integrity, TopOfBook};
pub use cross::{CrossLeg, CrossPolicy, CrossVenue, Exclusion, ExclusionReason, consolidate};
pub use cross_windows::{
    CrossWindowReading, WindowExclusion, WindowExclusionReason, WindowLeg, consolidate_windows,
};
pub use event::{EventKind, Level, MarketEvent, Side, SkewObservation, Symbol, VenueId};
pub use price::{ParseError, Price, Qty};
pub use stream::StreamId;
pub use time::{Clock, IngestTime, ScaledClock, SystemClock, TestClock};
pub use window::{RollingWindows, WindowReading, WindowSpec};
