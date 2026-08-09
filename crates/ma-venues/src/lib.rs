//! Venue wire formats and sync discipline.
//!
//! One trait, [`VenueSync`], with one implementation per venue. The three real
//! venues share almost nothing:
//!
//! | Venue    | Snapshot from | Ordering field   | Recovery                        |
//! |----------|---------------|------------------|---------------------------------|
//! | Bitstamp | REST only     | `microtimestamp` | [`RecoveryStrategy::RestSnapshot`] |
//! | Coinbase | the websocket | `sequence_num`   | [`RecoveryStrategy::Resubscribe`]  |
//! | Kraken   | the websocket | none at all      | [`RecoveryStrategy::Resubscribe`]  |
//!
//! Nothing in this crate opens a socket or awaits anything. A [`VenueSync`] is
//! fed frames and returns instructions, which is what makes the scripted
//! [`fake`] venue able to drive the identical code path a live feed does.

pub mod fake;
pub mod sync;
pub mod venues;

pub use fake::{FakeSync, Script, Tape, fake_checksum};
pub use sync::{
    Outcome, RawFrame, RecoveryStrategy, RestSnapshot, SyncAction, VenueBook, VenueError,
    VenueSync,
};
pub use venues::{CoinbaseSync, KrakenSync};
