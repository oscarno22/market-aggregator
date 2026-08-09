//! One `VenueSync` implementation per real venue.
//!
//! See each submodule's doc comment for that venue's specific wire format and
//! integrity discipline. The three, side by side:
//!
//! | Venue    | Module       | Sync type        | Integrity      | Recovery       |
//! |----------|--------------|-------------------|----------------|----------------|
//! | Coinbase | [`coinbase`] | [`CoinbaseSync`]  | `GapDetectable`| `Resubscribe`  |
//! | Kraken   | [`kraken`]   | [`KrakenSync`]    | `Verified`     | `Resubscribe`  |
//! | Bitstamp | [`bitstamp`] | [`BitstampSync`]  | `OrderOnly`    | `RestSnapshot` |

pub mod bitstamp;
pub mod coinbase;
pub mod kraken;

mod common;

pub use bitstamp::BitstampSync;
pub use coinbase::CoinbaseSync;
pub use kraken::KrakenSync;
