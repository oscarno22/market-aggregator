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

//!
//! [`endpoint`] holds the other half of each venue's identity — its URL and
//! subscribe payloads — as data, so that `ma-pipeline` can connect without
//! this crate ever growing a transport.

pub mod bitstamp;
pub mod coinbase;
pub mod endpoint;
pub mod kraken;

mod common;

pub use bitstamp::BitstampSync;
pub use coinbase::CoinbaseSync;
pub use endpoint::{VenueEndpoint, VenueSpec, native_symbol, spec_for};
pub use kraken::KrakenSync;
