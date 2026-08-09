//! One `VenueSync` implementation per real venue.
//!
//! See each submodule's doc comment for that venue's specific wire format and
//! integrity discipline.

pub mod coinbase;

mod common;

pub use coinbase::CoinbaseSync;
