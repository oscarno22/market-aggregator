//! Parsing helpers shared by more than one venue.

use ma_core::Level;

use crate::sync::VenueError;

/// Parse a `(price_str, qty_str)` pair into a [`Level`].
///
/// Used by Coinbase's `price_level`/`new_quantity` fields, and by Bitstamp's
/// `[price, qty]` wire pairs via [`levels_from_str_pairs`] below.
pub fn level_from_str_pair(price: &str, qty: &str) -> Result<Level, VenueError> {
    let price = price
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad price {price:?}")))?;
    let qty = qty
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad qty {qty:?}")))?;
    Ok(Level::new(price, qty))
}

/// Bitstamp sends every side of every book message as an array of
/// `[price, qty]` pairs, on both the diff channel and the REST snapshot.
pub fn levels_from_str_pairs(pairs: &[[String; 2]]) -> Result<Vec<Level>, VenueError> {
    pairs
        .iter()
        .map(|[p, q]| level_from_str_pair(p, q))
        .collect()
}
