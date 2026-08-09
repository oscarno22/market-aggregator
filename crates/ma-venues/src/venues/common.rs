//! Parsing helpers shared by more than one venue.

use ma_core::Level;

use crate::sync::VenueError;

/// Parse a `(price_str, qty_str)` pair into a [`Level`].
///
/// Used by Coinbase's `price_level`/`new_quantity` fields today; Bitstamp's
/// `[price, qty]` wire pairs reuse it too once that venue lands.
pub fn level_from_str_pair(price: &str, qty: &str) -> Result<Level, VenueError> {
    let price = price
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad price {price:?}")))?;
    let qty = qty
        .parse()
        .map_err(|_| VenueError::Malformed(format!("bad qty {qty:?}")))?;
    Ok(Level::new(price, qty))
}
