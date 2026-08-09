//! The identity of one subscription: a venue and a symbol.
//!
//! v1 tracked one symbol, so a [`VenueId`] was enough to name a book, a set of
//! counters, or the task that fed them. v2 tracks several, and the moment two
//! symbols exist, `VenueId` names a *group* of books rather than one — which is
//! the kind of ambiguity that produces a metric labelled `coinbase` holding the
//! sum of two feeds and nobody noticing.
//!
//! [`StreamId`] is that identity made explicit. It keys the aggregator's books,
//! the counters, and the resync signals, and it labels every metric.
//!
//! # One connection per stream, and why that is not the obvious waste it looks
//!
//! All three venues will happily accept several symbols on one socket, so the
//! cheap-looking design is one connection per venue carrying every symbol. This
//! project deliberately does not do that, and the reason is the resync path in
//! `ma_pipeline::resync`: **recovery here means dropping the socket.** Every
//! venue only sends a fresh snapshot on a new subscription, so a book broken by
//! a sequence gap is repaired by reconnecting.
//!
//! On a multiplexed connection, one symbol's gap would tear down every other
//! symbol's feed on that socket, desyncing books that were perfectly healthy —
//! turning a single-symbol fault into a venue-wide one. Per-stream connections
//! make the blast radius of a resync exactly the stream that needed it.
//!
//! The cost is connection count: venues rate-limit and ban on reconnect storms,
//! and this multiplies the connections by the symbol count. At the handful of
//! symbols this is built for that is a few sockets per venue and is not
//! interesting. It would become interesting at fifty, and at that point the
//! honest fix is v3's sharding, not multiplexing away the isolation.

use std::fmt;

use crate::event::{Symbol, VenueId};

/// One venue's feed for one symbol: the unit that has a connection, a book, a
/// set of counters, and a resync signal.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId {
    pub venue: VenueId,
    pub symbol: Symbol,
}

impl StreamId {
    pub fn new(venue: VenueId, symbol: Symbol) -> Self {
        Self { venue, symbol }
    }

    /// Stable identifier for logs and filenames, e.g. `coinbase:BTC-USD`.
    ///
    /// Deliberately not the metrics label: Prometheus gets two separate labels
    /// (`venue` and `symbol`) so a query can aggregate over either one. A
    /// single joined label would force every dashboard to do string surgery.
    pub fn key(&self) -> String {
        format!("{}:{}", self.venue, self.symbol)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.venue, self.symbol)
    }
}

impl fmt::Debug for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamId({}:{})", self.venue, self.symbol)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn id(venue: VenueId, symbol: &str) -> StreamId {
        StreamId::new(venue, Symbol::new(symbol))
    }

    #[test]
    fn one_venue_two_symbols_are_two_streams() {
        // The whole point. If these compared equal, two symbols would share one
        // book and one set of counters, and the sum would look plausible.
        assert_ne!(
            id(VenueId::Coinbase, "BTC-USD"),
            id(VenueId::Coinbase, "ETH-USD")
        );
        assert_ne!(
            id(VenueId::Coinbase, "BTC-USD"),
            id(VenueId::Kraken, "BTC-USD")
        );
        assert_eq!(
            id(VenueId::Coinbase, "BTC-USD"),
            id(VenueId::Coinbase, "BTC-USD")
        );
    }

    #[test]
    fn streams_order_by_venue_then_symbol() {
        // Ordering is not cosmetic: the aggregator holds a BTreeMap and the UI
        // renders it in iteration order, so a stable order keeps cards from
        // shuffling between ticks.
        let set: BTreeSet<StreamId> = [
            id(VenueId::Kraken, "ETH-USD"),
            id(VenueId::Coinbase, "ETH-USD"),
            id(VenueId::Kraken, "BTC-USD"),
            id(VenueId::Coinbase, "BTC-USD"),
        ]
        .into_iter()
        .collect();

        let order: Vec<String> = set.iter().map(StreamId::key).collect();
        assert_eq!(
            order,
            [
                "coinbase:BTC-USD",
                "coinbase:ETH-USD",
                "kraken:BTC-USD",
                "kraken:ETH-USD"
            ]
        );
    }
}
