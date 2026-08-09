//! The Arrow schema for normalised events, and the reasoning behind each
//! column that has any.
//!
//! # Answering "which clock is this?" in writing
//!
//! `ma_core::IngestTime` deliberately does not implement `Serialize`, and its
//! doc comment says why: an `Instant` is meaningless outside the process that
//! created it, so the persistence layer is *forced* to reach for a clock
//! explicitly and state what it chose. This module is where that debt comes
//! due, and it is paid three times over, because one column is not enough:
//!
//! - [`INGEST_WALL`] — `SystemTime` at ingest. The only column a human or a
//!   downstream query should join on, and the only one comparable across
//!   process restarts. It can also jump backwards when NTP steps the clock,
//!   which is precisely why it is not the ordering column.
//! - [`INGEST_ELAPSED`] — monotonic nanoseconds since the writer started.
//!   **This is what replay orders and paces by.** Not an `Instant`: an
//!   *elapsed duration*, which is meaningful in any process. Exactly the same
//!   decision the raw-frame tape made with `elapsed_nanos`, for exactly the
//!   same reason, and the two agreeing is not a coincidence — a second answer
//!   to a solved question would be the mistake.
//! - [`VENUE_TS`] — the venue's own claim, nullable. Never ordered by, never
//!   windowed on, present so skew can be measured. `docs/DESIGN.md` §7.
//!
//! # Why prices are strings
//!
//! The same reason the SSE payload uses strings, and it is load-bearing rather
//! than lazy. Arrow's `Decimal128` fixes one scale for the whole column, and
//! these venues do not share a scale — Kraken sends `0.00100000` and its
//! checksum is computed over those exact digits, trailing zeros included.
//! Normalising to a column-wide scale would silently rewrite the digits the
//! venue hashed, which is the *precise* failure `Price` wrapping `Decimal`
//! instead of `f64` exists to prevent. A float column would be worse again.
//!
//! Strings cost less than they look like they should: prices repeat heavily
//! within an hour, and Parquet dictionary-encodes them.
//!
//! # Why one row per level
//!
//! An alternative shape puts each event's levels in a nested
//! `list<struct<price, qty>>`, one row per event. This uses one row per
//! *level* instead, and the reason is what the file is for. A Parquet file
//! nobody can query is just an expensive tape — the raw-frame tape already
//! exists and is better at being a tape. Flat rows make "what was on the book
//! at 03:14" a predicate rather than an unnest, which is the entire reason to
//! reach for a columnar format at all.
//!
//! The cost is honest: a Coinbase opening snapshot is tens of thousands of
//! rows. It is also tens of thousands of facts, and run-length encoding on the
//! repeated `event_seq`/`venue`/`symbol` columns makes them nearly free.
//!
//! Events with no levels — heartbeats, checksums — still emit exactly one row,
//! with the level columns null. Emitting none would let a heartbeat vanish, and
//! a stream that silently drops the messages proving liveness is the specific
//! failure `docs/DESIGN.md` §4 spends a page on.

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, Field, Schema};

/// Groups the rows belonging to one [`ma_core::MarketEvent`].
///
/// Monotonic within a writer, and continuous across an hourly roll, so an
/// event can be reassembled by grouping consecutive rows with equal values.
pub const EVENT_SEQ: &str = "event_seq";
pub const VENUE: &str = "venue";
pub const SYMBOL: &str = "symbol";
/// `snapshot` | `delta` | `trade` | `checksum` | `heartbeat`.
pub const KIND: &str = "kind";
/// Wall clock at ingest. Display and joins; never ordering. See module docs.
pub const INGEST_WALL: &str = "ingest_wall_unix_nanos";
/// Monotonic nanoseconds since the writer started. **The ordering column.**
pub const INGEST_ELAPSED: &str = "ingest_elapsed_nanos";
/// The venue's own timestamp, nullable. Measured, never trusted.
pub const VENUE_TS: &str = "venue_ts_unix_nanos";
/// `bid` | `ask`, or null for an event with no levels.
pub const SIDE: &str = "side";
/// Exact decimal digits as the venue sent them. See module docs.
pub const PRICE: &str = "price";
pub const QTY: &str = "qty";
/// Position within its side, in the order the venue listed it.
///
/// Preserved because Kraken's checksum is computed over the book in a specific
/// order, and because a snapshot's ordering is information the venue gave us.
/// Reconstructing it from sorted prices would work for a book and not for a
/// delta, which is not sorted at all.
pub const LEVEL_INDEX: &str = "level_index";
/// Kraken's CRC32. Null for every other venue, and that is the point: a
/// replayed book is still checked against what the venue said it should be.
pub const CHECKSUM: &str = "checksum";
pub const HEARTBEAT_COUNTER: &str = "heartbeat_counter";
/// Which side took liquidity on a trade, when the venue says.
pub const TAKER_SIDE: &str = "taker_side";

/// The schema every file this crate writes conforms to.
pub static EVENT_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(EVENT_SEQ, DataType::Int64, false),
        Field::new(VENUE, DataType::Utf8, false),
        Field::new(SYMBOL, DataType::Utf8, false),
        Field::new(KIND, DataType::Utf8, false),
        Field::new(INGEST_WALL, DataType::Int64, false),
        Field::new(INGEST_ELAPSED, DataType::Int64, false),
        Field::new(VENUE_TS, DataType::Int64, true),
        Field::new(SIDE, DataType::Utf8, true),
        Field::new(PRICE, DataType::Utf8, true),
        Field::new(QTY, DataType::Utf8, true),
        Field::new(LEVEL_INDEX, DataType::Int32, true),
        Field::new(CHECKSUM, DataType::UInt32, true),
        Field::new(HEARTBEAT_COUNTER, DataType::Int64, true),
        Field::new(TAKER_SIDE, DataType::Utf8, true),
    ]))
});

/// The `kind` values, as written.
pub mod kind {
    pub const SNAPSHOT: &str = "snapshot";
    pub const DELTA: &str = "delta";
    pub const TRADE: &str = "trade";
    pub const CHECKSUM: &str = "checksum";
    pub const HEARTBEAT: &str = "heartbeat";
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn nothing_that_identifies_an_event_is_nullable() {
        // A row that cannot say which event, venue, symbol or instant it
        // belongs to is not a record of anything. Levels are nullable because
        // a heartbeat genuinely has none; identity never is.
        for name in [EVENT_SEQ, VENUE, SYMBOL, KIND, INGEST_WALL, INGEST_ELAPSED] {
            let field = EVENT_SCHEMA.field_with_name(name).expect(name);
            assert!(!field.is_nullable(), "{name} is nullable");
        }
    }

    #[test]
    fn the_venue_clock_is_nullable_and_the_ingest_clock_is_not() {
        // Venues omit timestamps — Kraken's snapshots carry none at all — so a
        // non-null venue_ts column could only be achieved by inventing values.
        // Our own clock is always available, so a null there would be a bug.
        assert!(
            EVENT_SCHEMA
                .field_with_name(VENUE_TS)
                .unwrap()
                .is_nullable()
        );
        assert!(
            !EVENT_SCHEMA
                .field_with_name(INGEST_WALL)
                .unwrap()
                .is_nullable()
        );
    }

    #[test]
    fn prices_are_strings_not_floats_or_fixed_scale_decimals() {
        // The single most load-bearing decision in this schema. A float column
        // would undo the exact-decimal discipline the whole project is built
        // on, and a Decimal128 would fix one scale across venues that do not
        // share one — silently rewriting the digits Kraken's checksum covers.
        for name in [PRICE, QTY] {
            assert_eq!(
                EVENT_SCHEMA.field_with_name(name).unwrap().data_type(),
                &DataType::Utf8,
                "{name} is not an exact-digit string"
            );
        }
    }

    #[test]
    fn both_ingest_clocks_are_present_and_named_for_what_they_are() {
        // `IngestTime` refuses to serialise precisely so this decision has to
        // be made explicitly. Carrying only the wall clock would make replay
        // ordering wrong across an NTP step; carrying only the elapsed offset
        // would make the file unreadable against any external timeline.
        assert!(INGEST_WALL.contains("wall"));
        assert!(INGEST_ELAPSED.contains("elapsed"));
        assert!(EVENT_SCHEMA.field_with_name(INGEST_WALL).is_ok());
        assert!(EVENT_SCHEMA.field_with_name(INGEST_ELAPSED).is_ok());
    }
}
