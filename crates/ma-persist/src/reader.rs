//! Reading normalised events back out of Parquet.
//!
//! # What this replay can and cannot prove
//!
//! `ma-pipeline`'s raw-frame tape and this are **two layers, not two copies**,
//! and CLAUDE.md §4 is explicit that neither replaces the other. The difference
//! is what evidence survives:
//!
//! - A **tape** stores bytes, before parsing. It can therefore reproduce a
//!   parser bug or a venue schema change — the two failures most likely to
//!   happen unattended, and the exact three bugs the first live recording
//!   found. It is the debugging artefact.
//! - **Parquet** stores what we concluded those bytes meant. It cannot
//!   reproduce a parser bug, because parsing already happened, once. What it
//!   can do is store *hours* rather than minutes, be queried, and outlive the
//!   process — it is the durability artefact.
//!
//! The one thing worth stating loudly is what Parquet replay does **not**
//! degrade: [`EventKind::Checksum`] is part of the normalised stream, so a book
//! rebuilt from Parquet is still verified against what Kraken said it should
//! be. The replayed book is checked against the venue's own opinion, not merely
//! against itself.
//!
//! # Reassembling events from rows
//!
//! The writer flattens one event into one row per level, so reading inverts
//! that: consecutive rows sharing an `event_seq` are one event. "Consecutive"
//! is safe because the writer appends in order and never interleaves — the
//! single-owner discipline that applies to the books applies to the writer too.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use ma_core::{EventKind, IngestTime, Level, MarketEvent, Side, StreamId, Symbol, VenueId};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::schema::kind;
use crate::store::{ObjectStore, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("object store: {0}")]
    Store(#[from] StoreError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("column {column:?} was missing or had an unexpected type")]
    Column { column: &'static str },
    #[error("row {row} carried an unknown {field} {value:?}")]
    Unknown {
        row: usize,
        field: &'static str,
        value: String,
    },
    #[error("row {row} of a {kind} event was missing {field}")]
    Incomplete {
        row: usize,
        kind: String,
        field: &'static str,
    },
}

/// One reconstructed event, plus the offset that orders it.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub stream: StreamId,
    pub event: MarketEvent,
    /// Monotonic offset from the writer's first event. **This**, not the wall
    /// clock on the event, is what replay paces and orders by — see
    /// [`crate::schema`]'s note on the three clock columns.
    pub elapsed: Duration,
}

/// Reads events back from a store, file by file, in key order.
#[derive(Debug)]
pub struct EventReader {
    store: Arc<dyn ObjectStore>,
    keys: VecDeque<String>,
    pending: VecDeque<StoredEvent>,
}

impl EventReader {
    /// Open every file under `prefix`, in lexicographic key order.
    ///
    /// Key order is chronological by construction: `date=YYYY-MM-DD/hour=HH`
    /// sorts correctly as text precisely because it is zero-padded and
    /// big-endian. That is why the layout looks the way it does.
    ///
    /// # Errors
    /// If the store cannot be listed.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: &str) -> Result<Self, ReadError> {
        let keys = store
            .list(prefix)
            .await?
            .into_iter()
            .filter(|k| k.ends_with(".parquet"))
            .collect();
        Ok(Self {
            store,
            keys,
            pending: VecDeque::new(),
        })
    }

    /// The next event, or `None` when every file is exhausted.
    ///
    /// # Errors
    /// If a file cannot be read or a row is malformed.
    pub async fn next_event(&mut self) -> Result<Option<StoredEvent>, ReadError> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            let Some(key) = self.keys.pop_front() else {
                return Ok(None);
            };
            let bytes = self.store.get(&key).await?;
            self.pending = decode(bytes)?.into();
        }
    }

    /// Read everything. Convenient for tests and for a whole-session replay,
    /// where the alternative is a loop that every caller writes identically.
    ///
    /// # Errors
    /// As [`Self::next_event`].
    pub async fn collect(mut self) -> Result<Vec<StoredEvent>, ReadError> {
        let mut out = Vec::new();
        while let Some(event) = self.next_event().await? {
            out.push(event);
        }
        Ok(out)
    }
}

/// Decode one Parquet file into events.
pub fn decode(bytes: Vec<u8>) -> Result<Vec<StoredEvent>, ReadError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?.build()?;

    let mut out: Vec<StoredEvent> = Vec::new();
    // Carried across batches: a row group boundary can fall in the middle of a
    // large snapshot, and an event split across two batches must not become
    // two events.
    let mut open: Option<(i64, StoredEvent, Vec<Level>, Vec<Level>)> = None;

    for batch in reader {
        let batch = batch?;
        decode_batch(&batch, &mut open, &mut out)?;
    }
    if let Some((_, mut event, bids, asks)) = open.take() {
        finish(&mut event, bids, asks);
        out.push(event);
    }
    Ok(out)
}

fn finish(event: &mut StoredEvent, bids: Vec<Level>, asks: Vec<Level>) {
    match &mut event.event.kind {
        EventKind::Snapshot { bids: b, asks: a } | EventKind::Delta { bids: b, asks: a } => {
            *b = bids;
            *a = asks;
        }
        _ => {}
    }
}

fn col<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a T, ReadError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or(ReadError::Column { column: name })
}

#[allow(clippy::too_many_lines)]
fn decode_batch(
    batch: &RecordBatch,
    open: &mut Option<(i64, StoredEvent, Vec<Level>, Vec<Level>)>,
    out: &mut Vec<StoredEvent>,
) -> Result<(), ReadError> {
    use crate::schema as s;

    let event_seq = col::<Int64Array>(batch, s::EVENT_SEQ)?;
    let venue = col::<StringArray>(batch, s::VENUE)?;
    let symbol = col::<StringArray>(batch, s::SYMBOL)?;
    let kind_col = col::<StringArray>(batch, s::KIND)?;
    let ingest_wall = col::<Int64Array>(batch, s::INGEST_WALL)?;
    let ingest_elapsed = col::<Int64Array>(batch, s::INGEST_ELAPSED)?;
    let venue_ts = col::<Int64Array>(batch, s::VENUE_TS)?;
    let side = col::<StringArray>(batch, s::SIDE)?;
    let price = col::<StringArray>(batch, s::PRICE)?;
    let qty = col::<StringArray>(batch, s::QTY)?;
    let checksum = col::<UInt32Array>(batch, s::CHECKSUM)?;
    let heartbeat = col::<Int64Array>(batch, s::HEARTBEAT_COUNTER)?;
    let taker = col::<StringArray>(batch, s::TAKER_SIDE)?;
    let _ = col::<Int32Array>(batch, s::LEVEL_INDEX)?;

    for row in 0..batch.num_rows() {
        let seq = event_seq.value(row);
        let kind_name = kind_col.value(row);

        // A new event_seq closes the previous event.
        if let Some((open_seq, _, _, _)) = open
            && *open_seq != seq
        {
            let Some((_, mut event, bids, asks)) = open.take() else {
                unreachable!("just matched Some")
            };
            finish(&mut event, bids, asks);
            out.push(event);
        }

        if open.is_none() {
            let venue = parse_venue(venue.value(row), row)?;
            let event = StoredEvent {
                stream: StreamId::new(venue, Symbol::new(symbol.value(row))),
                event: MarketEvent {
                    venue,
                    symbol: Symbol::new(symbol.value(row)),
                    venue_ts: venue_ts
                        .is_valid(row)
                        .then(|| from_unix_nanos(venue_ts.value(row))),
                    // Reconstructed against *this* process's clock by the
                    // caller; the wall reading is carried so a log line can
                    // name the original instant. See `StoredEvent::elapsed`.
                    ingest_ts: IngestTime::new(
                        std::time::Instant::now(),
                        from_unix_nanos(ingest_wall.value(row)),
                    ),
                    kind: empty_kind(kind_name, row, checksum, heartbeat)?,
                },
                elapsed: Duration::from_nanos(ingest_elapsed.value(row).unsigned_abs()),
            };
            *open = Some((seq, event, Vec::new(), Vec::new()));
        }

        // Trades carry their price on the row rather than in a level list.
        if kind_name == kind::TRADE {
            let Some((_, event, _, _)) = open.as_mut() else {
                unreachable!("just set")
            };
            let missing = |field| ReadError::Incomplete {
                row,
                kind: kind::TRADE.to_owned(),
                field,
            };
            if !price.is_valid(row) {
                return Err(missing("price"));
            }
            if !qty.is_valid(row) {
                return Err(missing("qty"));
            }
            event.event.kind = EventKind::Trade {
                price: price
                    .value(row)
                    .parse()
                    .map_err(|_| unknown(row, "price", price.value(row)))?,
                qty: qty
                    .value(row)
                    .parse()
                    .map_err(|_| unknown(row, "qty", qty.value(row)))?,
                taker_side: taker
                    .is_valid(row)
                    .then(|| parse_side(taker.value(row), row))
                    .transpose()?,
            };
            continue;
        }

        // A level row. A null side means an event that genuinely has none.
        if !side.is_valid(row) {
            continue;
        }
        let level = Level::new(
            price
                .value(row)
                .parse()
                .map_err(|_| unknown(row, "price", price.value(row)))?,
            qty.value(row)
                .parse()
                .map_err(|_| unknown(row, "qty", qty.value(row)))?,
        );
        let Some((_, _, bids, asks)) = open.as_mut() else {
            unreachable!("just set")
        };
        match parse_side(side.value(row), row)? {
            Side::Bid => bids.push(level),
            Side::Ask => asks.push(level),
        }
    }
    Ok(())
}

/// The event's kind with empty level lists, filled in as rows are read.
fn empty_kind(
    name: &str,
    row: usize,
    checksum: &UInt32Array,
    heartbeat: &Int64Array,
) -> Result<EventKind, ReadError> {
    Ok(match name {
        kind::SNAPSHOT => EventKind::Snapshot {
            bids: Vec::new(),
            asks: Vec::new(),
        },
        kind::DELTA => EventKind::Delta {
            bids: Vec::new(),
            asks: Vec::new(),
        },
        kind::CHECKSUM => {
            if !checksum.is_valid(row) {
                return Err(ReadError::Incomplete {
                    row,
                    kind: kind::CHECKSUM.to_owned(),
                    field: "checksum",
                });
            }
            EventKind::Checksum {
                value: checksum.value(row),
            }
        }
        kind::HEARTBEAT => EventKind::Heartbeat {
            counter: heartbeat
                .is_valid(row)
                .then(|| heartbeat.value(row).unsigned_abs()),
        },
        // A placeholder the caller immediately overwrites: the price columns
        // it needs live on the row, not here. Kept rather than restructuring
        // because every other kind is fully determined at this point, and
        // splitting the match to accommodate one would obscure that.
        kind::TRADE => EventKind::Trade {
            price: "0".parse().map_err(|_| unknown(row, "price", "0"))?,
            qty: "0".parse().map_err(|_| unknown(row, "qty", "0"))?,
            taker_side: None,
        },
        other => {
            return Err(ReadError::Unknown {
                row,
                field: "kind",
                value: other.to_owned(),
            });
        }
    })
}

fn unknown(row: usize, field: &'static str, value: &str) -> ReadError {
    ReadError::Unknown {
        row,
        field,
        value: value.to_owned(),
    }
}

fn parse_venue(raw: &str, row: usize) -> Result<VenueId, ReadError> {
    Ok(match raw {
        "coinbase" => VenueId::Coinbase,
        "kraken" => VenueId::Kraken,
        "bitstamp" => VenueId::Bitstamp,
        "fake" => VenueId::Fake,
        other => {
            return Err(ReadError::Unknown {
                row,
                field: "venue",
                value: other.to_owned(),
            });
        }
    })
}

fn parse_side(raw: &str, row: usize) -> Result<Side, ReadError> {
    Ok(match raw {
        "bid" => Side::Bid,
        "ask" => Side::Ask,
        other => {
            return Err(ReadError::Unknown {
                row,
                field: "side",
                value: other.to_owned(),
            });
        }
    })
}

fn from_unix_nanos(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos.unsigned_abs())
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_nanos(nanos.unsigned_abs())
    }
}
