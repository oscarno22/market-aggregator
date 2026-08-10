//! Reading normalised events back out of Parquet.
//!
//! # What this replay can and cannot prove
//!
//! `ma-pipeline`'s raw-frame tape and this are **two layers, not two copies**,
//! and CLAUDE.md §6 is explicit that neither replaces the other. The difference
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
//!
//! # Partitioning by symbol broke "key order is chronological", and this is
//! the repair
//!
//! Before v4 this reader listed every key, sorted, and read straight through.
//! That was correct for a reason it never stated: with `date=/hour=` as the
//! only partitions, lexicographic key order *is* time order, because the
//! layout is zero-padded and big-endian.
//!
//! Partitioning by symbol puts `symbol=` above `date=`, so the same walk now
//! yields all of BTC-USD, then all of ETH-USD. Nothing errors. The archive
//! simply replays as two recordings laid end to end — and because
//! [`crate::replay_archive`] paces on the gap between consecutive `elapsed`
//! values, the second symbol's whole history arrives in one burst with every
//! gap clamped to zero. A partitioning change presenting as a pacing bug, in
//! the layer that exists to make history trustworthy.
//!
//! So the reader now keeps one cursor per partition and merges them. Two
//! properties, kept apart deliberately:
//!
//! - **Within a partition, order is exact** — key order, then row order,
//!   unchanged from before. This is the order that matters, because a book
//!   only ever sees events for its own symbol. Nothing merges two symbols into
//!   one book.
//! - **Between partitions, order is by wall clock**, ties broken by partition
//!   so a given archive always replays identically. Wall is used rather than
//!   `event_seq` or `elapsed` because those two restart at zero in every
//!   writer run, and an archive is expected to contain several — one per
//!   process restart. Merging two runs by `event_seq` would interleave run
//!   B's tenth event with run A's tenth.
//!
//! The residue is a clock step inside one run reordering *independent*
//! symbols relative to each other. That is visible to nothing: it cannot
//! reorder a book's own input, which is the only ordering any consumer here
//! depends on.
//!
//! # An hour that several nodes wrote
//!
//! Under v3 sharding each node archives only the streams it owns, so an hour
//! of *everything* is a union of prefixes rather than a file.
//! [`EventReader::open_many`] merges them, and it needs no new machinery: keys
//! come back store-relative, so two nodes' files differ above `date=` and
//! [`partition_of`] already puts them on separate cursors. Node A's
//! `part-00000` and node B's `part-00000` are two sequences, and they stay two
//! sequences.
//!
//! The ordering question this raises looks like the one `docs/DESIGN.md` §7
//! and §14 refuse — comparing two machines' wall clocks — and the reason it is
//! answerable here is the sharding invariant itself. **At most one node runs a
//! given stream**, so every event for a given `(venue, symbol)` was written by
//! exactly one node. Cross-node ordering therefore only ever orders events
//! that belong to *different books*, and no book's own input is touched by it.
//! It is the same residue as the paragraph above, one machine wider: skew
//! between nodes can shuffle independent symbols against each other, and
//! nothing downstream can observe that.
//!
//! What does *not* survive the union is `elapsed`. It counts from each writer
//! run's first event, so two nodes' offsets share no origin and are not
//! comparable — which is why [`crate::replay_archive`] rebuilds its timeline
//! from the wall clock when it is given more than one prefix, rather than
//! pacing on differences between numbers that restart independently.

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

/// One partition's files, read in key order.
#[derive(Debug, Default)]
struct Cursor {
    keys: VecDeque<String>,
    pending: VecDeque<StoredEvent>,
}

/// Reads events back from a store, merging its partitions into one stream.
#[derive(Debug)]
pub struct EventReader {
    store: Arc<dyn ObjectStore>,
    /// In partition-name order, which is what makes the tie-break between two
    /// equally-timestamped events stable across runs.
    cursors: Vec<Cursor>,
}

impl EventReader {
    /// Open every file under `prefix`, merged across partitions.
    ///
    /// Within one partition, key order is chronological by construction:
    /// `date=YYYY-MM-DD/hour=HH` sorts correctly as text precisely because it
    /// is zero-padded and big-endian. Across partitions it is not — see the
    /// module docs.
    ///
    /// # Errors
    /// If the store cannot be listed.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: &str) -> Result<Self, ReadError> {
        Self::open_many(store, std::slice::from_ref(&prefix)).await
    }

    /// Open every file under each of `prefixes`, merged into one stream.
    ///
    /// This is how an hour written by a sharded cluster is read back: give it
    /// one prefix per node. See the module docs for why merging across nodes
    /// is answerable when §7 refuses to compare two machines' clocks — the
    /// short version is that at most one node ever runs a given stream, so the
    /// merge only ever orders events belonging to different books.
    ///
    /// Overlapping prefixes are safe. Keys are deduplicated, so passing both a
    /// root and something beneath it reads each file once rather than
    /// replaying the overlap twice — worth having, because "the bucket root
    /// and the events prefix" is exactly the pair an operator reaches for.
    ///
    /// # Errors
    /// If the store cannot be listed.
    pub async fn open_many(
        store: Arc<dyn ObjectStore>,
        prefixes: &[&str],
    ) -> Result<Self, ReadError> {
        // BTreeSet rather than VecDeque: it deduplicates overlapping prefixes
        // and keeps key order, which within a partition is chronological by
        // construction. `list` already sorts, but two lists concatenated are
        // not sorted, and this is the merge that has to be.
        let mut by_partition: std::collections::BTreeMap<
            String,
            std::collections::BTreeSet<String>,
        > = std::collections::BTreeMap::new();
        for prefix in prefixes {
            for key in store.list(prefix).await? {
                if !key.ends_with(".parquet") {
                    continue;
                }
                by_partition
                    .entry(partition_of(&key).to_owned())
                    .or_default()
                    .insert(key);
            }
        }
        Ok(Self {
            store,
            cursors: by_partition
                .into_values()
                .map(|keys| Cursor {
                    keys: keys.into_iter().collect(),
                    pending: VecDeque::new(),
                })
                .collect(),
        })
    }

    /// The next event, or `None` when every partition is exhausted.
    ///
    /// # Errors
    /// If a file cannot be read or a row is malformed.
    pub async fn next_event(&mut self) -> Result<Option<StoredEvent>, ReadError> {
        // Make sure every partition that still has data has its head decoded,
        // so the comparison below sees all of them. A partition whose current
        // file is exhausted loads its next one here.
        for i in 0..self.cursors.len() {
            while self.cursors[i].pending.is_empty() {
                let Some(key) = self.cursors[i].keys.pop_front() else {
                    break;
                };
                let bytes = self.store.get(&key).await?;
                self.cursors[i].pending = decode(bytes)?.into();
            }
        }

        // Earliest wall clock wins; the first cursor wins a tie. Linear rather
        // than a heap on purpose: the number of partitions is the number of
        // symbols, and a binary heap over three elements is a slower way to
        // scan three elements.
        let pick = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.pending.front().map(|e| (i, e.event.ingest_ts.wall())))
            .min_by_key(|(i, wall)| (*wall, *i))
            .map(|(i, _)| i);

        Ok(pick.and_then(|i| self.cursors[i].pending.pop_front()))
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

/// Which partition a key belongs to: everything before its `date=` component.
///
/// Written as a prefix rule rather than as "parse out `symbol=`" so that an
/// archive written before v4 — no `symbol=` in the path at all — is one
/// partition rather than an error or a misparse. That matters: files already
/// sitting in S3 under the old layout must keep replaying, the same reason the
/// raw-frame tape's `symbol` field is optional. It also means adding a further
/// partition column later needs no change here.
fn partition_of(key: &str) -> &str {
    match key.rsplit_once("/date=") {
        Some((head, _)) => head,
        // No date component at all: fall back to the containing directory, so
        // an unrecognised layout still groups rather than interleaving files
        // that may have nothing to do with each other.
        None => key.rsplit_once('/').map_or("", |(head, _)| head),
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
