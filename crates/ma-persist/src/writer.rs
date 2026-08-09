//! Turning normalised events into Parquet files, rolled hourly.
//!
//! # The roll boundary is a wall-clock concept, and that is not a contradiction
//!
//! Everything else in this project orders by the monotonic clock and treats
//! wall time as output only. Rolling files hourly is the one place wall time
//! genuinely decides something, because "hour" is a human unit: the whole point
//! of `date=2026-08-09/hour=03/` is that somebody can find 3am in it.
//!
//! The consequence is stated rather than hidden. An NTP step across an hour
//! boundary can produce two files for one hour, or a file covering ninety
//! minutes. Neither loses or reorders an event, because the *rows* still carry
//! [`INGEST_ELAPSED`](crate::schema::INGEST_ELAPSED) and replay reads that. The
//! roll decides which file a row lands in; it never decides what a row means.
//!
//! # Why the writer batches
//!
//! Parquet is columnar: its compression and its ability to skip data both come
//! from having many rows of a column together. Writing one row group per event
//! would produce a file that is larger than the JSON it came from and slower to
//! query. So rows accumulate in memory and flush as a row group at
//! [`WriterConfig::row_group_rows`].
//!
//! That buffer is also the answer to the unbounded event channel the
//! aggregator tees into. `ma_pipeline::Aggregator::publishing_events_to`
//! deliberately uses the claims-processing policy — durable history with a hole
//! in it silently invalidates everything built on it — which puts the
//! obligation to keep up here rather than on the sender. Batching is how that
//! obligation is met: the cost per event is an append to a `Vec`, and the
//! expensive part happens once per few thousand.
//!
//! # Symbol is a partition, and that costs one open file per symbol
//!
//! Through v3 symbol was a *column*. That was the right call at one symbol and
//! the wrong one the moment a query wants an hour of ETH-USD and has to read
//! BTC-USD's rows to find out they are not ETH-USD's. Now the key carries
//! `symbol=`, so a reader filtering on a symbol skips the rest by path alone,
//! before opening a file.
//!
//! The price is real and paid here: one open [`ArrowWriter`] per symbol, each
//! with its own row-group buffer and its own `max_open` clock, so a run over
//! *n* symbols holds *n* buffers and produces *n* times the files. At the
//! handful of symbols this project runs that is a few megabytes and a few
//! files; it is also why the partition is symbol and not `(venue, symbol)`,
//! which would multiply it again to separate the venues a book is compared
//! across.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{
    ArrayRef, Int32Builder, Int64Builder, RecordBatch, StringBuilder, UInt32Builder,
};
use ma_core::{EventKind, IngestTime, Level, MarketEvent, Side};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tracing::{debug, info, warn};

use crate::counters::ArchiveCounters;
use crate::schema::{EVENT_SCHEMA, kind};
use crate::store::{ObjectStore, StoreError};

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("object store: {0}")]
    Store(#[from] StoreError),
}

/// Knobs, with defaults chosen for a single node reading three venues.
#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    /// Rows buffered before a row group is flushed.
    pub row_group_rows: usize,
    /// How long a file covers. An hour by default, per CLAUDE.md's v2 list.
    ///
    /// Configurable mainly so tests can roll in milliseconds instead of
    /// waiting out an hour, which is the difference between the roll logic
    /// being tested and being hoped about.
    pub roll_every: Duration,
    /// Longest a file stays open before being closed and stored anyway.
    ///
    /// # Why hourly partitioning is not the same as hourly durability
    ///
    /// A Parquet file is only readable once its footer is written, so an open
    /// file is worth nothing to anyone but this process. With hourly rolls
    /// alone, a process killed at minute 59 loses fifty-nine minutes — and
    /// "killed" is the normal case, not the exceptional one: a container gets
    /// `SIGTERM` on every deploy.
    ///
    /// So the hour decides the *partition* and this decides how much is ever at
    /// risk. Files land as `part-00000`, `part-00001`, … inside the same
    /// `hour=HH/` directory, which every query engine reads as one hour
    /// regardless of how many parts it took.
    ///
    /// Five minutes trades a bounded loss window against small-file count: at
    /// three venues that is a handful of megabytes per part, comfortably above
    /// the size where Parquet's own overhead starts to matter.
    pub max_open: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            row_group_rows: 8192,
            roll_every: Duration::from_secs(3600),
            max_open: Duration::from_secs(300),
        }
    }
}

/// Accumulates events and writes hourly Parquet files into a store.
#[derive(Debug)]
pub struct EventWriter {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    config: WriterConfig,
    /// Anchors [`INGEST_ELAPSED`](crate::schema::INGEST_ELAPSED). Set from the
    /// first event rather than from construction, so a writer started before
    /// the feed does not stamp every file with a leading gap.
    ///
    /// One base for the whole writer, not one per symbol. `elapsed` is what
    /// replay paces by, so a per-symbol base would make each symbol restart at
    /// zero and a two-symbol archive replay as two overlaid recordings.
    base: Option<IngestTime>,
    /// Counts events across *every* partition, so it stays a total order over
    /// the writer's run. A per-symbol counter would be denser but would say
    /// nothing about how two symbols interleaved.
    event_seq: i64,
    /// One open file per symbol. See the module docs for what that costs.
    open: BTreeMap<String, OpenFile>,
    /// Next part number per *directory* — one `symbol=…/date=…/hour=…/` — not
    /// per symbol for the writer's lifetime. Keyed this way for two reasons.
    /// Within a run, numbering restarts at each hour, so a directory's part
    /// numbers say how many parts that hour holds. Across runs, the first
    /// touch of a directory lists what is already in it and resumes after the
    /// highest existing part — which is the fix for a real bug: a writer keyed
    /// by symbol alone started every process at `part-00000`, so a restart
    /// inside the same hour silently overwrote the previous run's file. The
    /// archive lost data on every same-hour redeploy and nothing reported it.
    parts: BTreeMap<String, u64>,
    /// The one set of tallies for this writer. Shared with `/metrics` when the
    /// caller passes its own `Arc` via [`EventWriter::with_counters`]; a fresh
    /// private one otherwise, so every code path counts identically whether
    /// or not anything is watching.
    counters: Arc<ArchiveCounters>,
}

/// The file currently being appended to.
struct OpenFile {
    /// Which roll window this file belongs to, as whole `roll_every` units
    /// since the Unix epoch. Comparing window indices rather than doing date
    /// arithmetic makes "did we cross a boundary?" a single integer compare,
    /// and makes a sub-hour `roll_every` work identically for tests.
    window: i64,
    /// When this file was opened, on the ingest clock. Monotonic, so a
    /// mid-session NTP step cannot make a file look hours old and roll it, nor
    /// make an old one look fresh and keep it open.
    opened_at: IngestTime,
    key: String,
    writer: ArrowWriter<Vec<u8>>,
    rows: Vec<Row>,
    /// Rows handed to this file, counted as they arrive.
    ///
    /// Not read back from `ArrowWriter::flushed_row_groups`: that reports what
    /// has reached the sink, and a batch written but not yet flushed is absent
    /// from it — which made the total read zero for any file smaller than one
    /// row group. Counting on append is both simpler and correct at every
    /// instant.
    rows_total: u64,
}

impl std::fmt::Debug for OpenFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenFile")
            .field("window", &self.window)
            .field("key", &self.key)
            .field("buffered_rows", &self.rows.len())
            .finish_non_exhaustive()
    }
}

/// One flattened row. See `schema`'s note on why a level is a row.
#[derive(Clone, Debug)]
struct Row {
    event_seq: i64,
    venue: String,
    symbol: String,
    kind: &'static str,
    ingest_wall: i64,
    ingest_elapsed: i64,
    venue_ts: Option<i64>,
    side: Option<&'static str>,
    price: Option<String>,
    qty: Option<String>,
    level_index: Option<i32>,
    checksum: Option<u32>,
    heartbeat_counter: Option<i64>,
    taker_side: Option<&'static str>,
}

impl EventWriter {
    /// `prefix` is the key namespace, e.g. `events`. Files land beneath it as
    /// `events/date=YYYY-MM-DD/hour=HH/part-NNNNN.parquet`.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: into_prefix(prefix.into()),
            config: WriterConfig::default(),
            base: None,
            event_seq: 0,
            open: BTreeMap::new(),
            parts: BTreeMap::new(),
            counters: Arc::new(ArchiveCounters::default()),
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: WriterConfig) -> Self {
        self.config = config;
        self
    }

    /// Share the writer's tallies with something that reads them — in
    /// practice, `/metrics`. The writer keeps no private copy: this `Arc` *is*
    /// the count, so the number scraped and the number logged at shutdown
    /// cannot disagree.
    #[must_use]
    pub fn with_counters(mut self, counters: Arc<ArchiveCounters>) -> Self {
        self.counters = counters;
        self
    }

    /// Files finished and uploaded so far.
    pub fn files_written(&self) -> u64 {
        self.counters.files_written()
    }

    /// Rows in files finished and uploaded so far.
    pub fn rows_written(&self) -> u64 {
        self.counters.rows_written()
    }

    /// Append one event, rolling its symbol's file first if it belongs to a
    /// new window.
    ///
    /// Rolling is per symbol, not global: an event for BTC-USD crossing an
    /// hour boundary must not close ETH-USD's file, whose own hour has not
    /// ended and whose `max_open` clock has its own deadline. A global roll
    /// would make every partition's part boundaries a function of whichever
    /// symbol happened to tick first.
    ///
    /// # Errors
    /// If the current file cannot be finished or uploaded.
    pub async fn append(&mut self, event: &MarketEvent) -> Result<(), WriteError> {
        let base = *self.base.get_or_insert(event.ingest_ts);
        let window = self.window_of(event.ingest_ts.wall());
        let partition = partition_name(event.symbol.as_str());

        // Two independent reasons to close a file: it belongs to a different
        // hour, or it has simply been open long enough that the unwritten
        // footer represents more risk than it is worth. See
        // `WriterConfig::max_open`.
        let rotate = self.open.get(&partition).is_some_and(|open| {
            open.window != window || event.ingest_ts.since(open.opened_at) >= self.config.max_open
        });
        if rotate {
            self.roll(&partition).await?;
        }
        if !self.open.contains_key(&partition) {
            let file = self
                .start_file(&partition, window, event.ingest_ts.wall(), event.ingest_ts)
                .await?;
            self.open.insert(partition.clone(), file);
            self.counters.set_open_files(self.open.len() as u64);
        }

        self.event_seq += 1;
        let rows = flatten(event, self.event_seq, base);

        // Just ensured above.
        let Some(open) = self.open.get_mut(&partition) else {
            return Ok(());
        };
        open.rows_total += rows.len() as u64;
        open.rows.extend(rows);

        if open.rows.len() >= self.config.row_group_rows {
            Self::flush_row_group(open)?;
        }
        Ok(())
    }

    /// Finish and upload every open file. Call before shutdown, or the last
    /// partial hour is lost — for every symbol, not just the one that happened
    /// to receive the final event.
    ///
    /// # Errors
    /// If a file cannot be finished or uploaded. Every remaining partition is
    /// still attempted: one symbol's failed upload must not discard the
    /// others' footers, which is the whole of what `close` is for.
    pub async fn close(&mut self) -> Result<(), WriteError> {
        let partitions: Vec<String> = self.open.keys().cloned().collect();
        let mut first_error = None;
        for partition in partitions {
            if let Err(e) = self.roll(&partition).await {
                warn!(%partition, error = %e, "could not close a partition's file");
                self.counters.record_failure();
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Which roll window a wall-clock instant falls in.
    fn window_of(&self, at: SystemTime) -> i64 {
        let secs = at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        let per = self.config.roll_every.as_secs().max(1);
        i64::try_from(secs / per).unwrap_or(i64::MAX)
    }

    async fn start_file(
        &mut self,
        partition: &str,
        window: i64,
        at: SystemTime,
        opened_at: IngestTime,
    ) -> Result<OpenFile, WriteError> {
        let props = WriterProperties::builder()
            // zstd over snappy: these files are written once, read rarely, and
            // kept. Compression ratio is worth more than decode speed, and the
            // repeated venue/symbol/event_seq columns compress extremely well.
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let writer = ArrowWriter::try_new(Vec::new(), EVENT_SCHEMA.clone(), Some(props))?;

        let dir = self.dir_for(partition, at);
        let part = match self.parts.get(&dir) {
            Some(next) => *next,
            // First touch of this directory in this process: ask the store
            // what is already there and resume after it. This is what makes a
            // restart inside the same hour add `part-00001` instead of
            // overwriting `part-00000`.
            //
            // A failed listing fails the write, deliberately. The fallback
            // that avoids an overwrite would be a name outside the readable
            // `part-NNNNN` scheme, and the caller already has the right shape
            // for a failed write: warn, count it, drop the event — the next
            // append retries the listing. A transient outage costs a bounded,
            // counted gap, exactly like a failed upload.
            None => next_part(&self.store.list(&dir).await?),
        };
        self.parts.insert(dir.clone(), part + 1);
        let key = format!("{dir}part-{part:05}.parquet");
        debug!(%key, window, "opening a new parquet file");
        Ok(OpenFile {
            window,
            opened_at,
            key,
            writer,
            rows: Vec::with_capacity(self.config.row_group_rows),
            rows_total: 0,
        })
    }

    /// Hive-style partitioning, which every query engine understands without
    /// being told: a reader filtering on one symbol or one hour can skip the
    /// rest by path alone, before opening a single file.
    ///
    /// Symbol comes **before** date, and that ordering is the whole point.
    /// `symbol=X/date=D/hour=H` lets a query for one symbol prune to a single
    /// subtree; `date=D/hour=H/symbol=X` would make it walk every hour in the
    /// range and prune inside each. The symbol set is small and near-static
    /// while the date set grows forever, which is the usual rule for ordering
    /// partition columns: coarsest and most selective first.
    ///
    /// Symbol stays a column as well as a partition. The path is a physical
    /// layout that a reader may or may not understand — Hive-aware engines
    /// recover it, `ParquetRecordBatchReader` reading a single file does not —
    /// so dropping the column would make a file's own contents unidentifiable
    /// when read outside its directory.
    fn dir_for(&self, partition: &str, at: SystemTime) -> String {
        let (date, hour) = date_hour(at);
        format!(
            "{}/symbol={partition}/date={date}/hour={hour:02}/",
            self.prefix
        )
    }

    fn flush_row_group(open: &mut OpenFile) -> Result<(), WriteError> {
        if open.rows.is_empty() {
            return Ok(());
        }
        let batch = to_batch(&open.rows)?;
        open.writer.write(&batch)?;
        open.rows.clear();
        Ok(())
    }

    /// Finish one partition's open file and hand it to the store.
    async fn roll(&mut self, partition: &str) -> Result<(), WriteError> {
        let Some(mut open) = self.open.remove(partition) else {
            return Ok(());
        };
        // Gauge updated at the moment of removal, not on success: a file that
        // failed to upload is just as closed, and an open-files gauge that
        // drifts on failure would overstate what is still at risk.
        self.counters.set_open_files(self.open.len() as u64);
        Self::flush_row_group(&mut open)?;

        let rows = open.rows_total;
        let bytes = open.writer.into_inner()?;
        let size = bytes.len();

        self.store.put(&open.key, bytes).await?;
        self.counters
            .record_file(rows, u64::try_from(size).unwrap_or(u64::MAX));

        info!(
            key = %open.key,
            rows,
            bytes = size,
            "parquet file closed and stored"
        );
        Ok(())
    }
}

fn into_prefix(raw: String) -> String {
    raw.trim_matches('/').to_owned()
}

/// The part number to write next, given what a directory already holds.
///
/// Tolerant on purpose: only keys shaped `…/part-NNNNN.parquet` count, and
/// anything else in the directory — a stray file, a future layout — is
/// ignored rather than an error. The one job here is to never re-issue a
/// number that exists, so the answer is max + 1 over what parses, and 0 for
/// an empty or unrecognisable listing.
fn next_part(existing: &[String]) -> u64 {
    existing
        .iter()
        .filter_map(|key| {
            key.rsplit('/')
                .next()?
                .strip_prefix("part-")?
                .strip_suffix(".parquet")?
                .parse::<u64>()
                .ok()
        })
        .max()
        .map_or(0, |max| max.saturating_add(1))
}

/// A symbol as a path component.
///
/// Sanitised rather than trusted, for the same reason `LocalStore::resolve` and
/// `DirRegistry::path_for` are: `Symbol::new` accepts any string, this string
/// becomes part of a key, and a key is a path on a `LocalStore`. `../../` in a
/// symbol must be a strange directory name rather than an escape. An empty
/// symbol becomes `unknown` so it stays one identifiable partition instead of
/// colliding with the prefix itself.
fn partition_name(symbol: &str) -> String {
    let safe: String = symbol
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if safe.trim_matches('.').is_empty() {
        "unknown".to_owned()
    } else {
        safe
    }
}

/// `(YYYY-MM-DD, hour)` in UTC.
///
/// UTC rather than local time, and not negotiable: a file named by local time
/// is ambiguous twice a year and unreadable by anyone in another timezone. The
/// same reason every timestamp column is epoch-based.
fn date_hour(at: SystemTime) -> (String, u32) {
    let secs = at
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let hour = u32::try_from((secs % 86_400) / 3600).unwrap_or(0);
    let (y, m, d) = civil_from_days(days);
    (format!("{y:04}-{m:02}-{d:02}"), hour)
}

/// Inverse of `ma_venues`' `days_from_civil` — Howard Hinnant's
/// `civil_from_days`, copied for the same reason: it is the formulation that
/// gets the leap-year special case right by construction.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (
        y + i64::from(m <= 2),
        u32::try_from(m).unwrap_or(1),
        u32::try_from(d).unwrap_or(1),
    )
}

fn unix_nanos(at: SystemTime) -> i64 {
    match at.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
}

/// One event to one or more rows.
///
/// Every event yields at least one row even when it carries no levels, so a
/// heartbeat cannot vanish from the record — see the schema module docs.
fn flatten(event: &MarketEvent, event_seq: i64, base: IngestTime) -> Vec<Row> {
    let template = Row {
        event_seq,
        venue: event.venue.to_string(),
        symbol: event.symbol.to_string(),
        kind: kind::HEARTBEAT,
        ingest_wall: unix_nanos(event.ingest_ts.wall()),
        ingest_elapsed: i64::try_from(event.ingest_ts.since(base).as_nanos()).unwrap_or(i64::MAX),
        venue_ts: event.venue_ts.map(unix_nanos),
        side: None,
        price: None,
        qty: None,
        level_index: None,
        checksum: None,
        heartbeat_counter: None,
        taker_side: None,
    };

    let level_rows = |kind_name: &'static str, bids: &[Level], asks: &[Level]| -> Vec<Row> {
        let mut rows = Vec::with_capacity(bids.len() + asks.len());
        for (side, levels) in [(Side::Bid, bids), (Side::Ask, asks)] {
            for (i, level) in levels.iter().enumerate() {
                rows.push(Row {
                    kind: kind_name,
                    side: Some(side_str(side)),
                    price: Some(level.price.to_string()),
                    qty: Some(level.qty.to_string()),
                    level_index: Some(i32::try_from(i).unwrap_or(i32::MAX)),
                    ..template.clone()
                });
            }
        }
        if rows.is_empty() {
            // An empty delta is not nothing: the venue sent a message and we
            // applied it. Dropping the row would make the event vanish and
            // leave `event_seq` with a hole nobody could explain.
            rows.push(Row {
                kind: kind_name,
                ..template.clone()
            });
        }
        rows
    };

    match &event.kind {
        EventKind::Snapshot { bids, asks } => level_rows(kind::SNAPSHOT, bids, asks),
        EventKind::Delta { bids, asks } => level_rows(kind::DELTA, bids, asks),
        EventKind::Trade {
            price,
            qty,
            taker_side,
        } => vec![Row {
            kind: kind::TRADE,
            price: Some(price.to_string()),
            qty: Some(qty.to_string()),
            taker_side: taker_side.map(side_str),
            ..template
        }],
        EventKind::Checksum { value } => vec![Row {
            kind: kind::CHECKSUM,
            checksum: Some(*value),
            ..template
        }],
        EventKind::Heartbeat { counter } => vec![Row {
            kind: kind::HEARTBEAT,
            heartbeat_counter: counter.map(|c| i64::try_from(c).unwrap_or(i64::MAX)),
            ..template
        }],
    }
}

fn to_batch(rows: &[Row]) -> Result<RecordBatch, WriteError> {
    let n = rows.len();
    let mut event_seq = Int64Builder::with_capacity(n);
    let mut venue = StringBuilder::new();
    let mut symbol = StringBuilder::new();
    let mut kind_b = StringBuilder::new();
    let mut ingest_wall = Int64Builder::with_capacity(n);
    let mut ingest_elapsed = Int64Builder::with_capacity(n);
    let mut venue_ts = Int64Builder::with_capacity(n);
    let mut side = StringBuilder::new();
    let mut price = StringBuilder::new();
    let mut qty = StringBuilder::new();
    let mut level_index = Int32Builder::with_capacity(n);
    let mut checksum = UInt32Builder::with_capacity(n);
    let mut heartbeat = Int64Builder::with_capacity(n);
    let mut taker = StringBuilder::new();

    for row in rows {
        event_seq.append_value(row.event_seq);
        venue.append_value(&row.venue);
        symbol.append_value(&row.symbol);
        kind_b.append_value(row.kind);
        ingest_wall.append_value(row.ingest_wall);
        ingest_elapsed.append_value(row.ingest_elapsed);
        venue_ts.append_option(row.venue_ts);
        side.append_option(row.side);
        price.append_option(row.price.as_deref());
        qty.append_option(row.qty.as_deref());
        level_index.append_option(row.level_index);
        checksum.append_option(row.checksum);
        heartbeat.append_option(row.heartbeat_counter);
        taker.append_option(row.taker_side);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(event_seq.finish()),
        Arc::new(venue.finish()),
        Arc::new(symbol.finish()),
        Arc::new(kind_b.finish()),
        Arc::new(ingest_wall.finish()),
        Arc::new(ingest_elapsed.finish()),
        Arc::new(venue_ts.finish()),
        Arc::new(side.finish()),
        Arc::new(price.finish()),
        Arc::new(qty.finish()),
        Arc::new(level_index.finish()),
        Arc::new(checksum.finish()),
        Arc::new(heartbeat.finish()),
        Arc::new(taker.finish()),
    ];
    Ok(RecordBatch::try_new(EVENT_SCHEMA.clone(), columns)?)
}

/// Drain an event channel into a writer until it closes.
///
/// This is the task `ma-server` spawns beside the aggregator. It owns the
/// writer exclusively — same single-owner discipline as the aggregator and the
/// books, and for the same reason: two writers appending to one hour would
/// produce two files that each look complete.
pub async fn run(
    mut events: tokio::sync::mpsc::UnboundedReceiver<MarketEvent>,
    mut writer: EventWriter,
) -> EventWriter {
    while let Some(event) = events.recv().await {
        if let Err(e) = writer.append(&event).await {
            // A failed write must not take down ingest. The live book is the
            // primary product; history is downstream of it, and a process that
            // died because S3 returned a 503 would trade a recoverable gap in
            // the archive for a total outage.
            //
            // Swallowed is not silent: the counter is what lets `/metrics`
            // distinguish a healthy archive from one whose every write is
            // being rejected.
            warn!(error = %e, "could not append to the parquet writer");
            writer.counters.record_failure();
        }
    }
    if let Err(e) = writer.close().await {
        // No counter bump here: `close` already recorded one failure per
        // partition that could not be finished, and this error is the first
        // of those, re-surfaced. Counting it again would double-book it.
        warn!(error = %e, "could not close the final parquet file");
    }
    info!(
        files = writer.files_written(),
        rows = writer.rows_written(),
        "parquet writer finished"
    );
    writer
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Price, Qty, Symbol, VenueId};

    fn lv(price: &str, qty: &str) -> Level {
        Level::new(price.parse::<Price>().unwrap(), qty.parse::<Qty>().unwrap())
    }

    fn at(unix_secs: u64) -> IngestTime {
        IngestTime::new(
            std::time::Instant::now(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(unix_secs),
        )
    }

    fn event(kind: EventKind, ingest_ts: IngestTime) -> MarketEvent {
        MarketEvent {
            venue: VenueId::Coinbase,
            symbol: Symbol::new("BTC-USD"),
            venue_ts: None,
            ingest_ts,
            kind,
        }
    }

    #[test]
    fn a_snapshot_becomes_one_row_per_level_on_both_sides() {
        let rows = flatten(
            &event(
                EventKind::Snapshot {
                    bids: vec![lv("100", "1"), lv("99", "2")],
                    asks: vec![lv("101", "3")],
                },
                at(0),
            ),
            7,
            at(0),
        );

        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.event_seq == 7), "rows must group");
        assert!(rows.iter().all(|r| r.kind == kind::SNAPSHOT));
        assert_eq!(rows[0].side, Some("bid"));
        assert_eq!(rows[0].level_index, Some(0));
        assert_eq!(rows[1].level_index, Some(1));
        assert_eq!(rows[2].side, Some("ask"));
        assert_eq!(rows[2].level_index, Some(0));
    }

    #[test]
    fn prices_are_written_as_the_exact_digits_the_venue_sent() {
        // Trailing zeros are digits Kraken hashes, not padding. A float column
        // or a fixed-scale decimal would erase them and the checksum would
        // never match again.
        let rows = flatten(
            &event(
                EventKind::Delta {
                    bids: vec![lv("0.00100000", "45000.10")],
                    asks: vec![],
                },
                at(0),
            ),
            1,
            at(0),
        );
        assert_eq!(rows[0].price.as_deref(), Some("0.00100000"));
        assert_eq!(rows[0].qty.as_deref(), Some("45000.10"));
    }

    #[test]
    fn an_event_with_no_levels_still_produces_exactly_one_row() {
        // Otherwise a heartbeat vanishes and `event_seq` acquires a hole
        // nobody can explain. A stream that silently drops the messages
        // proving liveness is the failure DESIGN.md §4 is about.
        for kind in [
            EventKind::Heartbeat { counter: Some(9) },
            EventKind::Checksum { value: 42 },
            EventKind::Delta {
                bids: vec![],
                asks: vec![],
            },
        ] {
            let rows = flatten(&event(kind.clone(), at(0)), 1, at(0));
            assert_eq!(rows.len(), 1, "{kind:?} produced {} rows", rows.len());
        }
    }

    #[test]
    fn a_checksum_survives_into_the_record() {
        // Not decoration: it is what lets a replayed book still be checked
        // against what Kraken said it should be, rather than only against
        // itself.
        let rows = flatten(
            &event(EventKind::Checksum { value: 994_251_236 }, at(0)),
            1,
            at(0),
        );
        assert_eq!(rows[0].checksum, Some(994_251_236));
        assert_eq!(rows[0].kind, kind::CHECKSUM);
    }

    #[test]
    fn utc_dates_and_hours_are_derived_correctly() {
        assert_eq!(date_hour(SystemTime::UNIX_EPOCH), ("1970-01-01".into(), 0));
        // 2026-08-09T03:47:11Z
        assert_eq!(
            date_hour(SystemTime::UNIX_EPOCH + Duration::from_secs(1_786_247_231)),
            ("2026-08-09".into(), 3)
        );
        // Leap day, which is the case a hand-rolled calendar gets wrong.
        assert_eq!(
            date_hour(SystemTime::UNIX_EPOCH + Duration::from_secs(1_709_164_800)),
            ("2024-02-29".into(), 0)
        );
    }

    #[tokio::test]
    async fn crossing_an_hour_boundary_rolls_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events");

        // 03:59:59 and 04:00:00 on 2026-08-09.
        let before = 1_786_247_999;
        let after = 1_786_248_000;
        writer
            .append(&event(EventKind::Heartbeat { counter: None }, at(before)))
            .await
            .unwrap();
        writer
            .append(&event(EventKind::Heartbeat { counter: None }, at(after)))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert_eq!(keys.len(), 2, "the hour boundary did not roll: {keys:?}");
        assert!(keys[0].contains("hour=03"), "{keys:?}");
        assert!(keys[1].contains("hour=04"), "{keys:?}");
        assert_eq!(writer.files_written(), 2);
    }

    #[tokio::test]
    async fn events_inside_one_hour_share_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events");

        for i in 0..50 {
            writer
                .append(&event(
                    EventKind::Heartbeat { counter: Some(i) },
                    at(1_786_247_000 + i),
                ))
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert_eq!(keys.len(), 1, "one hour produced several files: {keys:?}");
        assert_eq!(writer.rows_written(), 50);
    }

    #[tokio::test]
    async fn a_long_quiet_hour_still_lands_parts_on_disk() {
        // The gap a live run found: hourly *partitioning* is not hourly
        // *durability*. A Parquet file is unreadable until its footer is
        // written, so an hour-long open file means an hour of data that only
        // this process can see — and a container gets SIGTERM on every deploy.
        //
        // `max_open` bounds that. Several parts inside one `hour=HH/`
        // directory read as one hour to any query engine.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events").with_config(WriterConfig {
            roll_every: Duration::from_secs(3600),
            max_open: Duration::from_secs(60),
            ..WriterConfig::default()
        });

        // Twenty minutes of events from the top of an hour, so they all fall
        // inside one partition and only `max_open` can split them.
        let origin = at(1_786_244_400);
        for i in 0..20_u64 {
            writer
                .append(&MarketEvent {
                    ingest_ts: origin.advanced_by(Duration::from_secs(i * 60)),
                    ..event(EventKind::Heartbeat { counter: Some(i) }, origin)
                })
                .await
                .unwrap();
        }

        // Deliberately *not* closed: this is the state a killed process leaves
        // behind, and the point is that most of the data is already safe.
        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert!(
            keys.len() >= 15,
            "twenty minutes at a one-minute bound produced only {} parts: {keys:?}",
            keys.len()
        );
        assert!(
            keys.iter().all(|k| k.contains("hour=")),
            "parts escaped their hour partition: {keys:?}"
        );
        // All in the same hour, as distinct parts.
        let hours: std::collections::BTreeSet<&str> = keys
            .iter()
            .filter_map(|k| k.split("/part-").next())
            .collect();
        assert_eq!(
            hours.len(),
            1,
            "one hour was split across partitions: {keys:?}"
        );
    }

    #[tokio::test]
    async fn parts_within_an_hour_have_distinct_keys() {
        // If they collided, each part would overwrite the last and the bound
        // above would silently protect nothing.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events").with_config(WriterConfig {
            max_open: Duration::from_secs(1),
            ..WriterConfig::default()
        });

        let origin = at(1_786_247_000);
        for i in 0..5_u64 {
            writer
                .append(&MarketEvent {
                    ingest_ts: origin.advanced_by(Duration::from_secs(i * 2)),
                    ..event(EventKind::Heartbeat { counter: Some(i) }, origin)
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        let unique: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len(), "part keys collided: {keys:?}");
        assert_eq!(writer.rows_written(), 5, "a part overwrote another");
    }

    /// A store that refuses every write. What S3 looks like with a broken
    /// policy, an expired credential, or a deleted bucket — the failure mode
    /// the archive counters exist to make visible.
    #[derive(Debug)]
    struct RejectingStore;

    type TestFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

    impl crate::store::ObjectStore for RejectingStore {
        fn describe(&self) -> String {
            "rejecting:everything".to_owned()
        }

        fn put(
            &self,
            key: &str,
            _bytes: Vec<u8>,
        ) -> TestFuture<'_, Result<(), crate::store::StoreError>> {
            let key = key.to_owned();
            Box::pin(async move {
                Err(crate::store::StoreError::Rejected {
                    key,
                    message: "every write is refused".to_owned(),
                })
            })
        }

        fn get(&self, key: &str) -> TestFuture<'_, Result<Vec<u8>, crate::store::StoreError>> {
            let key = key.to_owned();
            Box::pin(async move {
                Err(crate::store::StoreError::Rejected {
                    key,
                    message: "every read is refused".to_owned(),
                })
            })
        }

        fn list(
            &self,
            _prefix: &str,
        ) -> TestFuture<'_, Result<Vec<String>, crate::store::StoreError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[tokio::test]
    async fn a_failed_final_upload_is_counted_not_silent() {
        // `run` swallows the close error by design — a dying store must not
        // take down ingest. The counter is what keeps that from being a
        // silent drop policy, which CLAUDE.md calls a bug by name.
        let counters = Arc::new(ArchiveCounters::default());
        let writer = EventWriter::new(Arc::new(RejectingStore), "events")
            .with_counters(Arc::clone(&counters));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(event(
            EventKind::Heartbeat { counter: None },
            at(1_786_247_000),
        ))
        .unwrap();
        drop(tx);
        let writer = run(rx, writer).await;

        assert_eq!(
            writer.files_written(),
            0,
            "a rejected put counted as written"
        );
        assert_eq!(counters.write_failures(), 1, "the failure was not counted");
    }

    #[tokio::test]
    async fn a_failed_mid_run_roll_is_counted_not_silent() {
        // Same policy, different code path: a rotation forced by an hour
        // boundary fails inside `append`, and `run` drops the event that
        // triggered it. One counted failure; files_written stays at zero.
        let counters = Arc::new(ArchiveCounters::default());
        let writer = EventWriter::new(Arc::new(RejectingStore), "events")
            .with_counters(Arc::clone(&counters));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Two events over an hour apart: the second forces a roll of the
        // first's file, which the store refuses.
        tx.send(event(
            EventKind::Heartbeat { counter: None },
            at(1_786_247_000),
        ))
        .unwrap();
        tx.send(event(
            EventKind::Heartbeat { counter: None },
            at(1_786_251_000),
        ))
        .unwrap();
        drop(tx);
        let writer = run(rx, writer).await;

        assert_eq!(writer.files_written(), 0);
        assert!(
            counters.write_failures() >= 1,
            "a roll the store refused went uncounted"
        );
    }

    #[tokio::test]
    async fn nothing_is_written_until_there_is_something_to_write() {
        // A writer started before the feed must not leave an empty file
        // behind: an empty hour and a missing hour mean different things, and
        // an empty file claims the first when the truth is the second.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events");
        writer.close().await.unwrap();

        assert!(
            crate::store::ObjectStore::list(&*store, "events/")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(writer.files_written(), 0);
    }

    #[tokio::test]
    async fn each_symbol_lands_in_its_own_partition() {
        // The whole point of v4's change: a query for one symbol's hour prunes
        // by path, before opening a file. As a column it had to read every
        // row of every other symbol to discover they were not this one's.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events");

        for (i, symbol) in ["BTC-USD", "ETH-USD", "BTC-USD"].into_iter().enumerate() {
            writer
                .append(&MarketEvent {
                    symbol: Symbol::new(symbol),
                    ..event(
                        EventKind::Heartbeat { counter: None },
                        at(1_786_247_000 + i as u64),
                    )
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert_eq!(keys.len(), 2, "symbols shared a file: {keys:?}");
        assert!(
            keys.iter().any(|k| k.starts_with("events/symbol=BTC-USD/")),
            "{keys:?}"
        );
        assert!(
            keys.iter().any(|k| k.starts_with("events/symbol=ETH-USD/")),
            "{keys:?}"
        );
        // Symbol above date, so one symbol is one subtree. The other order
        // would make a single-symbol query walk every hour in the range.
        assert!(
            keys.iter()
                .all(|k| k.contains("/date=") && k.contains("/hour=")),
            "the date and hour partitions were lost: {keys:?}"
        );
    }

    #[tokio::test]
    async fn one_symbols_hour_boundary_does_not_roll_another() {
        // Rolling is per partition. A global roll would make every symbol's
        // part boundaries a function of whichever symbol happened to tick
        // first across the hour — and would close a file whose own `max_open`
        // deadline is nowhere near, producing small parts for no reason.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events");

        // 03:59:59, then 04:00:00 — BTC-USD crosses, ETH-USD does not.
        let before = 1_786_247_999;
        let after = 1_786_248_000;
        for (symbol, secs) in [("BTC-USD", before), ("ETH-USD", before), ("BTC-USD", after)] {
            writer
                .append(&MarketEvent {
                    symbol: Symbol::new(symbol),
                    ..event(EventKind::Heartbeat { counter: None }, at(secs))
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        let btc: Vec<&String> = keys
            .iter()
            .filter(|k| k.contains("symbol=BTC-USD"))
            .collect();
        let eth: Vec<&String> = keys
            .iter()
            .filter(|k| k.contains("symbol=ETH-USD"))
            .collect();

        assert_eq!(btc.len(), 2, "BTC-USD did not roll at the hour: {btc:?}");
        assert!(
            btc[0].contains("hour=03") && btc[1].contains("hour=04"),
            "{btc:?}"
        );
        assert_eq!(
            eth.len(),
            1,
            "another symbol's hour boundary rolled ETH-USD: {eth:?}"
        );
    }

    #[tokio::test]
    async fn part_numbers_count_up_within_a_partition() {
        // Per-directory counters. A counter shared across symbols would leave
        // each partition's parts numbered wherever the other symbol's rolls
        // happened to land — readable, but it makes "how many parts does this
        // hour have?" unanswerable from the names.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store.clone(), "events").with_config(WriterConfig {
            max_open: Duration::from_secs(1),
            ..WriterConfig::default()
        });

        let origin = at(1_786_247_000);
        for i in 0..6_u64 {
            writer
                .append(&MarketEvent {
                    symbol: Symbol::new(if i % 2 == 0 { "BTC-USD" } else { "ETH-USD" }),
                    ingest_ts: origin.advanced_by(Duration::from_secs(i * 2)),
                    ..event(EventKind::Heartbeat { counter: Some(i) }, origin)
                })
                .await
                .unwrap();
        }
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        for symbol in ["BTC-USD", "ETH-USD"] {
            let parts: Vec<String> = keys
                .iter()
                .filter(|k| k.contains(symbol))
                .filter_map(|k| k.rsplit('/').next().map(str::to_owned))
                .collect();
            assert_eq!(
                parts,
                vec![
                    "part-00000.parquet".to_owned(),
                    "part-00001.parquet".to_owned(),
                    "part-00002.parquet".to_owned(),
                ],
                "{symbol} part numbers are not contiguous"
            );
        }
    }

    #[tokio::test]
    async fn a_restart_in_the_same_hour_resumes_instead_of_overwriting() {
        // The bug this layout had from v4 until now: part numbers lived only
        // in process memory, so a second writer over the same store — i.e. a
        // restart, i.e. any same-hour redeploy — started back at part-00000
        // and silently replaced the previous run's file. The archive lost
        // data on every deploy and nothing reported it.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));

        let mut first = EventWriter::new(store.clone(), "events");
        first
            .append(&event(
                EventKind::Heartbeat { counter: Some(1) },
                at(1_786_247_000),
            ))
            .await
            .unwrap();
        first.close().await.unwrap();

        let mut second = EventWriter::new(store.clone(), "events");
        second
            .append(&event(
                EventKind::Heartbeat { counter: Some(2) },
                at(1_786_247_100),
            ))
            .await
            .unwrap();
        second.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert_eq!(
            keys.len(),
            2,
            "the second run overwrote the first's file: {keys:?}"
        );
        assert!(keys[0].ends_with("part-00000.parquet"), "{keys:?}");
        assert!(
            keys[1].ends_with("part-00001.parquet"),
            "the restart did not resume after the existing part: {keys:?}"
        );
        // Both files are whole: each run's row survived, not just its name.
        for key in &keys {
            let bytes = crate::store::ObjectStore::get(&*store, key).await.unwrap();
            assert!(!bytes.is_empty(), "{key} is empty");
        }
    }

    #[tokio::test]
    async fn resume_ignores_keys_that_are_not_parts() {
        // The listing is advisory input, not a schema: a stray file in the
        // hour directory (a README, a checksum sidecar, some future layout)
        // must not break the writer or perturb the numbering.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let (date, hour) = date_hour(at(1_786_247_000).wall());
        let hour_dir = format!("events/symbol=BTC-USD/date={date}/hour={hour:02}");
        for stray in [
            format!("{hour_dir}/README.txt"),
            format!("{hour_dir}/part-abc.parquet"),
            format!("{hour_dir}/part-00003.parquet"),
        ] {
            crate::store::ObjectStore::put(&*store, &stray, vec![1])
                .await
                .unwrap();
        }

        let mut writer = EventWriter::new(store.clone(), "events");
        writer
            .append(&event(
                EventKind::Heartbeat { counter: None },
                at(1_786_247_000),
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let keys = crate::store::ObjectStore::list(&*store, "events/")
            .await
            .unwrap();
        assert!(
            keys.iter().any(|k| k.ends_with("part-00004.parquet")),
            "resume did not continue past the highest parseable part: {keys:?}"
        );
        assert!(
            !keys.iter().any(|k| k.ends_with("part-00000.parquet")),
            "a stray key reset the numbering: {keys:?}"
        );
    }

    /// A store whose listings fail — a bucket with `PutObject` but a broken
    /// `ListBucket` condition, which the S3 scope probe cannot rule out for
    /// sub-prefixes.
    #[derive(Debug)]
    struct ListFailsStore;

    impl crate::store::ObjectStore for ListFailsStore {
        fn describe(&self) -> String {
            "list-fails".to_owned()
        }

        fn put(
            &self,
            _key: &str,
            _bytes: Vec<u8>,
        ) -> TestFuture<'_, Result<(), crate::store::StoreError>> {
            Box::pin(async { Ok(()) })
        }

        fn get(&self, key: &str) -> TestFuture<'_, Result<Vec<u8>, crate::store::StoreError>> {
            let key = key.to_owned();
            Box::pin(async move {
                Err(crate::store::StoreError::Rejected {
                    key,
                    message: "no reads".to_owned(),
                })
            })
        }

        fn list(
            &self,
            prefix: &str,
        ) -> TestFuture<'_, Result<Vec<String>, crate::store::StoreError>> {
            let prefix = prefix.to_owned();
            Box::pin(async move {
                Err(crate::store::StoreError::Rejected {
                    key: prefix,
                    message: "listing refused".to_owned(),
                })
            })
        }
    }

    #[tokio::test]
    async fn a_failed_listing_fails_the_write_instead_of_guessing() {
        // The fallback that avoids an overwrite without a listing is a name
        // outside the readable part-NNNNN scheme — so there is no fallback.
        // The failure takes the same shape as a failed upload: the caller
        // warns, counts it, drops the event, and the next append retries.
        let counters = Arc::new(ArchiveCounters::default());
        let writer = EventWriter::new(Arc::new(ListFailsStore), "events")
            .with_counters(Arc::clone(&counters));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(event(
            EventKind::Heartbeat { counter: None },
            at(1_786_247_000),
        ))
        .unwrap();
        drop(tx);
        let writer = run(rx, writer).await;

        assert_eq!(writer.files_written(), 0);
        assert_eq!(
            counters.write_failures(),
            1,
            "the refused listing was not counted as a failed write"
        );
    }

    #[test]
    fn next_part_resumes_after_the_highest_existing_number() {
        assert_eq!(next_part(&[]), 0);
        assert_eq!(next_part(&["events/x/part-00000.parquet".to_owned()]), 1);
        assert_eq!(
            next_part(&[
                "events/x/part-00002.parquet".to_owned(),
                "events/x/part-00000.parquet".to_owned(),
                "events/x/README.txt".to_owned(),
                "events/x/part-junk.parquet".to_owned(),
            ]),
            3
        );
    }

    #[test]
    fn a_symbol_cannot_escape_the_prefix() {
        // `Symbol::new` accepts any string and this one becomes a path on a
        // LocalStore. Same check, and the same reasoning, as
        // `DirRegistry::path_for`: the day it matters is not the day anyone
        // will think to add it.
        assert_eq!(partition_name("BTC-USD"), "BTC-USD");
        assert_eq!(partition_name("../../etc/passwd"), ".._.._etc_passwd");
        assert!(!partition_name("a/b").contains('/'));
        // An empty or all-dots symbol must still be one identifiable
        // partition, not a name that resolves to the prefix itself.
        assert_eq!(partition_name(""), "unknown");
        assert_eq!(partition_name(".."), "unknown");
    }

    #[tokio::test]
    async fn the_elapsed_column_is_relative_to_the_first_event_not_the_writer() {
        // A writer constructed at startup but fed nothing for a minute must not
        // stamp its first event with a 60-second offset. The base is the first
        // event, so elapsed starts at zero and replay's pacing matches the
        // recording's.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::LocalStore::new(dir.path()));
        let mut writer = EventWriter::new(store, "events");

        let first = at(1_786_247_000);
        writer
            .append(&event(EventKind::Heartbeat { counter: None }, first))
            .await
            .unwrap();

        let rows = flatten(
            &event(EventKind::Heartbeat { counter: None }, first),
            1,
            writer.base.unwrap(),
        );
        assert_eq!(rows[0].ingest_elapsed, 0);
    }
}
