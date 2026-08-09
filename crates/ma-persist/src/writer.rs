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
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            row_group_rows: 8192,
            roll_every: Duration::from_secs(3600),
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
    base: Option<IngestTime>,
    event_seq: i64,
    open: Option<OpenFile>,
    /// Files finished and uploaded, for logs and for the tests that need to
    /// know a roll actually happened.
    pub files_written: u64,
    pub rows_written: u64,
}

/// The file currently being appended to.
struct OpenFile {
    /// Which roll window this file belongs to, as whole `roll_every` units
    /// since the Unix epoch. Comparing window indices rather than doing date
    /// arithmetic makes "did we cross a boundary?" a single integer compare,
    /// and makes a sub-hour `roll_every` work identically for tests.
    window: i64,
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
            open: None,
            files_written: 0,
            rows_written: 0,
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: WriterConfig) -> Self {
        self.config = config;
        self
    }

    /// Append one event, rolling the file first if it belongs to a new window.
    ///
    /// # Errors
    /// If the current file cannot be finished or uploaded.
    pub async fn append(&mut self, event: &MarketEvent) -> Result<(), WriteError> {
        let base = *self.base.get_or_insert(event.ingest_ts);
        let window = self.window_of(event.ingest_ts.wall());

        match &self.open {
            Some(open) if open.window == window => {}
            Some(_) => self.roll().await?,
            None => {}
        }
        if self.open.is_none() {
            self.open = Some(self.start_file(window, event.ingest_ts.wall())?);
        }

        self.event_seq += 1;
        let rows = flatten(event, self.event_seq, base);

        // `open` was just ensured above.
        let Some(open) = self.open.as_mut() else {
            return Ok(());
        };
        open.rows_total += rows.len() as u64;
        open.rows.extend(rows);

        if open.rows.len() >= self.config.row_group_rows {
            Self::flush_row_group(open)?;
        }
        Ok(())
    }

    /// Finish and upload the open file, if any. Call before shutdown, or the
    /// last partial hour is lost.
    ///
    /// # Errors
    /// If the file cannot be finished or uploaded.
    pub async fn close(&mut self) -> Result<(), WriteError> {
        if self.open.is_some() {
            self.roll().await?;
        }
        Ok(())
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

    fn start_file(&self, window: i64, at: SystemTime) -> Result<OpenFile, WriteError> {
        let props = WriterProperties::builder()
            // zstd over snappy: these files are written once, read rarely, and
            // kept. Compression ratio is worth more than decode speed, and the
            // repeated venue/symbol/event_seq columns compress extremely well.
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let writer = ArrowWriter::try_new(Vec::new(), EVENT_SCHEMA.clone(), Some(props))?;
        let key = self.key_for(at, self.files_written);
        debug!(%key, window, "opening a new parquet file");
        Ok(OpenFile {
            window,
            key,
            writer,
            rows: Vec::with_capacity(self.config.row_group_rows),
            rows_total: 0,
        })
    }

    /// Hive-style partitioning, which every query engine understands without
    /// being told: a reader filtering on one hour can skip the rest by path
    /// alone, before opening a single file.
    ///
    /// Symbol is a *column*, not a partition. Partitioning by it would mean one
    /// open file per symbol, and at this volume that trades a real cost (open
    /// file handles, more smaller files, worse compression) for a filter the
    /// column already supports. It becomes the right call when a single
    /// symbol's hour is big enough to be worth skipping whole, which is a v3
    /// problem.
    fn key_for(&self, at: SystemTime, part: u64) -> String {
        let (date, hour) = date_hour(at);
        format!(
            "{}/date={date}/hour={hour:02}/part-{part:05}.parquet",
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

    /// Finish the open file and hand it to the store.
    async fn roll(&mut self) -> Result<(), WriteError> {
        let Some(mut open) = self.open.take() else {
            return Ok(());
        };
        Self::flush_row_group(&mut open)?;

        let rows = open.rows_total;
        let bytes = open.writer.into_inner()?;
        let size = bytes.len();

        self.store.put(&open.key, bytes).await?;
        self.files_written += 1;
        self.rows_written += rows;

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
            warn!(error = %e, "could not append to the parquet writer");
        }
    }
    if let Err(e) = writer.close().await {
        warn!(error = %e, "could not close the final parquet file");
    }
    info!(
        files = writer.files_written,
        rows = writer.rows_written,
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
        assert_eq!(writer.files_written, 2);
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
        assert_eq!(writer.rows_written, 50);
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
        assert_eq!(writer.files_written, 0);
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
