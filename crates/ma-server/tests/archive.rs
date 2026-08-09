//! The v2 durability loop, closed end to end against a real recording.
//!
//! Replay the committed tape through the real pipeline while archiving to
//! Parquet, then replay *that archive* through a second, entirely fresh
//! pipeline, and require the two runs to reach the same books.
//!
//! # Why this is the test that matters
//!
//! `ma-persist`'s own round-trip tests prove the encoding loses nothing. This
//! proves the thing that actually gets relied on: that an archived hour, read
//! back weeks later, reconstructs *the market the live process believed it was
//! seeing*. A durability layer that is subtly lossy fails in the worst
//! available way — every conclusion drawn from it is a confident conclusion
//! about something that never happened, and unlike a live bug nothing will
//! ever contradict it.
//!
//! The strongest single assertion here is the Kraken one. Kraken's CRC32 is
//! part of the normalised stream, so the book rebuilt from Parquet is checked
//! against *the venue's own hash* of what it should contain — not merely
//! against the first run. That makes this a check of the archive against
//! reality, rather than of two of our own code paths against each other.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ma_core::{Symbol, VenueId};
use ma_persist::store::ObjectStore;
use ma_persist::{EventWriter, LocalStore, WriterConfig};
use ma_pipeline::aggregator::{BookStatus, Snapshot};
use ma_pipeline::tape::{Pacing, TapeReader, replay};
use ma_server::{DEFAULT_VENUES, Pipeline, replay_archive};

fn tape_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tapes/2026-08-09-btc-usd.jsonl.gz")
        .canonicalize()
        .expect("the committed tape should exist")
}

fn symbol() -> Symbol {
    Symbol::new("BTC-USD")
}

/// Drain the broadcast ring and keep the last snapshot.
///
/// Lagging is expected: this subscriber never polls while the run is applying,
/// so it is far behind the 32-snapshot ring. Skipping forward is exactly what
/// the SSE handler does for a slow browser, and treating it as an error here
/// would read as "the aggregator published nothing" when it published
/// hundreds.
fn last_snapshot(rx: &mut tokio::sync::broadcast::Receiver<Arc<Snapshot>>) -> Snapshot {
    use tokio::sync::broadcast::error::TryRecvError;
    let mut last = None;
    loop {
        match rx.try_recv() {
            Ok(snapshot) => last = Some(snapshot),
            Err(TryRecvError::Lagged(_)) => {}
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
    let last = last.expect("the aggregator published nothing");
    Arc::try_unwrap(last).unwrap_or_else(|arc| (*arc).clone())
}

/// Run 1: the committed tape through the real pipeline, teeing normalised
/// events into a Parquet archive.
async fn record_archive(store: Arc<dyn ObjectStore>) -> Snapshot {
    let (events_tx, events_rx) = tokio::sync::mpsc::unbounded_channel();
    let writer = EventWriter::new(Arc::clone(&store), "events").with_config(WriterConfig {
        // Small row groups so a 60-second recording still spans several,
        // exercising the reader's cross-batch event reassembly on real data
        // rather than only on a synthetic snapshot.
        row_group_rows: 512,
        ..WriterConfig::default()
    });
    let archive = tokio::spawn(ma_persist::run(events_rx, writer));

    let mut pipeline = Pipeline::new(vec![symbol()], DEFAULT_VENUES.to_vec())
        .expect("pipeline")
        .with_tick(Duration::from_millis(5))
        .recording_events_to(events_tx);

    let (handle, aggregator) = pipeline.spawn_aggregator().expect("aggregator");
    let mut snapshots = handle.subscribe();
    let tx = pipeline.channel();
    let clock = pipeline.clock();

    let mut reader = TapeReader::open(tape_path()).await.expect("open tape");
    let stats = replay(
        &mut reader,
        &tx,
        clock.as_ref(),
        Pacing::Faithful,
        &symbol(),
    )
    .await
    .expect("tape replay");
    assert!(stats.frames_sent > 1_000, "tape looks truncated: {stats:?}");
    assert_eq!(stats.dropped, 0, "Faithful pacing must not lose a frame");

    drop(tx);
    let trigger = pipeline.into_trigger();
    let _ = tokio::time::timeout(Duration::from_secs(20), aggregator).await;
    drop(trigger);

    let written = tokio::time::timeout(Duration::from_secs(20), archive)
        .await
        .expect("the archive writer hung")
        .expect("the archive writer panicked");
    assert!(written.files_written > 0, "nothing was archived");
    assert!(written.rows_written > 0);

    last_snapshot(&mut snapshots)
}

/// Run 2: the archive through a second, entirely fresh pipeline.
async fn replay_from_archive(store: Arc<dyn ObjectStore>) -> Snapshot {
    let mut pipeline = Pipeline::new(vec![symbol()], DEFAULT_VENUES.to_vec())
        .expect("pipeline")
        .with_tick(Duration::from_millis(5));

    let (handle, aggregator) = pipeline.spawn_aggregator().expect("aggregator");
    let mut snapshots = handle.subscribe();
    let tx = pipeline.channel();
    let clock = pipeline.clock();

    let stats = replay_archive(store, "events", &tx, clock.as_ref(), Pacing::Faithful)
        .await
        .expect("archive replay");
    assert!(stats.events_sent > 0, "the archive replayed nothing");
    assert_eq!(stats.dropped, 0, "Faithful pacing must not lose an event");

    drop(tx);
    let trigger = pipeline.into_trigger();
    let _ = tokio::time::timeout(Duration::from_secs(20), aggregator).await;
    drop(trigger);

    last_snapshot(&mut snapshots)
}

/// The books, and only the books.
///
/// `seq`, wall clocks and every `_ms` duration are read from a clock and
/// legitimately differ between two runs. `applied` differs too, and for a
/// reason worth stating rather than hiding: the archive run applies one message
/// per *event*, while the tape run applies one per *frame*, and a single frame
/// can carry several events (a Kraken book message is a delta and a checksum).
/// What must be identical is the market: prices, depth, and trust.
fn books(snapshot: &Snapshot) -> BTreeMap<String, String> {
    snapshot
        .symbol("BTC-USD")
        .expect("BTC-USD")
        .venues
        .iter()
        .map(|v| {
            let ladder = |levels: &[ma_core::Level]| {
                levels
                    .iter()
                    .map(|l| format!("{}@{}", l.qty, l.price))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            (
                v.venue.to_string(),
                format!(
                    "status={:?} integrity={:?} reason={:?} bid={:?} ask={:?} \
                     spread={:?} held={:?} bids=[{}] asks=[{}]",
                    v.status,
                    v.integrity,
                    v.desync_reason,
                    v.bid.map(|l| l.price.to_string()),
                    v.ask.map(|l| l.price.to_string()),
                    v.spread,
                    v.levels_held,
                    ladder(&v.bids),
                    ladder(&v.asks),
                ),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parquet_archive_rebuilds_the_books_the_live_run_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));

    let live = record_archive(Arc::clone(&store)).await;
    let restored = replay_from_archive(Arc::clone(&store)).await;

    assert_eq!(
        books(&live),
        books(&restored),
        "the archive rebuilt a different market than the live run served"
    );
    assert_eq!(books(&live).len(), DEFAULT_VENUES.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn kraken_is_still_checksum_verified_when_rebuilt_from_parquet() {
    // The claim that keeps Parquet replay from being strictly weaker than it
    // has to be, and the only assertion in this file that is not
    // self-referential. `EventKind::Checksum` survives into the archive, so
    // the rebuilt book is validated against *Kraken's* hash of what it should
    // contain — not merely against our first run of the same code.
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));

    record_archive(Arc::clone(&store)).await;
    let restored = replay_from_archive(store).await;

    let kraken = restored
        .symbol("BTC-USD")
        .expect("BTC-USD")
        .venues
        .iter()
        .find(|v| v.venue == VenueId::Kraken)
        .expect("kraken");

    assert_eq!(
        kraken.status,
        BookStatus::Live,
        "the rebuilt Kraken book is not trusted: {:?}",
        kraken.desync_reason
    );
    assert_eq!(kraken.integrity, Some(ma_core::Integrity::Verified));
    assert_eq!(
        kraken.counters.desyncs, 0,
        "a book rebuilt from Parquet failed Kraken's own checksum — the \
         archive does not describe the market the live run saw"
    );
    assert!(
        kraken.last_verified_ms.is_some(),
        "the rebuilt book was never actually verified against a checksum"
    );
    assert_eq!(
        kraken.levels_held,
        [10, 10],
        "the rebuilt book holds a different depth than Kraken hashes"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_archive_is_partitioned_by_symbol_then_hour() {
    // The layout is load-bearing rather than cosmetic.
    //
    // Symbol is the outermost partition so that a query for one symbol prunes
    // to one subtree instead of walking every hour in the range; date and hour
    // below it stay zero-padded and big-endian so that *within* a symbol,
    // lexicographic key order is chronological — which is what lets a reader
    // stream one partition without sorting anything.
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));
    record_archive(Arc::clone(&store)).await;

    let keys = store.list("events").await.expect("list");
    assert!(!keys.is_empty(), "nothing was archived");
    for key in &keys {
        assert!(key.starts_with("events/symbol="), "{key}");
        assert!(key.contains("/date="), "{key}");
        assert!(key.contains("/hour="), "{key}");
        assert!(key.ends_with(".parquet"), "{key}");
    }

    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "the store did not list keys in order");
}
