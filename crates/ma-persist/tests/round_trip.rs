//! Write events to Parquet, read them back, and require them to be identical.
//!
//! This is the property CLAUDE.md's testing notes ask for, in the form that
//! matters for a durability layer: *applying a delta stream to a book, then
//! replaying from snapshot + remaining deltas, yields identical state.* If a
//! round trip through Parquet is not lossless, then every conclusion drawn
//! from an archived hour is a conclusion about something that never happened —
//! and unlike a live bug, nothing would ever contradict it.
//!
//! The book-level version of this property lives in `ma-server`'s replay
//! tests, which drive a real recording all the way through. This file proves
//! the narrower claim the wider one depends on: the encoding itself loses
//! nothing.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ma_core::{EventKind, IngestTime, Level, MarketEvent, Side, Symbol, VenueId};
use ma_persist::store::ObjectStore;
use ma_persist::{EventReader, EventWriter, LocalStore, WriterConfig};

fn lv(price: &str, qty: &str) -> Level {
    Level::new(price.parse().unwrap(), qty.parse().unwrap())
}

/// A base observation both clocks are advanced from.
///
/// Advancing *both* matters: `ingest_elapsed` is derived from the monotonic
/// reading, so a helper that only moved the wall clock would produce rows
/// spaced by however long the test itself took to run — which is exactly the
/// confusion `IngestTime` carrying two clocks exists to prevent.
fn base(unix_nanos: u64) -> IngestTime {
    IngestTime::new(
        std::time::Instant::now(),
        SystemTime::UNIX_EPOCH + Duration::from_nanos(unix_nanos),
    )
}

fn event(venue: VenueId, symbol: &str, kind: EventKind, ingest_ts: IngestTime) -> MarketEvent {
    MarketEvent {
        venue,
        symbol: Symbol::new(symbol),
        venue_ts: None,
        ingest_ts,
        kind,
    }
}

/// One of every kind, across two venues and two symbols.
fn sample() -> Vec<MarketEvent> {
    let origin = base(1_786_247_000_000_000_000);
    let t = |n: u64| origin.advanced_by(Duration::from_millis(n));

    vec![
        event(
            VenueId::Coinbase,
            "BTC-USD",
            EventKind::Snapshot {
                bids: vec![lv("64740.08", "0.41225831"), lv("64740.07", "0.18871297")],
                asks: vec![lv("64740.09", "0.18759049")],
            },
            t(0),
        ),
        event(
            VenueId::Coinbase,
            "BTC-USD",
            EventKind::Delta {
                bids: vec![lv("64740.08", "0")],
                asks: vec![],
            },
            t(1),
        ),
        // Kraken's exact digits, including the trailing zeros its checksum is
        // computed over. This is the row that would break under a float column
        // or a fixed-scale decimal.
        event(
            VenueId::Kraken,
            "BTC-USD",
            EventKind::Delta {
                bids: vec![lv("0.00100000", "45000.10")],
                asks: vec![lv("64741.39000", "0.030")],
            },
            t(2),
        ),
        event(
            VenueId::Kraken,
            "BTC-USD",
            EventKind::Checksum { value: 994_251_236 },
            t(3),
        ),
        event(
            VenueId::Bitstamp,
            "ETH-USD",
            EventKind::Trade {
                price: "3210.55".parse().unwrap(),
                qty: "1.25".parse().unwrap(),
                taker_side: Some(Side::Ask),
            },
            t(4),
        ),
        event(
            VenueId::Coinbase,
            "BTC-USD",
            EventKind::Heartbeat { counter: Some(77) },
            t(5),
        ),
        event(
            VenueId::Kraken,
            "BTC-USD",
            EventKind::Heartbeat { counter: None },
            t(6),
        ),
        // An empty delta: the venue sent a message and we applied it. It has
        // to survive, or `event_seq` acquires a hole nobody can explain.
        event(
            VenueId::Bitstamp,
            "ETH-USD",
            EventKind::Delta {
                bids: vec![],
                asks: vec![],
            },
            t(7),
        ),
    ]
}

/// Everything about an event that a round trip must preserve. The reconstructed
/// `ingest_ts.mono()` is deliberately excluded: an `Instant` has no meaning
/// across processes, which is the whole reason `elapsed` exists.
fn comparable(e: &MarketEvent) -> String {
    format!(
        "{} {} {:?} venue_ts={:?} wall={:?}",
        e.venue,
        e.symbol,
        e.kind,
        e.venue_ts,
        e.ingest_ts.wall()
    )
}

async fn round_trip(events: &[MarketEvent], config: WriterConfig) -> Vec<ma_persist::StoredEvent> {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));

    let mut writer = EventWriter::new(Arc::clone(&store), "events").with_config(config);
    for e in events {
        writer.append(e).await.unwrap();
    }
    writer.close().await.unwrap();

    EventReader::open(Arc::clone(&store), "events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn every_event_kind_survives_a_parquet_round_trip() {
    let original = sample();
    let read = round_trip(&original, WriterConfig::default()).await;

    assert_eq!(
        read.len(),
        original.len(),
        "event count changed: wrote {}, read {}",
        original.len(),
        read.len()
    );

    for (i, (before, after)) in original.iter().zip(read.iter()).enumerate() {
        assert_eq!(
            comparable(before),
            comparable(&after.event),
            "event {i} changed through the round trip"
        );
    }
}

#[tokio::test]
async fn exact_decimal_digits_survive_including_trailing_zeros() {
    // The single most load-bearing property of the schema. Kraken's checksum
    // hashes the digits the venue sent: `0.00100000` re-serialised as `0.001`
    // becomes `"1"` instead of `"100000"` after the checksum's zero-stripping,
    // which is a completely different hash for a numerically identical value.
    // A float column, or Arrow's fixed-scale Decimal128, would erase them.
    let read = round_trip(&sample(), WriterConfig::default()).await;

    let kraken_delta = read
        .iter()
        .find(|e| {
            e.event.venue == VenueId::Kraken && matches!(e.event.kind, EventKind::Delta { .. })
        })
        .expect("the kraken delta");

    let EventKind::Delta { bids, asks } = &kraken_delta.event.kind else {
        panic!("not a delta");
    };
    assert_eq!(bids[0].price.to_string(), "0.00100000");
    assert_eq!(bids[0].qty.to_string(), "45000.10");
    assert_eq!(asks[0].price.to_string(), "64741.39000");
    assert_eq!(asks[0].qty.to_string(), "0.030");
}

#[tokio::test]
async fn an_event_split_across_row_groups_is_still_one_event() {
    // A row group boundary can fall in the middle of a snapshot — Coinbase's
    // opening book is tens of thousands of levels and every row group boundary
    // lands inside one. If the reader closed an event at a batch boundary, the
    // book would be rebuilt from two half-snapshots and the second would
    // silently replace the first.
    let big: Vec<Level> = (0..500)
        .map(|i| {
            let price = format!("{}", 64_000 - i);
            Level::new(price.parse().unwrap(), "1.5".parse().unwrap())
        })
        .collect();

    let events = vec![event(
        VenueId::Coinbase,
        "BTC-USD",
        EventKind::Snapshot {
            bids: big.clone(),
            asks: big.iter().rev().copied().collect(),
        },
        base(1_786_247_000_000_000_000),
    )];

    // Row groups far smaller than the snapshot, so it is guaranteed to span
    // several of them.
    let read = round_trip(
        &events,
        WriterConfig {
            row_group_rows: 64,
            ..WriterConfig::default()
        },
    )
    .await;

    assert_eq!(read.len(), 1, "one snapshot became {} events", read.len());
    let EventKind::Snapshot { bids, asks } = &read[0].event.kind else {
        panic!("not a snapshot");
    };
    assert_eq!(bids.len(), 500, "levels were lost across a row group");
    assert_eq!(asks.len(), 500);
    assert_eq!(bids[0].price.to_string(), "64000");
    assert_eq!(bids[499].price.to_string(), "63501");
}

#[tokio::test]
async fn level_order_within_a_side_is_preserved() {
    // Not cosmetic. Kraken's checksum is computed over the book in a specific
    // order, and a delta's ordering is information the venue gave us that
    // cannot be recovered by sorting — a delta is not sorted at all.
    let events = vec![event(
        VenueId::Kraken,
        "BTC-USD",
        EventKind::Delta {
            bids: vec![lv("100", "1"), lv("102", "2"), lv("101", "3")],
            asks: vec![],
        },
        base(1_786_247_000_000_000_000),
    )];

    let read = round_trip(&events, WriterConfig::default()).await;
    let EventKind::Delta { bids, .. } = &read[0].event.kind else {
        panic!("not a delta");
    };
    let prices: Vec<String> = bids.iter().map(|l| l.price.to_string()).collect();
    assert_eq!(
        prices,
        ["100", "102", "101"],
        "the venue's own ordering was rewritten"
    );
}

#[tokio::test]
async fn the_elapsed_offsets_reproduce_the_original_spacing() {
    // Replay orders and paces by this, not by the wall clock — an NTP step
    // mid-session must not reorder a replay. Same decision the raw-frame tape
    // made with `elapsed_nanos`.
    let read = round_trip(&sample(), WriterConfig::default()).await;

    assert_eq!(
        read[0].elapsed,
        Duration::ZERO,
        "the first event is the base"
    );
    for (i, e) in read.iter().enumerate() {
        assert_eq!(
            e.elapsed,
            Duration::from_millis(i as u64),
            "event {i} was not spaced as recorded"
        );
    }
}

#[tokio::test]
async fn events_are_read_back_in_the_order_they_were_written() {
    // Across an hourly roll, which is where an unsorted listing would show up
    // as a book from the future.
    // Exactly on an hour boundary, then one full hour per event.
    let origin = base(1_786_244_400_000_000_000);
    let events: Vec<MarketEvent> = (0..6)
        .map(|i| {
            event(
                VenueId::Coinbase,
                "BTC-USD",
                EventKind::Heartbeat { counter: Some(i) },
                origin.advanced_by(Duration::from_secs(3600 * i)),
            )
        })
        .collect();

    let read = round_trip(&events, WriterConfig::default()).await;
    assert_eq!(read.len(), 6);

    let counters: Vec<Option<u64>> = read
        .iter()
        .map(|e| match e.event.kind {
            EventKind::Heartbeat { counter } => counter,
            _ => panic!("not a heartbeat"),
        })
        .collect();
    assert_eq!(
        counters,
        (0..6).map(Some).collect::<Vec<_>>(),
        "six hourly files were read out of order"
    );
}

#[tokio::test]
async fn the_stream_identity_survives_so_replay_can_route() {
    // A replayed event has to find the right book. With two symbols and three
    // venues in one file, losing either half of the identity would route
    // ETH-USD deltas into the BTC-USD book — and the book would look
    // completely plausible.
    let read = round_trip(&sample(), WriterConfig::default()).await;

    let keys: Vec<String> = read.iter().map(|e| e.stream.key()).collect();
    assert!(keys.contains(&"coinbase:BTC-USD".to_owned()));
    assert!(keys.contains(&"kraken:BTC-USD".to_owned()));
    assert!(keys.contains(&"bitstamp:ETH-USD".to_owned()));

    for e in &read {
        assert_eq!(e.stream.venue, e.event.venue);
        assert_eq!(e.stream.symbol, e.event.symbol);
    }
}

#[tokio::test]
async fn the_checksum_survives_so_a_replayed_book_is_still_verified() {
    // The claim that keeps Parquet replay from being strictly weaker than it
    // needs to be. `EventKind::Checksum` is part of the normalised stream, so a
    // book rebuilt from an archive is still checked against what Kraken said it
    // should be — not merely against itself.
    let read = round_trip(&sample(), WriterConfig::default()).await;
    let checksum = read
        .iter()
        .find(|e| matches!(e.event.kind, EventKind::Checksum { .. }))
        .expect("the checksum event");

    assert!(matches!(
        checksum.event.kind,
        EventKind::Checksum { value: 994_251_236 }
    ));
}

#[tokio::test]
async fn two_symbols_read_back_interleaved_rather_than_one_after_the_other() {
    // The regression v4's partitioning introduced, and the reason the reader
    // now merges instead of walking keys.
    //
    // Symbol partitions sort above date partitions, so a reader listing keys
    // and reading straight through yields all of BTC-USD, then all of ETH-USD.
    // Nothing errors — the archive simply replays as two recordings laid end
    // to end. Worse, `replay_archive` paces on the gap between consecutive
    // `elapsed` values, so the second symbol arrives in one burst with every
    // gap clamped to zero: a partitioning change presenting as a pacing bug.
    let origin = base(1_786_247_000_000_000_000);
    let events: Vec<MarketEvent> = (0..20_u64)
        .map(|i| {
            event(
                VenueId::Coinbase,
                if i % 2 == 0 { "BTC-USD" } else { "ETH-USD" },
                EventKind::Heartbeat { counter: Some(i) },
                origin.advanced_by(Duration::from_millis(i * 50)),
            )
        })
        .collect();

    let read = round_trip(&events, WriterConfig::default()).await;
    assert_eq!(read.len(), 20);

    let symbols: Vec<String> = read.iter().map(|e| e.event.symbol.to_string()).collect();
    let expected: Vec<String> = events.iter().map(|e| e.symbol.to_string()).collect();
    assert_eq!(
        symbols, expected,
        "the archive replayed grouped by symbol instead of in the order it was \
         written"
    );

    // And the counters, which is the same claim stated where it is impossible
    // to satisfy by accident.
    let counters: Vec<Option<u64>> = read
        .iter()
        .map(|e| match e.event.kind {
            EventKind::Heartbeat { counter } => counter,
            _ => panic!("not a heartbeat"),
        })
        .collect();
    assert_eq!(counters, (0..20).map(Some).collect::<Vec<_>>());
}

#[tokio::test]
async fn elapsed_offsets_stay_monotonic_across_partitions() {
    // What the merge is actually protecting. `replay_archive` sleeps for
    // `elapsed - previous` and clamps a negative gap to zero, so an
    // out-of-order merge does not fail — it silently replays at the wrong
    // speed, which looks exactly like a fast machine.
    let origin = base(1_786_247_000_000_000_000);
    let events: Vec<MarketEvent> = (0..12_u64)
        .map(|i| {
            event(
                VenueId::Kraken,
                ["BTC-USD", "ETH-USD", "SOL-USD"][(i % 3) as usize],
                EventKind::Heartbeat { counter: Some(i) },
                origin.advanced_by(Duration::from_millis(i * 100)),
            )
        })
        .collect();

    let read = round_trip(&events, WriterConfig::default()).await;
    let offsets: Vec<Duration> = read.iter().map(|e| e.elapsed).collect();

    assert_eq!(offsets.len(), 12);
    assert!(
        offsets.windows(2).all(|w| w[0] <= w[1]),
        "elapsed went backwards across the merge: {offsets:?}"
    );
    assert_eq!(offsets[0], Duration::ZERO);
    assert_eq!(offsets[11], Duration::from_millis(1100));
}

#[tokio::test]
async fn an_archive_written_before_symbol_partitioning_still_reads() {
    // Files already sitting in S3 use the v2 layout, with no `symbol=`
    // component at all. Same argument as the raw-frame tape's optional
    // `symbol` field: a format change that silently invalidated existing
    // recordings would throw away the artefacts that are hardest to replace.
    //
    // Built by writing normally and then re-keying the object into the old
    // shape, so the bytes under test are bytes this writer actually produces.
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));

    let mut writer = EventWriter::new(Arc::clone(&store), "staging");
    for e in sample() {
        writer.append(&e).await.unwrap();
    }
    writer.close().await.unwrap();

    // Re-key every part under the v2 layout, preserving relative order.
    let staged = store.list("staging/").await.unwrap();
    assert!(!staged.is_empty());
    for (i, key) in staged.iter().enumerate() {
        let bytes = store.get(key).await.unwrap();
        store
            .put(
                &format!("legacy/date=2026-08-09/hour=03/part-{i:05}.parquet"),
                bytes,
            )
            .await
            .unwrap();
    }

    let read = EventReader::open(Arc::clone(&store), "legacy")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        read.len(),
        sample().len(),
        "a v2-layout archive did not read back"
    );
    let symbols: std::collections::BTreeSet<String> =
        read.iter().map(|e| e.event.symbol.to_string()).collect();
    assert!(symbols.contains("BTC-USD") && symbols.contains("ETH-USD"));
}

/// Archive two nodes' shares under their own prefixes, each written by its own
/// `EventWriter` so their `elapsed` origins are genuinely independent.
///
/// `later_by` is what makes this a cluster rather than two copies: node B's
/// session starts well after node A's, so its offsets restart from zero at a
/// point where node A is already far along.
async fn two_node_archive(later_by: Duration) -> (Arc<dyn ObjectStore>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::new(dir.path()));
    let origin = base(1_786_247_000_000_000_000);

    // Disjoint symbols, because that is the only arrangement v3 sharding can
    // produce: at most one node runs a given stream, so no two nodes ever
    // write events for the same book. See ma_persist::reader's module docs.
    for (prefix, symbol, offset) in [
        ("node-a/events", "BTC-USD", Duration::ZERO),
        ("node-b/events", "ETH-USD", later_by),
    ] {
        let mut writer = EventWriter::new(Arc::clone(&store), prefix);
        for i in 0..4_u64 {
            writer
                .append(&event(
                    VenueId::Coinbase,
                    symbol,
                    EventKind::Heartbeat { counter: Some(i) },
                    origin.advanced_by(offset + Duration::from_millis(i * 100)),
                ))
                .await
                .unwrap();
        }
        writer.close().await.unwrap();
    }

    (store, dir)
}

#[tokio::test]
async fn an_hour_two_nodes_wrote_reads_back_as_one_session() {
    // v3 shards streams across nodes and each node archives only its own
    // share, so an hour of *everything* is a union of prefixes. This is that
    // union read back.
    let (store, _dir) = two_node_archive(Duration::from_secs(1)).await;

    let read = EventReader::open_many(Arc::clone(&store), &["node-a/events", "node-b/events"])
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(read.len(), 8, "a node's share went missing from the union");

    // Ordered by wall clock, which is the only column comparable across two
    // writer runs — so node A's whole session precedes node B's here.
    let walls: Vec<SystemTime> = read.iter().map(|e| e.event.ingest_ts.wall()).collect();
    assert!(
        walls.windows(2).all(|w| w[0] <= w[1]),
        "the union replayed out of wall order: {walls:?}"
    );
    let symbols: Vec<String> = read.iter().map(|e| e.event.symbol.to_string()).collect();
    assert_eq!(
        symbols,
        ["BTC-USD"; 4]
            .into_iter()
            .chain(["ETH-USD"; 4])
            .collect::<Vec<_>>()
    );

    // And the reason `replay_archive_many` cannot pace on `elapsed`: node B's
    // offsets restart at zero a second into the merged session, so the
    // sequence goes backwards. Asserted rather than described, because the
    // failure it causes — every gap saturating to zero, the archive arriving
    // in one burst — is silent, and looks exactly like a fast machine.
    let offsets: Vec<Duration> = read.iter().map(|e| e.elapsed).collect();
    assert!(
        offsets.windows(2).any(|w| w[0] > w[1]),
        "expected elapsed to restart across the node boundary, got {offsets:?}"
    );
}

#[tokio::test]
async fn overlapping_prefixes_read_each_file_once() {
    // "The bucket root and the events prefix" is exactly the pair an operator
    // reaches for, and the archive URI already has a documented footgun of
    // this shape (see the justfile's archive-s3 comment). Duplicating events
    // would replay a market with every trade counted twice.
    let (store, _dir) = two_node_archive(Duration::from_secs(1)).await;

    let read = EventReader::open_many(
        Arc::clone(&store),
        &["node-a/events", "node-a/events/symbol=BTC-USD", "node-a"],
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    assert_eq!(read.len(), 4, "an overlapping prefix replayed events twice");
}
