//! End-to-end replay against a real recording.
//!
//! Everything else in the suite drives one component with input written by
//! someone who already knew what the component does. This file drives the
//! whole pipeline — channel, sync state machines, books, checksum, aggregator,
//! metrics — with 60 seconds of bytes three real venues actually sent, and
//! nothing in it was authored by hand.
//!
//! That difference is the point. The hand-written fixtures were all passing
//! while three separate bugs sat in the parsers, because a fixture author
//! writes the messages they are thinking about: Coinbase's `sequence_num`
//! counts every message on the connection rather than every `l2_data`
//! message, Kraken sends a `status` frame with no `symbol`, and Coinbase says
//! `offer` where the documentation says `ask`. A tape does not have opinions
//! about which messages matter.
//!
//! **This test needs no network.** The recording is committed. If it ever
//! starts failing, either the pipeline regressed or a venue changed its wire
//! format — and distinguishing those is what `git diff` on the tape is for.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ma_core::{Symbol, VenueId};
use ma_pipeline::aggregator::{BookStatus, Snapshot};
use ma_pipeline::tape::{Pacing, TapeReader, replay};
use ma_server::{DEFAULT_VENUES, Pipeline};

fn tape_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tapes/2026-08-09-btc-usd.jsonl.gz")
        .canonicalize()
        .expect("the committed tape should exist")
}

/// Replay the whole tape and return the final snapshot.
async fn run() -> Snapshot {
    let mut pipeline = Pipeline::new(vec![Symbol::new("BTC-USD")], DEFAULT_VENUES.to_vec())
        .expect("pipeline")
        // Fast enough that the run does not depend on wall-clock timing, and
        // the final snapshot is what everything gets asserted against anyway.
        .with_tick(Duration::from_millis(5));

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
        // The committed tape predates the tape format's symbol field, so
        // replay is told what it holds. That this still works is the point of
        // the field being optional: a v1 recording, the artefact that found
        // three real parser bugs, keeps replaying unchanged.
        &Symbol::new("BTC-USD"),
    )
    .await
    .expect("replay");

    assert!(stats.frames_sent > 1_000, "tape looks truncated: {stats:?}");
    assert_eq!(
        stats.dropped, 0,
        "Faithful pacing must not lose a single frame; a lossy replay cannot \
         be deterministic and will invent desyncs the recording never had"
    );

    // Close the producer so the aggregator drains and publishes its final
    // snapshot, then take the last one it sent.
    drop(tx);
    let trigger = pipeline.into_trigger();
    let _ = tokio::time::timeout(Duration::from_secs(20), aggregator).await;
    drop(trigger);

    // Lagging is expected here and must not end the drain: this subscriber
    // never polled while the tape was being applied, so it is far behind the
    // 32-snapshot ring. Skipping forward is exactly what the SSE handler does
    // for a slow browser, and getting it wrong here reads as "the aggregator
    // published nothing" when it published hundreds.
    use tokio::sync::broadcast::error::TryRecvError;
    let mut last = None;
    loop {
        match snapshots.try_recv() {
            Ok(snapshot) => last = Some(snapshot),
            Err(TryRecvError::Lagged(_)) => {}
            Err(TryRecvError::Empty | TryRecvError::Closed) => break,
        }
    }
    Arc::try_unwrap(last.expect("the aggregator published nothing"))
        .unwrap_or_else(|arc| (*arc).clone())
}

/// The comparable content of a snapshot: everything except the parts that are
/// *supposed* to differ between two runs.
///
/// `seq` counts ticks, and `wall_unix_ms` and every `_ms` duration are read
/// from a clock, so all of them legitimately vary with how fast the machine
/// happened to be. What must not vary is the books: the same bytes applied in
/// the same order have to produce the same prices, the same depth, and the
/// same trust.
fn comparable(snapshot: &Snapshot) -> BTreeMap<VenueId, String> {
    btc(snapshot)
        .venues
        .iter()
        .map(|v| {
            (
                v.venue,
                format!(
                    "status={:?} integrity={:?} reason={:?} bid={:?} ask={:?} \
                     spread={:?} levels_held={:?} bids={:?} asks={:?} \
                     applied={} desyncs={} parse_errors={}",
                    v.status,
                    v.integrity,
                    v.desync_reason,
                    v.bid.map(|l| (l.price.to_string(), l.qty.to_string())),
                    v.ask.map(|l| (l.price.to_string(), l.qty.to_string())),
                    v.spread,
                    v.levels_held,
                    ladder(&v.bids),
                    ladder(&v.asks),
                    v.counters.applied,
                    v.counters.desyncs,
                    v.counters.parse_errors,
                ),
            )
        })
        .collect()
}

/// The one symbol this tape holds.
fn btc(snapshot: &Snapshot) -> &ma_pipeline::SymbolView {
    snapshot.symbol("BTC-USD").expect("BTC-USD in snapshot")
}

/// A depth ladder as comparable text.
fn ladder(levels: &[ma_core::Level]) -> Vec<(String, String)> {
    levels
        .iter()
        .map(|l| (l.price.to_string(), l.qty.to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_session_reaches_a_trusted_book_at_every_venue() {
    let snapshot = run().await;
    let group = btc(&snapshot);
    let views: BTreeMap<VenueId, _> = group.venues.iter().map(|v| (v.venue, v)).collect();

    for venue in DEFAULT_VENUES {
        let v = views.get(&venue).expect("venue in snapshot");
        assert_eq!(
            v.status,
            BookStatus::Live,
            "{venue} did not reach a trusted book: {:?}",
            v.desync_reason
        );
        assert!(v.bid.is_some() && v.ask.is_some(), "{venue} has no quote");
        assert_eq!(v.counters.parse_errors, 0, "{venue} rejected live frames");
    }

    // Each venue's integrity is a property of its protocol, not of how well
    // this run went, so these are fixed expectations rather than observations.
    assert_eq!(
        views[&VenueId::Kraken].integrity,
        Some(ma_core::Integrity::Verified),
        "Kraken's CRC32 must match the book we built, on live data"
    );
    assert_eq!(
        views[&VenueId::Coinbase].integrity,
        Some(ma_core::Integrity::GapDetectable)
    );
    assert_eq!(
        views[&VenueId::Bitstamp].integrity,
        Some(ma_core::Integrity::OrderOnly)
    );
    assert_eq!(
        group.weakest_integrity,
        Some(ma_core::Integrity::OrderOnly),
        "a combined view must report its weakest input"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kraken_stays_checksum_verified_across_the_whole_recording() {
    // The strongest correctness evidence in the project, and the only one that
    // is not self-referential: Kraken hashes the top 10 levels of the book the
    // *client* built and sends its own CRC32 with every message. Matching it
    // continuously over 60 seconds of live updates means our book is byte-for-
    // byte the book Kraken thinks we should have — including the exact decimal
    // digits, which is what `Price` wrapping `Decimal` instead of `f64` buys.
    let snapshot = run().await;
    let kraken = btc(&snapshot)
        .venues
        .iter()
        .find(|v| v.venue == VenueId::Kraken)
        .expect("kraken");

    assert_eq!(kraken.status, BookStatus::Live);
    assert_eq!(kraken.integrity, Some(ma_core::Integrity::Verified));
    assert_eq!(
        kraken.counters.desyncs, 0,
        "the checksum disagreed with our book at least once"
    );
    assert_eq!(
        kraken.desynced_total_ms, 0,
        "Kraken spent time untrusted during a clean recording"
    );
    assert!(
        kraken.last_verified_ms.is_some(),
        "a Verified book must know when it was last actually verified"
    );
    assert_eq!(
        kraken.levels_held,
        [10, 10],
        "the subscribed depth and the retained depth must agree, or the \
         checksum is being computed over a different book than Kraken hashed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bitstamp_recovers_through_the_rest_splice_and_only_once() {
    // Bitstamp sends no snapshot over the socket, so it must start untrusted,
    // buffer, and be repaired by a REST body that travelled the tape as a
    // frame. Exactly one desync means the splice worked first time — the
    // buffered diffs joined onto the snapshot without the ordering check
    // firing afterwards.
    let snapshot = run().await;
    let bitstamp = btc(&snapshot)
        .venues
        .iter()
        .find(|v| v.venue == VenueId::Bitstamp)
        .expect("bitstamp");

    assert_eq!(bitstamp.status, BookStatus::Live);
    assert_eq!(
        bitstamp.counters.desyncs, 1,
        "expected exactly one desync — the initial AwaitingSnapshot"
    );
    assert!(
        bitstamp.desynced_total_ms > 0,
        "the wait for the REST snapshot should be counted as untrusted time"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replaying_the_same_tape_twice_produces_the_same_books() {
    // The determinism property the plan asks for. It is what makes every other
    // offline test trustworthy: if the pipeline could reach different states
    // from identical input, a passing run would mean nothing.
    //
    // Timestamps and tick counts are excluded deliberately — see `comparable`.
    // Those are supposed to differ; the books are not.
    let first = comparable(&run().await);
    let second = comparable(&run().await);

    assert_eq!(
        first, second,
        "two replays of one tape produced different books"
    );
    assert_eq!(first.len(), DEFAULT_VENUES.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_round_trips_through_json_unchanged() {
    // v4 made `Snapshot` a wire format in *both* directions: the gateway parses
    // what a node publishes. For three milestones it was write-only, so nothing
    // ever checked that what goes out can come back — and a field that
    // serialises but cannot be read would break nothing visible until a cluster
    // was actually merged.
    //
    // Run against a snapshot off the real tape rather than a hand-built one, so
    // every optional field that only appears with real data — a desync reason, a
    // Kraken `last_verified_ms`, an audit count, a populated ladder, a window
    // reading with `None` prices — is actually present to be lost.
    let snapshot = run().await;
    let out = serde_json::to_value(&snapshot).expect("serialise");
    let back: Snapshot = serde_json::from_value(out.clone()).expect("the snapshot did not parse");
    let again = serde_json::to_value(&back).expect("re-serialise");

    assert_eq!(
        out, again,
        "a snapshot changed shape through a JSON round trip"
    );
    // Not vacuous: the tape produces books, ladders and counters, so an empty
    // document passing this comparison would still be a failure.
    assert!(
        out.get("symbols")
            .and_then(|s| s.as_array())
            .is_some_and(|s| !s.is_empty()),
        "the round trip was tested against an empty snapshot"
    );
    assert_eq!(back.symbol("BTC-USD").expect("BTC-USD").venues.len(), 3);
}
