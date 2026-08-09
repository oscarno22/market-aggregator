//! Recovery, proven against a recording of real reconnects.
//!
//! This is the artefact `docs/DESIGN.md` listed as missing since v1 — see §4's
//! "What a recorded reconnect proves". The
//! gap-fill state machine had two kinds of evidence and neither was this one:
//! the scripted fake venue, which proves the *logic* against messages someone
//! wrote by hand, and two live tapes, which prove the *parsers* against real
//! bytes but contain no session boundary at all — both are clean runs.
//!
//! So the one path that mattered most was the one path never exercised by real
//! bytes: what each venue actually sends on resubscribe, and whether a book
//! rebuilt from it is right. The fixtures could not say, because a fixture
//! author writes the resubscribe they are imagining. That is precisely the
//! mistake that hid three parser bugs before the first tape existed
//! (`docs/DESIGN.md` §8).
//!
//! `tapes/2026-08-09-btc-usd-reconnect.jsonl.gz` closes it: 105 seconds of
//! three live venues with a reconnect forced at 30s, 55s and 80s — one venue
//! each, deliberately staggered. See the `record` binary's docs for what an
//! *induced* reconnect proves (every venue's resubscribe behaviour, and the
//! recovery built on it) and what it does not (detection — the socket was
//! closed by us, so nothing here exercises the idle watchdog).
//!
//! **Needs no network.** The recording is committed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ma_core::{Symbol, VenueId};
use ma_pipeline::aggregator::{BookStatus, Snapshot, VenueView};
use ma_pipeline::tape::{Pacing, TapeReader, replay};
use ma_server::{DEFAULT_VENUES, Pipeline};

fn tape_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tapes/2026-08-09-btc-usd-reconnect.jsonl.gz")
        .canonicalize()
        .expect("the committed reconnect tape should exist")
}

/// Replay the whole tape and return the final snapshot.
async fn run() -> Snapshot {
    let mut pipeline = Pipeline::new(vec![Symbol::new("BTC-USD")], DEFAULT_VENUES.to_vec())
        .expect("pipeline")
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
        &Symbol::new("BTC-USD"),
    )
    .await
    .expect("replay");

    assert!(stats.frames_sent > 2_000, "tape looks truncated: {stats:?}");
    assert_eq!(
        stats.dropped, 0,
        "Faithful pacing must not lose a frame; a lossy replay of a tape whose \
         whole subject is recovery would invent desyncs the recording never had"
    );

    drop(tx);
    let trigger = pipeline.into_trigger();
    let _ = tokio::time::timeout(Duration::from_secs(30), aggregator).await;
    drop(trigger);

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

fn views(snapshot: &Snapshot) -> BTreeMap<VenueId, &VenueView> {
    snapshot
        .symbol("BTC-USD")
        .expect("BTC-USD in snapshot")
        .venues
        .iter()
        .map(|v| (v.venue, v))
        .collect()
}

/// The session boundaries the tape actually carries, in order.
async fn boundaries() -> Vec<(VenueId, Duration)> {
    let mut reader = TapeReader::open(tape_path()).await.expect("open tape");
    let mut out = Vec::new();
    while let Some(frame) = reader.next_frame().await.expect("read tape") {
        if frame.session_ended {
            out.push((frame.venue, frame.elapsed));
        }
    }
    out
}

#[tokio::test]
async fn the_tape_carries_one_staggered_boundary_per_venue() {
    // Asserted against the file rather than against the run, because it is a
    // property of the *recording* and everything below depends on it. A tape
    // that quietly lost its boundaries would make every recovery assertion
    // here pass by never testing recovery at all — the failure mode that makes
    // a green suite worthless.
    let boundaries = boundaries().await;
    assert_eq!(
        boundaries.len(),
        3,
        "expected one recorded reconnect per venue, got {boundaries:?}"
    );

    let venues: Vec<VenueId> = boundaries.iter().map(|(v, _)| *v).collect();
    let mut sorted = venues.clone();
    sorted.sort_by_key(|v| v.as_str());
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        3,
        "two boundaries came from one venue, so some venue's resubscribe was \
         never recorded: {venues:?}"
    );

    // Staggered, not simultaneous. A tape where all three drop together proves
    // only that all three recover; one where they drop in turn also records
    // the other two carrying on — which is the claim "one connection per
    // (venue, symbol)" is making, and the only place it is tested against real
    // venues rather than the fake one.
    for pair in boundaries.windows(2) {
        let gap = pair[1].1.saturating_sub(pair[0].1);
        assert!(
            gap > Duration::from_secs(10),
            "boundaries {:?} and {:?} are too close together to show a venue \
             reconnecting while its neighbours keep running",
            pair[0],
            pair[1]
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_venue_rebuilds_a_trusted_book_after_a_real_reconnect() {
    let snapshot = run().await;
    let views = views(&snapshot);

    for venue in DEFAULT_VENUES {
        let v = views.get(&venue).expect("venue in snapshot");
        assert_eq!(
            v.status,
            BookStatus::Live,
            "{venue} never recovered from its reconnect: {:?}",
            v.desync_reason
        );
        assert!(v.bid.is_some() && v.ask.is_some(), "{venue} has no quote");
        assert_eq!(
            v.counters.parse_errors, 0,
            "{venue} rejected frames from its second session"
        );
        assert!(
            v.desynced_total_ms > 0,
            "{venue} reports no untrusted time across a recording that contains \
             its own disconnect — the reconnect window was not counted"
        );
    }

    // Coinbase is the venue this tape most needed to record. Its `sequence_num`
    // is connection-scoped, so a resubscribed socket restarts it from a fresh
    // base; reading that as a gap is what the first live tape caught, and the
    // fix was only ever exercised against a boundary this project synthesised
    // for itself. Now it is exercised against one Coinbase actually produced.
    assert_eq!(
        views[&VenueId::Coinbase].integrity,
        Some(ma_core::Integrity::GapDetectable),
        "Coinbase's book did not come back gap-detectable after resubscribing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn krakens_checksum_agrees_with_the_book_rebuilt_after_the_disconnect() {
    // The strongest evidence a reconnect can produce, and the reason it is
    // worth recording one. Kraken hashes the top ten levels of the book the
    // *client* built and sends its CRC32 with every message. A book assembled
    // from a snapshot Kraken sent on a brand new subscription, and then agreed
    // with continuously for the rest of the recording, is not merely "live
    // again" — it is byte-for-byte the book Kraken thinks we should hold.
    //
    // Nothing offline can produce this. A fixture's checksum is a number
    // someone computed from the fixture.
    let snapshot = run().await;
    let kraken = views(&snapshot)[&VenueId::Kraken];

    assert_eq!(kraken.status, BookStatus::Live);
    assert_eq!(kraken.integrity, Some(ma_core::Integrity::Verified));
    assert!(
        kraken.last_verified_ms.is_some(),
        "the rebuilt book was never actually verified against a checksum"
    );
    assert_eq!(
        kraken.levels_held,
        [10, 10],
        "the resubscribed depth differs from the subscribed depth, so the \
         checksum is being computed over a different book than Kraken hashed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_costs_exactly_one_desync_and_only_on_its_own_stream() {
    // Both halves of the blast-radius claim in one set of counters.
    //
    // *Exactly one*: recovery is a single transition, not a flap. A book that
    // spliced its new snapshot badly would desync again on the next delta, and
    // could well end the tape `Live` anyway once the churn settled — so the
    // final status alone cannot tell a clean recovery from a noisy one.
    //
    // *Only its own stream*: three boundaries produce three reconnect desyncs
    // in total. A resync keyed by venue rather than by stream, or a multiplexed
    // connection, would take its neighbours down with it and show up here as a
    // count nobody asked for.
    let snapshot = run().await;
    let views = views(&snapshot);

    assert_eq!(
        views[&VenueId::Coinbase].counters.desyncs,
        1,
        "Coinbase: expected exactly the one desync its own reconnect caused"
    );
    assert_eq!(
        views[&VenueId::Kraken].counters.desyncs,
        1,
        "Kraken: expected exactly the one desync its own reconnect caused"
    );
    // Bitstamp starts untrusted by construction — it sends no snapshot over the
    // socket, so it is `AwaitingSnapshot` until the first REST body lands — and
    // does that again on the new subscription. Two, and the second one is the
    // REST splice being re-run from scratch against a live venue.
    assert_eq!(
        views[&VenueId::Bitstamp].counters.desyncs,
        2,
        "Bitstamp: expected the initial AwaitingSnapshot plus its reconnect"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replaying_a_reconnect_twice_produces_the_same_books() {
    // Determinism across the path most likely to lack it. Recovery resets
    // state, discards a buffer and splices a snapshot in, and every one of
    // those is somewhere a stray ordering dependency could hide — where the
    // steady-state path already proven deterministic by `replay.rs` has none
    // of them.
    let first = comparable(&run().await);
    let second = comparable(&run().await);
    assert_eq!(
        first, second,
        "two replays of one reconnect produced different books"
    );
    assert_eq!(first.len(), DEFAULT_VENUES.len());
}

/// The comparable content of a snapshot: the books, and nothing read from a
/// clock. Same split, and same reasoning, as `replay.rs`.
fn comparable(snapshot: &Snapshot) -> BTreeMap<VenueId, String> {
    views(snapshot)
        .iter()
        .map(|(venue, v)| {
            (
                *venue,
                format!(
                    "status={:?} integrity={:?} reason={:?} bid={:?} ask={:?} \
                     levels_held={:?} applied={} desyncs={} parse_errors={}",
                    v.status,
                    v.integrity,
                    v.desync_reason,
                    v.bid.map(|l| (l.price.to_string(), l.qty.to_string())),
                    v.ask.map(|l| (l.price.to_string(), l.qty.to_string())),
                    v.levels_held,
                    v.counters.applied,
                    v.counters.desyncs,
                    v.counters.parse_errors,
                ),
            )
        })
        .collect()
}
