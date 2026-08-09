//! The trade parsers, proven against real bytes — and, unplanned, the first
//! recording of Bitstamp's only loss signal firing against live data.
//!
//! Commit-ordering matters here and is deliberate: the venue-layer trade
//! parsers landed one commit before this tape existed, verified only by
//! hand-authored fixtures — and this project's own history says a fixture
//! suite can be fully green over a broken parser, three separate times
//! (`docs/DESIGN.md` §8). This tape is the authority the fixtures were
//! standing in for.
//!
//! `tapes/2026-08-09-btc-usd-trades.jsonl.gz`: 120 seconds of all three
//! venues with their trades channels subscribed beside the book. What the
//! recording settled, that the fixtures could only assume:
//!
//! - Coinbase `market_trades` opens with exactly one `snapshot` burst of
//!   recent history (dropped, or every reconnect would replay it) and sends
//!   `update` events after; `side` arrives upper-case `BUY`/`SELL`; 152
//!   update events in two minutes.
//! - Kraken's `trade` subscription ack carries `"snapshot": false` and no
//!   snapshot burst arrived; the drop-the-burst path stays as defence, but
//!   on this venue it is precaution rather than observed necessity. Prices
//!   and quantities are bare JSON numbers — what `exact_decimal` is for.
//! - Bitstamp `live_trades_*` frames carry both float (`price`, `amount`)
//!   and string (`price_str`, `amount_str`) forms; the parser reads only
//!   the strings. `type` is 0/1.
//!
//! # The find
//!
//! Two minutes was enough for the recording to catch a real integrity
//! failure: partway in, **Bitstamp's own diff stream produces a crossed
//! book**. Reconstructing the book offline from the tape's diffs alone —
//! REST snapshot frame, then every `diff_order_book` message, no trade
//! frames involved — reproduces the cross exactly, at the same prices the
//! desync reason names. A diff was lost or reordered somewhere between the
//! venue's matching engine and our socket, the stale level sat there until
//! the other side moved past it, and the crossed-book guard — the one loss
//! signal an `OrderOnly` venue has — fired. In a live run the resync path
//! would refetch and recover; in replay nothing serves REST, so the tape
//! ends with the book honestly untrusted. The assertions below pin all of
//! it, because a recording of the failure mode §3 of the README can only
//! describe is worth more than a clean run.
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
        .join("../../tapes/2026-08-09-btc-usd-trades.jsonl.gz")
        .canonicalize()
        .expect("the committed trades tape should exist")
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

    assert!(stats.frames_sent > 4_000, "tape looks truncated: {stats:?}");
    assert_eq!(stats.dropped, 0, "Faithful pacing must not lose a frame");

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

#[tokio::test(flavor = "multi_thread")]
async fn every_venue_parses_its_own_trade_channel_from_real_bytes() {
    let snapshot = run().await;
    let views = views(&snapshot);

    for venue in DEFAULT_VENUES {
        let v = views.get(&venue).expect("venue in snapshot");
        assert_eq!(
            v.counters.parse_errors, 0,
            "{venue} rejected frames from a channel it subscribes to — the \
             parser and the wire have drifted"
        );
        assert!(
            v.counters.trades > 0,
            "{venue} subscribed to trades for two minutes on BTC-USD and \
             forwarded none — the subscribe payload and the parser disagree \
             about the channel"
        );
        let trade = v
            .last_trade
            .as_ref()
            .unwrap_or_else(|| panic!("{venue} counted trades but surfaced none"));
        assert!(
            trade.qty.parse::<f64>().map(|q| q > 0.0).unwrap_or(false),
            "{venue} last trade qty is not positive: {trade:?}"
        );
        assert!(
            trade.taker_side.is_some(),
            "{venue} sends a side with every print on this recording; losing \
             it means the side parse is wrong, not that the venue went quiet"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_two_verifying_venues_end_live_with_zero_desyncs() {
    // The interleaving claims from the venue layer, held against two minutes
    // of real traffic: Coinbase's connection-scoped sequence check counted
    // the third channel (or every trade frame reads as a book gap — there
    // were 153 of them), and Kraken's checksum state was untouched by 20
    // interleaved prints (or per-message verification fails within seconds).
    let snapshot = run().await;
    let views = views(&snapshot);

    for venue in [VenueId::Coinbase, VenueId::Kraken] {
        let v = views[&venue];
        assert_eq!(
            v.status,
            BookStatus::Live,
            "{venue} ended untrusted: {:?}",
            v.desync_reason
        );
        assert_eq!(
            v.counters.desyncs, 0,
            "{venue}: a desync on this recording means a trade frame was \
             read as book damage"
        );
    }

    let kraken = views[&VenueId::Kraken];
    assert_eq!(kraken.integrity, Some(ma_core::Integrity::Verified));
    assert!(
        kraken.last_verified_ms.is_some(),
        "the book was never verified against a checksum"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_tape_caught_bitstamps_one_loss_signal_firing_for_real() {
    // See the module docs: the diff stream alone — no trade frame involved,
    // proven by offline reconstruction — produces a crossed book partway
    // through the recording. This is the failure OrderOnly cannot see any
    // other way, caught on tape for the first time, and the assertions pin
    // the system's whole response to it.
    let snapshot = run().await;
    let views = views(&snapshot);
    let v = views[&VenueId::Bitstamp];

    assert_eq!(
        v.status,
        BookStatus::Desynced,
        "the crossed book in this recording was not detected"
    );
    let reason = v.desync_reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("crossed book"),
        "desynced for a different reason than the recording contains: {reason:?}"
    );
    // The exact prices, because the replay is deterministic and this pins
    // the desync to the same cross the offline reconstruction locates in
    // the raw diffs — not to anything a trade frame did.
    assert!(
        reason.contains("65028.17") && reason.contains("65028.16"),
        "the cross moved: {reason:?} — the book being built from this tape \
         has changed"
    );
    assert_eq!(
        v.counters.desyncs, 2,
        "expected the structural AwaitingSnapshot plus the crossed book"
    );

    // And the design point the trade path exists for: prints kept flowing
    // and kept being counted while the book was untrusted, because a print
    // is the venue's fact about its own matches, not a claim about our book.
    assert!(
        v.counters.trades > 0,
        "an untrusted book stopped counting trades"
    );
    assert!(
        v.last_trade.is_some(),
        "an untrusted book stopped surfacing the last print"
    );
}
