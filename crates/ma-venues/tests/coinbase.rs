//! Golden fixture tests for the Coinbase `l2_data` parser.
//!
//! Fixtures under `fixtures/coinbase/` are hand-authored to match the
//! documented wire shape exactly (field names, `sequence_num` semantics,
//! `side: "bid"|"ask"`, zero-quantity-deletes). They double as the seed a
//! future `just record` capture would replace once we can run against the
//! live feed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ma_core::{
    Book, BookState, Clock, DesyncReason, EventKind, Integrity, Side, StreamId, Symbol,
    SystemClock, VenueId,
};
use ma_venues::CoinbaseSync;
use ma_venues::sync::{Outcome, RawFrame, VenueBook};

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/coinbase/",
            $name
        ))
    };
}

fn book() -> VenueBook {
    VenueBook::new(
        Box::new(CoinbaseSync::new("BTC-USD")),
        Symbol::new("BTC-USD"),
    )
}

fn frame(json: &str) -> RawFrame {
    RawFrame::new(
        StreamId::new(VenueId::Coinbase, Symbol::new("BTC-USD")),
        json.as_bytes().to_vec(),
        SystemClock.now(),
    )
}

fn levels(book: &Book, side: Side) -> Vec<(String, String)> {
    book.top_levels(side, 100)
        .iter()
        .map(|l| (l.price.to_string(), l.qty.to_string()))
        .collect()
}

#[test]
fn snapshot_establishes_a_gap_detectable_live_book() {
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap();

    assert_eq!(
        vb.book().state().integrity(),
        Some(Integrity::GapDetectable)
    );
    assert_eq!(
        levels(vb.book(), Side::Bid),
        [
            ("60000.00".to_owned(), "1.20000000".to_owned()),
            ("59950.00".to_owned(), "0.50000000".to_owned())
        ]
    );
    assert_eq!(
        levels(vb.book(), Side::Ask),
        [
            ("60010.00".to_owned(), "0.75000000".to_owned()),
            ("60020.00".to_owned(), "2.00000000".to_owned())
        ]
    );
}

#[test]
fn update_applies_deltas_and_a_zero_quantity_deletes_the_level() {
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap();
    vb.feed(&frame(fixture!("update.json"))).unwrap();

    assert!(vb.book().state().is_live());
    assert_eq!(
        levels(vb.book(), Side::Bid)[0],
        ("60000.00".to_owned(), "0.90000000".to_owned())
    );
    assert_eq!(
        levels(vb.book(), Side::Ask),
        [("60020.00".to_owned(), "2.00000000".to_owned())],
        "ask 60010.00 carried new_quantity \"0\" and should have been deleted"
    );
}

#[test]
fn a_sequence_gap_desyncs_the_book() {
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap(); // seq 0
    vb.feed(&frame(fixture!("update.json"))).unwrap(); // seq 1
    vb.feed(&frame(fixture!("update_gap.json"))).unwrap(); // seq 3, skipping 2

    match vb.book().state() {
        BookState::Desynced {
            reason: DesyncReason::SequenceGap { expected, got },
            ..
        } => {
            assert_eq!(expected, 2);
            assert_eq!(got, 3);
        }
        other => panic!("expected a SequenceGap desync, got {other:?}"),
    }
}

#[test]
fn heartbeats_forward_as_events_without_touching_book_trust() {
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap();
    let outcomes = vb.feed(&frame(fixture!("heartbeat.json"))).unwrap();

    assert!(
        vb.book().state().is_live(),
        "a heartbeat must not affect book trust"
    );
    let forwarded = outcomes.iter().any(|o| {
        matches!(
            o,
            Outcome::Event(ev) if matches!(ev.kind, EventKind::Heartbeat { counter: Some(42) })
        )
    });
    assert!(
        forwarded,
        "heartbeat_counter 42 was not forwarded: {outcomes:?}"
    );
}

#[test]
fn subscription_acks_are_ignored_and_create_no_book() {
    let mut vb = book();
    let outcomes = vb.feed(&frame(fixture!("subscriptions_ack.json"))).unwrap();

    assert!(
        outcomes.is_empty(),
        "an ack alone should produce no outcomes"
    );
    assert_eq!(
        vb.book().state(),
        BookState::Uninitialized,
        "an ack must not be mistaken for book content"
    );
}

#[test]
fn events_for_a_different_product_are_skipped_not_applied() {
    // A connection sharing multiple products would interleave their events;
    // ours must ignore what isn't ours without treating it as an error, and
    // — since sequence_num is a property of the connection, not the product —
    // without breaking gap detection for our own product either.
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap(); // seq 0, BTC-USD
    let outcomes = vb
        .feed(&frame(fixture!("update_other_product.json")))
        .unwrap(); // seq 1, ETH-USD

    assert!(
        outcomes.is_empty(),
        "a foreign product's event should produce no outcomes"
    );
    assert!(
        vb.book().state().is_live(),
        "seq 0 -> 1 is contiguous; this must not desync"
    );
    assert_eq!(
        levels(vb.book(), Side::Bid)[0],
        ("60000.00".to_owned(), "1.20000000".to_owned()),
        "the foreign product's update must not have touched our book"
    );
}

#[test]
fn both_spellings_of_the_ask_side_are_accepted() {
    // Coinbase's live l2_data uses "offer"; parts of the documentation, and
    // this crate's hand-authored fixtures, use "ask". Getting this wrong fails
    // in a nasty way rather than an obvious one: every ask update becomes a
    // parse error, the book holds bids only, and a one-sided book can never
    // cross, so the crossed-book detector -- the last-resort correctness check
    // -- never fires either.
    let snapshot = |side: &str| {
        format!(
            r#"{{"channel":"l2_data","sequence_num":0,"events":[{{"type":"snapshot",
               "product_id":"BTC-USD","updates":[
                 {{"side":"bid","price_level":"100.00","new_quantity":"1"}},
                 {{"side":"{side}","price_level":"101.00","new_quantity":"2"}}]}}]}}"#
        )
    };

    for side in ["ask", "offer"] {
        let mut vb = book();
        vb.feed(&frame(&snapshot(side)))
            .unwrap_or_else(|e| panic!("side {side:?} did not parse: {e}"));
        assert_eq!(
            levels(vb.book(), Side::Ask),
            [("101.00".to_owned(), "2".to_owned())],
            "side {side:?} produced no ask levels"
        );
    }
}

#[test]
fn sequence_numbers_count_every_message_on_the_connection() {
    // Not just the ones on l2_data. Coinbase numbers the *connection*, and
    // this project necessarily subscribes to two channels — heartbeats is not
    // optional, because Coinbase closes a sparse subscription after 60-90s.
    //
    // Counting only l2_data meant every heartbeat and every subscription ack
    // read as a hole. Against a real 25-second recording the book desynced
    // within two messages and then kept re-desyncing, so a genuine gap and a
    // perfectly healthy connection produced identical output. No hand-written
    // fixture caught it, because a fixture author numbers the messages they
    // are thinking about; replaying a live tape did.
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap(); // seq 0, l2_data
    vb.feed(&frame(fixture!("heartbeat.json"))).unwrap(); // seq 1, heartbeats

    assert!(
        vb.book().state().is_live(),
        "a heartbeat consuming a sequence number must not read as a gap: {:?}",
        vb.book().state()
    );

    // ...and the numbering must still be enforced across channels: an
    // l2_data at 3 after a heartbeat at 1 has skipped 2, whatever channel 2
    // was on.
    let mut vb = book();
    vb.feed(&frame(fixture!("snapshot.json"))).unwrap(); // 0
    vb.feed(&frame(fixture!("heartbeat.json"))).unwrap(); // 1
    vb.feed(&frame(fixture!("update_gap.json"))).unwrap(); // 3

    match vb.book().state() {
        BookState::Desynced {
            reason: DesyncReason::SequenceGap { expected, got },
            ..
        } => {
            assert_eq!((expected, got), (2, 3));
        }
        other => panic!("expected a SequenceGap across channels, got {other:?}"),
    }
}

#[test]
fn the_venue_clock_is_reported_but_never_used_for_ordering() {
    // v1 carried `venue_ts` on `MarketEvent` and never populated it, so clock
    // skew was structurally unmeasurable and v2's Parquet `venue_ts` column
    // would have been entirely null. Coinbase stamps its envelope in RFC 3339
    // with nanosecond precision.
    let mut vb = book();
    let outcomes = vb.feed(&frame(fixture!("snapshot.json"))).unwrap();

    let event = outcomes
        .iter()
        .find_map(|o| match o {
            Outcome::Event(e) if matches!(e.kind, EventKind::Snapshot { .. }) => Some(e),
            _ => None,
        })
        .expect("a snapshot event");

    let venue_ts = event.venue_ts.expect("coinbase stamps every envelope");
    let nanos = venue_ts
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch")
        .as_nanos();
    assert_eq!(nanos, 1_786_190_400_000_000_000, "2026-08-08T12:00:00Z");

    // And the rule that makes carrying it safe: ordering and book age come off
    // `ingest_ts`, never this. `docs/DESIGN.md` §6.
    assert_ne!(
        event.ingest_ts.wall(),
        venue_ts,
        "the venue clock must not have become the ingest clock"
    );
}
