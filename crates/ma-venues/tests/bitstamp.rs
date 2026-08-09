//! Golden fixture tests for the Bitstamp `diff_order_book` parser and its
//! REST-splice recovery — the one venue that actually needs the algorithm the
//! original design brief described.
//!
//! The story these fixtures tell, in order:
//!
//! 1. Two diffs arrive before any snapshot exists — the ordinary state for a
//!    freshly-opened Bitstamp connection, which never sends one unprompted.
//! 2. A REST snapshot lands, timestamped *between* the two diffs.
//! 3. The earlier diff is already covered by the snapshot and must be
//!    discarded; the later one is not and must be spliced in on top.
//!
//! `diff_before_snapshot.json` deliberately sets bid `59999.00` to a
//! quantity (`0.99999999`) that the REST snapshot does *not* have, so that if
//! discarding it were ever implemented with an off-by-one, this test would
//! catch it by observing the wrong quantity survive — not just by observing
//! that *something* survived.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ma_core::{
    Book, BookState, Clock, DesyncReason, Integrity, Side, StreamId, Symbol, SystemClock, VenueId,
};
use ma_venues::BitstampSync;
use ma_venues::sync::{Outcome, RawFrame, VenueBook};
use ma_venues::venues::bitstamp::parse_rest_snapshot;

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/bitstamp/",
            $name
        ))
    };
}

fn book() -> VenueBook {
    VenueBook::new(
        Box::new(BitstampSync::new("diff_order_book_btcusd")),
        Symbol::new("BTC-USD"),
    )
}

fn frame(json: &str) -> RawFrame {
    RawFrame::new(
        StreamId::new(VenueId::Bitstamp, Symbol::new("BTC-USD")),
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
fn a_fresh_connection_is_desynced_awaiting_a_snapshot_not_uninitialized() {
    // Bitstamp never sends a snapshot over the socket, so the very first
    // frame already means "I have data I can't trust yet", not "no data".
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap();

    match vb.book().state() {
        BookState::Desynced {
            reason: DesyncReason::AwaitingSnapshot,
            ..
        } => {}
        other => panic!("expected Desynced{{AwaitingSnapshot}}, got {other:?}"),
    }
}

#[test]
fn buffered_diffs_while_awaiting_snapshot_report_no_repeated_state_change() {
    // Only the first buffered frame should surface a StateChanged outcome;
    // otherwise `since` on the Desynced state would keep sliding forward with
    // every buffered diff instead of reflecting when trust was actually lost.
    let mut vb = book();
    let first = vb
        .feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap();
    let second = vb
        .feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();

    assert!(
        first
            .iter()
            .any(|o| matches!(o, ma_venues::Outcome::StateChanged { .. })),
        "the first buffered frame should report the loss of trust"
    );
    assert!(
        second.is_empty(),
        "a second buffered frame while still awaiting a snapshot should report nothing new: {second:?}"
    );
}

#[test]
fn rest_snapshot_discards_covered_diffs_and_splices_the_rest() {
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap(); // micros ...100000, discarded
    vb.feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap(); // micros ...200000, survives

    let snapshot = parse_rest_snapshot(fixture!("rest_order_book.json")).unwrap(); // as_of ...150000
    let at = SystemClock.now();
    let outcomes = vb.apply_rest_snapshot(snapshot, at);

    assert!(
        vb.book().state().is_live(),
        "the splice should have restored trust"
    );
    assert_eq!(vb.book().state().integrity(), Some(Integrity::OrderOnly));
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, ma_venues::Outcome::StateChanged { .. })),
        "recovery from Desynced to Live must be reported"
    );

    assert_eq!(
        levels(vb.book(), Side::Bid),
        [
            ("59999.00".to_owned(), "0.10000000".to_owned()),
            ("59998.00".to_owned(), "0.35000000".to_owned()),
        ],
        "59999.00 must be the snapshot's value (the earlier diff was covered \
         and should have been discarded), and 59998.00 must reflect the \
         surviving diff spliced on top of the snapshot"
    );
    assert_eq!(
        levels(vb.book(), Side::Ask),
        [("60002.00".to_owned(), "0.40000000".to_owned())],
        "no ask diffs were spliced; this should be exactly the snapshot's value"
    );
}

#[test]
fn ordering_is_still_enforced_after_recovery() {
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap();
    vb.feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();
    let snapshot = parse_rest_snapshot(fixture!("rest_order_book.json")).unwrap();
    vb.apply_rest_snapshot(snapshot, SystemClock.now());
    assert!(vb.book().state().is_live());

    // diff_regression.json's microtimestamp (...180000) is earlier than the
    // last one actually applied during the splice (...200000).
    vb.feed(&frame(fixture!("diff_regression.json"))).unwrap();

    match vb.book().state() {
        BookState::Desynced {
            reason:
                DesyncReason::TimestampRegression {
                    last_micros,
                    got_micros,
                },
            ..
        } => {
            assert_eq!(last_micros, 1_700_000_000_200_000);
            assert_eq!(got_micros, 1_700_000_000_180_000);
        }
        other => panic!("expected a TimestampRegression desync, got {other:?}"),
    }
}

#[test]
fn a_dropped_diff_is_invisible_here_too_the_same_as_the_fake_venue_showed() {
    // The real-parser confirmation of the property `ma-venues/tests/desync.rs`
    // already proved against the fake: OrderOnly cannot see a gap. Feed the
    // "after" diff alone, as if the "before" one had simply never arrived,
    // and the book must still end up looking perfectly healthy.
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();
    let snapshot = parse_rest_snapshot(fixture!("rest_order_book.json")).unwrap();
    vb.apply_rest_snapshot(snapshot, SystemClock.now());

    assert!(
        vb.book().state().is_live(),
        "a missing diff must not be visible to an OrderOnly venue, by construction"
    );
}

#[test]
fn subscription_acks_with_an_empty_data_object_are_ignored() {
    // Bitstamp's ack carries `"data": {}` -- present, but not a valid diff
    // payload. This is the case that broke a naive `Option<DiffData>` field.
    let mut vb = book();
    let outcomes = vb.feed(&frame(fixture!("subscription_ack.json"))).unwrap();

    assert!(outcomes.is_empty());
    assert_eq!(vb.book().state(), BookState::Uninitialized);
}

#[test]
fn a_frame_for_the_wrong_channel_is_an_error() {
    let mut vb = book();
    assert!(
        vb.feed(&frame(fixture!("diff_wrong_channel.json")))
            .is_err()
    );
}

#[test]
fn a_diff_the_snapshot_already_covers_is_ignored_not_called_a_regression() {
    // The race the concurrent REST fetch creates. ma_pipeline::ingest runs the
    // depth request alongside the read loop -- deliberately, so diffs arriving
    // during the fetch are buffered rather than lost -- which means a diff
    // generated before the snapshot can be *delivered* after it.
    //
    // Such a diff is redundant by definition: the snapshot already contains
    // it. Treating it as a timestamp regression would desync a book that is
    // exactly correct, and would do so intermittently, under network timing,
    // on the venue whose integrity is already the weakest of the three.
    let mut vb = book();
    let snapshot = parse_rest_snapshot(fixture!("rest_order_book.json")).unwrap(); // as_of ...150000
    vb.apply_rest_snapshot(snapshot, SystemClock.now());
    assert!(vb.book().state().is_live());
    let before = levels(vb.book(), Side::Bid);

    // ...100000, earlier than the snapshot it would be applied on top of.
    vb.feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap();

    assert!(
        vb.book().state().is_live(),
        "a diff the snapshot already covers desynced the book: {:?}",
        vb.book().state()
    );
    assert_eq!(
        levels(vb.book(), Side::Bid),
        before,
        "a redundant diff was applied on top of the snapshot"
    );
}

#[test]
fn a_rest_body_arriving_as_a_frame_drives_the_splice() {
    // The ingest task delivers the REST body through the same channel as
    // websocket frames, tagged with FrameSource::RestSnapshot, so that a
    // recorded tape contains it and replays into a synced book. Without this
    // dispatch a Bitstamp tape could never leave AwaitingSnapshot.
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();
    assert!(
        !vb.book().state().is_live(),
        "should be awaiting a snapshot"
    );

    let rest = RawFrame::rest_snapshot(
        StreamId::new(VenueId::Bitstamp, Symbol::new("BTC-USD")),
        fixture!("rest_order_book.json").as_bytes().to_vec(),
        SystemClock.now(),
    );
    vb.feed(&rest).unwrap();

    assert!(vb.book().state().is_live());
    assert_eq!(vb.book().state().integrity(), Some(Integrity::OrderOnly));
}

#[test]
fn a_rest_body_offered_to_a_resubscribe_venue_is_an_error() {
    // Coinbase and Kraken have no REST endpoint, so a frame tagged as one of
    // their snapshots means something upstream mislabelled it. Failing loudly
    // beats parsing a websocket frame as a depth response.
    let mut vb = VenueBook::new(
        Box::new(ma_venues::KrakenSync::new("BTC/USD")),
        Symbol::new("BTC-USD"),
    );
    let rest = RawFrame::rest_snapshot(
        StreamId::new(VenueId::Kraken, Symbol::new("BTC-USD")),
        b"{}".to_vec(),
        SystemClock.now(),
    );
    assert!(vb.feed(&rest).is_err());
}

#[test]
fn a_mid_stream_rest_audit_re_anchors_without_a_buffer_to_splice() {
    // The v2 periodic re-snapshot audit: a REST snapshot can arrive while the
    // book is already Live, not just during initial recovery. There is no
    // buffer to splice in that case -- it just becomes the new ordering
    // anchor.
    //
    // Note this *does* reset the book's `since` marker: `Book::apply_snapshot`
    // always starts a fresh "live since" run, recovery or not, so a
    // StateChanged outcome here is expected, not a bug. Whether a
    // *confirming* re-audit ought instead to preserve continuous uptime
    // rather than reset it is a real question -- left open for v2's design
    // rather than answered by this test. What this test actually guards is
    // narrower and already decided: a healthy re-audit must never look like a
    // desync/recovery blip.
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();
    let snapshot = parse_rest_snapshot(fixture!("rest_order_book.json")).unwrap();
    vb.apply_rest_snapshot(snapshot.clone(), SystemClock.now());
    assert!(vb.book().state().is_live());

    let outcomes = vb.apply_rest_snapshot(snapshot, SystemClock.now());
    assert!(vb.book().state().is_live());
    assert_eq!(vb.book().state().integrity(), Some(Integrity::OrderOnly));
    for outcome in &outcomes {
        if let ma_venues::Outcome::StateChanged { from, to } = outcome {
            assert!(
                from.is_live() && to.is_live(),
                "a healthy re-audit must not look like a desync/recovery cycle: {outcome:?}"
            );
        }
    }
}

#[test]
fn the_ordering_field_and_the_venue_clock_are_the_same_number() {
    // Bitstamp is the one venue where these coincide: `microtimestamp` is both
    // the only ordering signal it offers and its wall clock. That coincidence
    // is exactly what `Integrity::OrderOnly` names -- ordering by a clock is
    // all this venue makes possible, and it cannot detect a hole.
    let mut vb = book();
    vb.feed(&frame(fixture!("diff_before_snapshot.json")))
        .unwrap();
    let rest = RawFrame::rest_snapshot(
        StreamId::new(VenueId::Bitstamp, Symbol::new("BTC-USD")),
        fixture!("rest_order_book.json").as_bytes().to_vec(),
        SystemClock.now(),
    );
    vb.feed(&rest).unwrap();

    let outcomes = vb
        .feed(&frame(fixture!("diff_after_snapshot.json")))
        .unwrap();
    let venue_ts = outcomes
        .iter()
        .find_map(|o| match o {
            Outcome::Event(e) => e.venue_ts,
            Outcome::StateChanged { .. } => None,
        })
        .expect("bitstamp stamps every diff");

    let micros = venue_ts
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch")
        .as_micros();
    assert_eq!(
        micros, 1_700_000_000_200_000,
        "the fixture's microtimestamp"
    );
}
