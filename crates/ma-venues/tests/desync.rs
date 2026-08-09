//! Gap-fill correctness, proven offline.
//!
//! The brief states that gap-fill correctness is proven against a fake venue
//! rather than in production. This file is where that happens.
//!
//! The organising idea: run **one** scripted stream, damaged in **one** way,
//! under **all three** integrity disciplines, and assert that each one detects
//! exactly what its protocol makes detectable — no more, and no less. The "no
//! more" half matters as much as the other: if a `OrderOnly` venue ever starts
//! catching dropped messages in these tests, the enum has stopped telling the
//! truth and the v2 REST-audit work is being justified by a fiction.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ma_core::{Book, BookState, DesyncReason, Integrity, Side, Symbol};
use ma_venues::fake::{FakeSync, Script, Tape};
use ma_venues::sync::{Outcome, RecoveryStrategy, VenueBook};

const ALL: [Integrity; 3] = [
    Integrity::OrderOnly,
    Integrity::GapDetectable,
    Integrity::Verified,
];

/// A short, well-formed session: snapshot, three deltas, a checkpoint.
///
/// Frame indices, which the damage functions below refer to:
/// ```text
///   0  snapshot  seq 1   bids 100:1 99:2 / asks 101:1 102:2
///   1  delta     seq 2   bid 100 -> 5
///   2  delta     seq 3   bid 98 -> 3        (new level)
///   3  delta     seq 4   ask 101 -> 7
///   4  checksum  seq 5
/// ```
fn session() -> Tape {
    Script::new()
        .snapshot(&[("100", "1"), ("99", "2")], &[("101", "1"), ("102", "2")])
        .delta(&[("100", "5")], &[])
        .delta(&[("98", "3")], &[])
        .delta(&[], &[("101", "7")])
        .checkpoint()
        .build()
}

fn run(tape: &Tape, integrity: Integrity) -> VenueBook {
    let mut vb = VenueBook::new(Box::new(FakeSync::new(integrity)), Symbol::new("BTC-USD"));
    for frame in tape.frames() {
        vb.feed(frame).expect("fake tapes are always parseable");
    }
    vb
}

fn run_collecting(tape: &Tape, integrity: Integrity) -> (VenueBook, Vec<Outcome>) {
    let mut vb = VenueBook::new(Box::new(FakeSync::new(integrity)), Symbol::new("BTC-USD"));
    let mut outcomes = Vec::new();
    for frame in tape.frames() {
        outcomes.extend(vb.feed(frame).expect("fake tapes are always parseable"));
    }
    (vb, outcomes)
}

fn levels(book: &Book, side: Side) -> Vec<(String, String)> {
    book.top_levels(side, 100)
        .iter()
        .map(|l| (l.price.to_string(), l.qty.to_string()))
        .collect()
}

fn desync_reason(book: &Book) -> Option<DesyncReason> {
    match book.state() {
        BookState::Desynced { reason, .. } => Some(reason),
        _ => None,
    }
}

// ---------------------------------------------------------------- clean case

#[test]
fn a_clean_stream_ends_live_at_the_venues_own_integrity() {
    for integrity in ALL {
        let vb = run(&session(), integrity);
        assert_eq!(
            vb.book().state().integrity(),
            Some(integrity),
            "{integrity:?}: clean stream should end live"
        );
    }
}

#[test]
fn a_clean_stream_builds_the_same_book_under_every_discipline() {
    // Integrity governs what gets *noticed*, never what gets applied. If these
    // ever diverge, a detection path has started mutating the book.
    let reference = run(&session(), Integrity::Verified);
    for integrity in [Integrity::OrderOnly, Integrity::GapDetectable] {
        let vb = run(&session(), integrity);
        assert_eq!(
            levels(vb.book(), Side::Bid),
            levels(reference.book(), Side::Bid),
            "{integrity:?}: bids diverged from the reference run"
        );
        assert_eq!(
            levels(vb.book(), Side::Ask),
            levels(reference.book(), Side::Ask),
            "{integrity:?}: asks diverged from the reference run"
        );
    }
}

#[test]
fn a_clean_stream_leaves_a_verified_book_actually_verified() {
    let vb = run(&session(), Integrity::Verified);
    match vb.book().state() {
        BookState::Live { last_verified, .. } => assert!(
            last_verified.is_some(),
            "checkpoint did not record a successful verification"
        ),
        other => panic!("expected Live, got {other:?}"),
    }
}

// ------------------------------------------------------------- a lost message

#[test]
fn a_dropped_message_is_caught_by_sequence_numbers() {
    // Coinbase's discipline: the hole is visible the moment the next frame
    // lands, before any wrong data reaches the book.
    let vb = run(&session().drop_at(2), Integrity::GapDetectable);
    assert_eq!(
        desync_reason(vb.book()),
        Some(DesyncReason::SequenceGap {
            expected: 3,
            got: 4
        })
    );
}

#[test]
fn a_dropped_message_is_caught_by_checksum_with_no_sequence_numbers_at_all() {
    // Kraken's discipline, and the reason it is the strongest of the three:
    // the checksum validates the book we *built*, so it catches loss without
    // any ordering field to reason about — and would equally catch a delta we
    // applied to the wrong side, which sequence numbers never would.
    let vb = run(&session().drop_at(2), Integrity::Verified);
    match desync_reason(vb.book()) {
        Some(DesyncReason::ChecksumMismatch { .. }) => {}
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }
}

#[test]
fn a_dropped_message_is_invisible_to_an_order_only_venue() {
    // The load-bearing test in this file.
    //
    // Bitstamp gives us monotonic microtimestamps and nothing else. A dropped
    // diff leaves no trace: timestamps still increase, so the stream looks
    // healthy while the book is quietly wrong from that point on.
    //
    // This test asserts the *absence* of detection deliberately. It is the
    // evidence behind `Integrity::OrderOnly`, and behind the periodic REST
    // re-snapshot audit planned for v2 — without which this failure mode has
    // no mitigation at all.
    let damaged = run(&session().drop_at(2), Integrity::OrderOnly);
    let clean = run(&session(), Integrity::OrderOnly);

    assert!(
        damaged.book().state().is_live(),
        "OrderOnly cannot detect a gap; if it now does, update Integrity's docs \
         and reconsider whether the v2 REST audit is still needed"
    );

    // ...and the book really is wrong, which is what makes the above alarming
    // rather than merely tolerable.
    assert_ne!(
        levels(damaged.book(), Side::Bid),
        levels(clean.book(), Side::Bid),
        "the dropped delta should have left the book measurably wrong"
    );
    assert_eq!(
        levels(damaged.book(), Side::Bid),
        [("100".into(), "5".into()), ("99".into(), "2".into())],
        "expected the 98 level to be missing"
    );
}

// --------------------------------------------------- duplicates and reordering

#[test]
fn a_duplicated_message_is_caught_by_both_ordered_disciplines() {
    // Venues replay on reconnect, so duplicates are routine rather than exotic.
    let vb = run(&session().duplicate_at(1), Integrity::GapDetectable);
    assert_eq!(
        desync_reason(vb.book()),
        Some(DesyncReason::SequenceGap {
            expected: 3,
            got: 2
        })
    );

    // OrderOnly requires strictly increasing timestamps, so it catches this one
    // even though it cannot catch a gap.
    let vb = run(&session().duplicate_at(1), Integrity::OrderOnly);
    assert!(matches!(
        desync_reason(vb.book()),
        Some(DesyncReason::TimestampRegression { .. })
    ));
}

#[test]
fn reordered_messages_are_caught_by_both_ordered_disciplines() {
    for integrity in [Integrity::OrderOnly, Integrity::GapDetectable] {
        let vb = run(&session().swap(1, 2), integrity);
        assert!(
            !vb.book().state().is_live(),
            "{integrity:?} failed to notice reordering"
        );
    }
}

// -------------------------------------------------------------------- recovery

/// A session that desyncs partway through and then receives a fresh snapshot.
///
/// ```text
///   0  snapshot  seq 1
///   1  delta     seq 2      <- dropped, causing the gap
///   2  delta     seq 3
///   3  delta     seq 4      <- arrives while desynced; must NOT be applied
///   4  snapshot  seq 5      <- recovery
/// ```
fn recovery_session() -> Tape {
    Script::new()
        .snapshot(&[("100", "1")], &[("101", "1")])
        .delta(&[("99", "1")], &[])
        .delta(&[("98", "1")], &[])
        .delta(&[("97", "1")], &[])
        .snapshot(&[("200", "1")], &[("201", "1")])
        .build()
        .drop_at(1)
}

#[test]
fn a_snapshot_restores_a_desynced_book() {
    let vb = run(&recovery_session(), Integrity::GapDetectable);
    assert!(
        vb.book().state().is_live(),
        "the recovery snapshot should have restored trust"
    );
    assert_eq!(
        levels(vb.book(), Side::Bid),
        [("200".into(), "1".into())],
        "recovered book should be exactly the recovery snapshot"
    );
}

#[test]
fn deltas_arriving_while_desynced_are_discarded_not_applied() {
    // The single most important rule in the whole pipeline. Applying deltas to
    // an untrusted book produces something that looks plausible and is wrong,
    // which the brief correctly calls worse than being obviously down.
    let tape = recovery_session();
    let mut vb = VenueBook::new(
        Box::new(FakeSync::new(Integrity::GapDetectable)),
        Symbol::new("BTC-USD"),
    );

    let mut bids_while_desynced = Vec::new();
    for frame in tape.frames() {
        vb.feed(frame).unwrap();
        if !vb.book().state().is_live() {
            bids_while_desynced.push(levels(vb.book(), Side::Bid));
        }
    }

    assert!(
        !bids_while_desynced.is_empty(),
        "test is not exercising the desynced path at all"
    );
    for observed in &bids_while_desynced {
        assert_eq!(
            observed,
            &[("100".to_owned(), "1".to_owned())],
            "book mutated while desynced"
        );
    }
}

#[test]
fn a_reconnect_distrusts_the_book_until_a_snapshot_arrives() {
    let tape = session();
    let mut vb = VenueBook::new(
        Box::new(FakeSync::new(Integrity::GapDetectable)),
        Symbol::new("BTC-USD"),
    );
    for frame in tape.frames() {
        vb.feed(frame).unwrap();
    }
    assert!(vb.book().state().is_live());

    let at = tape.frames().last().unwrap().ingest_ts;
    vb.reset(at);

    assert_eq!(desync_reason(vb.book()), Some(DesyncReason::ConnectionLost));
}

#[test]
fn a_reconnect_clears_the_sequence_expectation() {
    // If reset() forgot to clear it, the first frame after reconnecting would
    // be reported as a spurious gap and the book would never recover.
    let tape = session();
    let mut vb = VenueBook::new(
        Box::new(FakeSync::new(Integrity::GapDetectable)),
        Symbol::new("BTC-USD"),
    );
    for frame in tape.frames() {
        vb.feed(frame).unwrap();
    }
    vb.reset(tape.frames().last().unwrap().ingest_ts);

    // Replay the same session from the top, as a real resubscribe would.
    for frame in tape.frames() {
        vb.feed(frame).unwrap();
    }
    assert!(
        vb.book().state().is_live(),
        "book failed to recover after a clean resubscribe"
    );
}

// ------------------------------------------------------------------ reporting

#[test]
fn every_state_change_is_reported() {
    // A silent transition is the failure this project exists to prevent, so the
    // outcomes stream must carry both the loss of trust and its recovery.
    let (_, outcomes) = run_collecting(&recovery_session(), Integrity::GapDetectable);

    let transitions: Vec<(bool, bool)> = outcomes
        .iter()
        .filter_map(|o| match o {
            Outcome::StateChanged { from, to } => Some((from.is_live(), to.is_live())),
            Outcome::Event(_) => None,
        })
        .collect();

    assert!(
        transitions.contains(&(true, false)),
        "loss of trust was never reported: {transitions:?}"
    );
    assert!(
        transitions.contains(&(false, true)),
        "recovery was never reported: {transitions:?}"
    );
}

// ------------------------------------------------------------------- wiring

#[test]
fn recovery_strategy_follows_where_the_snapshot_comes_from() {
    // Venues that resend a snapshot unprompted recover by resubscribing; the
    // one that does not has to go and fetch it over REST.
    let resubscribe = VenueBook::new(
        Box::new(FakeSync::new(Integrity::Verified)),
        Symbol::new("BTC-USD"),
    );
    assert_eq!(resubscribe.recovery(), RecoveryStrategy::Resubscribe);

    let rest = VenueBook::new(
        Box::new(FakeSync::new(Integrity::OrderOnly)),
        Symbol::new("BTC-USD"),
    );
    assert_eq!(rest.recovery(), RecoveryStrategy::RestSnapshot);
}

#[test]
fn a_truncated_stream_leaves_the_book_stale_rather_than_wrong() {
    // A dropped connection mid-session is not corruption: what we have is
    // correct as of the last frame. Staleness is the aggregator's problem,
    // signalled by book age, not by desync.
    let vb = run(&session().truncate_from(3), Integrity::GapDetectable);
    assert!(vb.book().state().is_live());
    assert_eq!(
        levels(vb.book(), Side::Bid),
        [
            ("100".into(), "5".into()),
            ("99".into(), "2".into()),
            ("98".into(), "3".into())
        ]
    );
}

#[test]
fn a_malformed_frame_is_an_error_not_a_silent_skip() {
    use ma_core::{Clock, SystemClock, VenueId};
    use ma_venues::sync::RawFrame;

    let mut vb = VenueBook::new(
        Box::new(FakeSync::new(Integrity::Verified)),
        Symbol::new("BTC-USD"),
    );
    let junk = RawFrame::new(VenueId::Fake, b"{not json".to_vec(), SystemClock.now());

    assert!(
        vb.feed(&junk).is_err(),
        "unparseable frames must surface, not vanish"
    );
}
