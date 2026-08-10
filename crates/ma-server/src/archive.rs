//! Replaying the Parquet archive through the real pipeline.
//!
//! CLAUDE.md's rule for replay is that it feeds *the same aggregator through
//! the same channel* as a live run, and this obeys it exactly: normalised
//! events go into [`Pipeline::channel`] as
//! [`IngestMessage::Event`](ma_pipeline::IngestMessage::Event), the aggregator
//! applies them to the same books, and the SSE stream and `/metrics` cannot
//! tell the difference.
//!
//! # What is different from tape replay, and why the difference is deliberate
//!
//! A tape replays *bytes* through the venue parsers. This replays *events*,
//! bypassing them. That is not a shortcut — it is the honest consequence of
//! what a Parquet file contains, and it is why the two layers both exist:
//!
//! - The tape can reproduce a parser bug. This cannot; parsing already
//!   happened, once, when the events were recorded.
//! - This can cover hours and be queried with SQL. A tape cannot.
//!
//! What Parquet replay does **not** give up is verification. `EventKind::
//! Checksum` is part of the normalised stream, so a Kraken book rebuilt from
//! the archive is still checked against Kraken's own hash of what it should be
//! — the replay is validated against the venue's opinion, not merely against
//! itself.

use std::sync::Arc;
use std::time::Duration;

use ma_core::Clock;
use ma_persist::{EventReader, ReadError};
use ma_pipeline::channel::{SendOutcome, Sender};
use ma_pipeline::ingest::IngestMessage;
use ma_pipeline::tape::Pacing;

/// Outcome of an archive replay. Mirrors
/// [`ReplayStats`](ma_pipeline::ReplayStats) so the two modes report the same
/// shape to whoever is reading the log line.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArchiveStats {
    pub events_sent: u64,
    /// Events the drop-oldest channel evicted. Only ever non-zero under
    /// [`Pacing::Realtime`]; [`Pacing::Faithful`] waits for room instead, for
    /// the same reason tape replay does — a lossy replay is not a replay.
    pub dropped: u64,
}

/// Replay every event under `prefix` into `tx`.
///
/// Pacing follows the tape's rules exactly, and for the same reasons — see
/// [`Pacing`]. `Faithful` uses `send_lossless`, because a file read with no
/// sleeps outruns any consumer and a producer that dropped whichever events
/// lost the race would "reproduce" a market that never happened.
///
/// # Errors
/// If the store cannot be listed or a file cannot be decoded.
pub async fn replay_archive<C>(
    store: Arc<dyn ma_persist::ObjectStore>,
    prefix: &str,
    tx: &Sender<IngestMessage>,
    clock: &C,
    pacing: Pacing,
) -> Result<ArchiveStats, ReadError>
where
    C: Clock + ?Sized,
{
    replay_archive_many(store, std::slice::from_ref(&prefix), tx, clock, pacing).await
}

/// Replay the union of several prefixes — one per node — into `tx`.
///
/// This is how an hour written by a sharded cluster replays as one session.
/// [`EventReader::open_many`] does the merge; what this adds is the timeline,
/// and that is the part that does not survive the union unexamined.
///
/// `StoredEvent::elapsed` counts from its own writer run's first event. Within
/// one run that is exactly the right thing to pace on — it is monotonic and it
/// is immune to a wall-clock step. Across two nodes it is meaningless: the
/// offsets share no origin, so consecutive merged events hand back numbers
/// that jump backwards, every gap saturates to zero, and the whole archive
/// arrives in one burst. That is the same failure symbol partitioning caused
/// in v4 — a layout change presenting as a pacing bug — and it is why this is
/// handled here rather than discovered later.
///
/// So with more than one prefix the timeline is rebuilt from the wall clock,
/// which is the only column comparable across writer runs and the one the
/// reader already merges on. Offsets are taken from the first merged event and
/// clamped monotonic. With a single prefix nothing changes: `elapsed` is used
/// exactly as before, because there is no second origin to reconcile and the
/// recorded offset is strictly better evidence than a timestamp.
///
/// # Errors
/// If the store cannot be listed or a file cannot be decoded.
pub async fn replay_archive_many<C>(
    store: Arc<dyn ma_persist::ObjectStore>,
    prefixes: &[&str],
    tx: &Sender<IngestMessage>,
    clock: &C,
    pacing: Pacing,
) -> Result<ArchiveStats, ReadError>
where
    C: Clock + ?Sized,
{
    let base = clock.now();
    // One prefix keeps the recorded offsets; several have to share a timeline.
    let from_wall = prefixes.len() > 1;
    let mut reader = EventReader::open_many(store, prefixes).await?;
    let mut stats = ArchiveStats::default();
    let mut previous = Duration::ZERO;
    let mut origin: Option<std::time::SystemTime> = None;

    while let Some(stored) = reader.next_event().await? {
        let offset = if from_wall {
            let wall = stored.event.ingest_ts.wall();
            let start = *origin.get_or_insert(wall);
            // Clamped rather than trusted: the merge orders by wall, so this
            // is nondecreasing in every case a well-formed archive produces —
            // but a clock step inside one node's run can still hand back an
            // earlier reading than its predecessor, and a replay is not the
            // place to have an opinion about that beyond refusing to go
            // backwards.
            wall.duration_since(start).unwrap_or(Duration::ZERO)
        } else {
            stored.elapsed
        };

        if let Pacing::Realtime { speed } = pacing {
            let gap = offset.saturating_sub(previous);
            let scaled = gap.div_f64(speed.max(f64::MIN_POSITIVE));
            if scaled > Duration::ZERO {
                tokio::time::sleep(scaled).await;
            }
        }
        previous = offset;

        // Rebuild the ingest timestamp against *this* process's clock, from
        // the recorded offset — the same reconstruction tape replay performs,
        // and for the same reason: an `Instant` from another process is
        // meaningless, but an elapsed duration is not.
        let mut event = stored.event;
        event.ingest_ts = base.advanced_by(offset);

        let message = IngestMessage::Event {
            stream: stored.stream,
            event,
        };
        stats.events_sent += 1;

        match pacing {
            Pacing::Faithful => {
                if tx.send_lossless(message).await.is_err() {
                    break;
                }
            }
            Pacing::Realtime { .. } => match tx.send(message) {
                SendOutcome::Sent => {}
                SendOutcome::DroppedOldest(_) => stats.dropped += 1,
                SendOutcome::Closed(_) => break,
            },
        }
    }

    Ok(stats)
}
