//! Recording and replaying raw venue frames.
//!
//! CLAUDE.md calls replay "the cheapest testing leverage in the project" and
//! asks that it be built early, before real ingest tasks exist, so that
//! everything built on top of it can be exercised with no network at all.
//! This module is that leverage: [`TapeWriter`] appends every [`RawFrame`] a
//! venue task receives to a file, unparsed, and [`TapeReader`] plus
//! [`replay`] read one back and feed it into the *same* [`Sender<RawFrame>`]
//! a live ingest task would use. Nothing downstream — the
//! [`VenueSync`](ma_venues::VenueSync) state machine, the aggregator, none of
//! it — can tell replay from a live socket.
//!
//! # Why raw frames, not normalised events
//!
//! [`RawFrame`]'s own doc comment gives the reason: recording after parsing
//! means a session can never reproduce a parser bug or a venue schema change,
//! which are exactly the two failures most likely to happen while nobody is
//! watching. The Parquet writer planned for v2 records normalised events for
//! a different purpose — durable history for the persistence layer — and is
//! a second, independent replay source layered on top of this one, not a
//! replacement for it.
//!
//! # Format
//!
//! One JSON object per line (JSONL), so a tape can be inspected with `less`
//! or diffed in review. Every real venue in this project sends WebSocket
//! *text* frames — JSON — so the payload is stored as a JSON string field
//! rather than as encoded bytes. A frame that is not valid UTF-8 is refused
//! at write time ([`TapeError::NonUtf8Payload`]) rather than silently
//! transcoded or dropped: if a venue ever needs binary framing, the format
//! should grow a variant for it deliberately, not lose bytes quietly.
//!
//! Timing is recorded as an offset from the tape's first frame
//! (`elapsed_nanos`), not as an absolute timestamp. [`IngestTime`]'s doc
//! comment explains why: an `Instant` has no meaning outside the process that
//! created it, so replay reconstructs each frame's `IngestTime` from *its
//! own* base reading plus the recorded offset, via
//! [`IngestTime::advanced_by`]. The wall clock is also stored, but only for a
//! human reading the file — replay's ordering and pacing always come from
//! the offset, never from the wall field.

use std::path::Path;
use std::time::{Duration, SystemTime};

use ma_core::{Clock, IngestTime, VenueId};
use ma_venues::{FrameSource, RawFrame};

use crate::ingest::{IngestMessage, SessionEnd};
use serde::{Deserialize, Serialize};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Lines,
};

use crate::channel::{SendOutcome, Sender};

#[derive(Debug, thiserror::Error)]
pub enum TapeError {
    #[error("i/o error reading or writing tape: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed tape record: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("frame payload was not valid UTF-8; the tape format is text-only, see module docs")]
    NonUtf8Payload,
}

/// On-disk shape of one line. Kept private and separate from [`TapedFrame`]
/// so the wire format (JSON field names, `elapsed_nanos` as a raw `u64`) can
/// change without disturbing the public replay API.
#[derive(Serialize, Deserialize)]
struct TapeRecord {
    venue: VenueId,
    elapsed_nanos: u64,
    recorded_wall_unix_nanos: u64,
    /// Empty for a [`RecordKind::SessionEnded`] record, which carries no
    /// bytes — only the fact that the stream restarted here.
    payload: String,
    /// Websocket frame or REST snapshot body. Recorded because a Bitstamp
    /// tape without its snapshot replays into a book that can never leave
    /// `AwaitingSnapshot` — see [`FrameSource`]. Defaulted on read so the
    /// field is additive: `WebSocket` is what every record lacking it was.
    #[serde(default, skip_serializing_if = "is_websocket")]
    source: FrameSource,
    #[serde(default, skip_serializing_if = "RecordKind::is_frame")]
    kind: RecordKind,
}

/// Whether a line is bytes from a venue or a session boundary.
///
/// Recording the boundaries is what makes a *reconnect* replayable, not just
/// the data around one. A tape that silently stitched two sessions together
/// would replay a sequence-number restart as a gap and produce a permanently
/// desynced book — a bug in the tape presenting as a bug in the parser.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RecordKind {
    #[default]
    Frame,
    SessionEnded,
}

impl RecordKind {
    fn is_frame(&self) -> bool {
        matches!(self, Self::Frame)
    }
}

/// Keeps the common case out of the file. Every venue frame but a handful of
/// Bitstamp snapshots is a websocket frame, and a tape is meant to stay
/// readable with `less`.
fn is_websocket(source: &FrameSource) -> bool {
    matches!(source, FrameSource::WebSocket)
}

fn nanos(d: Duration) -> u64 {
    // Saturating rather than panicking, same reasoning as `TestClock::advance`
    // and `IngestTime::advanced_by`: overflow needs a duration no real tape
    // will ever have, so this arm exists to degrade rather than to fire.
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn wall_nanos(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(nanos)
        .unwrap_or(0)
}

/// Appends [`RawFrame`]s to a tape, unparsed, in arrival order.
#[derive(Debug)]
pub struct TapeWriter<W> {
    out: W,
    start: IngestTime,
}

impl TapeWriter<BufWriter<tokio::fs::File>> {
    /// Create (or truncate) a tape file. `start` anchors every frame's
    /// recorded offset — pass the same clock reading the ingest task used to
    /// stamp its first frame, so the tape's offsets and wall-clock field
    /// agree with each other.
    pub async fn create(path: impl AsRef<Path>, start: IngestTime) -> Result<Self, TapeError> {
        let file = tokio::fs::File::create(path).await?;
        Ok(Self::new(BufWriter::new(file), start))
    }
}

impl<W: AsyncWrite + Unpin> TapeWriter<W> {
    pub fn new(out: W, start: IngestTime) -> Self {
        Self { out, start }
    }

    /// Append one frame.
    ///
    /// Does not flush — call [`Self::flush`] at whatever cadence the caller
    /// wants, so tee-ing a tape recorder off a hot ingest path is not an
    /// fsync per message.
    pub async fn write_frame(&mut self, frame: &RawFrame) -> Result<(), TapeError> {
        let payload = frame.as_str().map_err(|_| TapeError::NonUtf8Payload)?;
        self.write_record(&TapeRecord {
            venue: frame.venue,
            elapsed_nanos: nanos(frame.ingest_ts.since(self.start)),
            recorded_wall_unix_nanos: wall_nanos(frame.ingest_ts.wall()),
            payload: payload.to_owned(),
            source: frame.source,
            kind: RecordKind::Frame,
        })
        .await
    }

    /// Append whatever an ingest task produced — a frame, or the boundary
    /// where one connection ended and the next began.
    pub async fn write_message(&mut self, message: &IngestMessage) -> Result<(), TapeError> {
        match message {
            IngestMessage::Frame(frame) => self.write_frame(frame).await,
            IngestMessage::SessionEnded { venue, at, .. } => {
                self.write_record(&TapeRecord {
                    venue: *venue,
                    elapsed_nanos: nanos(at.since(self.start)),
                    recorded_wall_unix_nanos: wall_nanos(at.wall()),
                    payload: String::new(),
                    source: FrameSource::WebSocket,
                    kind: RecordKind::SessionEnded,
                })
                .await
            }
        }
    }

    async fn write_record(&mut self, record: &TapeRecord) -> Result<(), TapeError> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        self.out.write_all(&line).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), TapeError> {
        self.out.flush().await.map_err(TapeError::Io)
    }
}

/// One record read off a tape, before its `IngestTime` is reconstructed for
/// the replay run currently reading it — see [`Self::into_message`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TapedFrame {
    pub venue: VenueId,
    /// Empty for a session boundary, which carries no bytes.
    pub payload: Vec<u8>,
    /// Offset from the tape's first frame. This, not `recorded_wall`, is
    /// what replay uses for both ordering and pacing.
    pub elapsed: Duration,
    /// Wall clock at original recording time. Informational only — see the
    /// module docs on why replay does not reuse this directly.
    pub recorded_wall: SystemTime,
    pub source: FrameSource,
    /// Whether this record is a frame or the boundary between two sessions.
    pub session_ended: bool,
}

impl TapedFrame {
    /// Reconstruct as an [`IngestMessage`] for this replay run, anchored to
    /// `base`. Two replays of the same tape from different `base` readings
    /// produce messages with different wall clocks but identical relative
    /// ordering and spacing — the property that makes replay deterministic.
    pub fn into_message(self, base: IngestTime) -> IngestMessage {
        let at = base.advanced_by(self.elapsed);
        if self.session_ended {
            return IngestMessage::SessionEnded {
                venue: self.venue,
                at,
                // A tape records that the stream restarted, not why. The
                // aggregator's response is identical for every cause — reset
                // the sync, distrust the book — so the distinction would be
                // decoration, and inventing a specific cause on replay would
                // be worse than admitting the tape does not know.
                end: SessionEnd::Closed,
            };
        }
        IngestMessage::Frame(match self.source {
            FrameSource::WebSocket => RawFrame::new(self.venue, self.payload, at),
            FrameSource::RestSnapshot => RawFrame::rest_snapshot(self.venue, self.payload, at),
        })
    }
}

/// Reads a tape back as [`TapedFrame`]s, in the order they were written.
#[derive(Debug)]
pub struct TapeReader<R> {
    lines: Lines<BufReader<R>>,
}

/// A tape on disk, plain or gzipped.
///
/// Boxed rather than generic because the decision is made from a filename at
/// runtime, and every caller wants "open this path" rather than "open this
/// path, whose compression I already know".
pub type FileTape = TapeReader<Box<dyn AsyncRead + Unpin + Send>>;

impl TapeReader<Box<dyn AsyncRead + Unpin + Send>> {
    /// Open a tape, transparently decompressing a `.gz`.
    ///
    /// Committed tapes are gzipped because they are dominated by one enormous
    /// message: Coinbase's opening `level2` snapshot carries the entire book,
    /// tens of thousands of levels, and it compresses by roughly 10x. The
    /// alternative — committing only a trimmed or synthetic tape — would give
    /// up the one property that makes a recording worth keeping, which is that
    /// it is exactly what the venue sent.
    ///
    /// # Errors
    /// If the file cannot be opened.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, TapeError> {
        let path = path.as_ref();
        let gzipped = path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("gz"));
        let file = tokio::fs::File::open(path).await?;

        let reader: Box<dyn AsyncRead + Unpin + Send> = if gzipped {
            Box::new(async_compression::tokio::bufread::GzipDecoder::new(
                BufReader::new(file),
            ))
        } else {
            Box::new(file)
        };
        Ok(Self::new(reader))
    }
}

impl<R: AsyncRead + Unpin> TapeReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            lines: BufReader::new(reader).lines(),
        }
    }

    /// Read the next frame, or `None` at end of tape.
    pub async fn next_frame(&mut self) -> Result<Option<TapedFrame>, TapeError> {
        loop {
            let Some(line) = self.lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                // Tolerate a trailing blank line (e.g. one an editor added).
                // Every line this module's own writer produces is non-empty,
                // so this can never mask a real record.
                continue;
            }
            let record: TapeRecord = serde_json::from_str(&line)?;
            return Ok(Some(TapedFrame {
                venue: record.venue,
                payload: record.payload.into_bytes(),
                elapsed: Duration::from_nanos(record.elapsed_nanos),
                recorded_wall: SystemTime::UNIX_EPOCH
                    + Duration::from_nanos(record.recorded_wall_unix_nanos),
                source: record.source,
                session_ended: record.kind == RecordKind::SessionEnded,
            }));
        }
    }
}

/// How fast to replay, and — inseparably — what to do when the consumer
/// cannot keep up.
///
/// The two decisions are one decision, which is why they are one type. Pacing
/// determines whether the producer can outrun the consumer at all, and that in
/// turn determines whether dropping means "this consumer is too slow for the
/// market" or merely "this file reads faster than a CPU applies it".
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Pacing {
    /// As fast as the consumer will accept, losing nothing
    /// ([`Sender::send_lossless`]).
    ///
    /// What the offline suite wants, and what makes "the same tape produces
    /// the same snapshot sequence" achievable. Reading a file with no sleeps
    /// outruns any consumer, so a drop-oldest producer here would discard
    /// whichever frames happened to lose the race and the run would
    /// "reproduce" events the recording never contained.
    ///
    /// Not a hypothetical: the first full-speed replay of a real three-venue
    /// tape dropped Kraken's opening snapshot, and every update after it
    /// applied to a book that did not exist — hundreds of checksum failures
    /// that never happened live.
    Faithful,
    /// Reproduce the recording's own spacing, scaled by `speed`, and drop the
    /// oldest event when the consumer falls behind — exactly as a live venue
    /// would ([`Sender::send`]).
    ///
    /// `1.0` is the recording's original pace. This is the honest mode for a
    /// demo and the only one where [`ReplayStats::dropped`] means what it
    /// means live: the consumer could not keep up with the market.
    Realtime { speed: f64 },
}

impl Pacing {
    /// Map a CLI `--speed` flag: absent or non-positive means [`Self::Faithful`].
    pub fn from_speed(speed: Option<f64>) -> Self {
        match speed {
            Some(speed) if speed > 0.0 => Self::Realtime { speed },
            _ => Self::Faithful,
        }
    }
}

/// Outcome of a full replay run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayStats {
    pub frames_sent: u64,
    /// Frames replay itself pushed out of the channel because it was full —
    /// see `channel`'s drop-oldest policy. A nonzero count here means the
    /// consumer is slower than the requested pacing, exactly as it would be
    /// against a live venue; it is not a replay bug.
    pub dropped: u64,
}

/// Replay one tape into `tx`, in order, reconstructing each frame's
/// `IngestTime` from `clock`'s current reading plus the tape's recorded
/// offsets.
///
/// [`Pacing`] chooses both the timing and the loss policy — see its variants
/// for why those two decisions belong together.
///
/// Either way the frames arrive at the *same* channel the same aggregator
/// reads, which is the property that matters: nothing downstream can tell
/// replay from a socket.
pub async fn replay<R, C>(
    reader: &mut TapeReader<R>,
    tx: &Sender<IngestMessage>,
    clock: &C,
    pacing: Pacing,
) -> Result<ReplayStats, TapeError>
where
    R: AsyncRead + Unpin,
    // `?Sized` so a caller holding an `Arc<dyn Clock>` — which is how the
    // pipeline passes its clock around — can hand over `&*clock` directly.
    C: Clock + ?Sized,
{
    let base = clock.now();
    let mut previous_elapsed = Duration::ZERO;
    let mut stats = ReplayStats::default();

    while let Some(taped) = reader.next_frame().await? {
        let elapsed = taped.elapsed;
        if let Pacing::Realtime { speed } = pacing {
            let gap = elapsed.saturating_sub(previous_elapsed);
            let scaled = gap.div_f64(speed.max(f64::MIN_POSITIVE));
            if scaled > Duration::ZERO {
                tokio::time::sleep(scaled).await;
            }
        }
        previous_elapsed = elapsed;

        let message = taped.into_message(base);
        stats.frames_sent += 1;

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::channel::bounded;
    use ma_core::TestClock;
    use std::time::Duration;

    fn frame(venue: VenueId, payload: &str, at: IngestTime) -> RawFrame {
        RawFrame::new(venue, payload.as_bytes().to_vec(), at)
    }

    #[tokio::test]
    async fn round_trips_venue_payload_and_elapsed_through_a_real_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tape.jsonl");

        let clock = TestClock::new();
        let start = clock.now();
        let mut writer = TapeWriter::create(&path, start).await.expect("create");

        writer
            .write_frame(&frame(
                VenueId::Coinbase,
                r#"{"type":"snapshot"}"#,
                clock.now(),
            ))
            .await
            .expect("write frame 1");

        clock.advance(Duration::from_millis(10));
        writer
            .write_frame(&frame(VenueId::Kraken, r#"{"type":"delta"}"#, clock.now()))
            .await
            .expect("write frame 2");
        writer.flush().await.expect("flush");
        drop(writer);

        let mut reader = TapeReader::open(&path).await.expect("open");

        let f1 = reader.next_frame().await.expect("read 1").expect("some 1");
        assert_eq!(f1.venue, VenueId::Coinbase);
        assert_eq!(f1.payload, br#"{"type":"snapshot"}"#);
        assert_eq!(f1.elapsed, Duration::ZERO);

        let f2 = reader.next_frame().await.expect("read 2").expect("some 2");
        assert_eq!(f2.venue, VenueId::Kraken);
        assert_eq!(f2.payload, br#"{"type":"delta"}"#);
        assert_eq!(f2.elapsed, Duration::from_millis(10));

        assert!(
            reader.next_frame().await.expect("read eof").is_none(),
            "tape had more frames than were written"
        );
    }

    #[tokio::test]
    async fn write_frame_refuses_non_utf8_payloads_instead_of_corrupting_the_tape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut writer = TapeWriter::create(dir.path().join("tape.jsonl"), TestClock::new().now())
            .await
            .expect("create");

        let bad = RawFrame::new(VenueId::Fake, vec![0xff, 0xfe], TestClock::new().now());
        let err = writer.write_frame(&bad).await.unwrap_err();
        assert!(matches!(err, TapeError::NonUtf8Payload));
    }

    #[tokio::test]
    async fn reader_tolerates_a_trailing_blank_line() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tape.jsonl");

        let clock = TestClock::new();
        let mut writer = TapeWriter::create(&path, clock.now())
            .await
            .expect("create");
        writer
            .write_frame(&frame(VenueId::Bitstamp, "{}", clock.now()))
            .await
            .expect("write");
        writer.flush().await.expect("flush");
        drop(writer);

        // Append a blank line the way an editor's "insert final newline"
        // setting would.
        tokio::fs::write(
            &path,
            format!(
                "{}\n\n",
                tokio::fs::read_to_string(&path).await.expect("read")
            ),
        )
        .await
        .expect("append blank line");

        let mut reader = TapeReader::open(&path).await.expect("open");
        assert!(reader.next_frame().await.expect("frame").is_some());
        assert!(reader.next_frame().await.expect("eof").is_none());
    }

    #[tokio::test]
    async fn replay_reconstructs_ingest_time_from_recorded_offsets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tape.jsonl");

        let record_clock = TestClock::new();
        let mut writer = TapeWriter::create(&path, record_clock.now())
            .await
            .expect("create");
        for _ in 0..3 {
            writer
                .write_frame(&frame(VenueId::Coinbase, "{}", record_clock.now()))
                .await
                .expect("write");
            record_clock.advance(Duration::from_millis(5));
        }
        writer.flush().await.expect("flush");
        drop(writer);

        let mut reader = TapeReader::open(&path).await.expect("open");
        let (tx, rx) = bounded::<IngestMessage>(8);
        let replay_clock = TestClock::new();

        let stats = replay(&mut reader, &tx, &replay_clock, Pacing::Faithful)
            .await
            .expect("replay");
        drop(tx);

        assert_eq!(stats.frames_sent, 3);
        assert_eq!(stats.dropped, 0);

        let base = replay_clock.now();
        let mut received = Vec::new();
        while let Some(message) = rx.recv().await {
            match message {
                IngestMessage::Frame(frame) => received.push(frame),
                IngestMessage::SessionEnded { .. } => panic!("no boundaries on this tape"),
            }
        }
        assert_eq!(received.len(), 3);
        for (i, f) in received.iter().enumerate() {
            assert_eq!(
                f.ingest_ts.since(base),
                Duration::from_millis(5 * i as u64),
                "frame {i} was not spaced according to the tape's recorded offsets"
            );
        }
    }

    #[tokio::test]
    async fn replay_into_a_full_channel_drops_oldest_and_reports_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tape.jsonl");

        let record_clock = TestClock::new();
        let mut writer = TapeWriter::create(&path, record_clock.now())
            .await
            .expect("create");
        for _ in 0..5 {
            writer
                .write_frame(&frame(VenueId::Kraken, "{}", record_clock.now()))
                .await
                .expect("write");
        }
        writer.flush().await.expect("flush");
        drop(writer);

        let mut reader = TapeReader::open(&path).await.expect("open");
        let (tx, rx) = bounded::<IngestMessage>(2);

        // Realtime pacing keeps the live drop-oldest policy. `Faithful` would
        // wait for room here and never finish, which is the whole point of
        // the two modes being different: the frames a *live* venue sends into
        // a full buffer are stale opinions to be discarded, and the frames a
        // *file* holds are a record to be reproduced.
        let stats = replay(
            &mut reader,
            &tx,
            &TestClock::new(),
            Pacing::Realtime { speed: 1e9 },
        )
        .await
        .expect("replay");

        assert_eq!(stats.frames_sent, 5);
        assert_eq!(
            stats.dropped, 3,
            "5 sent into capacity 2 must drop exactly 3"
        );
        assert_eq!(
            stats.dropped,
            tx.metrics().dropped,
            "replay's own count must agree with the channel's own counter"
        );
        drop(tx);

        let mut left = 0;
        while rx.recv().await.is_some() {
            left += 1;
        }
        assert_eq!(left, 2);
    }
}
