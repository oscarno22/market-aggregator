//! `record` — capture raw venue frames to a tape.
//!
//! **Tier 2.** This is the one task whose entire purpose is to feed Tier 1:
//! run it once with a network, and everything downstream can then be developed
//! and tested with none. Frames are written exactly as they arrived, before
//! any parsing, so a tape can reproduce a parser bug or a venue schema change
//! — the two failures most likely to happen while nobody is watching.
//!
//! The tape tee is unbounded rather than drop-oldest. A recording with a hole
//! in it silently invalidates every offline test built on it; see
//! `Ingest::recording_to` for why that is the opposite policy to the live
//! channel beside it, and why both are right.
//!
//! # Recording a reconnect on purpose
//!
//! `--reconnect-at` exists because waiting for a venue to drop us is not a
//! plan. Both tapes committed before v4 are clean runs with zero session
//! boundaries, so every claim about the recovery path rested on the scripted
//! fake venue — the same position the parsers were in before the first tape,
//! and that went badly (see `docs/DESIGN.md` §8).
//!
//! What an induced reconnect does and does not prove is worth being exact
//! about, because the flag makes it easy to overclaim:
//!
//! - **Proven.** What each venue actually does on resubscribe, in its own
//!   bytes: Coinbase restarts `sequence_num` from a fresh base, Kraken sends a
//!   new snapshot, Bitstamp sends nothing until the REST body lands. And that
//!   the pipeline rebuilds a trusted book from those bytes — for Kraken, one
//!   its own CRC32 agrees with.
//! - **Not proven.** *Detection*. The socket here is closed by us, so nothing
//!   in the recording exercises the idle watchdog or a mid-stream socket error.
//!   Those are proven against the fake venue, where a silent socket can be
//!   produced on demand and a live venue cannot be asked for one.
//!
//! The reconnect goes through [`Pipeline::resync`] — the same request the
//! aggregator makes when a book desyncs from bad data — rather than through a
//! second disconnect path invented for recording. A boundary the production
//! code could not produce would be a fixture wearing a tape's clothes.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use ma_pipeline::ingest::IngestMessage;
use ma_pipeline::tape::TapeWriter;
use ma_server::{Pipeline, init_tracing, parse_symbols, parse_venues};

#[derive(Parser, Debug)]
#[command(about = "Record raw venue websocket frames to a tape (needs network)")]
struct Args {
    /// Comma-separated venues.
    #[arg(long, default_value = "coinbase,kraken,bitstamp")]
    venue: String,

    /// Comma-separated symbols, in normalised BASE-QUOTE form.
    #[arg(long, default_value = "BTC-USD")]
    symbol: String,

    /// How long to record for.
    #[arg(long, default_value_t = 120)]
    secs: u64,

    /// Where to write. Defaults to `tapes/<date>-<venues>-<symbol>.jsonl`.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Seconds into the recording at which to force one stream to reconnect,
    /// comma-separated — e.g. `--reconnect-at 30,60,90`.
    ///
    /// Each offset takes the *next* stream in order, rather than all of them,
    /// and that is the point: a tape where three venues drop together proves
    /// only that they all recover, while one where they drop in turn also
    /// records the other two carrying on untouched. One connection per
    /// `(venue, symbol)` is what makes that true, and this is the recording
    /// that shows it against real venues rather than against the fake one.
    #[arg(long, value_delimiter = ',')]
    reconnect_at: Vec<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    init_tracing("info");

    let venues = parse_venues(&args.venue)?;
    let symbols = parse_symbols(&args.symbol)?;
    let path = args.out.unwrap_or_else(|| {
        let names: Vec<&str> = venues.iter().map(|v| v.as_str()).collect();
        let pairs: Vec<String> = symbols
            .iter()
            .map(|s| s.to_string().to_lowercase())
            .collect();
        PathBuf::from("tapes").join(format!(
            "{}-{}-{}.jsonl",
            date_stamp(),
            names.join("+"),
            pairs.join("+")
        ))
    });
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }

    let pipeline = Pipeline::new(symbols, venues)?;
    let clock = pipeline.clock();
    let schedule = reconnect_schedule(&args.reconnect_at, &pipeline);
    let resync = pipeline.resync();
    let (tape_tx, mut tape_rx) = tokio::sync::mpsc::unbounded_channel::<IngestMessage>();
    let ingest = pipeline.spawn_ingest(Some(tape_tx))?;

    // Nothing consumes the ingest channel in this mode, so it fills and starts
    // dropping. That is correct and costs the tape nothing: the tee is a
    // separate, lossless path. The drop counter climbing here is the channel
    // doing its job with no aggregator attached, not a recording fault.
    tracing::info!(path = %path.display(), secs = args.secs, "recording");

    let start = clock.now();
    let mut writer = TapeWriter::create(&path, start).await?;
    let mut frames = 0_u64;
    let mut boundaries = 0_u64;

    let origin = tokio::time::Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(args.secs));
    tokio::pin!(deadline);
    let mut schedule = schedule;
    let mut forced = 0_u64;

    loop {
        // Absolute, so re-creating this future on every loop turn costs
        // nothing and cannot drift — the same reason `tape::replay` schedules
        // against a fixed origin rather than sleeping for each gap.
        let next_reconnect = schedule.front().map(|(at, _)| origin + *at);

        tokio::select! {
            () = &mut deadline => break,
            () = ma_server::stop_requested() => {
                tracing::info!("interrupted; flushing what we have");
                break;
            }

            () = async move {
                match next_reconnect {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some((at, stream)) = schedule.pop_front() {
                    // `false` means the ingest task is not listening, which for
                    // this binary means the stream is not running at all. Worth
                    // saying loudly: the recording will then be missing the
                    // boundary it was started to capture, and nothing later in
                    // the run would reveal that.
                    let heard = resync.request(&stream);
                    forced += 1;
                    if heard {
                        tracing::info!(%stream, ?at, "forcing a reconnect");
                    } else {
                        tracing::error!(
                            %stream, ?at,
                            "nobody is listening for this stream's resync; no boundary \
                             will be recorded"
                        );
                    }
                }
            }

            message = tape_rx.recv() => {
                let Some(message) = message else { break };
                match &message {
                    IngestMessage::Frame(_) => frames += 1,
                    IngestMessage::SessionEnded { .. } => boundaries += 1,
                    // Live ingest never emits these; only Parquet replay does,
                    // and `write_message` refuses them anyway.
                    IngestMessage::Event { .. } => continue,
                }
                writer.write_message(&message).await?;
                // Flushing per second rather than per frame: a tape is worth
                // little if a crash loses the last minute of it, and worth
                // nothing if fsync back-pressures the ingest path.
                if frames.is_multiple_of(256) {
                    writer.flush().await?;
                }
            }
        }
    }

    // Stop ingest before the final flush so nothing arrives mid-write.
    drop(pipeline.into_trigger());
    for task in ingest {
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    while let Ok(message) = tape_rx.try_recv() {
        writer.write_message(&message).await?;
        frames += 1;
    }
    writer.flush().await?;

    let bytes = tokio::fs::metadata(&path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    tracing::info!(
        path = %path.display(),
        frames,
        session_boundaries = boundaries,
        forced_reconnects = forced,
        bytes,
        "recorded"
    );
    if frames == 0 {
        tracing::error!("no frames recorded — check connectivity and the subscribe payloads");
    }
    // A tape asked for reconnects and carrying none is not a tape of a
    // reconnect, and would fail its offline test in a way that reads as a
    // pipeline regression. Say so here, where the cause is still visible.
    if forced > 0 && boundaries == 0 {
        tracing::error!(
            forced,
            "reconnects were requested but no session boundary was recorded"
        );
    }
    Ok(())
}

/// Pair each `--reconnect-at` offset with a stream, round-robin.
///
/// Offsets are sorted first, so `--reconnect-at 60,30` means the same as
/// `30,60`: the flag names a set of moments, not an order. Which stream goes
/// with which moment then follows from the pipeline's own stable stream order,
/// so a rerun of the same command reconnects the same venues at the same times.
fn reconnect_schedule(
    offsets: &[u64],
    pipeline: &Pipeline,
) -> std::collections::VecDeque<(Duration, ma_core::StreamId)> {
    let streams: Vec<ma_core::StreamId> = pipeline.streams().collect();
    if streams.is_empty() {
        return std::collections::VecDeque::new();
    }
    let mut offsets = offsets.to_vec();
    offsets.sort_unstable();
    offsets
        .into_iter()
        .enumerate()
        .map(|(i, secs)| {
            (
                Duration::from_secs(secs),
                streams[i % streams.len()].clone(),
            )
        })
        .collect()
}

/// `YYYY-MM-DD` without pulling in a date library for one filename.
fn date_stamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86_400;
    // Civil-from-days, Howard Hinnant's algorithm.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}
