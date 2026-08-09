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

    let deadline = tokio::time::sleep(Duration::from_secs(args.secs));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => break,
            () = async { let _ = tokio::signal::ctrl_c().await; } => {
                tracing::info!("interrupted; flushing what we have");
                break;
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
        bytes,
        "recorded"
    );
    if frames == 0 {
        tracing::error!("no frames recorded — check connectivity and the subscribe payloads");
    }
    Ok(())
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
