//! `replay` — run a recorded tape through the whole pipeline. No network.
//!
//! **Tier 1.** This is the demo that works on a plane, and the reason the
//! offline suite can exercise the real aggregator rather than a stand-in. The
//! tape's frames go into the *same* bounded channel a live ingest task writes
//! to, so the sync state machines, the books, the metrics, the SSE fan-out and
//! the page are all the production ones. Nothing downstream can tell.
//!
//! With `--serve` it also brings up the HTTP surface, so a tape can be
//! browsed exactly as a live run would be.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use ma_core::Symbol;
use ma_pipeline::tape::{Pacing, TapeReader, replay};
use ma_server::{DEFAULT_VENUES, Pipeline, http, init_tracing, parse_symbols};

#[derive(Parser, Debug)]
#[command(about = "Replay a recorded tape through the full pipeline (no network)")]
struct Args {
    /// Raw-frame tape to read, as written by `record`.
    ///
    /// Mutually exclusive with `--archive`. The two are different replay
    /// layers, not two ways of saying the same thing: a tape replays bytes
    /// through the venue parsers and can reproduce a parser bug; an archive
    /// replays normalised events and cannot, but covers hours instead of
    /// minutes. See `ma_persist`'s crate docs.
    #[arg(long, conflicts_with = "archive")]
    tape: Option<PathBuf>,

    /// Parquet archive to replay: a local path, or `s3://bucket/prefix` on a
    /// build with `--features s3`.
    #[arg(long)]
    archive: Option<String>,

    /// Key namespace inside the archive.
    #[arg(long, default_value = ma_persist::DEFAULT_PREFIX)]
    archive_prefix: String,

    /// Symbol(s) to serve, comma-separated.
    ///
    /// For a tape recorded before v2 this is also the *assertion* about what
    /// the tape contains: those recordings carry no symbol field, so replay
    /// stamps their frames with the first symbol given here. Getting it wrong
    /// produces a book under the wrong name rather than an error, which is why
    /// it is a flag rather than a guess. See `TapedFrame::into_message`.
    #[arg(long, default_value = "BTC-USD")]
    symbols: String,

    /// Playback speed. Omit to run as fast as the pipeline can consume, which
    /// is what the test suite wants; `1.0` reproduces the original pacing,
    /// which is what a demo wants.
    #[arg(long)]
    speed: Option<f64>,

    /// Also serve the chart page, so a tape can be browsed like a live run.
    #[arg(long)]
    serve: bool,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    init_tracing("info");

    // Every venue is registered regardless of what the tape holds: the
    // aggregator ignores messages for venues it does not track, and a tape
    // recorded from one venue should still render on a page that has cards for
    // three. The alternative — inferring the venue set by pre-scanning the
    // file — would mean reading the tape twice to learn something that costs
    // nothing to over-provision.
    let symbols = parse_symbols(&args.symbols)?;
    let fallback = symbols
        .first()
        .cloned()
        .unwrap_or_else(|| Symbol::new("BTC-USD"));
    let mut pipeline = Pipeline::new(symbols, DEFAULT_VENUES.to_vec())?;
    let (handle, aggregator) = pipeline.spawn_aggregator()?;
    let tx = pipeline.channel();
    let clock = pipeline.clock();
    let shutdown = pipeline.shutdown();

    let source = match (&args.tape, &args.archive) {
        (Some(tape), None) => tape.display().to_string(),
        (None, Some(archive)) => format!("archive {archive}"),
        _ => return Err("give exactly one of --tape or --archive".into()),
    };

    let server = args
        .serve
        .then(|| tokio::spawn(http::serve(args.addr, handle, shutdown)));
    if args.serve {
        tracing::info!("open http://{} — replaying {source}", args.addr);
    }

    let pacing = Pacing::from_speed(args.speed);
    let stats = match (&args.tape, &args.archive) {
        (Some(tape), _) => {
            let mut reader = TapeReader::open(tape).await?;
            let stats = replay(&mut reader, &tx, clock.as_ref(), pacing, &fallback).await?;
            (stats.frames_sent, stats.dropped)
        }
        (_, Some(archive)) => {
            let store = ma_persist::store_from_uri(archive).await?;
            let stats =
                ma_server::replay_archive(store, &args.archive_prefix, &tx, clock.as_ref(), pacing)
                    .await?;
            (stats.events_sent, stats.dropped)
        }
        _ => unreachable!("checked above"),
    };
    let (sent, dropped) = stats;

    tracing::info!(
        source = %source,
        sent,
        dropped,
        ?pacing,
        "replay finished"
    );
    if dropped > 0 {
        // Not a replay bug: the consumer was slower than the requested pacing,
        // exactly as it would have been against a live venue, and the
        // drop-oldest policy did what it says. Worth saying out loud so it is
        // never mistaken for corruption.
        tracing::warn!(
            dropped,
            "the consumer fell behind the requested pacing; the drop-oldest policy applied"
        );
    }

    if let Some(server) = server {
        tracing::info!("tape exhausted; still serving the final state — ctrl-c to stop");
        tokio::signal::ctrl_c().await?;
        drop(pipeline.into_trigger());
        let _ = tokio::time::timeout(Duration::from_secs(3), server).await;
    } else {
        // Closing the channel is what tells the aggregator there will be no
        // more messages, which is how a headless replay terminates on its own.
        drop(tx);
        drop(pipeline.into_trigger());
    }
    let _ = tokio::time::timeout(Duration::from_secs(3), aggregator).await;
    Ok(())
}
