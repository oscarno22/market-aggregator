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
use ma_server::{DEFAULT_VENUES, Pipeline, http, init_tracing};

#[derive(Parser, Debug)]
#[command(about = "Replay a recorded tape through the full pipeline (no network)")]
struct Args {
    /// Tape to read, as written by `record`.
    #[arg(long)]
    tape: PathBuf,

    #[arg(long, default_value = "BTC-USD")]
    symbol: String,

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
    let mut pipeline = Pipeline::new(Symbol::new(&args.symbol), DEFAULT_VENUES.to_vec())?;
    let (handle, aggregator) = pipeline.spawn_aggregator()?;
    let tx = pipeline.channel();
    let clock = pipeline.clock();
    let shutdown = pipeline.shutdown();

    let server = args
        .serve
        .then(|| tokio::spawn(http::serve(args.addr, handle, shutdown)));
    if args.serve {
        tracing::info!(
            "open http://{} — replaying {}",
            args.addr,
            args.tape.display()
        );
    }

    let mut reader = TapeReader::open(&args.tape).await?;
    let pacing = Pacing::from_speed(args.speed);
    let stats = replay(&mut reader, &tx, clock.as_ref(), pacing).await?;

    tracing::info!(
        tape = %args.tape.display(),
        frames = stats.frames_sent,
        dropped = stats.dropped,
        ?pacing,
        "replay finished"
    );
    if stats.dropped > 0 {
        // Not a replay bug: the consumer was slower than the requested pacing,
        // exactly as it would have been against a live venue, and the
        // drop-oldest policy did what it says. Worth saying out loud so it is
        // never mistaken for corruption.
        tracing::warn!(
            dropped = stats.dropped,
            "the consumer fell behind the tape's pacing; the drop-oldest policy applied"
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
