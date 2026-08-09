//! `ma-server` — connect to the venues, aggregate, serve.
//!
//! Tier 2: this opens real sockets. The reconnect backoff it relies on was
//! tested offline first (`ma_pipeline::backoff`), which is the ordering the
//! risk register asks for.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use ma_core::Symbol;
use ma_server::{DEFAULT_VENUES, Pipeline, http, init_tracing, parse_venues};

#[derive(Parser, Debug)]
#[command(about = "Multi-venue crypto market data aggregator")]
struct Args {
    /// Symbol in normalised BASE-QUOTE form. Translated per venue.
    #[arg(long, default_value = "BTC-USD")]
    symbol: String,

    /// Comma-separated venues.
    #[arg(long, default_value = "coinbase,kraken,bitstamp")]
    venues: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,

    /// Snapshot publish interval, in milliseconds.
    #[arg(long, default_value_t = 250)]
    tick_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    init_tracing("info,ma_pipeline=info,ma_server=info");

    let venues = if args.venues.trim().is_empty() {
        DEFAULT_VENUES.to_vec()
    } else {
        parse_venues(&args.venues)?
    };

    let mut pipeline = Pipeline::new(Symbol::new(&args.symbol), venues)?
        .with_tick(Duration::from_millis(args.tick_ms));

    let (handle, aggregator) = pipeline.spawn_aggregator()?;
    let ingest = pipeline.spawn_ingest(None)?;
    let shutdown = pipeline.shutdown();

    tracing::info!(
        symbol = %args.symbol,
        venues = ?pipeline.venues(),
        "open http://{} for the chart", args.addr
    );

    let server = tokio::spawn(http::serve(args.addr, handle, shutdown));

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");

    // Holding the trigger until here is what has kept everything running;
    // dropping it now is the shutdown signal. See `Pipeline::into_trigger`.
    drop(pipeline.into_trigger());

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = server.await;
        let _ = aggregator.await;
        for task in ingest {
            let _ = task.await;
        }
    })
    .await;

    Ok(())
}
