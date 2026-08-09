//! `ma-server` — connect to the venues, aggregate, serve.
//!
//! Tier 2: this opens real sockets. The reconnect backoff it relies on was
//! tested offline first (`ma_pipeline::backoff`), which is the ordering the
//! risk register asks for.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use ma_server::{
    DEFAULT_VENUES, Pipeline, http, init_tracing, parse_symbols, parse_venues, parse_windows,
    stop_requested,
};

#[derive(Parser, Debug)]
#[command(about = "Multi-venue crypto market data aggregator")]
struct Args {
    /// Comma-separated symbols in normalised BASE-QUOTE form, translated per
    /// venue. Each (venue, symbol) pair gets its own connection — see
    /// `ma_core::stream` for why they are not multiplexed onto one socket.
    #[arg(long, default_value = "BTC-USD")]
    symbols: String,

    /// Comma-separated venues.
    #[arg(long, default_value = "coinbase,kraken,bitstamp")]
    venues: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,

    /// Snapshot publish interval, in milliseconds. Also the bucket resolution
    /// of every rolling window.
    #[arg(long, default_value_t = 250)]
    tick_ms: u64,

    /// How stale a book may be and still be a leg of the cross-venue touch.
    ///
    /// The guard that stops a stalled feed's frozen quote from showing up as
    /// an arbitrage against the venues still moving. See `ma_core::cross`.
    #[arg(long, default_value_t = 2_000)]
    cross_max_age_ms: u64,

    /// Rolling indicator windows, e.g. `1s,10s,1m`. Suffixes: ms, s, m, h.
    ///
    /// Several spans cost nothing at ingest: they share one bucket ring per
    /// stream, sized to the longest. See `ma_core::window`.
    #[arg(long, default_value = "1s,10s,1m")]
    windows: String,

    /// This node's name in the cluster. Enables sharding when set with
    /// `--cluster-dir`.
    ///
    /// Must be stable across restarts of this process and unique in the
    /// cluster: a node that renames itself on every restart looks like one
    /// node dying and another joining, which rebalances the whole cluster on
    /// every deploy. See `ma_coord::NodeId`.
    #[arg(long, requires = "cluster_dir")]
    node_id: Option<String>,

    /// Directory holding the cluster's lease records — one file per node.
    ///
    /// Every node must be started with the *same* `--symbols` and `--venues`:
    /// the assignment is a pure function of the stream set, so a node
    /// configured with a different one computes a different answer and the
    /// at-most-one-owner guarantee no longer holds. Nothing can check that
    /// from inside a single process.
    #[arg(long, requires = "node_id")]
    cluster_dir: Option<String>,

    /// Lease lifetime in milliseconds. A dead node's streams move this long
    /// after its last renewal; a joining node waits this plus the guard before
    /// taking any.
    #[arg(long, default_value_t = 15_000)]
    cluster_ttl_ms: u64,

    /// Archive normalised events to Parquet, rolled hourly.
    ///
    /// A path writes locally; `s3://bucket/prefix` needs `--features s3` and,
    /// per CLAUDE.md, an IAM user scoped to that one prefix. Omit to run with
    /// no persistence at all, which is what every v1 run did.
    #[arg(long)]
    archive: Option<String>,

    /// Key namespace inside the archive.
    #[arg(long, default_value = ma_persist::DEFAULT_PREFIX)]
    archive_prefix: String,
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

    let symbols = parse_symbols(&args.symbols)?;
    let mut pipeline = Pipeline::new(symbols, venues)?
        .with_tick(Duration::from_millis(args.tick_ms))
        .with_windows(parse_windows(&args.windows)?)
        .with_cross_max_age(Duration::from_millis(args.cross_max_age_ms));

    // Attached before the aggregator is spawned, because the aggregator is the
    // only thing that can produce normalised events — see
    // `Aggregator::publishing_events_to`.
    let archive = match &args.archive {
        Some(uri) => {
            let store = ma_persist::store_from_uri(uri).await?;
            tracing::info!(store = %store.describe(), prefix = %args.archive_prefix, "archiving");
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let writer = ma_persist::EventWriter::new(store, args.archive_prefix.clone());
            pipeline = pipeline.recording_events_to(tx);
            Some(tokio::spawn(ma_persist::run(rx, writer)))
        }
        None => None,
    };

    // Before the aggregator, which reads the ownership channel this creates.
    let coordinator = match (&args.node_id, &args.cluster_dir) {
        (Some(node), Some(dir)) => {
            let registry = ma_coord::registry_from_uri(dir)?;
            let ttl = Duration::from_millis(args.cluster_ttl_ms);
            let config = ma_coord::LeaseConfig {
                ttl,
                // Five renewals inside one lease, so four consecutive failures
                // are survivable before the node stands down.
                renew: ttl / 5,
                guard: ttl / 7,
            };
            tracing::info!(
                node,
                dir,
                ?ttl,
                "clustered: this node will own a share of the streams. Every node must be \
                 started with the same --symbols and --venues."
            );
            Some(pipeline.clustered(ma_coord::NodeId::new(node), registry, config)?)
        }
        _ => None,
    };

    let (handle, aggregator) = pipeline.spawn_aggregator()?;
    let ingest = pipeline.spawn_ingest(None)?;
    let shutdown = pipeline.shutdown();

    tracing::info!(
        symbols = ?pipeline.symbols(),
        venues = ?pipeline.venues(),
        streams = pipeline.streams().count(),
        "open http://{} for the chart", args.addr
    );

    let server = tokio::spawn(http::serve(args.addr, handle, shutdown));

    stop_requested().await;
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
        // Last, and inside the same budget: the coordinator's final act is to
        // withdraw this node's lease so the rest of the cluster rebalances now
        // rather than after `ttl`. Nothing depends on it — a hard kill cannot
        // run it and the lease timing out has to produce the same outcome —
        // but it is cheap and it saves every other node a full lease of
        // unowned streams on a rolling deploy.
        if let Some(coordinator) = coordinator {
            let _ = coordinator.await;
        }
    })
    .await;

    // Waited on last and separately, with its own budget. The aggregator
    // closing its sender is what ends the writer's loop, and the writer still
    // has to finish and upload the open file — dropping that on the floor
    // would lose the final partial hour on every clean shutdown, which is the
    // one outage a graceful stop is supposed to avoid.
    if let Some(archive) = archive {
        match tokio::time::timeout(Duration::from_secs(30), archive).await {
            Ok(Ok(writer)) => tracing::info!(
                files = writer.files_written,
                rows = writer.rows_written,
                "archive flushed"
            ),
            Ok(Err(e)) => tracing::error!(error = %e, "the archive writer panicked"),
            Err(_) => tracing::error!(
                "the archive writer did not finish in 30s; the last file may be incomplete"
            ),
        }
    }

    Ok(())
}
