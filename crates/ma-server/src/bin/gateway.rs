//! `gateway` — one view across every node.
//!
//! **Tier 2 by association.** It opens no venue sockets of its own; it opens
//! one HTTP connection per node and merges what they publish. Point it at
//! `just cluster`'s two processes and it serves the view neither of them can:
//! a consolidated touch over every venue in the cluster, and the only place a
//! doubly-owned stream is visible.
//!
//! ```text
//! just cluster                       # two nodes, :8081 and :8082
//! just gateway                       # merged view at :8080
//! ```
//!
//! See `ma_server::gateway` for the timing argument — every age served here is
//! a node's own monotonic book age plus this process's monotonic lag, and
//! nothing anywhere compares two machines' wall clocks.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ma_core::{CrossPolicy, SystemClock};
use ma_pipeline::ingest::shutdown;
use ma_server::gateway::{GatewayPolicy, NodeReport, feed, http};
use ma_server::{init_tracing, stop_requested};

#[derive(Parser, Debug)]
#[command(about = "Merge every node's snapshot into one cross-cluster view")]
struct Args {
    /// Comma-separated node base URLs, optionally labelled:
    /// `http://host:8081` or `node-a=http://host:8081`.
    ///
    /// The nodes do not have to be a cluster. Two independent processes each
    /// running different venues are a perfectly good thing to merge, and
    /// neither has a node id to borrow — which is why the label defaults to
    /// the URL rather than being read from `/cluster`.
    #[arg(long, default_value = "http://127.0.0.1:8081,http://127.0.0.1:8082")]
    nodes: String,

    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,

    /// How often to merge and publish, in milliseconds.
    ///
    /// A tick rather than merging on every arrival: with n nodes each ticking
    /// four times a second, per-arrival merging would publish 4n views a
    /// second, most differing only in which node had just spoken. See
    /// `gateway::http::spawn`.
    #[arg(long, default_value_t = 250)]
    tick_ms: u64,

    /// How long since a node's last snapshot before it is dropped from the
    /// merge entirely.
    ///
    /// Coarser than `--cross-max-age-ms`, and a different question: that asks
    /// whether a *book* has stalled, this asks whether a *node* is still
    /// there.
    #[arg(long, default_value_t = 3_000)]
    node_max_age_ms: u64,

    /// How stale a book may be and still be a leg of the merged touch.
    ///
    /// Compared against the book's age *plus* the lag of the node that
    /// reported it, so a node that dies mid-tick stops contributing legs
    /// rather than freezing a fresh-looking quote forever.
    #[arg(long, default_value_t = 2_000)]
    cross_max_age_ms: u64,

    /// Connect timeout for a node, in milliseconds. Deliberately not a read
    /// timeout — an SSE stream is meant to stay open, and liveness is measured
    /// by snapshots arriving rather than by a timer.
    #[arg(long, default_value_t = 5_000)]
    connect_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    init_tracing("info,ma_server=info");

    let nodes = feed::parse_nodes(&args.nodes)?;
    let client = feed::client(Duration::from_millis(args.connect_timeout_ms))?;
    let clock: Arc<dyn ma_core::Clock> = Arc::new(SystemClock);
    let (trigger, stop) = shutdown();

    let mut receivers = Vec::with_capacity(nodes.len());
    let mut feeds = Vec::with_capacity(nodes.len());
    for (label, url) in nodes {
        let (tx, rx) = tokio::sync::watch::channel(NodeReport::new(label.clone(), url.clone()));
        receivers.push(rx);
        feeds.push(tokio::spawn(feed::follow(
            client.clone(),
            label,
            url,
            tx,
            Arc::clone(&clock),
            stop.clone(),
        )));
    }

    let (merged, _) = tokio::sync::broadcast::channel(http::BROADCAST_CAPACITY);
    let handle = http::GatewayHandle {
        merged,
        nodes: receivers,
    };

    let policy = GatewayPolicy {
        max_node_age: Duration::from_millis(args.node_max_age_ms),
        cross: CrossPolicy {
            max_age: Duration::from_millis(args.cross_max_age_ms),
        },
    };
    let merger = http::spawn(
        handle.clone(),
        Arc::clone(&clock),
        Duration::from_millis(args.tick_ms),
        policy,
        stop.clone(),
    );

    tracing::info!(
        nodes = handle.nodes.len(),
        "open http://{} for the merged view; /nodes for who is contributing",
        args.addr
    );
    let server = tokio::spawn(http::serve(args.addr, handle, stop));

    stop_requested().await;
    tracing::info!("shutting down");
    drop(trigger);

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = server.await;
        let _ = merger.await;
        for task in feeds {
            let _ = task.await;
        }
    })
    .await;
    Ok(())
}
