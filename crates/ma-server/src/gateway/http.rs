//! The gateway's HTTP surface, and the loop that keeps it fed.
//!
//! The routes mirror a node's deliberately — `/`, `/events`, `/api/snapshot`,
//! `/metrics`, `/health` — because the merged view serialises as a
//! [`Snapshot`](ma_pipeline::aggregator::Snapshot) with two extra fields. The
//! chart page is served unchanged and does not know it is looking at a cluster.
//!
//! `/nodes` is the one addition, and it is the endpoint an operator actually
//! wants: every configured node, whether it is contributing, how far behind it
//! is, and any stream two nodes both claim.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::Stream;
use ma_core::Clock;
use ma_pipeline::ingest::Shutdown;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::{GatewayPolicy, MergedSnapshot, NodeReport, merge};

/// The same page a node serves. It reads `symbols`, which a merged snapshot
/// carries in the same shape — so the cluster view and the single-process view
/// are literally the same UI.
const INDEX: &str = include_str!("../index.html");

/// How many merged snapshots the fan-out ring holds. Same size and same
/// reasoning as a node's.
pub const BROADCAST_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct GatewayHandle {
    pub merged: broadcast::Sender<Arc<MergedSnapshot>>,
    pub nodes: Vec<watch::Receiver<NodeReport>>,
}

impl std::fmt::Debug for GatewayHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayHandle")
            .field("nodes", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

impl GatewayHandle {
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<MergedSnapshot>> {
        self.merged.subscribe()
    }

    /// The latest report from every node, as the merge would read it.
    pub fn reports(&self) -> Vec<NodeReport> {
        self.nodes.iter().map(|rx| rx.borrow().clone()).collect()
    }
}

/// Merge on a tick and publish, until shutdown.
///
/// Ticks on its own schedule rather than merging whenever a node speaks, and
/// that is the point: with *n* nodes each publishing four times a second, a
/// merge per arrival would produce 4n views a second, most of them differing
/// from the last only in which node had just spoken. A fixed tick makes every
/// published view a reading of the same instant across every node — which is
/// the only kind of view a cross-venue number can honestly be drawn from.
pub fn spawn(
    handle: GatewayHandle,
    clock: Arc<dyn Clock>,
    tick: Duration,
    policy: GatewayPolicy,
    mut shutdown: Shutdown,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut seq = 0_u64;
        let mut ticker = tokio::time::interval(tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.wait() => break,
                _ = ticker.tick() => {}
            }
            seq += 1;
            let reports = handle.reports();
            let merged = merge(&reports, clock.now(), seq, policy);
            if !merged.duplicated.is_empty() {
                // Loud on purpose. This is the violation v3's whole design
                // exists to prevent, and the gateway is the only place in the
                // system that can observe it.
                warn!(
                    duplicated = ?merged.duplicated,
                    "two nodes are running the same stream"
                );
            }
            let _ = handle.merged.send(Arc::new(merged));
        }
    })
}

pub fn router(handle: GatewayHandle) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/events", get(events))
        .route("/api/snapshot", get(snapshot))
        .route("/nodes", get(nodes))
        .route("/metrics", get(metrics))
        .route("/health", get(health))
        .with_state(handle)
}

async fn index() -> Html<&'static str> {
    Html(INDEX)
}

async fn health() -> &'static str {
    "ok\n"
}

async fn snapshot(State(handle): State<GatewayHandle>) -> impl IntoResponse {
    let mut rx = handle.subscribe();
    match rx.recv().await {
        Ok(merged) => Json(serde_json::json!(&*merged)).into_response(),
        Err(e) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("no merged snapshot available: {e}"),
        )
            .into_response(),
    }
}

/// Just the node table and any duplicates — the operator's view.
async fn nodes(State(handle): State<GatewayHandle>) -> impl IntoResponse {
    let mut rx = handle.subscribe();
    match rx.recv().await {
        Ok(merged) => Json(serde_json::json!({
            "nodes": merged.nodes,
            "duplicated": merged.duplicated,
        }))
        .into_response(),
        Err(e) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("no merged snapshot available: {e}"),
        )
            .into_response(),
    }
}

/// Server-sent events, with the same `Lagged` handling a node uses — skip
/// forward, and say how far. See `crate::http` for the argument; it does not
/// change because the producer is now a merge rather than an aggregator.
async fn events(
    State(handle): State<GatewayHandle>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = handle.subscribe();
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(merged) => match Event::default().event("snapshot").json_data(&*merged) {
                    Ok(event) => yield Ok(event),
                    Err(e) => warn!(error = %e, "could not serialise a merged snapshot"),
                },
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    debug!(missed, "client fell behind; skipping to the latest merged view");
                    if let Ok(event) = Event::default()
                        .event("lagged")
                        .json_data(serde_json::json!({ "missed": missed }))
                    {
                        yield Ok(event);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// Prometheus text.
///
/// # Why this does not re-export the nodes' per-stream counters
///
/// Every node already exposes `ma_frames_total`, `ma_book_live` and the rest,
/// labelled by venue and symbol, and a Prometheus setup scrapes all of them.
/// Re-publishing those here would double-count in any query that sums across
/// targets — and silently, because both copies are correct and neither says it
/// is a copy.
///
/// So this exposes only what a gateway is the *sole* source of: how the nodes
/// are doing as seen from outside, the merged cross-venue reading, and the
/// duplicate-stream alarm no single node can raise.
async fn metrics(State(handle): State<GatewayHandle>) -> impl IntoResponse {
    let mut out = String::new();
    let Ok(merged) = handle.subscribe().recv().await else {
        return (
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            out,
        );
    };

    out.push_str(
        "# HELP ma_gateway_nodes_configured Nodes this gateway was told about.\n\
         # TYPE ma_gateway_nodes_configured gauge\n",
    );
    out.push_str(&format!(
        "ma_gateway_nodes_configured {}\n",
        merged.nodes.len()
    ));
    out.push_str(
        "# HELP ma_gateway_nodes_merged Nodes currently contributing. Below \
         ma_gateway_nodes_configured means the merged view is narrower than it was asked to \
         be, which every number derived from it inherits.\n\
         # TYPE ma_gateway_nodes_merged gauge\n",
    );
    out.push_str(&format!(
        "ma_gateway_nodes_merged {}\n",
        merged.nodes_used()
    ));

    out.push_str(
        "# HELP ma_gateway_duplicated_streams Streams claimed by more than one node. \
         ALERT ON THIS: at most one node running a given stream is the property the whole \
         sharding design exists to guarantee, and a gateway is the only vantage point from \
         which a violation is visible. Anything above zero means two websocket connections \
         to a venue that bans for exactly that.\n\
         # TYPE ma_gateway_duplicated_streams gauge\n",
    );
    out.push_str(&format!(
        "ma_gateway_duplicated_streams {}\n",
        merged.duplicated.len()
    ));

    out.push_str(
        "# HELP ma_gateway_node_lag_ms Time since this node's last snapshot arrived, on the \
         gateway's own monotonic clock. Never a difference of two machines' wall clocks — \
         see the gateway module docs.\n\
         # TYPE ma_gateway_node_lag_ms gauge\n",
    );
    for node in &merged.nodes {
        if let Some(lag) = node.lag_ms {
            out.push_str(&format!(
                "ma_gateway_node_lag_ms{{node=\"{}\"}} {lag}\n",
                node.node
            ));
        }
    }
    out.push_str(
        "# HELP ma_gateway_node_included 1 when this node contributed to the latest merge.\n\
         # TYPE ma_gateway_node_included gauge\n",
    );
    for node in &merged.nodes {
        out.push_str(&format!(
            "ma_gateway_node_included{{node=\"{}\"}} {}\n",
            node.node,
            u8::from(node.included)
        ));
    }
    out.push_str(
        "# HELP ma_gateway_node_streams Streams this node reported. Summed over included \
         nodes this should equal the cluster's configured stream count: less means a stream \
         nobody is running.\n\
         # TYPE ma_gateway_node_streams gauge\n",
    );
    for node in &merged.nodes {
        out.push_str(&format!(
            "ma_gateway_node_streams{{node=\"{}\"}} {}\n",
            node.node, node.streams
        ));
    }
    out.push_str(
        "# HELP ma_gateway_node_failures_total Connection or parse failures against this \
         node, since the gateway started.\n\
         # TYPE ma_gateway_node_failures_total counter\n",
    );
    for node in &merged.nodes {
        out.push_str(&format!(
            "ma_gateway_node_failures_total{{node=\"{}\"}} {}\n",
            node.node, node.failures
        ));
    }

    // The reason the gateway exists: a consolidated touch over every venue in
    // the cluster rather than over the venues one node happens to own. Same
    // series names as a node's, because they mean the same thing — but read
    // `ma_gateway_nodes_merged` beside them, exactly as a node's are read
    // beside `ma_cross_venues_used`.
    out.push_str(
        "# HELP ma_cross_spread_bps Best ask minus best bid across every venue in the \
         cluster, in basis points of the consolidated mid. SIGNED: negative means crossed. \
         Read ma_cross_oldest_leg_ms beside it — the quotes were never simultaneous, and \
         across nodes they also travelled a network.\n\
         # TYPE ma_cross_spread_bps gauge\n",
    );
    for symbol in &merged.snapshot.symbols {
        if let Some(bps) = &symbol.cross.spread_bps {
            out.push_str(&format!(
                "ma_cross_spread_bps{{symbol=\"{}\"}} {bps}\n",
                symbol.symbol
            ));
        }
    }
    out.push_str(
        "# HELP ma_cross_venues_used Venues contributing a side to the merged touch.\n\
         # TYPE ma_cross_venues_used gauge\n",
    );
    for symbol in &merged.snapshot.symbols {
        out.push_str(&format!(
            "ma_cross_venues_used{{symbol=\"{}\"}} {}\n",
            symbol.symbol, symbol.cross.venues_used
        ));
    }
    out.push_str(
        "# HELP ma_cross_oldest_leg_ms Age of the older leg, including the network hop that \
         delivered it. Bounds how simultaneous the merged reading is.\n\
         # TYPE ma_cross_oldest_leg_ms gauge\n",
    );
    for symbol in &merged.snapshot.symbols {
        if let Some(age) = symbol.cross.oldest_leg_ms {
            out.push_str(&format!(
                "ma_cross_oldest_leg_ms{{symbol=\"{}\"}} {age}\n",
                symbol.symbol
            ));
        }
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        out,
    )
}

/// Serve until `shutdown` fires.
///
/// # Errors
/// If the port cannot be bound, or the server fails while running.
pub async fn serve(
    addr: std::net::SocketAddr,
    handle: GatewayHandle,
    mut shutdown: Shutdown,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "gateway http listening");
    axum::serve(listener, router(handle))
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await
}
