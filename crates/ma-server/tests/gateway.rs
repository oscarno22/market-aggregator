//! Two real nodes, two real sockets, one merged view.
//!
//! `gateway::merge` is pure and unit-tested against a `TestClock`, which is
//! where the staleness and duplicate rules are actually proven. What that
//! cannot reach is everything between two processes: whether a node's snapshot
//! survives its own `/events` encoder, arrives across TCP in whatever chunks
//! the kernel chose, is reassembled by the SSE framing, and parses back into
//! the same type.
//!
//! That path is new in v4 and it is the one with a history. `Snapshot` was a
//! *write-only* wire format for three milestones; the gateway makes it a
//! contract in both directions, and a field that serialises but cannot be read
//! back would break nothing visible until a cluster was actually merged.
//!
//! So this test starts two real `ma-server` HTTP surfaces on ephemeral ports —
//! each fed by replaying the committed tape, so their books hold real venue
//! data — and follows them with the real gateway client. **No network beyond
//! loopback, and no venue.**

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ma_core::{CrossPolicy, Symbol, SystemClock, VenueId};
use ma_pipeline::ingest::{Shutdown, ShutdownTrigger, shutdown};
use ma_pipeline::tape::{Pacing, TapeReader, replay};
use ma_server::gateway::{GatewayPolicy, NodeReport, feed, merge};
use ma_server::{Pipeline, http};

fn tape_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tapes/2026-08-09-btc-usd-live.jsonl.gz")
        .canonicalize()
        .expect("the committed tape should exist")
}

/// A node serving the venues it "owns", with real books built from the tape.
struct Node {
    addr: std::net::SocketAddr,
    /// Kept alive: dropping it stops the pipeline this node is serving.
    _trigger: ShutdownTrigger,
    _pipeline: Pipeline,
}

/// Start a node over `venues`, replay the tape into it, and serve it.
///
/// Replay runs to completion before the gateway looks, which is what makes the
/// test deterministic: the books stop changing, so the assertions are about the
/// *transport* rather than about which tick happened to be current. The
/// aggregator keeps ticking and publishing afterwards, which is what the
/// gateway needs.
async fn node(venues: Vec<VenueId>, stop: Shutdown) -> Node {
    let mut pipeline = Pipeline::new(vec![Symbol::new("BTC-USD")], venues)
        .expect("pipeline")
        .with_tick(Duration::from_millis(20));
    let (handle, _aggregator) = pipeline.spawn_aggregator().expect("aggregator");

    let tx = pipeline.channel();
    let clock = pipeline.clock();
    let mut reader = TapeReader::open(tape_path()).await.expect("open tape");
    replay(
        &mut reader,
        &tx,
        clock.as_ref(),
        Pacing::Faithful,
        &Symbol::new("BTC-USD"),
    )
    .await
    .expect("replay");

    // Let the aggregator drain what replay just pushed, so the first snapshot
    // the gateway sees already holds the tape's books rather than empty ones.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = http::router(handle);
    let mut serve_stop = stop.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { serve_stop.wait().await })
            .await;
    });

    Node {
        addr,
        _trigger: {
            // `Pipeline::stop` is on the trigger, and the trigger is what keeps
            // the aggregator alive. Take a fresh pair rather than consuming the
            // pipeline's, which is still needed to hold the channel open.
            let (trigger, _) = shutdown();
            trigger
        },
        _pipeline: pipeline,
    }
}

/// Follow both nodes until each has reported, then merge once.
async fn merged_over(nodes: &[&Node], stop: Shutdown) -> ma_server::gateway::MergedSnapshot {
    let client = feed::client(Duration::from_secs(2)).expect("client");
    let clock: Arc<dyn ma_core::Clock> = Arc::new(SystemClock);

    let mut receivers = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let label = format!("node-{i}");
        let url = format!("http://{}", node.addr);
        let (tx, rx) = tokio::sync::watch::channel(NodeReport::new(label.clone(), url.clone()));
        receivers.push(rx);
        tokio::spawn(feed::follow(
            client.clone(),
            label,
            url,
            tx,
            Arc::clone(&clock),
            stop.clone(),
        ));
    }

    // Wait for a snapshot from every node rather than sleeping a fixed time: a
    // fixed sleep is either flaky or slow, and usually both on a loaded CI box.
    let ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if receivers.iter().all(|rx| rx.borrow().snapshot.is_some()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(ready.is_ok(), "a node never delivered a snapshot over SSE");

    let reports: Vec<NodeReport> = receivers.iter().map(|rx| rx.borrow().clone()).collect();
    merge(
        &reports,
        clock.now(),
        1,
        GatewayPolicy {
            // The tape has finished, so the books stop advancing and their ages
            // grow with wall time from here. The staleness guard is proven
            // against a `TestClock` in the unit tests, where it can be exercised
            // exactly; widening it here keeps *this* test about the transport
            // rather than about how fast the machine ran.
            cross: CrossPolicy {
                max_age: Duration::from_secs(3600),
            },
            ..GatewayPolicy::default()
        },
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_merges_two_nodes_into_one_cross_venue_view() {
    // The thing neither node can serve. Node A holds Coinbase and Kraken, node
    // B holds Bitstamp — so B's own "cross-venue" touch is one venue wearing a
    // cross-venue label, and A's is missing the weakest venue in the cluster.
    let (trigger, stop) = shutdown();
    let a = node(vec![VenueId::Coinbase, VenueId::Kraken], stop.clone()).await;
    let b = node(vec![VenueId::Bitstamp], stop.clone()).await;

    let merged = merged_over(&[&a, &b], stop.clone()).await;

    assert_eq!(merged.nodes.len(), 2);
    assert_eq!(merged.nodes_used(), 2, "{:?}", merged.nodes);
    assert!(
        merged.duplicated.is_empty(),
        "the nodes were given disjoint venues: {:?}",
        merged.duplicated
    );

    let symbol = merged
        .snapshot
        .symbols
        .iter()
        .find(|s| s.symbol == "BTC-USD")
        .expect("BTC-USD in the merged view");

    let venues: BTreeSet<VenueId> = symbol.venues.iter().map(|v| v.venue).collect();
    assert_eq!(
        venues,
        BTreeSet::from([VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp]),
        "the merged view did not carry every node's venues"
    );

    // The number that only exists because the nodes were merged.
    assert!(
        symbol.cross.venues_used >= 2,
        "the consolidated touch was drawn from {} venues: {:?}",
        symbol.cross.venues_used,
        symbol.cross.excluded
    );
    assert!(
        !symbol.cross.single_venue,
        "a touch merged from two nodes reported as single-venue"
    );

    // The weakest guarantee in the cluster is Bitstamp's, and it lives on the
    // *other* node. A merged view that reported Kraken-grade trust here would
    // be the exact failure `Integrity` being `Ord` exists to prevent.
    assert_eq!(
        symbol.weakest_integrity,
        Some(ma_core::Integrity::OrderOnly),
        "the merged view claimed stronger trust than its weakest node supports"
    );

    // Ages are the gateway's own composite, and they say so.
    assert_eq!(merged.snapshot.clock, ma_server::gateway::GATEWAY_CLOCK);
    assert_eq!(symbol.cross.clock, ma_server::gateway::GATEWAY_CLOCK);

    drop(trigger);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_survives_a_nodes_sse_encoder_and_the_gateways_parser() {
    // The contract change v4 made, tested where it actually happens rather than
    // in a serde round trip. A node's `/events` writes the snapshot; TCP
    // delivers it in whatever chunks it likes — a Coinbase ladder does not fit
    // in one — and the gateway's SSE framing has to put it back together before
    // serde ever sees it.
    let (trigger, stop) = shutdown();
    let a = node(
        vec![VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp],
        stop.clone(),
    )
    .await;
    let merged = merged_over(&[&a], stop.clone()).await;

    let symbol = &merged.snapshot.symbols[0];
    assert_eq!(symbol.venues.len(), 3);

    for view in &symbol.venues {
        assert!(view.bid.is_some() && view.ask.is_some());
        // The ladders are the bulky part, and the part a naive
        // one-chunk-is-one-event parser would silently truncate.
        assert!(
            !view.bids.is_empty() && !view.asks.is_empty(),
            "{} arrived with an empty ladder",
            view.venue
        );
        assert!(
            view.levels_held[0] > 0 && view.levels_held[1] > 0,
            "{} lost its held-depth counters",
            view.venue
        );
    }

    let by_venue: std::collections::BTreeMap<VenueId, _> =
        symbol.venues.iter().map(|v| (v.venue, v)).collect();

    // Kraken's integrity is a property of its protocol, so it has to survive
    // the hop unchanged — the strongest single claim in the system, now made by
    // a process that never spoke to Kraken.
    assert_eq!(
        by_venue[&VenueId::Kraken].status,
        ma_pipeline::aggregator::BookStatus::Live
    );
    assert_eq!(
        by_venue[&VenueId::Kraken].integrity,
        Some(ma_core::Integrity::Verified)
    );
    assert_eq!(
        by_venue[&VenueId::Bitstamp].integrity,
        Some(ma_core::Integrity::OrderOnly)
    );

    // **The assertion worth having.** This tape ends with Coinbase desynced by
    // a real depth-audit finding — the same price wrong on two consecutive
    // audits, which is what `AuditPolicy`'s same-price rule exists to catch.
    // So the hop is carrying an *untrusted* book, and the whole premise of this
    // project is that a consumer can tell "no data" from "data I do not trust".
    // Both halves have to arrive: the status, and the reason.
    let coinbase = by_venue[&VenueId::Coinbase];
    assert_eq!(
        coinbase.status,
        ma_pipeline::aggregator::BookStatus::Desynced,
        "this tape's Coinbase book ends desynced by a depth audit; if it now \
         arrives live, either the audit stopped firing or the state was lost \
         crossing the hop"
    );
    assert!(
        coinbase
            .desync_reason
            .as_deref()
            .is_some_and(|r| r.contains("depth audit disagreed")),
        "the desync reason did not survive the hop: {:?}",
        coinbase.desync_reason
    );
    assert_eq!(
        coinbase.integrity, None,
        "a desynced book arrived still claiming an integrity, which is exactly \
         the coupling VenueView exists to enforce"
    );

    // And the merged consolidation acts on it: an untrusted book is not a
    // quote, so Coinbase is excluded by name from a touch drawn over a network.
    assert!(
        symbol
            .cross
            .excluded
            .iter()
            .any(|e| e.venue == VenueId::Coinbase && e.reason.contains("not trusted")),
        "an untrusted book was silently dropped from the merged touch: {:?}",
        symbol.cross.excluded
    );

    drop(trigger);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_is_not_listening_is_reported_rather_than_hidden() {
    // A gateway that quietly served one node's data while two were configured
    // would be publishing the same lie `CrossView::excluded` exists to prevent,
    // one level up. The unreachable node has to appear, with a reason.
    let (trigger, stop) = shutdown();
    let a = node(vec![VenueId::Kraken], stop.clone()).await;

    let client = feed::client(Duration::from_millis(200)).expect("client");
    let clock: Arc<dyn ma_core::Clock> = Arc::new(SystemClock);
    let mut receivers = Vec::new();

    // A port nothing is bound to: bind, read the address, drop the listener.
    let dead = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };

    for (label, addr) in [("live", a.addr), ("dead", dead)] {
        let url = format!("http://{addr}");
        let (tx, rx) = tokio::sync::watch::channel(NodeReport::new(label.to_owned(), url.clone()));
        receivers.push(rx);
        tokio::spawn(feed::follow(
            client.clone(),
            label.to_owned(),
            url,
            tx,
            Arc::clone(&clock),
            stop.clone(),
        ));
    }

    let ready = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let live = receivers[0].borrow().snapshot.is_some();
            let tried = receivers[1].borrow().failures > 0;
            if live && tried {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(ready.is_ok(), "the live node or the failure never arrived");

    let reports: Vec<NodeReport> = receivers.iter().map(|rx| rx.borrow().clone()).collect();
    let merged = merge(&reports, clock.now(), 1, GatewayPolicy::default());

    assert_eq!(merged.nodes.len(), 2, "a configured node vanished");
    assert_eq!(merged.nodes_used(), 1);

    let dead_node = merged.nodes.iter().find(|n| n.node == "dead").unwrap();
    assert!(!dead_node.included);
    assert_eq!(
        dead_node.excluded_because.as_deref(),
        Some("no snapshot received yet")
    );
    assert!(
        dead_node.last_error.is_some(),
        "the reason the node is unreachable was not published"
    );
    assert!(dead_node.failures > 0);

    drop(trigger);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_a_gateway_payload_carries_the_nodes_field() {
    // The contract the page's cluster panel gates on. One index.html serves
    // both a node and a gateway, and the thing that decides whether the
    // panel renders is the presence of `nodes` in the payload — not a build
    // flag, not a URL. So the contract has two halves and both must hold: a
    // gateway's serialised snapshot carries `nodes` and `duplicated`, and a
    // plain node's must never grow them, or every node would draw a cluster
    // panel over data it does not have.
    let (trigger, stop) = shutdown();
    let a = node(vec![VenueId::Kraken], stop.clone()).await;
    let merged = merged_over(&[&a], stop.clone()).await;

    let gateway_json = serde_json::to_value(&merged).expect("serialise the merged snapshot");
    assert!(
        gateway_json.get("nodes").is_some(),
        "a gateway payload lost its nodes field; the page can no longer \
         render the cluster it merges"
    );
    assert!(gateway_json.get("duplicated").is_some());
    assert!(
        gateway_json.get("symbols").is_some(),
        "flattening broke: the node-shaped fields must sit beside nodes, \
         not under a wrapper, or the page's card rendering stops working \
         on a gateway"
    );

    let client = feed::client(Duration::from_secs(2)).expect("client");
    let body = client
        .get(format!("http://{}/api/snapshot", a.addr))
        .send()
        .await
        .expect("node snapshot")
        .text()
        .await
        .expect("body");
    let node_json: serde_json::Value = serde_json::from_str(&body).expect("node JSON");
    assert!(
        node_json.get("nodes").is_none(),
        "a plain node's payload grew a nodes field — every node would now \
         draw a cluster panel"
    );
    assert!(node_json.get("symbols").is_some());

    drop(trigger);
}
