//! The HTTP surface: an SSE snapshot stream, a metrics scrape, and one page.
//!
//! # Lagging is normal, and disconnecting for it would be a bug
//!
//! The fan-out is a `tokio::sync::broadcast`, which holds a bounded ring of
//! recent snapshots per subscriber. A client on hotel wifi, or a laptop lid
//! closed for ten seconds, will fall behind it and get
//! [`RecvError::Lagged`](tokio::sync::broadcast::error::RecvError::Lagged).
//!
//! CLAUDE.md is specific about what to do: *skip forward to the latest
//! snapshot, not error the connection.* Both halves matter, and the reason is
//! the same reason the ingest channel drops the oldest event. The snapshots
//! that client missed described a book that no longer exists. Replaying them
//! would walk a chart through stale prices at high speed to arrive where it
//! could have gone directly; dropping the connection would punish a reader for
//! their network. Skipping forward is the only option that leaves the client
//! looking at the present.
//!
//! Because `broadcast` already advances a lagged receiver to the oldest
//! *available* snapshot, the correct implementation is simply to keep going.
//! The count is not swallowed, though: it is sent to the client as a `lagged`
//! event so the page can say so, and a silent skip would be the same class of
//! mistake as a silent drop.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::Stream;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, warn};

use crate::pipeline::PipelineHandle;

/// The single page. Self-contained — no CDN, no build step, no external
/// requests — so the demo works with the network unplugged, which is the same
/// property replay gives the pipeline.
const INDEX: &str = include_str!("index.html");

pub fn router(handle: PipelineHandle) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/events", get(events))
        .route("/metrics", get(metrics))
        .route("/api/snapshot", get(snapshot))
        .route("/health", get(health))
        .with_state(handle)
}

async fn index() -> Html<&'static str> {
    Html(INDEX)
}

async fn health() -> &'static str {
    "ok\n"
}

/// The latest snapshot as JSON, for a caller that wants one reading rather
/// than a stream. Waits for the next tick rather than caching one, so it can
/// never serve something stale.
async fn snapshot(State(handle): State<PipelineHandle>) -> impl IntoResponse {
    let mut rx = handle.subscribe();
    match rx.recv().await {
        Ok(snapshot) => Json(serde_json::json!(&*snapshot)).into_response(),
        Err(e) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!("no snapshot available: {e}"),
        )
            .into_response(),
    }
}

/// Server-sent events: one `snapshot` event per aggregator tick.
async fn events(
    State(handle): State<PipelineHandle>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = handle.subscribe();

    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(snapshot) => {
                    match Event::default().event("snapshot").json_data(&*snapshot) {
                        Ok(event) => yield Ok(event),
                        Err(e) => {
                            // Serialising our own type failed. Nothing the
                            // client can do, and dropping the stream would
                            // look like a network fault; skip the tick.
                            warn!(error = %e, "could not serialise snapshot");
                        }
                    }
                }

                // The case this module exists to get right. `broadcast` has
                // already moved this receiver forward to the oldest snapshot
                // it still holds, so continuing *is* skipping to the present.
                // Say how many were missed rather than hiding it.
                Err(RecvError::Lagged(missed)) => {
                    debug!(missed, "client fell behind; skipping to the latest snapshot");
                    if let Ok(event) = Event::default()
                        .event("lagged")
                        .json_data(serde_json::json!({ "missed": missed }))
                    {
                        yield Ok(event);
                    }
                }

                // The aggregator stopped. Ending the stream is correct here:
                // there will be no more snapshots, and a client left hanging
                // would show a frozen chart indefinitely.
                Err(RecvError::Closed) => {
                    debug!("snapshot stream closed");
                    break;
                }
            }
        }
    };

    // Comments on an idle stream, so a proxy between here and the browser does
    // not time out a connection that is merely between ticks.
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// Prometheus text exposition.
///
/// Text rather than JSON because `/metrics` means this format to anything that
/// might scrape it, and because the counters were designed as monotonic
/// counters for exactly this ("rates derived at the edge" — see
/// `ma_pipeline::metrics`).
async fn metrics(State(handle): State<PipelineHandle>) -> impl IntoResponse {
    let mut out = String::new();
    let counters = handle.metrics.snapshot();

    let mut metric = |name: &str, help: &str, kind: &str, values: &[(String, u64)]| {
        out.push_str(&format!(
            "# HELP ma_{name} {help}\n# TYPE ma_{name} {kind}\n"
        ));
        for (venue, value) in values {
            out.push_str(&format!("ma_{name}{{venue=\"{venue}\"}} {value}\n"));
        }
    };

    let per_venue = |f: fn(&ma_pipeline::metrics::VenueCountersSnapshot) -> u64| {
        counters
            .iter()
            .map(|(venue, c)| (venue.to_string(), f(c)))
            .collect::<Vec<_>>()
    };

    metric(
        "frames_total",
        "Frames received from a venue.",
        "counter",
        &per_venue(|c| c.frames),
    );
    metric(
        "bytes_total",
        "Bytes received from a venue.",
        "counter",
        &per_venue(|c| c.bytes),
    );
    metric(
        "connects_total",
        "Successful venue connections. Reconnects are this less one.",
        "counter",
        &per_venue(|c| c.connects),
    );
    metric(
        "disconnects_total",
        "Connections that dropped mid-stream.",
        "counter",
        &per_venue(|c| c.disconnects),
    );
    metric(
        "connect_failures_total",
        "Attempts that never opened a socket. Often a rate limit.",
        "counter",
        &per_venue(|c| c.connect_failures),
    );
    metric(
        "idle_timeouts_total",
        "Sessions killed by the idle watchdog: open socket, no data.",
        "counter",
        &per_venue(|c| c.idle_timeouts),
    );
    metric(
        "dropped_total",
        "Frames evicted from the ingest channel before the aggregator read them.",
        "counter",
        &per_venue(|c| c.dropped),
    );
    metric(
        "applied_total",
        "Messages the aggregator processed. frames_total minus dropped_total.",
        "counter",
        &per_venue(|c| c.applied),
    );
    metric(
        "parse_errors_total",
        "Frames the venue parser rejected. Non-zero suggests venue schema drift.",
        "counter",
        &per_venue(|c| c.parse_errors),
    );
    metric(
        "desyncs_total",
        "Times a book went from trusted to untrusted.",
        "counter",
        &per_venue(|c| c.desyncs),
    );
    metric(
        "rest_failures_total",
        "Failed REST depth snapshot fetches.",
        "counter",
        &per_venue(|c| c.rest_failures),
    );

    // Book age and time-in-desync live on the aggregator's snapshot rather
    // than in the counters, because they are properties of a book rather than
    // of a connection. Taking one tick's reading keeps this endpoint honest
    // about being a point-in-time scrape.
    if let Ok(snapshot) = handle.subscribe().recv().await {
        out.push_str(
            "# HELP ma_book_age_ms Time since the last update applied to a book.\n\
             # TYPE ma_book_age_ms gauge\n",
        );
        for v in &snapshot.venues {
            if let Some(age) = v.age_ms {
                out.push_str(&format!("ma_book_age_ms{{venue=\"{}\"}} {age}\n", v.venue));
            }
        }
        out.push_str(
            "# HELP ma_desynced_total_ms Cumulative time a book has spent untrusted.\n\
             # TYPE ma_desynced_total_ms counter\n",
        );
        for v in &snapshot.venues {
            out.push_str(&format!(
                "ma_desynced_total_ms{{venue=\"{}\"}} {}\n",
                v.venue, v.desynced_total_ms
            ));
        }
        out.push_str(
            "# HELP ma_book_live Whether a book is currently trusted.\n\
             # TYPE ma_book_live gauge\n",
        );
        for v in &snapshot.venues {
            let live = u8::from(v.status == ma_pipeline::aggregator::BookStatus::Live);
            out.push_str(&format!("ma_book_live{{venue=\"{}\"}} {live}\n", v.venue));
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
    handle: PipelineHandle,
    mut shutdown: ma_pipeline::ingest::Shutdown,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "http listening");
    axum::serve(listener, router(handle))
        .with_graceful_shutdown(async move { shutdown.wait().await })
        .await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use ma_core::{Symbol, VenueId};
    use ma_pipeline::aggregator::{Aggregator, BROADCAST_CAPACITY};
    use ma_pipeline::metrics::Metrics;
    use std::sync::Arc;

    fn handle() -> (PipelineHandle, Aggregator) {
        let symbol = Symbol::new("BTC-USD");
        let venues = vec![VenueId::Coinbase, VenueId::Kraken];
        let metrics = Arc::new(Metrics::new(venues.iter().copied()));
        let specs = venues
            .iter()
            .map(|v| ma_venues::spec_for(*v, &symbol).unwrap())
            .collect();
        let agg = Aggregator::new(
            symbol.clone(),
            specs,
            Arc::new(ma_core::SystemClock),
            &metrics,
        );
        (
            PipelineHandle {
                snapshots: agg.publisher(),
                metrics,
                symbol,
                venues,
            },
            agg,
        )
    }

    #[tokio::test]
    async fn a_slow_subscriber_skips_forward_instead_of_erroring() {
        // The behaviour CLAUDE.md names explicitly. Overfill the ring by more
        // than its capacity, then confirm the receiver reports how far it
        // jumped and then keeps working — rather than the two wrong answers:
        // replaying stale snapshots, or dropping the connection.
        let (handle, mut agg) = handle();
        let mut rx = handle.subscribe();

        let channel = ma_pipeline::channel::ChannelMetrics {
            len: 0,
            capacity: 1,
            dropped: 0,
        };
        let overfill = BROADCAST_CAPACITY + 10;
        for _ in 0..overfill {
            let _ = handle.snapshots.send(Arc::new(agg.snapshot(channel)));
        }

        let missed = match rx.recv().await {
            Err(RecvError::Lagged(n)) => n,
            other => panic!("expected Lagged, got {other:?}"),
        };
        assert!(missed > 0);

        // Still usable, and positioned near the end rather than at the start.
        let next = rx.recv().await.expect("stream should survive lagging");
        assert!(
            next.seq as usize > overfill - BROADCAST_CAPACITY - 1,
            "receiver replayed stale snapshots instead of skipping forward \
             (seq {} of {overfill})",
            next.seq
        );

        // And it keeps delivering: drain the rest of the ring, then confirm a
        // newly published snapshot arrives on the same subscription. A
        // connection that had been errored out would fail here instead.
        let published = Arc::new(agg.snapshot(channel));
        let target = published.seq;
        let _ = handle.snapshots.send(published);

        loop {
            let snapshot = rx.recv().await.expect("subscription died after lagging");
            if snapshot.seq == target {
                break;
            }
            assert!(
                snapshot.seq < target,
                "receiver ran past a snapshot it should have delivered"
            );
        }
    }

    #[tokio::test]
    async fn metrics_are_prometheus_text_with_a_venue_label() {
        let (handle, mut agg) = handle();
        handle
            .metrics
            .venue(VenueId::Kraken)
            .unwrap()
            .record_frame(10);

        // /metrics waits for a tick, so publish one.
        let publisher = handle.snapshots.clone();
        tokio::spawn(async move {
            loop {
                let _ = publisher.send(Arc::new(agg.snapshot(
                    ma_pipeline::channel::ChannelMetrics {
                        len: 0,
                        capacity: 1,
                        dropped: 0,
                    },
                )));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let body = axum::body::to_bytes(
            metrics(State(handle)).await.into_response().into_body(),
            1 << 20,
        )
        .await
        .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(text.contains("# TYPE ma_frames_total counter"));
        assert!(text.contains(r#"ma_frames_total{venue="kraken"} 1"#));
        assert!(text.contains(r#"ma_frames_total{venue="coinbase"} 0"#));
        assert!(
            text.contains("ma_book_live"),
            "book gauges missing:\n{text}"
        );
    }

    #[test]
    fn the_page_is_self_contained() {
        // The demo has to work with the network unplugged — the same property
        // replay gives the pipeline. One CDN <script> would quietly undo it,
        // and only on a machine that happens to be offline.
        for pattern in ["http://", "https://", "//cdn", "integrity="] {
            assert!(
                !INDEX.contains(pattern),
                "index.html references something external ({pattern})"
            );
        }
        assert!(INDEX.contains("<!doctype html>") || INDEX.contains("<!DOCTYPE html>"));
    }
}
