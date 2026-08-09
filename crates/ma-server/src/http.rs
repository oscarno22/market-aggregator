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
use ma_core::WindowReading;
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

    // Two labels, not one joined `stream` label. A dashboard has to be able to
    // ask both "how is Coinbase doing" and "how is BTC-USD doing", and a single
    // `coinbase:BTC-USD` label would force string surgery in every query to get
    // either. See `ma_core::stream::StreamId::key`.
    let mut metric = |name: &str, help: &str, kind: &str, values: &[(String, String, u64)]| {
        out.push_str(&format!(
            "# HELP ma_{name} {help}\n# TYPE ma_{name} {kind}\n"
        ));
        for (venue, symbol, value) in values {
            out.push_str(&format!(
                "ma_{name}{{venue=\"{venue}\",symbol=\"{symbol}\"}} {value}\n"
            ));
        }
    };

    let per_stream = |f: fn(&ma_pipeline::metrics::VenueCountersSnapshot) -> u64| {
        counters
            .iter()
            .map(|(stream, c)| (stream.venue.to_string(), stream.symbol.to_string(), f(c)))
            .collect::<Vec<_>>()
    };

    metric(
        "frames_total",
        "Frames received from a venue.",
        "counter",
        &per_stream(|c| c.frames),
    );
    metric(
        "bytes_total",
        "Bytes received from a venue.",
        "counter",
        &per_stream(|c| c.bytes),
    );
    metric(
        "connects_total",
        "Successful venue connections. Reconnects are this less one.",
        "counter",
        &per_stream(|c| c.connects),
    );
    metric(
        "disconnects_total",
        "Connections that dropped mid-stream.",
        "counter",
        &per_stream(|c| c.disconnects),
    );
    metric(
        "connect_failures_total",
        "Attempts that never opened a socket. Often a rate limit.",
        "counter",
        &per_stream(|c| c.connect_failures),
    );
    metric(
        "idle_timeouts_total",
        "Sessions killed by the idle watchdog: open socket, no data.",
        "counter",
        &per_stream(|c| c.idle_timeouts),
    );
    metric(
        "dropped_total",
        "Frames evicted from the ingest channel before the aggregator read them.",
        "counter",
        &per_stream(|c| c.dropped),
    );
    metric(
        "applied_total",
        "Messages the aggregator processed. frames_total minus dropped_total.",
        "counter",
        &per_stream(|c| c.applied),
    );
    metric(
        "parse_errors_total",
        "Frames the venue parser rejected. Non-zero suggests venue schema drift.",
        "counter",
        &per_stream(|c| c.parse_errors),
    );
    metric(
        "desyncs_total",
        "Times a book went from trusted to untrusted.",
        "counter",
        &per_stream(|c| c.desyncs),
    );
    metric(
        "rest_failures_total",
        "Failed REST depth snapshot fetches (recovery, not audit).",
        "counter",
        &per_stream(|c| c.rest_failures),
    );
    metric(
        "audit_fetches_total",
        "Periodic REST depth audits requested.",
        "counter",
        &per_stream(|c| c.audit_fetches),
    );
    metric(
        "audit_failures_total",
        "Periodic depth audits that could not be fetched. A book that cannot be \
         checked, which is not the same as a book that failed a check.",
        "counter",
        &per_stream(|c| c.audit_failures),
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
        for (symbol, v) in snapshot.views() {
            if let Some(age) = v.age_ms {
                out.push_str(&format!(
                    "ma_book_age_ms{{venue=\"{}\",symbol=\"{symbol}\"}} {age}\n",
                    v.venue
                ));
            }
        }
        out.push_str(
            "# HELP ma_desynced_total_ms Cumulative time a book has spent untrusted.\n\
             # TYPE ma_desynced_total_ms counter\n",
        );
        for (symbol, v) in snapshot.views() {
            out.push_str(&format!(
                "ma_desynced_total_ms{{venue=\"{}\",symbol=\"{symbol}\"}} {}\n",
                v.venue, v.desynced_total_ms
            ));
        }
        out.push_str(
            "# HELP ma_book_live Whether a book is currently trusted.\n\
             # TYPE ma_book_live gauge\n",
        );
        for (symbol, v) in snapshot.views() {
            let live = u8::from(v.status == ma_pipeline::aggregator::BookStatus::Live);
            out.push_str(&format!(
                "ma_book_live{{venue=\"{}\",symbol=\"{symbol}\"}} {live}\n",
                v.venue
            ));
        }
        out.push_str(
            "# HELP ma_audits_total Depth audits compared against a live book.\n\
             # TYPE ma_audits_total counter\n",
        );
        for (symbol, v) in snapshot.views() {
            out.push_str(&format!(
                "ma_audits_total{{venue=\"{}\",symbol=\"{symbol}\"}} {}\n",
                v.venue, v.audits
            ));
        }
        out.push_str(
            "# HELP ma_audit_mismatches_total Depth audits that disagreed with our book. \
             The drift signal for venues that publish no checksum; a single mismatch is \
             expected noise, a climbing count is not.\n\
             # TYPE ma_audit_mismatches_total counter\n",
        );
        for (symbol, v) in snapshot.views() {
            out.push_str(&format!(
                "ma_audit_mismatches_total{{venue=\"{}\",symbol=\"{symbol}\"}} {}\n",
                v.venue, v.audit_mismatches
            ));
        }
        out.push_str(
            "# HELP ma_book_levels Levels currently held per side. The full book, not \
             the depth served to clients.\n\
             # TYPE ma_book_levels gauge\n",
        );
        for (symbol, v) in snapshot.views() {
            for (side, n) in [("bid", v.levels_held[0]), ("ask", v.levels_held[1])] {
                out.push_str(&format!(
                    "ma_book_levels{{venue=\"{}\",symbol=\"{symbol}\",side=\"{side}\"}} {n}\n",
                    v.venue
                ));
            }
        }

        // Rolling windows carry a third label, `window`, naming the span. It
        // is derived from the *measured* `span_ms` rather than echoing what
        // was typed on the command line, so `--windows 1m` labels itself `60s`
        // and a span rounded up to a whole bucket labels itself with the span
        // that was actually examined.
        //
        // Not an index: a series named after position would silently re-point
        // at a different span the day someone reorders `--windows`, and every
        // historical query would change meaning without breaking.
        //
        // `ma_window_trusted_ms` is emitted first and deliberately: it is the
        // denominator for everything below it. A range or a change read
        // without it is a number over an unknown amount of time.
        out.push_str(
            "# HELP ma_window_trusted_ms Milliseconds of this window during which the book \
             was trusted. Compare against ma_window_span_ms: anything less means every other \
             window series for this stream covers less time than its label.\n\
             # TYPE ma_window_trusted_ms gauge\n",
        );
        for (symbol, v) in snapshot.views() {
            for w in &v.windows {
                out.push_str(&format!(
                    "ma_window_trusted_ms{{venue=\"{}\",symbol=\"{symbol}\",window=\"{}\"}} {}\n",
                    v.venue,
                    window_label(w.span_ms),
                    w.trusted_ms
                ));
            }
        }
        out.push_str(
            "# HELP ma_window_span_ms The window actually examined, rounded up to a whole \
             bucket.\n\
             # TYPE ma_window_span_ms gauge\n",
        );
        for (symbol, v) in snapshot.views() {
            for w in &v.windows {
                out.push_str(&format!(
                    "ma_window_span_ms{{venue=\"{}\",symbol=\"{symbol}\",window=\"{}\"}} {}\n",
                    v.venue,
                    window_label(w.span_ms),
                    w.span_ms
                ));
            }
        }

        // The consolidated touch is per *symbol*, so these carry no `venue`
        // label — which is the point of them. The two legs are named in the
        // JSON snapshot rather than as labels here, because a series whose
        // labels changed every time the best bid moved between venues would
        // start a new time series on each hop and be unqueryable.
        out.push_str(
            "# HELP ma_cross_spread_bps Best ask minus best bid across venues, in basis points \
             of the consolidated mid. SIGNED: negative means the venues' books are crossed, and \
             the magnitude is an apparent arbitrage gross of fees, latency and transfer time. \
             Read ma_cross_oldest_leg_ms beside it — the two quotes were never simultaneous.\n\
             # TYPE ma_cross_spread_bps gauge\n",
        );
        for symbol in &snapshot.symbols {
            if let Some(bps) = &symbol.cross.spread_bps {
                out.push_str(&format!(
                    "ma_cross_spread_bps{{symbol=\"{}\"}} {bps}\n",
                    symbol.symbol
                ));
            }
        }
        out.push_str(
            "# HELP ma_cross_venues_used Venues contributing a side to the consolidated touch. \
             A drop here narrows every cross-venue number without changing its shape, which is \
             why it is published beside them.\n\
             # TYPE ma_cross_venues_used gauge\n",
        );
        for symbol in &snapshot.symbols {
            out.push_str(&format!(
                "ma_cross_venues_used{{symbol=\"{}\"}} {}\n",
                symbol.symbol, symbol.cross.venues_used
            ));
        }
        out.push_str(
            "# HELP ma_cross_oldest_leg_ms Age of the older of the two legs. Bounds how \
             simultaneous the consolidated reading is.\n\
             # TYPE ma_cross_oldest_leg_ms gauge\n",
        );
        for symbol in &snapshot.symbols {
            if let Some(age) = symbol.cross.oldest_leg_ms {
                out.push_str(&format!(
                    "ma_cross_oldest_leg_ms{{symbol=\"{}\"}} {age}\n",
                    symbol.symbol
                ));
            }
        }

        // Prometheus has no notion of "absent" inside a sample, so a window
        // with no data must emit no series rather than a zero. A zero range
        // and an unknown range are the same line otherwise, and the whole
        // point of `Option` on WindowReading is that they are not the same
        // thing.
        for (name, help, pick) in WINDOW_GAUGES {
            out.push_str(&format!(
                "# HELP ma_{name} {help}\n# TYPE ma_{name} gauge\n"
            ));
            for (symbol, v) in snapshot.views() {
                for w in &v.windows {
                    let Some(value) = pick(w) else { continue };
                    out.push_str(&format!(
                        "ma_{name}{{venue=\"{}\",symbol=\"{symbol}\",window=\"{}\"}} {value}\n",
                        v.venue,
                        window_label(w.span_ms),
                    ));
                }
            }
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

/// The `window` label for a measured span. Seconds when it divides evenly,
/// milliseconds otherwise — the two spellings a configured span can have.
fn window_label(span_ms: u64) -> String {
    if span_ms.is_multiple_of(1000) {
        format!("{}s", span_ms / 1000)
    } else {
        format!("{span_ms}ms")
    }
}

/// The optional window series, and how to read each one.
///
/// A table rather than six near-identical loops because they differ only in
/// name and accessor, and the thing that must not vary between them — emitting
/// *nothing* when the reading is `None` rather than a zero — is then written
/// once. A window with no trusted samples has no range; publishing `0` for it
/// would put a flat line on a dashboard where the honest rendering is a gap.
type WindowGauge = (
    &'static str,
    &'static str,
    fn(&WindowReading) -> Option<String>,
);

const WINDOW_GAUGES: [WindowGauge; 6] = [
    (
        "window_samples",
        "Book updates that produced a two-sided mid inside the window. Small with a full \
         ma_window_trusted_ms means a quiet market; small with a small one means no data.",
        |w| Some(w.samples.to_string()),
    ),
    (
        "window_mid",
        "Mean mid over the window, sample-weighted.",
        |w| w.mean.map(|d| d.to_string()),
    ),
    ("window_high", "Highest mid observed in the window.", |w| {
        w.high.map(|d| d.to_string())
    }),
    ("window_low", "Lowest mid observed in the window.", |w| {
        w.low.map(|d| d.to_string())
    }),
    (
        "window_change_bps",
        "Signed move from the first mid in the window to the last, in basis points.",
        |w| w.change_bps.map(|d| d.to_string()),
    ),
    (
        "window_range_bps",
        "High minus low over the window's mean mid, in basis points. The volatility proxy \
         this build publishes; see ma_core::window for why it is not realised volatility.",
        |w| w.range_bps.map(|d| d.to_string()),
    ),
];

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
        handle_over(&[Symbol::new("BTC-USD")])
    }

    fn handle_over(symbols: &[Symbol]) -> (PipelineHandle, Aggregator) {
        let venues = vec![VenueId::Coinbase, VenueId::Kraken];
        let streams: Vec<ma_core::StreamId> = venues
            .iter()
            .flat_map(|v| {
                symbols
                    .iter()
                    .map(|s| ma_core::StreamId::new(*v, s.clone()))
            })
            .collect();
        let metrics = Arc::new(Metrics::new(streams));
        let specs = venues
            .iter()
            .flat_map(|v| symbols.iter().map(|s| ma_venues::spec_for(*v, s).unwrap()))
            .collect();
        let agg = Aggregator::new(specs, Arc::new(ma_core::SystemClock), &metrics);
        (
            PipelineHandle {
                snapshots: agg.publisher(),
                metrics,
                symbols: symbols.to_vec(),
                venues,
                windows: ma_core::WindowSpec::default(),
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

    /// Scrape `/metrics`, publishing snapshots in the background because the
    /// handler waits for a tick rather than caching one.
    async fn scrape(handle: PipelineHandle, mut agg: Aggregator) -> String {
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
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn window_series_are_labelled_by_span_and_absent_when_there_is_no_data() {
        let (handle, agg) = handle();
        let text = scrape(handle, agg).await;

        // Labelled by span, not by position in the list. A positional label
        // would silently re-point at a different window the day `--windows` is
        // reordered, and every historical query would change meaning.
        assert!(
            text.contains(
                r#"ma_window_trusted_ms{venue="coinbase",symbol="BTC-USD",window="60s"}"#
            ),
            "window series are not labelled by span:\n{text}"
        );

        // No book has ever synced here, so there is no range to report. The
        // series must be *absent* rather than zero: Prometheus cannot express
        // "unknown" inside a sample, and a zero range on a dashboard is a flat
        // line where the honest rendering is a gap.
        assert!(
            text.contains("# TYPE ma_window_range_bps gauge"),
            "the range metric was not declared at all:\n{text}"
        );
        assert!(
            !text.contains("ma_window_range_bps{"),
            "a window with no samples published a range anyway:\n{text}"
        );
    }

    #[tokio::test]
    async fn metrics_are_prometheus_text_with_venue_and_symbol_labels() {
        let (handle, mut agg) = handle_over(&[Symbol::new("BTC-USD"), Symbol::new("ETH-USD")]);
        handle
            .metrics
            .stream(&ma_core::StreamId::new(
                VenueId::Kraken,
                Symbol::new("BTC-USD"),
            ))
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
        // Both labels, so a query can aggregate over either axis. A single
        // joined `stream` label would force string surgery in every dashboard.
        assert!(
            text.contains(r#"ma_frames_total{venue="kraken",symbol="BTC-USD"} 1"#),
            "missing the labelled counter:\n{text}"
        );
        // The claim that makes the label worth having: the *same venue's*
        // other symbol is a separate series and did not absorb the count.
        assert!(
            text.contains(r#"ma_frames_total{venue="kraken",symbol="ETH-USD"} 0"#),
            "a second symbol on the same venue shared its counters:\n{text}"
        );
        assert!(text.contains(r#"ma_frames_total{venue="coinbase",symbol="BTC-USD"} 0"#));
        assert!(
            text.contains("ma_book_live"),
            "book gauges missing:\n{text}"
        );
        assert!(
            text.contains(r#"ma_book_levels{venue="kraken",symbol="BTC-USD",side="bid"}"#),
            "depth gauge missing:\n{text}"
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
