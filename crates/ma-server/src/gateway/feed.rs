//! Reading one node's SSE stream, forever.
//!
//! # Why this is a client of the same endpoint a browser uses
//!
//! The gateway consumes `/events`, exactly as the chart page does, rather than
//! a private node-to-node protocol. That is a deliberate constraint and it buys
//! two things. A second endpoint would be a second serialisation of the same
//! state, and this project's recurring finding is that two derivations of one
//! thing eventually disagree — `docs/DESIGN.md` §8 on why Parquet is teed off
//! the aggregator rather than reading the raw channel. And a contract already
//! exercised by every page load and every replay test is a contract that stays
//! honest.
//!
//! The cost is that `Snapshot` is now a wire format in **both** directions.
//! `ma-server/tests/replay.rs` pins it: a real snapshot, off a real tape,
//! serialised and read back has to be identical.
//!
//! # Reconnecting, with the schedule the ingest tasks already use
//!
//! A node restarting or being deployed is normal, and a gateway that hammered
//! it while it came up would be doing to its own cluster what a reconnect storm
//! does to a venue. So this reuses [`ma_pipeline::backoff`] — exponential with
//! equal jitter, capped, reset only after a session lasts `min_stable` — rather
//! than growing a second retry policy with different opinions.
//!
//! # What a failure here is *not*
//!
//! It is not a reason to stop, and not a reason to drop what the node last
//! said. The report keeps its last snapshot and the merge decides whether it is
//! still usable, on age. Those are different questions: a gateway that dropped
//! a node's books the instant its connection blipped would flap the
//! consolidated touch on every deploy, while one that kept them forever would
//! serve a dead node's quotes. Age answers both, and it is the same answer
//! `ma_core::cross` gives one layer down.

use std::sync::Arc;
use std::time::Duration;

use ma_core::Clock;
use ma_pipeline::aggregator::Snapshot;
use ma_pipeline::backoff::{Backoff, BackoffPolicy, EqualJitter};
use ma_pipeline::ingest::Shutdown;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::NodeReport;

/// Split `--nodes` into `(label, url)` pairs.
///
/// Accepts `http://host:port` or `name=http://host:port`. The label defaults to
/// the URL because a gateway does not require its nodes to be clustered at all
/// — two independent processes each running different venues are a perfectly
/// good thing to merge, and neither has a `node_id` to borrow.
///
/// # Errors
/// If the list is empty, a URL is missing a scheme, or two nodes share a label.
/// All three are startup failures rather than something to discover at the
/// first merge: a duplicate label would make two nodes indistinguishable in
/// exactly the output whose job is telling them apart.
pub fn parse_nodes(raw: &str) -> Result<Vec<(String, String)>, String> {
    let mut out: Vec<(String, String)> = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        // `rsplit_once` so a label may not contain '=' but a URL may.
        let (label, url) = match entry.split_once('=') {
            Some((label, url)) if !label.contains("://") => (label.trim(), url.trim()),
            _ => (entry, entry),
        };
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(format!(
                "{url:?} is not an http(s) URL. Nodes are named as \
                 http://host:port, optionally as label=http://host:port."
            ));
        }
        let url = url.trim_end_matches('/').to_owned();
        if out.iter().any(|(existing, _)| existing == label) {
            return Err(format!(
                "two nodes are both labelled {label:?}; labels have to be distinct or the \
                 node table cannot tell them apart"
            ));
        }
        out.push((label.to_owned(), url));
    }
    if out.is_empty() {
        return Err("no nodes given".to_owned());
    }
    Ok(out)
}

/// Follow one node's `/events` stream until shutdown, publishing every snapshot
/// it sends onto `tx`.
pub async fn follow(
    client: reqwest::Client,
    node: String,
    url: String,
    tx: watch::Sender<NodeReport>,
    clock: Arc<dyn Clock>,
    mut shutdown: Shutdown,
) {
    let events = format!("{url}/events");
    let mut backoff = Backoff::new(BackoffPolicy::DEFAULT, EqualJitter::from_entropy());

    while !shutdown.is_set() {
        let started = clock.now();
        let outcome = tokio::select! {
            () = shutdown.wait() => return,
            outcome = session(&client, &events, &node, &tx, clock.as_ref()) => outcome,
        };

        if let Err(e) = outcome {
            warn!(%node, url = %events, error = %e, "node feed failed");
            tx.send_modify(|report| {
                report.failures += 1;
                report.last_error = Some(e);
            });
        }

        // The last snapshot is deliberately kept. Whether it is still usable is
        // a question about its age, and the merge is what answers it — see the
        // module docs on why dropping it here would flap the consolidated touch
        // on every node deploy.
        if backoff.note_session(clock.now().since(started)) {
            info!(%node, "node feed ended after a stable run");
        }
        let delay = backoff.next_delay();
        debug!(%node, ?delay, attempt = backoff.attempt(), "reconnecting to node");
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = shutdown.wait() => return,
        }
    }
}

/// One connection, from `GET` to whatever ends it.
///
/// `Ok(())` means the stream ended cleanly — the node shut down — which is not
/// an error but is still a reason to reconnect.
async fn session(
    client: &reqwest::Client,
    events: &str,
    node: &str,
    tx: &watch::Sender<NodeReport>,
    clock: &dyn Clock,
) -> Result<(), String> {
    let response = client
        .get(events)
        .header("accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("node answered {}", response.status()));
    }
    info!(%node, url = %events, "following node");
    tx.send_modify(|report| report.connects += 1);

    let mut response = response;
    let mut buffer = String::new();

    // `chunk` rather than a `Stream` combinator: it is on reqwest's base API,
    // and pulling in the `stream` feature for one `next()` would add a
    // dependency to get something already here.
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        // SSE separates events with a blank line. A snapshot is tens of
        // kilobytes and arrives across several TCP reads, so a parser that
        // assumed one chunk was one event would silently produce nothing on a
        // large book and everything on a small one — which is the sort of bug
        // that only shows up against a real Coinbase ladder.
        while let Some(end) = buffer.find("\n\n") {
            let block: String = buffer.drain(..end + 2).collect();
            let Some((event, data)) = parse_block(&block) else {
                continue;
            };
            if event.as_deref() != Some("snapshot") {
                // `lagged` and keep-alive comments both land here. A gateway
                // that fell behind a node is skipping to the present, which is
                // what it should do — same rule the SSE handler applies to a
                // slow browser.
                continue;
            }
            match serde_json::from_str::<Snapshot>(&data) {
                Ok(snapshot) => {
                    let at = clock.now();
                    tx.send_modify(|report| {
                        report.snapshot = Some(snapshot);
                        report.received_at = Some(at);
                    });
                }
                Err(e) => {
                    // A parse failure is a schema disagreement between this
                    // build and the node's, which is a real thing to know about
                    // during a rolling deploy. It does not tear down the
                    // connection: the next snapshot may well parse, and the
                    // node's existing report ages out on its own if none does.
                    warn!(%node, error = %e, "could not parse a node snapshot");
                    tx.send_modify(|report| {
                        report.failures += 1;
                        report.last_error = Some(format!("snapshot did not parse: {e}"));
                    });
                }
            }
        }

        // A buffer that only grows is a node sending something that never
        // terminates an event. Bounded rather than trusted: this is memory in a
        // long-running process, driven by a remote peer.
        if buffer.len() > MAX_EVENT_BYTES {
            return Err(format!(
                "node sent {} bytes with no event boundary; dropping the connection",
                buffer.len()
            ));
        }
    }
    Ok(())
}

/// Sixteen megabytes. Coinbase's opening `level2` frame is the largest thing
/// this project handles and compresses to under a megabyte; a snapshot event is
/// far smaller. This is a runaway guard, not a tuning knob.
const MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

/// Pull the `event:` name and joined `data:` payload out of one SSE block.
///
/// Returns `None` for a block with no data — a keep-alive comment, or the
/// `\n\n` that follows one. Split out as a pure function because SSE framing is
/// exactly the kind of thing that looks obvious and has three edge cases, and
/// none of them should need a socket to test.
fn parse_block(block: &str) -> Option<(Option<String>, String)> {
    let mut event = None;
    let mut data = String::new();
    for line in block.lines() {
        // A line starting with ':' is a comment. axum's keep-alive sends one.
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim_start().to_owned());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                // Multi-line data fields are joined with a newline, per the
                // spec. Nothing here emits one today; dropping the rule would
                // corrupt a payload the day something does.
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    (!data.is_empty()).then_some((event, data))
}

/// An HTTP client for talking to nodes.
///
/// # Errors
/// If the client cannot be built.
pub fn client(timeout: Duration) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        // A *connect* timeout only. A read timeout would be wrong here and
        // subtly so: an SSE stream is meant to stay open indefinitely, and a
        // healthy but quiet node would be torn down on a timer. Liveness is
        // established by snapshots arriving, which is what `lag_ms` measures
        // and what `max_node_age` acts on — the same argument the ingest
        // tasks' idle watchdog makes about a silent socket.
        .connect_timeout(timeout)
        .build()
        .map_err(|e| e.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_url_is_its_own_label() {
        assert_eq!(
            parse_nodes("http://127.0.0.1:8081,http://127.0.0.1:8082").unwrap(),
            vec![
                (
                    "http://127.0.0.1:8081".to_owned(),
                    "http://127.0.0.1:8081".to_owned()
                ),
                (
                    "http://127.0.0.1:8082".to_owned(),
                    "http://127.0.0.1:8082".to_owned()
                ),
            ]
        );
    }

    #[test]
    fn a_node_can_be_named() {
        assert_eq!(
            parse_nodes("node-a=http://127.0.0.1:8081/").unwrap(),
            vec![("node-a".to_owned(), "http://127.0.0.1:8081".to_owned())]
        );
    }

    #[test]
    fn nonsense_node_lists_are_refused_at_startup() {
        // Including the duplicate-label case, which is the one that would
        // otherwise fail quietly: two nodes sharing a label are
        // indistinguishable in exactly the table whose job is telling them
        // apart, and the merge would look entirely healthy.
        for raw in ["", "127.0.0.1:8081", "ws://host", "a=http://x,a=http://y"] {
            assert!(parse_nodes(raw).is_err(), "{raw:?} was accepted");
        }
    }

    #[test]
    fn an_sse_block_yields_its_event_and_data() {
        assert_eq!(
            parse_block("event: snapshot\ndata: {\"seq\":1}\n\n"),
            Some((Some("snapshot".to_owned()), "{\"seq\":1}".to_owned()))
        );
        // Only one space is stripped: JSON does not care, but a payload that
        // was deliberately indented would.
        assert_eq!(
            parse_block("data:  x\n\n").unwrap().1,
            " x",
            "more than one leading space was stripped from the payload"
        );
    }

    #[test]
    fn a_keep_alive_comment_is_not_an_event() {
        // axum sends these on an idle stream so a proxy does not time the
        // connection out. Reading one as an empty snapshot would make every
        // quiet moment look like a node reporting no books at all.
        assert_eq!(parse_block(":\n\n"), None);
        assert_eq!(parse_block(": keep-alive\n\n"), None);
        assert_eq!(parse_block("\n"), None);
    }

    #[test]
    fn multi_line_data_is_joined_with_newlines() {
        assert_eq!(
            parse_block("event: snapshot\ndata: {\ndata: }\n\n")
                .unwrap()
                .1,
            "{\n}"
        );
    }
}
