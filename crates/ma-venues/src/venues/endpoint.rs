//! Where each venue lives, and what to say to it once connected.
//!
//! This module is data, not I/O. It hands `ma-pipeline` a URL and the exact
//! text frames to send after the socket opens; opening the socket is somebody
//! else's job. That keeps `ma-venues` free of a transport dependency — the
//! property `ma-core`'s manifest test enforces one crate down, and that this
//! crate maintains by convention.
//!
//! # The coupling this module has to get right
//!
//! Each venue's subscribe payload names a channel, and each [`VenueSync`]
//! matches on a channel name in the frames that come back. If those two
//! strings drift apart the failure is silent in the worst way: the socket
//! connects, the subscribe succeeds, frames arrive, every one of them is
//! ignored as "not our channel", and the book sits in `Uninitialized` forever
//! while every connection-level metric reports health. Coinbase makes this
//! especially easy to get wrong, because you subscribe to `level2` and the
//! data comes back labelled `l2_data`.
//!
//! [`tests::subscribing_and_parsing_agree_on_the_channel`] closes that gap by
//! feeding each venue's own sync a frame built from that venue's own subscribe
//! payload, and asserting the sync recognises it.
//!
//! # Symbol spelling
//!
//! `ma_core::Symbol` is normalised as `BASE-QUOTE` (`BTC-USD`). The three
//! venues disagree — `BTC-USD`, `BTC/USD`, `btcusd` — and [`native_symbol`] is
//! the single place that disagreement is resolved.

use std::time::Duration;

use ma_core::{Symbol, VenueId};

use crate::sync::{VenueError, VenueSync};
use crate::venues::{BitstampSync, CoinbaseSync, KrakenSync};

/// Kraken's book channel depth. The checksum covers the top 10 levels, so
/// subscribing at exactly 10 makes the book we hold and the book Kraken hashes
/// the same object.
const KRAKEN_DEPTH: usize = 10;

/// Levels per side to request for a periodic audit.
///
/// Sized from measurement, not intuition. The audit's guard band is expressed
/// in basis points ([`AuditPolicy::guard_bps`](ma_core::AuditPolicy::guard_bps)),
/// so the request has to reach *past* it or every audit is inconclusive — and on
/// a dense book that takes far more levels than it looks like it should.
/// Coinbase BTC-USD:
///
/// | `limit` | price span |
/// |---|---|
/// | 50 | 2.4 bps |
/// | 1000 | 139 bps |
///
/// At the 10 bps default guard, `limit=50` reaches nowhere near the compared
/// region; `limit=1000` leaves ~825 levels beyond it. Bitstamp ignores this and
/// returns its whole book regardless.
const AUDIT_DEPTH: usize = 1000;

// A basis-point guard on a dense book is hundreds of levels in. Requesting
// fewer than this makes every audit inconclusive, which reads as "nothing to
// report" — the audit failing silently in the direction that looks fine.
const _: () = assert!(AUDIT_DEPTH >= 500);

/// Everything needed to open and drive one venue connection, as plain data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VenueEndpoint {
    pub venue: VenueId,
    pub ws_url: String,
    /// Text frames to send, in order, immediately after the socket opens.
    /// Sent on every connection including reconnects — there is no session to
    /// resume at any of these venues.
    pub subscribe: Vec<String>,
    /// Where to GET a depth snapshot, for a
    /// [`RecoveryStrategy::RestSnapshot`](crate::RecoveryStrategy::RestSnapshot)
    /// venue. `None` for the two venues that send their snapshot over the
    /// websocket, which is why neither needs an HTTP client at all.
    pub rest_snapshot_url: Option<String>,
    /// Silence longer than this means the connection is dead even though the
    /// socket is still open. See [`VenueEndpoint::idle_timeout`]'s per-venue
    /// values in [`spec_for`] for why they differ.
    pub idle_timeout: Duration,
    /// Where to GET a depth snapshot for the **periodic audit**, and how often.
    ///
    /// `None` means this venue is not audited. Exactly one venue is not, and
    /// the reason is worth stating: Kraken hashes the book we actually built
    /// and sends the hash with *every message*. A REST comparison four times an
    /// hour would be strictly weaker evidence than what it already provides
    /// continuously, at the cost of extra requests to a venue that rate-limits.
    /// Auditing it would be ceremony.
    ///
    /// The other two publish nothing that checks our book at all, which is what
    /// makes this the only independent evidence available for either. See
    /// [`ma_core::audit`].
    pub rest_audit: Option<RestAudit>,
}

/// The periodic depth check for one venue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestAudit {
    pub url: String,
    /// How often to fetch.
    ///
    /// Deliberately slow. Every venue here rate-limits, the audit competes with
    /// nothing else for that budget but should not have to, and the failure it
    /// detects — a lost delta deep in the book — persists indefinitely once it
    /// happens. There is no benefit to finding it in five seconds rather than
    /// sixty, and a real cost to asking sixty times as often.
    pub interval: Duration,
    /// Comparison strictness. See [`AuditPolicy`](ma_core::AuditPolicy).
    pub policy: ma_core::AuditPolicy,
}

/// An endpoint paired with the sync state machine that understands what comes
/// back from it.
///
/// The two are constructed together, by one function, because they are two
/// halves of one decision — see the module docs on the channel-name coupling.
#[derive(Debug)]
pub struct VenueSpec {
    pub endpoint: VenueEndpoint,
    pub sync: Box<dyn VenueSync>,
    /// The normalised symbol this spec was built for.
    ///
    /// Carried rather than re-derived because the venue-native spelling inside
    /// `sync` is a one-way translation: `btcusd` cannot be turned back into
    /// `BTC-USD` without knowing where the base ends. Keeping the normalised
    /// form here means a caller that crossed a venue list with a symbol list
    /// does not have to remember which spec came from which pairing.
    pub symbol: Symbol,
    /// Levels to retain per side, or `None` to retain everything the venue
    /// sends.
    ///
    /// Only Kraken sets this, and the reason is worth stating because the
    /// asymmetry looks arbitrary: pruning is safe *only* when the venue is
    /// already sending a depth-limited feed. Kraken publishes the top 10 and
    /// expects the client to hold exactly the top 10, so truncating there
    /// reproduces what the venue itself is doing. Coinbase and Bitstamp send
    /// full-depth books, and pruning those would hit the hazard
    /// [`Book::with_max_depth`](ma_core::Book::with_max_depth) warns about —
    /// a delete inside the retained window exposes a level we threw away and
    /// can never recover from deltas. So they retain everything.
    pub max_depth: Option<usize>,
}

/// Translate a normalised `BASE-QUOTE` symbol into a venue's own spelling.
pub fn native_symbol(venue: VenueId, symbol: &Symbol) -> Result<String, VenueError> {
    let raw = symbol.as_str();
    let Some((base, quote)) = raw.split_once('-') else {
        return Err(VenueError::Malformed(format!(
            "symbol {raw:?} is not in normalised BASE-QUOTE form"
        )));
    };
    if base.is_empty() || quote.is_empty() {
        return Err(VenueError::Malformed(format!(
            "symbol {raw:?} has an empty base or quote"
        )));
    }

    Ok(match venue {
        VenueId::Coinbase => raw.to_owned(),
        VenueId::Kraken => format!("{base}/{quote}"),
        VenueId::Bitstamp => format!("{base}{quote}").to_lowercase(),
        VenueId::Fake => raw.to_owned(),
    })
}

/// Build the endpoint and the matching sync for one venue and one symbol.
pub fn spec_for(venue: VenueId, symbol: &Symbol) -> Result<VenueSpec, VenueError> {
    let native = native_symbol(venue, symbol)?;

    Ok(match venue {
        VenueId::Coinbase => VenueSpec {
            endpoint: VenueEndpoint {
                venue,
                ws_url: "wss://advanced-trade-ws.coinbase.com".to_owned(),
                subscribe: vec![
                    subscribe_coinbase(&native, "level2"),
                    // Not optional. Coinbase closes a sparse subscription
                    // after 60–90 seconds of silence, so a quiet book would
                    // look exactly like a dead connection and then become one.
                    // Subscribing to heartbeats keeps traffic flowing and
                    // gives the idle watchdog something to count.
                    subscribe_coinbase(&native, "heartbeats"),
                ],
                rest_snapshot_url: None,
                // Heartbeats arrive every second, so 15s of total silence is
                // 15 missed heartbeats — dead, not quiet.
                idle_timeout: Duration::from_secs(15),
                // `GapDetectable` catches a *lost message* and nothing else.
                // A delta applied to the wrong side, or a level our own code
                // dropped, leaves the sequence perfectly contiguous. This is
                // the only thing that would ever notice.
                rest_audit: Some(RestAudit {
                    url: format!(
                        "https://api.coinbase.com/api/v3/brokerage/market/product_book\
                         ?product_id={native}&limit={AUDIT_DEPTH}"
                    ),
                    interval: Duration::from_secs(60),
                    policy: ma_core::AuditPolicy::DEFAULT,
                }),
            },
            sync: Box::new(CoinbaseSync::new(native)),
            symbol: symbol.clone(),
            max_depth: None,
        },

        VenueId::Kraken => VenueSpec {
            endpoint: VenueEndpoint {
                venue,
                ws_url: "wss://ws.kraken.com/v2".to_owned(),
                subscribe: vec![format!(
                    r#"{{"method":"subscribe","params":{{"channel":"book","symbol":["{native}"],"depth":{KRAKEN_DEPTH}}}}}"#
                )],
                rest_snapshot_url: None,
                // Kraken sends its own heartbeat channel roughly every second
                // to any connection holding a subscription.
                idle_timeout: Duration::from_secs(15),
                // Not audited, and this is the interesting `None`. See
                // `VenueEndpoint::rest_audit`: a book checksummed against the
                // venue's own hash on every single message has stronger
                // evidence than a periodic REST comparison could ever add.
                rest_audit: None,
            },
            sync: Box::new(KrakenSync::new(native)),
            symbol: symbol.clone(),
            max_depth: Some(KRAKEN_DEPTH),
        },

        VenueId::Bitstamp => VenueSpec {
            endpoint: VenueEndpoint {
                venue,
                ws_url: "wss://ws.bitstamp.net".to_owned(),
                subscribe: vec![format!(
                    r#"{{"event":"bts:subscribe","data":{{"channel":"diff_order_book_{native}"}}}}"#
                )],
                rest_snapshot_url: Some(format!(
                    "https://www.bitstamp.net/api/v2/order_book/{native}/"
                )),
                // The weakest of the three, and not by choice: Bitstamp sends
                // no heartbeat, so the only evidence the connection is alive
                // is book traffic. On a liquid pair diffs arrive several times
                // a second and 30s of silence is unambiguous; on an illiquid
                // one this would produce false reconnects, which is a reason
                // to be careful about which pairs get added at v2's
                // multi-symbol step.
                idle_timeout: Duration::from_secs(30),
                // The venue that needs this most. `OrderOnly` means a dropped
                // diff leaves no trace whatsoever in the protocol, so without
                // an audit a Bitstamp book can be wrong indefinitely while
                // every counter reports health.
                rest_audit: Some(RestAudit {
                    url: format!("https://www.bitstamp.net/api/v2/order_book/{native}/"),
                    interval: Duration::from_secs(60),
                    policy: ma_core::AuditPolicy::DEFAULT,
                }),
            },
            sync: Box::new(BitstampSync::new(format!("diff_order_book_{native}"))),
            symbol: symbol.clone(),
            max_depth: None,
        },

        // The fake venue is driven by a script or a tape, not a socket. Asking
        // for its endpoint is a caller bug, not a runtime condition.
        VenueId::Fake => return Err(VenueError::NoEndpoint { venue }),
    })
}

fn subscribe_coinbase(product_id: &str, channel: &str) -> String {
    format!(r#"{{"type":"subscribe","product_ids":["{product_id}"],"channel":"{channel}"}}"#)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::sync::{RawFrame, SyncAction};
    use ma_core::{Clock, StreamId, SystemClock};

    fn sym() -> Symbol {
        Symbol::new("BTC-USD")
    }

    fn spec(venue: VenueId) -> VenueSpec {
        spec_for(venue, &sym()).expect("spec")
    }

    const REAL_VENUES: [VenueId; 3] = [VenueId::Coinbase, VenueId::Kraken, VenueId::Bitstamp];

    #[test]
    fn each_venue_gets_its_own_spelling() {
        assert_eq!(native_symbol(VenueId::Coinbase, &sym()).unwrap(), "BTC-USD");
        assert_eq!(native_symbol(VenueId::Kraken, &sym()).unwrap(), "BTC/USD");
        assert_eq!(native_symbol(VenueId::Bitstamp, &sym()).unwrap(), "btcusd");
    }

    #[test]
    fn a_symbol_that_is_not_normalised_is_refused() {
        // Passing a venue-native spelling back in by mistake is the likely
        // error, and it must not silently produce "btc/usdbtc/usd".
        for bad in ["BTCUSD", "BTC/USD", "-USD", "BTC-"] {
            assert!(
                native_symbol(VenueId::Kraken, &Symbol::new(bad)).is_err(),
                "{bad:?} was accepted as normalised"
            );
        }
    }

    #[test]
    fn subscribe_payloads_are_valid_json_naming_our_symbol() {
        for venue in REAL_VENUES {
            let spec = spec(venue);
            let native = native_symbol(venue, &sym()).unwrap();
            assert!(!spec.endpoint.subscribe.is_empty(), "{venue} sends nothing");

            for payload in &spec.endpoint.subscribe {
                let parsed: serde_json::Value = serde_json::from_str(payload)
                    .unwrap_or_else(|e| panic!("{venue} subscribe is not JSON: {e}\n{payload}"));
                assert!(parsed.is_object());
                assert!(
                    payload.contains(&native),
                    "{venue} subscribe does not name {native:?}: {payload}"
                );
            }
        }
    }

    #[test]
    fn every_endpoint_is_a_secure_websocket() {
        for venue in REAL_VENUES {
            let ep = spec(venue).endpoint;
            assert!(
                ep.ws_url.starts_with("wss://"),
                "{venue} is not using TLS: {}",
                ep.ws_url
            );
            if let Some(url) = &ep.rest_snapshot_url {
                assert!(url.starts_with("https://"), "{venue} REST is not TLS");
            }
        }
    }

    #[test]
    fn only_the_rest_splice_venue_carries_a_rest_url() {
        // The coupling stated on `VenueEndpoint::rest_snapshot_url`: a venue
        // needs an HTTP client exactly when its recovery is a REST splice.
        for venue in REAL_VENUES {
            let spec = spec(venue);
            let needs_rest = spec.sync.recovery() == crate::RecoveryStrategy::RestSnapshot;
            assert_eq!(
                spec.endpoint.rest_snapshot_url.is_some(),
                needs_rest,
                "{venue}'s REST url and its recovery strategy disagree"
            );
        }
    }

    #[test]
    fn the_two_venues_that_prove_nothing_about_our_book_are_the_audited_ones() {
        // The audit is the only independent evidence Coinbase and Bitstamp can
        // ever produce. Kraken already hashes the book we built, on every
        // message — auditing it would be strictly weaker evidence bought with
        // extra requests to a venue that rate-limits.
        assert!(spec(VenueId::Coinbase).endpoint.rest_audit.is_some());
        assert!(spec(VenueId::Bitstamp).endpoint.rest_audit.is_some());
        assert!(
            spec(VenueId::Kraken).endpoint.rest_audit.is_none(),
            "a continuously checksum-verified book does not need a periodic \
             REST comparison"
        );

        for venue in [VenueId::Coinbase, VenueId::Bitstamp] {
            let spec = spec(venue);
            let audit = spec.endpoint.rest_audit.expect("audited");
            assert!(
                audit.url.starts_with("https://"),
                "{venue} audit is not TLS"
            );
            assert!(
                audit.url.contains(&native_symbol(venue, &sym()).unwrap()),
                "{venue} audit url does not name the symbol: {}",
                audit.url
            );
            // The guard is a price distance, so the request must reach past
            // it. A level count cannot express that directly; what it can do
            // is refuse to be small enough that the question never arises —
            // `limit=50` on Coinbase spans 2.4 bps and would make every audit
            // at a 10 bps guard inconclusive.
            assert!(
                audit.policy.guard_bps > 0,
                "{venue} audits right up to the touch, where the fetch races \
                 the stream"
            );
            // Coinbase caps its response at `limit`; Bitstamp ignores the
            // question and returns its whole book. Only the former can be got
            // wrong, so only the former is checked.
            if venue == VenueId::Coinbase {
                assert!(
                    audit.url.contains(&format!("limit={AUDIT_DEPTH}")),
                    "coinbase's audit url does not request a depth: {}",
                    audit.url
                );
            }
            assert!(audit.policy.max_levels > 0);
            assert!(
                audit.interval >= Duration::from_secs(30),
                "{venue} audits often enough to matter to a rate limiter"
            );
        }
    }

    #[test]
    fn an_audited_venue_can_parse_the_body_it_will_be_sent() {
        // The same class of coupling as `subscribing_and_parsing_agree_on_the
        // _channel`: an audit URL whose response this venue cannot parse
        // produces a parse error every interval, forever, and never audits
        // anything. Both bodies below are trimmed captures of the real
        // responses.
        let cases: [(VenueId, &str); 2] = [
            (
                VenueId::Coinbase,
                r#"{"pricebook":{"product_id":"BTC-USD","bids":[{"price":"64740.08","size":"0.41225831"}],"asks":[{"price":"64740.09","size":"0.18759049"}],"time":"2026-08-09T03:47:11.486806Z"}}"#,
            ),
            (
                VenueId::Bitstamp,
                r#"{"timestamp":"1786278896","microtimestamp":"1786278896123456","bids":[["64740.08","0.41225831"]],"asks":[["64740.09","0.18759049"]]}"#,
            ),
        ];

        for (venue, body) in cases {
            let spec = spec(venue);
            let parsed = spec
                .sync
                .parse_rest_snapshot(body)
                .unwrap_or_else(|e| panic!("{venue} cannot parse its own audit body: {e}"));
            assert_eq!(parsed.bids.len(), 1, "{venue} lost the bid side");
            assert_eq!(parsed.asks.len(), 1, "{venue} lost the ask side");
            // Exact digits, not f64 round trips — the whole reason `Price`
            // wraps `Decimal`.
            assert_eq!(parsed.bids[0].price.to_string(), "64740.08");
            assert_eq!(parsed.bids[0].qty.to_string(), "0.41225831");
        }
    }

    #[test]
    fn coinbase_can_read_a_rest_book_but_still_cannot_recover_with_one() {
        // Parsing the wire shape and being able to splice it are different
        // capabilities, and Coinbase has only the first. Its REST response
        // carries no ordering marker, and `sequence_num` is connection-scoped,
        // so there is no position at which a splice could join. The audit
        // compares and never applies.
        let spec = spec(VenueId::Coinbase);
        assert_eq!(spec.sync.recovery(), crate::RecoveryStrategy::Resubscribe);
        assert!(
            spec.endpoint.rest_snapshot_url.is_none(),
            "a resubscribe venue must not carry a recovery REST url"
        );
    }

    #[test]
    fn subscribing_and_parsing_agree_on_the_channel() {
        // The silent failure this module's docs describe: subscribe to one
        // channel name, match on another, and the connection looks perfectly
        // healthy while the book never initialises. Each venue's sync is fed a
        // frame carrying the channel name that venue's own subscription
        // implies, and must not treat it as unrecognised.
        //
        // Coinbase is the one that actually bites: you subscribe to "level2"
        // and the data comes back labelled "l2_data".
        let now = SystemClock.now();
        let cases: [(VenueId, &str); 3] = [
            (
                VenueId::Coinbase,
                r#"{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]}]}"#,
            ),
            (
                VenueId::Kraken,
                r#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":100,"qty":1}],"asks":[{"price":101,"qty":1}],"checksum":1}]}"#,
            ),
            (
                VenueId::Bitstamp,
                r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"microtimestamp":"1","bids":[["100","1"]],"asks":[["101","1"]]}}"#,
            ),
        ];

        for (venue, payload) in cases {
            let mut spec = spec(venue);
            let frame = RawFrame::new(
                StreamId::new(venue, sym()),
                payload.as_bytes().to_vec(),
                now,
            );
            let ingested = spec
                .sync
                .ingest(&frame)
                .unwrap_or_else(|e| panic!("{venue} could not parse its own channel: {e}"));

            assert!(
                !ingested.actions.iter().all(|a| *a == SyncAction::Ignore),
                "{venue} ignored a frame on the channel it subscribes to — \
                 the subscribe payload and the parser have drifted apart"
            );
        }
    }

    #[test]
    fn only_kraken_prunes_and_it_prunes_to_its_checksum_window() {
        // Pruning is safe only because Kraken itself publishes a depth-limited
        // feed. The other two send full books, where pruning would discard
        // levels a later delete would expose.
        assert_eq!(spec(VenueId::Kraken).max_depth, Some(10));
        assert_eq!(spec(VenueId::Coinbase).max_depth, None);
        assert_eq!(spec(VenueId::Bitstamp).max_depth, None);

        assert!(
            spec(VenueId::Kraken)
                .endpoint
                .subscribe
                .iter()
                .any(|s| s.contains(r#""depth":10"#)),
            "the subscribed depth and the retained depth must be the same number"
        );
    }

    #[test]
    fn the_fake_venue_has_no_endpoint() {
        let err = spec_for(VenueId::Fake, &sym()).unwrap_err();
        assert!(matches!(
            err,
            VenueError::NoEndpoint {
                venue: VenueId::Fake
            }
        ));
    }
}
