//! **Tier 2 diagnostic.** Connect to one venue, build a book from the
//! websocket, then fetch that venue's REST depth and print exactly how the two
//! disagree.
//!
//! This exists because the v2 depth audit went live and immediately declared
//! every Coinbase and Bitstamp book untrustworthy. Two REST fetches 150ms apart
//! agree with each other almost perfectly, so the timing race the audit was
//! designed around could not explain it — which left either a real bug in our
//! book or a structural difference between the two endpoints. Neither could be
//! distinguished from a counter.
//!
//! ```bash
//! cargo run -p ma-server --example audit_probe -- --venue coinbase --secs 20
//! ```

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ma_core::{Side, StreamId, Symbol, SystemClock, VenueId};
use ma_pipeline::channel::bounded;
use ma_pipeline::ingest::{Ingest, IngestMessage, shutdown};
use ma_pipeline::metrics::VenueCounters;
use ma_pipeline::net::{LiveNetwork, Network};
use ma_venues::{VenueBook, spec_for};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "coinbase")]
    venue: String,
    #[arg(long, default_value = "BTC-USD")]
    symbol: String,
    /// How long to let the websocket book settle before comparing.
    #[arg(long, default_value_t = 20)]
    secs: u64,
    /// Levels per side to print.
    #[arg(long, default_value_t = 60)]
    depth: usize,
}

#[tokio::main]
// A diagnostic that reports percentages. The float discipline the rest of the
// workspace keeps is about *prices*, which must never round; a disagreement
// rate printed to one decimal place is not one.
#[allow(clippy::too_many_lines, clippy::float_arithmetic)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    ma_server::init_tracing("warn");

    let venue = match args.venue.as_str() {
        "coinbase" => VenueId::Coinbase,
        "kraken" => VenueId::Kraken,
        "bitstamp" => VenueId::Bitstamp,
        other => return Err(format!("unknown venue {other}").into()),
    };
    let symbol = Symbol::new(&args.symbol);
    let stream = StreamId::new(venue, symbol.clone());
    let spec = spec_for(venue, &symbol)?;
    let audit = spec
        .endpoint
        .rest_audit
        .clone()
        .ok_or("this venue is not audited")?;

    // Drive one real ingest task into one real book.
    let net = Arc::new(LiveNetwork::new()?);
    let (tx, rx) = bounded::<IngestMessage>(4096);
    let (trigger, shut) = shutdown();
    let counters = Arc::new(VenueCounters::default());
    let ingest = Ingest::new(
        Arc::clone(&net),
        stream.clone(),
        spec.endpoint.clone(),
        tx,
        Arc::new(SystemClock),
        counters,
        shut,
    );
    let task = tokio::spawn(ingest.run());

    let mut book = VenueBook::new(spec.sync, symbol.clone());
    if let Some(depth) = spec.max_depth {
        book = book.with_max_depth(depth);
    }

    println!("building a book from the websocket for {}s…", args.secs);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(IngestMessage::Frame(frame))) => {
                let _ = book.feed(&frame);
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    // Fetch REST as close as possible to the last applied frame.
    let body = net.get(&audit.url).await?;
    let snapshot =
        ma_venues::VenueSync::parse_rest_snapshot(&*spec_for(venue, &symbol)?.sync, &body)?;
    trigger.stop();
    let _ = tokio::time::timeout(Duration::from_secs(2), task).await;

    println!("\nbook state: {:?}", book.book().state());
    println!(
        "ws book holds {:?} levels; rest returned {} bids / {} asks",
        book.book().depth(),
        snapshot.bids.len(),
        snapshot.asks.len()
    );

    for (side, theirs) in [(Side::Bid, &snapshot.bids), (Side::Ask, &snapshot.asks)] {
        println!("\n===== {side:?} =====");
        let ours = book.book().top_levels(side, args.depth);
        let mut sorted = theirs.clone();
        match side {
            Side::Bid => sorted.sort_by(|a, b| b.price.cmp(&a.price)),
            Side::Ask => sorted.sort_by(|a, b| a.price.cmp(&b.price)),
        }
        sorted.truncate(args.depth);

        println!(
            "{:>4}  {:>14} {:>16}   {:>14} {:>16}",
            "#", "ws price", "ws qty", "rest price", "rest qty"
        );
        for i in 0..args.depth.min(ours.len().max(sorted.len())) {
            let o = ours.get(i);
            let t = sorted.get(i);
            let same = match (o, t) {
                (Some(o), Some(t)) => o.price == t.price && o.qty == t.qty,
                _ => false,
            };
            println!(
                "{:>4}  {:>14} {:>16}   {:>14} {:>16}  {}",
                i,
                o.map_or("—".to_owned(), |l| l.price.to_string()),
                o.map_or("—".to_owned(), |l| l.qty.to_string()),
                t.map_or("—".to_owned(), |l| l.price.to_string()),
                t.map_or("—".to_owned(), |l| l.qty.to_string()),
                if same { "" } else { "  <— differs" }
            );
        }

        // Disagreement rate by distance from the touch. This is the profile
        // that shows whether the guard band is in the right place: churn
        // should be concentrated near the touch and vanish beyond it.
        {
            use rust_decimal::prelude::ToPrimitive;
            let ours_all = book.book().top_levels(side, usize::MAX);
            let best = ours_all.first().map(|l| l.price.as_decimal());
            let mut theirs_all: Vec<ma_core::Level> = theirs.clone();
            theirs_all.retain(|l| !l.qty.is_delete());
            if let Some(best) = best {
                let bps = |p: ma_core::Price| {
                    ((p.as_decimal() - best).abs() / best * rust_decimal::Decimal::from(10_000))
                        .to_f64()
                        .unwrap_or(0.0)
                };
                let ours_m: std::collections::BTreeMap<_, _> =
                    ours_all.iter().map(|l| (l.price, l.qty)).collect();
                let theirs_m: std::collections::BTreeMap<_, _> =
                    theirs_all.iter().map(|l| (l.price, l.qty)).collect();
                // Only compare where both books actually have coverage.
                let (Some(tl), Some(th)) = (theirs_m.keys().next(), theirs_m.keys().next_back())
                else {
                    continue;
                };
                println!("  disagreement by distance from touch (within rest coverage):");
                for (lo, hi) in [
                    (0.0, 1.0),
                    (1.0, 5.0),
                    (5.0, 10.0),
                    (10.0, 25.0),
                    (25.0, 1e9),
                ] {
                    let prices: std::collections::BTreeSet<_> = ours_m
                        .range(*tl..=*th)
                        .map(|(p, _)| *p)
                        .chain(theirs_m.range(*tl..=*th).map(|(p, _)| *p))
                        .filter(|p| (lo..hi).contains(&bps(*p)))
                        .collect();
                    if prices.is_empty() {
                        continue;
                    }
                    let bad = prices
                        .iter()
                        .filter(|p| ours_m.get(p) != theirs_m.get(p))
                        .count();
                    println!(
                        "    {lo:6.1}-{hi:<8.1} bps: {:5} levels, {bad:4} disagree ({:5.1}%)",
                        prices.len(),
                        bad as f64 / prices.len() as f64 * 100.0
                    );
                }
            }
        }

        // The comparison the audit actually performs: by price, over the
        // overlapping range.
        let ours_map: std::collections::BTreeMap<_, _> =
            ours.iter().map(|l| (l.price, l.qty)).collect();
        let theirs_map: std::collections::BTreeMap<_, _> =
            sorted.iter().map(|l| (l.price, l.qty)).collect();
        if let (Some(ol), Some(oh), Some(tl), Some(th)) = (
            ours_map.keys().next(),
            ours_map.keys().next_back(),
            theirs_map.keys().next(),
            theirs_map.keys().next_back(),
        ) {
            let lo = *ol.max(tl);
            let hi = *oh.min(th);
            let prices: std::collections::BTreeSet<_> = ours_map
                .range(lo..=hi)
                .map(|(p, _)| *p)
                .chain(theirs_map.range(lo..=hi).map(|(p, _)| *p))
                .collect();
            let disagree = prices
                .iter()
                .filter(|p| ours_map.get(p) != theirs_map.get(p))
                .count();
            println!(
                "overlap {lo}..={hi}: {} prices compared, {disagree} disagree",
                prices.len()
            );
            for p in prices
                .iter()
                .filter(|p| ours_map.get(p) != theirs_map.get(p))
                .take(8)
            {
                println!(
                    "   {p}: ws={:?} rest={:?}",
                    ours_map.get(p).map(ToString::to_string),
                    theirs_map.get(p).map(ToString::to_string)
                );
            }
        }
    }

    Ok(())
}
