//! Wiring, and the HTTP surface.
//!
//! Three binaries share this crate — `ma-server` (serve), `record`, and
//! `replay` — and the reason they can is the point of the whole layout: a
//! replay run and a live run construct the *same* aggregator behind the *same*
//! channel. The only difference is what fills the channel. Nothing downstream
//! of [`Pipeline::channel`] can tell which it is.

pub mod archive;
pub mod http;
pub mod pipeline;

pub use archive::{ArchiveStats, replay_archive};
pub use pipeline::{Pipeline, PipelineHandle};

/// The venues a default run connects to.
pub const DEFAULT_VENUES: [ma_core::VenueId; 3] = [
    ma_core::VenueId::Coinbase,
    ma_core::VenueId::Kraken,
    ma_core::VenueId::Bitstamp,
];

/// Resolve when the process is asked to stop, by either signal that means it.
///
/// # Why `SIGTERM` and not just Ctrl-C
///
/// v1 waited on `ctrl_c()` alone, which is correct for a terminal and wrong
/// everywhere else: an orchestrator sends `SIGTERM`, and an unhandled
/// `SIGTERM` kills the process outright. That was harmless while the process
/// held no state worth flushing. v2's Parquet writer changes that — a file is
/// only readable once its footer is written, so dying without running the
/// shutdown path discards everything since the last part roll.
///
/// Found by killing a live run with `pkill` and discovering the archive was
/// empty. `WriterConfig::max_open` bounds how much that can ever cost;
/// handling the signal is what makes the common case cost nothing.
///
/// On non-Unix targets this waits on Ctrl-C alone, which is all that exists.
pub async fn stop_requested() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(term) => term,
            Err(e) => {
                // Registering the handler failed, which is not a reason to
                // refuse to run — fall back to Ctrl-C and say so.
                tracing::warn!(error = %e, "could not listen for SIGTERM; Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("interrupted"),
            _ = term.recv() => tracing::info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Install `tracing` with `RUST_LOG` support, defaulting to something useful.
pub fn init_tracing(default: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    fmt().with_env_filter(filter).with_target(false).init();
}

/// Parse a comma-separated symbol list, e.g. `BTC-USD,ETH-USD`.
///
/// # Errors
/// If a symbol is not in normalised `BASE-QUOTE` form, or the list is empty.
/// Rejected here rather than at the first connection, so a typo costs a
/// startup failure instead of a book that silently never initialises.
pub fn parse_symbols(raw: &str) -> Result<Vec<ma_core::Symbol>, String> {
    let symbols: Vec<ma_core::Symbol> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ma_core::Symbol::new)
        .collect();

    if symbols.is_empty() {
        return Err("no symbols given".to_owned());
    }
    for symbol in &symbols {
        // Any venue will do for the check — `native_symbol` rejects a
        // non-normalised spelling before it ever looks at the venue.
        ma_venues::native_symbol(ma_core::VenueId::Coinbase, symbol)
            .map_err(|e| format!("{symbol}: {e}"))?;
    }

    let mut sorted = symbols.clone();
    sorted.sort();
    sorted.dedup();
    if sorted.len() != symbols.len() {
        return Err(format!("duplicate symbol in {raw:?}"));
    }
    Ok(symbols)
}

/// Parse a comma-separated venue list, e.g. `coinbase,kraken`.
///
/// # Errors
/// If a name is not one of the venues this build knows how to speak to.
pub fn parse_venues(raw: &str) -> Result<Vec<ma_core::VenueId>, String> {
    use ma_core::VenueId;
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|name| match name.to_ascii_lowercase().as_str() {
            "coinbase" => Ok(VenueId::Coinbase),
            "kraken" => Ok(VenueId::Kraken),
            "bitstamp" => Ok(VenueId::Bitstamp),
            other => Err(format!(
                "unknown venue {other:?} (known: coinbase, kraken, bitstamp)"
            )),
        })
        .collect()
}
