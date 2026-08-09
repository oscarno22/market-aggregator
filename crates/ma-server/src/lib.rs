//! Wiring, and the HTTP surface.
//!
//! Three binaries share this crate — `ma-server` (serve), `record`, and
//! `replay` — and the reason they can is the point of the whole layout: a
//! replay run and a live run construct the *same* aggregator behind the *same*
//! channel. The only difference is what fills the channel. Nothing downstream
//! of [`Pipeline::channel`] can tell which it is.

pub mod http;
pub mod pipeline;

pub use pipeline::{Pipeline, PipelineHandle};

/// The venues a default run connects to.
pub const DEFAULT_VENUES: [ma_core::VenueId; 3] = [
    ma_core::VenueId::Coinbase,
    ma_core::VenueId::Kraken,
    ma_core::VenueId::Bitstamp,
];

/// Install `tracing` with `RUST_LOG` support, defaulting to something useful.
pub fn init_tracing(default: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    fmt().with_env_filter(filter).with_target(false).init();
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
