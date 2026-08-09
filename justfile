# Commands are grouped by which testing tier they belong to (see docs/DESIGN.md).
# Tier 1 runs offline. Tier 2 touches live venues. Tier 3 touches AWS.

default:
    @just --list

# ---------------------------------------------------------------- Tier 1: local

# Full offline suite. Must pass with the machine in airplane mode.
test:
    cargo test --workspace

# Proves the book logic carries no async or I/O dependency.
test-core:
    cargo test -p ma-core

check:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --all --check

fmt:
    cargo fmt --all

# ------------------------------------------------------- Tier 2: live venues
#
# Not part of `just test`. These open real sockets. Reconnect backoff is proven
# offline against the fake venue before anything here is run, because venues
# ban on reconnect storms.

# Record raw frames from a live venue into tapes/ for offline replay.
record venue symbol="BTC-USD" secs="120":
    cargo run -p ma-server --bin record -- --venue {{venue}} --symbol {{symbol}} --secs {{secs}}

# Replay a recorded tape through the full pipeline. No network.
replay tape:
    cargo run -p ma-server --bin replay -- --tape {{tape}}

# Replay a tape *and* serve the chart page, at the recording's original pace.
# This is the demo that works on a plane.
demo tape speed="1.0":
    cargo run -p ma-server --bin replay -- --tape {{tape}} --speed {{speed}} --serve

# ------------------------------------------------------- Tier 2: live venues

# Connect to the venues and serve the chart page at http://127.0.0.1:8080.
serve venues="coinbase,kraken,bitstamp" symbol="BTC-USD":
    cargo run -p ma-server -- --venues {{venues}} --symbol {{symbol}}
