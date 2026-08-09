# Commands are grouped by which testing tier they belong to (see docs/DESIGN.md).
# Tier 1 runs offline. Tier 2 touches live venues. Tier 3 touches AWS.

default:
    @just --list

# ---------------------------------------------------------------- Tier 1: local
#
# Everything here must pass with the machine in airplane mode. That includes
# both replay layers: a committed tape and a local Parquet archive are both
# read from disk.

# Full offline suite.
test:
    cargo test --workspace

# Proves the book logic carries no async or I/O dependency.
test-core:
    cargo test -p ma-core

check:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo fmt --all --check

# Lint with the AWS SDK compiled in. Touches no bucket: the feature only adds
# code, and `S3Store` refuses to start without MA_S3_ACK_SCOPED_IAM=1.
check-s3:
    cargo clippy --workspace --all-targets --features ma-server/s3 -- -D warnings

fmt:
    cargo fmt --all

# Replay a recorded raw-frame tape through the full pipeline.
replay tape symbols="BTC-USD":
    cargo run -p ma-server --bin replay -- --tape {{tape}} --symbols {{symbols}}

# Replay a tape *and* serve the chart page, at the recording's original pace.
# This is the demo that works on a plane.
demo tape speed="1.0" symbols="BTC-USD":
    cargo run -p ma-server --bin replay -- --tape {{tape}} --symbols {{symbols}} --speed {{speed}} --serve

# Replay a Parquet archive — v2's other replay layer. It reproduces books, not
# parser behaviour; see ma-persist's crate docs for why both layers exist and
# why neither replaces the other.
replay-archive archive symbols="BTC-USD":
    cargo run -p ma-server --bin replay -- --archive {{archive}} --symbols {{symbols}} --serve

# ------------------------------------------------------- Tier 2: live venues
#
# Not part of `just test`. These open real sockets. Reconnect backoff is proven
# offline against the fake venue before anything here is run, because venues
# ban on reconnect storms.

# Connect to the venues and serve the chart page at http://127.0.0.1:8080.
#
# One connection per (venue, symbol): three venues and two symbols is six
# sockets, not three. See docs/DESIGN.md §2 for why they are not multiplexed.
serve venues="coinbase,kraken,bitstamp" symbols="BTC-USD":
    cargo run -p ma-server -- --venues {{venues}} --symbols {{symbols}}

# Serve, and archive normalised events to Parquet rolled hourly.
archive dir venues="coinbase,kraken,bitstamp" symbols="BTC-USD":
    cargo run -p ma-server -- --venues {{venues}} --symbols {{symbols}} --archive {{dir}}

# Record raw frames from a live venue into tapes/ for offline replay.
record venue symbol="BTC-USD" secs="120":
    cargo run -p ma-server --bin record -- --venue {{venue}} --symbol {{symbol}} --secs {{secs}}

# Diagnose a depth-audit disagreement: build a book from the websocket, fetch
# the venue's REST depth, and print exactly where the two differ. This is what
# established that the first audit's guard band was measured in the wrong unit
# — see ma_core::audit.
audit-probe venue="coinbase" symbol="BTC-USD" secs="20":
    cargo run -p ma-server --example audit_probe -- --venue {{venue}} --symbol {{symbol}} --secs {{secs}}

# ------------------------------------------------------------- Tier 3: AWS
#
# Nothing here runs until an IAM user scoped to one bucket prefix has replaced
# any root credentials. `S3Store` refuses to start unless MA_S3_ACK_SCOPED_IAM=1
# says that has been done — an assertion, not a verification, because nothing in
# the process can tell a scoped key from a root one.

# Serve, archiving to S3. Needs --features s3 and the acknowledgement above.
archive-s3 uri venues="coinbase,kraken,bitstamp" symbols="BTC-USD":
    cargo run -p ma-server --features s3 -- --venues {{venues}} --symbols {{symbols}} --archive {{uri}}
