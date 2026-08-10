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

# Scan Cargo.lock against the RUSTSEC advisory database.
#
# The one recipe in this tier that needs a network — it fetches the advisory
# db — and deliberately *not* a gate. A new advisory against a dependency is
# worth knowing about, but it lands with no change on our side, and a gate that
# goes red for something nobody did is a gate people learn to ignore. CI runs
# this and reports it; it never fails the build.
#
# Exceptions live in .cargo/audit.toml with the argument for each written out,
# so bare `cargo audit` agrees with this recipe. The assumption the current
# exception rests on is enforced by `just test`, not by that file's comment.
audit:
    cargo audit

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

# Replay an hour a *cluster* wrote: one prefix per node, merged into one session.
#
# Under v3 sharding each node archives only the streams it owns, so an hour of
# everything is a union of prefixes rather than a file. Two nodes' keys differ
# above date=, so symbol partitioning already puts them on separate cursors and
# the merge needs no new machinery — but the *timeline* does, because `elapsed`
# restarts with every writer run and two nodes share no origin. With more than
# one prefix it is rebuilt from the wall clock instead.
#
# Merging across machines is answerable here for a reason worth knowing: at
# most one node ever runs a given stream, so this only ever orders events
# belonging to different books. See ma_persist::reader's module docs.
#
# Overlapping prefixes are deduplicated rather than replayed twice — pass the
# root and a subtree if you like.
replay-cluster archive prefixes="node-a/events,node-b/events" symbols="BTC-USD":
    cargo run -p ma-server --bin replay -- --archive {{archive}} \
        --archive-prefix {{prefixes}} --symbols {{symbols}} --serve

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

# Record a tape that contains reconnects, one venue at a time.
#
# Each offset drops the *next* stream and lets it resubscribe, through the same
# resync request the aggregator makes for a real desync. Staggering them is the
# point: the tape then also records the other two venues carrying on untouched,
# which is the claim "one connection per (venue, symbol)" is making.
#
# Gzip the result before committing — see ma_pipeline::tape on why tapes are
# stored compressed. This is how tapes/2026-08-09-btc-usd-reconnect.jsonl.gz
# was made.
record-reconnect symbol="BTC-USD" secs="105" at="30,55,80":
    cargo run --release -p ma-server --bin record -- \
        --venue coinbase,kraken,bitstamp --symbol {{symbol}} \
        --secs {{secs}} --reconnect-at {{at}}

# Run two nodes sharding the streams between them, against live venues.
#
# Six streams (three venues x two symbols) split across two processes, with
# neither ever running the same one. Pages at :8081 and :8082; each node shows
# only what it owns, and /cluster on either says who has what.
#
# Try killing one with `kill -9` and watching the other pick its streams up:
# that takes ttl (7s here) plus a renewal, because a hard kill cannot withdraw
# and the survivor has to wait the lease out. `kill -TERM` hands over at once.
cluster dir="/tmp/ma-cluster" symbols="BTC-USD,ETH-USD":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{dir}}
    trap 'kill 0' EXIT
    cargo run -p ma-server -- --node-id node-a --cluster-dir {{dir}} \
        --cluster-ttl-ms 7000 --symbols {{symbols}} --addr 127.0.0.1:8081 &
    cargo run -p ma-server -- --node-id node-b --cluster-dir {{dir}} \
        --cluster-ttl-ms 7000 --symbols {{symbols}} --addr 127.0.0.1:8082 &
    wait

# Merge every node's snapshot into one view, at http://127.0.0.1:8080.
#
# Run `just cluster` first. The gateway opens no venue sockets of its own — it
# follows each node's /events, re-consolidates, and serves the same page and the
# same JSON shape a node does, so the chart cannot tell it is looking at a
# cluster.
#
# It is the only place two things are visible:
#   - a consolidated touch over *every* venue, rather than over the venues one
#     node happens to own
#   - a stream two nodes both claim. /nodes and ma_gateway_duplicated_streams.
gateway nodes="http://127.0.0.1:8081,http://127.0.0.1:8082" addr="127.0.0.1:8080":
    cargo run -p ma-server --bin gateway -- --nodes {{nodes}} --addr {{addr}}

# Who is contributing to the merged view, and what any two nodes both claim.
gateway-status addr="127.0.0.1:8080":
    @curl -s "http://{{addr}}/nodes" || echo "  gateway not responding"

# Print who owns what, across a running cluster.
cluster-status:
    @for port in 8081 8082; do \
        echo "--- :$port"; \
        curl -s "http://127.0.0.1:$port/cluster" || echo "  not responding"; \
        echo; \
    done

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
#
# Pass the BUCKET ROOT, not the events prefix: the URI's path and
# --archive-prefix (default "events") compose, so `s3://bucket/events` writes
# under events/events/. Found the embarrassing way on 2026-08-09.
archive-s3 uri venues="coinbase,kraken,bitstamp" symbols="BTC-USD":
    cargo run -p ma-server --features s3 -- --venues {{venues}} --symbols {{symbols}} --archive {{uri}}

# Two nodes sharding live streams through a cluster registry in S3.
#
# The same run as `just cluster`, with the shared directory replaced by a
# bucket prefix — which is what lets the nodes be on different machines rather
# than merely different processes. `Registry` needs PutObject, ListObjects and
# DeleteObject and no conditional write; docs/DESIGN.md §13 has the argument
# for why that is enough.
#
# `ttl` is larger than the directory recipe's because a round trip is now a
# network call. A node that cannot reach S3 stands down rather than retrying,
# so a too-short ttl is safe and pointless.
#
# Give the registry its OWN prefix, not the archive's. If your IAM user is
# scoped to the archive prefix — this project's is — nest it there and say so:
#   just cluster-s3 s3://my-bucket/events/cluster
cluster-s3 uri symbols="BTC-USD,ETH-USD" ttl="15000":
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill 0' EXIT
    cargo run --release -p ma-server --features s3 -- --node-id node-a \
        --cluster-registry {{uri}} --cluster-ttl-ms {{ttl}} \
        --symbols {{symbols}} --addr 127.0.0.1:8081 &
    cargo run --release -p ma-server --features s3 -- --node-id node-b \
        --cluster-registry {{uri}} --cluster-ttl-ms {{ttl}} \
        --symbols {{symbols}} --addr 127.0.0.1:8082 &
    wait
