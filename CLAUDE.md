# Real-Time Market Data Aggregator

Multi-venue crypto market data ingestion, normalization, and fan-out. Rust +
tokio. The point of this project is **coordination under async failure**, not
throughput benchmarks.

## Design constraints

- Long-lived websocket connections, one task per `(venue, symbol)` stream.
- All venue-specific wire formats normalize to a single internal event type at
  the edge. Nothing downstream knows which venue an event came from except by
  an explicit field.
- Bounded channels everywhere. Ingest must never block on a slow consumer.
- Drop policy for stale data is explicit and documented, not incidental.
- Everything that runs against a live feed must also run against a recorded
  replay. If a feature can't be tested in replay mode, it's not done.

## Architecture

```
stream ws ──┐                                         ┌──> SSE clients
stream ws ──┼── normalize ──> mpsc ──> aggregator ────┤    (lagging subs skip)
stream ws ──┘   (per-task)   (bounded)  (books,       │
   ▲                          depth, windows,         └──> writer ──> Parquet
   │                          cross-venue touch)                       ──> S3
   │                                        │
   └── REST depth audit ─────────────────────┘
   ▲
   └── supervisor ◄── owned streams ◄── coordinator ◄──> lease registry
       (start/stop)                     (rendezvous hash + lease)
```

v3 shards the stream set across nodes. Which streams *this* process runs is
decided by the coordinator and changes while it is up; everything to the right
of the channel is unchanged and does not know a cluster exists.

A **stream** is one `(venue, symbol)` pair. It owns a connection, a book, a set
of counters and a resync signal. Three venues × two symbols is six streams and
six sockets — deliberately not multiplexed, because a resync *is* a disconnect
and multiplexing would make one book's gap tear down every other symbol on that
venue.

**Ingest tasks.** One `tokio` task per stream. Owns reconnect, heartbeat/ping,
deserialization, and the periodic REST depth audit. Emits `MarketEvent` into the
shared bounded channel. Never touches shared state.

**Aggregator.** Single task owning all mutable state (order books, rolling
windows). Because it's single-owner, no locks. Reads from the mpsc, applies
updates, publishes snapshots to a `broadcast` channel on a tick.

**Fan-out.** `tokio::sync::broadcast` to per-client SSE tasks. Slow clients hit
`RecvError::Lagged`; correct handling is to skip forward to the latest snapshot,
not to error the connection.

**Persistence.** A task consuming normalized events *teed from the aggregator*,
writing Parquet rolled hourly behind an `ObjectStore`. Teed from the aggregator
rather than reading the raw channel, because normalizing is the venue layer's
job and a second consumer would have to duplicate every `VenueSync` — and would
eventually disagree with the first. Feeds Parquet replay mode.

## The hard parts (do these deliberately)

### 1. Reconnect and gap-fill

The core correctness problem. An order book with a missed delta is silently
wrong, which is worse than being obviously down.

Sequence on reconnect:
1. Open websocket, immediately start buffering incoming deltas. Do not apply.
2. Fetch REST depth snapshot for the symbol.
3. Discard buffered deltas with sequence <= snapshot sequence.
4. Verify the remaining buffer starts exactly at snapshot sequence + 1. If
   there's a hole, throw everything away and restart from step 1.
5. Apply buffer, then go live.

Book must expose a `Desynced` state. Downstream consumers must be able to tell
"no data" from "data I don't trust."

Reconnect backoff: exponential with jitter, capped. Venues will rate-limit or
ban on reconnect storms.

### 2. Auditing the venues that can't prove anything

Kraken checksums the book we built, on every message. Coinbase detects a *lost
message* and nothing else. Bitstamp detects nothing at all. So for two of three
venues a periodic REST depth fetch is the only independent evidence that exists,
and v2 added one.

The subtlety is that a naive version is worse than none, because the REST
snapshot and our book are from different instants and will disagree almost
every time. Two properties make it work, and both had to be corrected against
live data before they did:

- **Compare only levels far enough from the touch** — measured in **basis
  points**, not levels. On a dense book those are wildly different quantities:
  fifty levels of Coinbase BTC-USD span 2.4 bps.
- **Require the *same price* to disagree on consecutive audits.** Something is
  almost always mid-flight somewhere, so "two mismatching audits" is not
  evidence. A lost delta corrupts one price and leaves it wrong; churn picks a
  new price each time.

The audit is advisory first: every comparison feeds a counter, and only a
persistent finding is allowed to desync a book — into the same recovery path
every other desync uses. Kraken is deliberately not audited, because a periodic
comparison is strictly weaker than a per-message checksum.

`just audit-probe` prints the disagreement profile by distance from the touch.
That tool is how both corrections above were found; reach for it before
changing `AuditPolicy`.

### 3. Backpressure

Bounded mpsc. When full, ingest drops the oldest event and increments a
counter, rather than awaiting send. Rationale: for market data, a stale tick
has negative value — delivering it late is worse than not delivering it. This
is a defensible position and should be written down in the code as a comment,
because it is the opposite of the right answer for the claims-processing case
where every message matters.

Expose drop counts as a metric. A silent drop policy is a bug.

### 4. Clock skew

Venue timestamps disagree, sometimes by seconds, and some venues lie. Every
event carries both `venue_ts` and `ingest_ts` (local monotonic + wall). All
windowing uses `ingest_ts`. Any cross-venue comparison surfaced in the UI must
label which clock it's using.

### 5. Sharding without a consensus system

The property is **at most one node runs a given stream**. Its dual — every
stream running somewhere — is weaker in consequence: an unowned stream is
loudly `uninitialized`, a doubly-owned one looks fine until the venue starts
refusing connections. Prefer the visible gap to the silent duplicate.

Assignment is rendezvous hashing over the live membership — a pure function, so
every node computes the same answer without a leader. Modulo would reshuffle
two thirds of the assignments when a node joins, and every reassignment is a
disconnect and a resync against a venue that bans for reconnect storms. The
hash is hand-written, because `DefaultHasher`'s keys are not stable across
processes and two nodes disagreeing about the hash means two nodes claiming one
stream.

Membership is a lease per node, with two rules that are each easy to get
half-right:

- **The holder enforces its own expiry**, releasing everything at `ttl - guard`
  if it cannot complete a registry round trip. Readers deciding who is alive is
  safe only if the dead node agrees, and a partitioned node's sockets are fine.
- **A joining node waits `ttl + guard`** before acquiring, covering the mirror
  case where it sees the new membership before the incumbent does.

Both are load-bearing: reverting either makes the disjointness assertion in
`ma-coord/tests/cluster.rs` fire. Each node writes only its own key, so there is
no compare-and-swap anywhere — which is why a directory is a complete registry.
`docs/DESIGN.md` §13 has the full argument. **Kafka was revisited here as
planned, and declined**: the problem is membership, not a log.

### 6. Replay

Replay feeds the *same* aggregator through the *same* channel, with an
optional speed multiplier. This makes the whole pipeline deterministically
testable and lets the project demo with no network. Build this early — it's
the cheapest testing leverage in the project.

In practice this is **two layers, not one**:
- A raw-frame tape (`ma-pipeline::tape`), recorded *before* parsing. This is
  what makes a recorded session able to reproduce a parser bug or a venue
  schema change, which recording post-normalization can never do. Built —
  see Status below.
- The Parquet-backed replay described in the Architecture diagram, over
  *normalized* events, which is v2's durability story and a separate
  concern from the tape.

Neither layer replaces the other.

## Milestones

**v1 — the demonstrable core** — **complete**
- [x] 3 venues, 1 symbol, top-of-book only — **Coinbase, Kraken, Bitstamp**,
      not Binance as originally planned: Binance 451s on requests from US
      IPs, so Bitstamp took its slot. Its integrity guarantee is weaker
      (`OrderOnly`, no gap detection) than the other two, which turned out to
      be a more interesting three-venue spread than the original plan anyway.
- [x] Normalization, bounded channel
- [x] Single-owner aggregator task
- [x] SSE endpoint + minimal chart page
- [x] Gap-fill state machine and `Desynced` state, proven offline per-venue
- [x] Real reconnect over a live socket (exponential backoff + **equal**
      jitter, capped; resets only after a session lasts `min_stable`, so a
      flapping venue keeps escalating)
- [x] Metrics surface at `/metrics` (Prometheus text): events/sec, drop
      count, reconnect count, book age, **time-in-Desynced**
- [x] First live connection, tape recorded and committed

**v2 — depth and durability** — **complete**
- [x] Full L2 order books. Depth **served** and depth **retained** are
      deliberately different numbers: pruning is only safe where the venue
      publishes a depth-limited feed (Kraken), so Coinbase and Bitstamp retain
      everything and the ladder is a projection. `ma_book_levels` publishes the
      gap.
- [x] Periodic REST re-snapshot audit for Bitstamp and Coinbase, with a
      **basis-point** guard band and a same-price persistence rule. Both of
      those were corrections forced by live data — see Status.
- [x] Parquet writer behind an `ObjectStore` trait, hourly rolls, `ma-persist`
- [x] Replay mode over normalized/Parquet events (raw-frame tape replay is a
      v1 deliverable and was already done — see §6)
- [x] Multi-symbol, one connection per `(venue, symbol)` stream
- [x] S3 store written, compiled, and — during v3 — **run against a real
      bucket** once an IAM user scoped to one prefix replaced the root
      credentials. `S3Store::connect` now *verifies* that scoping rather than
      taking the operator's word for it

**v3 — sharding, indicators, cross-venue** — **complete**
- [x] Shard streams across nodes with a coordination layer — `ma-coord`:
      rendezvous-hash assignment (pure, no I/O) plus a lease per node,
      **enforced by its holder**, with a settling period on join. No
      consensus system and no compare-and-swap: each node writes only its
      own key, so a shared directory is a complete registry. Verified live
      across two processes, a `kill -9`, and a registry made unreadable
      under six healthy sockets
- [x] Rolling indicators over configurable windows, every reading carrying
      `trusted_ms`/`span_ms` coverage and an integrity floor — a window
      spanning a desync is not a window over the market
- [x] Cross-venue consolidated touch, with untrusted and stalled books
      excluded **by name and reason** rather than silently, and the
      integrity floor taken over the legs actually used

**v4 — next**
- A cross-node view: a gateway merging every node's snapshot. It inherits
  the whole of §12's problem across a network hop rather than a socket
- Symbol-partitioned Parquet, once one symbol's hour is worth skipping whole
- A tape recorded across a real reconnect — the last artefact that would
  make the recovery path as well-evidenced as the parsers now are

## Status

Snapshot as of the last session; update this when a milestone item lands or
a design decision changes.

**v1, v2 and v3 are complete.** Six-crate workspace, **298 tests passing,
clippy-clean (`-D warnings`), `cargo fmt` clean**. The full suite runs offline;
`just demo tapes/2026-08-09-btc-usd-live.jsonl.gz` plays a real recording
through the real pipeline with the network unplugged, and `just cluster` shards
six live streams across two processes.

Read `docs/DESIGN.md` first — it carries the reasoning, the gap-fill and audit
sequence diagrams, and the operating runbook. This section is only the current
state.

- **`ma-core`** — no I/O, no async deps (enforced by a manifest test).
  `Decimal`-based `Price`/`Qty`, `IngestTime` carrying both clocks,
  `Book`/`BookState` with the three-way `Integrity` model, `StreamId`, the
  depth-audit comparison, the rolling-window bucket ring, and the cross-venue
  consolidation.
- **`ma-venues`** — `VenueSync` state machines and golden fixtures for all
  three venues, plus `endpoint.rs`: URLs, subscribe payloads, REST snapshot and
  audit URLs as data, so this crate still opens no sockets.
- **`ma-pipeline`** — bounded drop-oldest channel (plus `send_lossless` for
  replay, which must not drop), reconnect backoff, per-stream ingest tasks
  behind a `Network` trait, the single-owner aggregator, counters, and the
  raw-frame tape recorder/replay.
- **`ma-persist`** — Arrow schema, Parquet writer with hourly rolls, the
  `ObjectStore` trait with a local implementation, and S3 behind a default-off
  feature. The only crate that sees `arrow`/`parquet`.
- **`ma-coord`** — v3. Rendezvous-hash assignment (pure, no async, like
  `ma-core`), the lease loop with holder-side expiry and a settling period, and
  a `Registry` trait with directory and in-process implementations. No
  compare-and-swap in the trait, deliberately: each node writes only its own
  key. The offline suite steps several coordinators through one registry and
  asserts disjointness after every pass.
- **`ma-server`** — axum SSE with correct `Lagged` handling, `/metrics`,
  `/cluster`, a self-contained chart page with L2 ladders, rolling-window rows
  and the consolidated touch, the cluster supervisor that starts and stops
  streams as ownership moves, three binaries (`ma-server`, `record`, `replay`)
  and the `audit_probe` diagnostic example.

**Three bugs the first live tape found, that every hand-written fixture
missed** — the argument for the tape recorder, recorded here so it is not
re-learned: Coinbase's `sequence_num` is connection-scoped rather than
per-channel (every heartbeat read as a gap); Coinbase says `offer`, not
`ask`; Kraken's `status` frame has no `symbol` and broke an eagerly-typed
envelope. See `docs/DESIGN.md` §8.

**Two more the live venues found in v2, both in the depth audit** — same
lesson, different layer, and worth recording because both looked obviously
correct offline:

1. **A guard band counted in *levels* is not a distance.** The top fifty levels
   of Coinbase BTC-USD span 2.4 basis points; five levels in is 0.2 bps. The
   touch moves further than that during the REST round trip, so the guard sat
   entirely inside the churn it existed to exclude. It is now measured in basis
   points, and the REST requests ask for enough depth to reach past it.
2. **"Two mismatching audits in a row" is not evidence.** Measured churn is
   ~2% of levels in the 1–10 bps band, so an audit comparing hundreds of levels
   finds *something* wrong nearly every time — at a different price each time.
   The rule is now "the same price wrong on consecutive audits", which is what
   the physical argument actually supports: noise moves, a lost delta does not.

Both were caught by running against live venues and diagnosed with
`just audit-probe`, which prints the disagreement profile by distance from the
touch. Neither was reachable from a fixture.

**And one in the archive, found by killing a live run:** hourly *partitioning*
is not hourly *durability*. A Parquet file is unreadable until its footer is
written, and the process only handled Ctrl-C — so `pkill` (i.e. what any
orchestrator sends on deploy) discarded everything since the last roll. Fixed
in two places: `SIGTERM` is handled, and `WriterConfig::max_open` closes a part
every five minutes regardless, so the hour decides the partition and that
decides how much is ever at risk.

**Two more the second tape found, while v3 was being built** — both in v1/v2
code, both only observable over a long run against real traffic:

1. **A matching checksum was being read as a state transition.** `Live` carries
   `last_verified`, which advances on every Kraken CRC32 match, so comparing
   whole `BookState`s saw a transition per message: 1006 "book is live" INFO
   lines for 1108 messages, and the "live for" clock resetting on each one. Only
   Kraken publishes a checksum, so only Kraken showed it, and no fixture could —
   it takes a *stream* of matching checksums. `BookState::same_status` is the
   comparison that was wanted.
2. **Realtime replay drifted a timer tick per frame.** Pacing slept for the gap
   between frames; a sleep may only overshoot, so the debt accumulated — ~7s
   into a 5827-frame tape. Because replayed frames carry reconstructed
   `IngestTime` while the aggregator reads a real clock, every book reported a
   seven-second age while frames arrived normally, which greyed every card and
   made the cross-venue staleness guard exclude every venue. Now scheduled
   against a fixed origin. **The bug was in the test harness every offline claim
   rests on, and presented as a bug in the system under test.**

3. **Replay had no clock of its own.** Frames carry `base + recorded_offset`,
   so at `--speed n` they advance `n` times faster than the wall clock the
   aggregator read. Book ages saturated to `0ms` and every rolling window read
   empty — and none of that looks like a clock problem. Invisible through v1
   and v2 because nothing before v3 published a number derived from a duration.
   `ma_core::ScaledClock` now advances with the tape.

**The S3 gate is now open, and was opened properly.** As of 2026-08-09 an IAM
user (`market-aggregator`) scoped to exactly one bucket prefix replaced the root
credentials, and the store has been run end to end: written under
`s3://market-aggregator-…/events`, flushed 58,574 rows on `SIGTERM`, and
replayed back out of S3 into three live checksum-verified books.

`MA_S3_ACK_SCOPED_IAM=1` is still required, but it is no longer the whole
control: `S3Store::connect` now lists the bucket *outside* its configured prefix
and refuses to start if that succeeds. Root is allowed there, a scoped user is
denied, and **any other error is also a refusal** — never read as proof of
scoping. Run with `AWS_PROFILE=market-aggregator`; the ambient default is still
a root login session.

An S3-backed cluster registry is now unblocked. It needs only PutObject,
ListObjects and DeleteObject — deliberately no conditional write.

**Next up:** v4 — a cross-node merged view, symbol-partitioned Parquet, a tape
across a real reconnect.

## Non-goals

- **No Kafka, and no etcd.** Revisited at v3 sharding as planned, and
  declined: the coordination problem is *membership*, not a log, and
  membership by lease needs a clock and one writable key per node. Adding a
  consensus system would be the largest operational dependency in the project,
  bought to solve a problem the lease argument in `docs/DESIGN.md` §13 closes.
- **No equities.** Real-time equity data is paywalled; crypto is free and
  public.
- **No trading.** Read-only. No order placement, no keys with trade permissions.
- **No AI features.** The point is the concurrency story.

## Stack

`tokio`, `tokio-tungstenite`, `serde` / `serde_json`, `axum` for SSE,
`arrow` + `parquet` for persistence, `tracing` for structured logs,
`criterion` if benchmarking becomes relevant.

## Testing notes

- Order book application logic must be unit-testable without any network.
- Write a fake venue that emits scripted sequences including gaps, duplicates,
  and out-of-order deltas. Gap-fill correctness is proven here, not in prod.
- Property test: applying a delta stream to a book, then replaying from
  snapshot + remaining deltas, yields identical state.