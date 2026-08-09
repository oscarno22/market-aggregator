# market-aggregator

Multi-venue crypto market data ingest, normalisation and fan-out, in Rust and
tokio. Three venues, any number of symbols, one process.

The interesting problem in a market data feed is not throughput. It is that
**an order book with a missed delta is silently wrong**, and silently wrong is
worse than obviously down. A process that crashes gets noticed. A process that
serves confident prices from a book that quietly lost a message at 02:14 does
not, and everything downstream prices against a market that does not exist.

So the whole system is organised around one question:

> For every number we publish, can we say how much it should be believed?

---

## Try it without a network

The repository ships real recordings of all three venues. They replay through
the actual pipeline — the same parsers, books, aggregator, metrics and web page
a live run uses — with the network unplugged.

```bash
just demo tapes/2026-08-09-btc-usd-live.jsonl.gz
```

Three tapes, for different jobs:

| Tape | Length | What it is good for |
|---|---|---|
| `2026-08-09-btc-usd-live.jsonl.gz` | 3 min, 5827 frames | The default. Its touch moves — 114 distinct top-of-book states, 1.8 bps of range — so rolling indicators and the cross-venue view show real numbers. |
| `2026-08-09-btc-usd-reconnect.jsonl.gz` | 105 s, 2874 frames | Recovery. Three live venues, each forced to drop and resubscribe in turn (30 s, 55 s, 80 s), so the tape carries what each venue *actually sends* on a new subscription. Kraken's CRC32 agrees with the book rebuilt from it. |
| `2026-08-09-btc-usd.jsonl.gz` | 60 s, 1503 frames | The original, kept because it is the recording that found the three parser bugs below. Its Coinbase touch never moves once across 49,940 level updates, so it exercises parsing and books but not anything derived over time. |

Then open <http://127.0.0.1:8080>. To connect to the live venues instead:

```bash
just serve coinbase,kraken,bitstamp BTC-USD,ETH-USD
```

Or shard those six streams across two processes, neither ever running the same
one:

```bash
just cluster
```

Endpoints: `/` the page, `/events` SSE, `/metrics` Prometheus text,
`/api/snapshot` one JSON reading, `/health`.

Requires Rust 1.94+ and [`just`](https://github.com/casey/just). `just --list`
shows everything, grouped by whether it runs offline, touches a venue, or
touches AWS.

---

## The shape of it

```mermaid
flowchart LR
  subgraph ingest["one task per (venue, symbol) stream"]
    CB["Coinbase<br/>ws + heartbeats"]
    KR["Kraken<br/>ws"]
    BS["Bitstamp<br/>ws + REST depth"]
  end

  CH{{"bounded channel<br/>drop-oldest, 1024"}}
  AGG["aggregator<br/>one task, owns every book"]
  BC{{"broadcast<br/>32 snapshots"}}

  CB --> CH
  KR --> CH
  BS --> CH
  CH --> AGG
  AGG -- "every 250ms" --> BC
  BC --> SSE["SSE clients"]
  AGG -- "normalised events" --> PQ[("Parquet, hourly<br/>local or S3")]
  REST["periodic REST<br/>depth audit"] --> CH
```

**The aggregator owns every book, exclusively.** One task, no locks — not
because a `Mutex` would be slow at this scale, but because a lock makes it
*possible* to read one venue's book while another is mid-update, and eventually
someone does. The channel receiver is *moved* into the aggregator, so a second
one cannot be constructed.

**Crate boundaries are the proof, not the documentation.** `ma-core` has no
`tokio`, no `reqwest`, no I/O of any kind in its manifest — and a test fails the
build if one is ever added. "The book logic is unit-testable without a network"
is therefore checked by the compiler.

| Crate | Contains | May use |
|---|---|---|
| `ma-core` | `Book`, `MarketEvent`, `Price`, `IngestTime`, the depth audit | nothing async |
| `ma-venues` | wire formats, per-venue sync, endpoints as data | `serde` only |
| `ma-pipeline` | channel, ingest tasks, aggregator, raw-frame tape | `tokio` |
| `ma-persist` | Arrow schema, Parquet writer, `ObjectStore`, S3 | `arrow`, `parquet` |
| `ma-server` | axum SSE, `/metrics`, the page, the binaries | everything |

---

## Three venues, three different guarantees

This is the heart of the design. The venues do not merely differ in spelling;
they differ in **what they can prove about your book**.

| Venue | Snapshot from | Ordering field | Detects loss? | Verifies state? | `Integrity` |
|---|---|---|---|---|---|
| Bitstamp | REST only | `microtimestamp` | **No** | No | `OrderOnly` |
| Coinbase | the websocket | `sequence_num` | Yes | No | `GapDetectable` |
| Kraken | the websocket | *(none)* | No | **Yes** — CRC32 | `Verified` |

Kraken has no sequence number at all and is nonetheless the *strongest* of the
three: a checksum over the book you actually built validates the **resulting
state** rather than the path taken to it. A sequence number proves you received
every message; it says nothing about whether you applied them correctly.

So a book is never just "up". It is one of three things, and the third is the
one most systems miss:

```rust
Uninitialized                               // no data
Desynced { since, reason }                  // data I do not trust
Live { integrity, since, last_verified }    // data I trust, to a stated degree
```

`Integrity` is `Ord`, weakest first, so any view spanning venues takes the
minimum and reports the truth about the whole. The page does exactly that, in a
banner above the cards — a cross-venue spread that does not say "order-only at
best" is claiming more than the data supports.

---

## What live data taught, that a green test suite did not

The most useful thing this project produced. Every one of these passed a fully
green suite; all were found by recording real venue traffic or by watching a
live run.

**From the first recorded tape:**

1. Coinbase's `sequence_num` is scoped to the *connection*, not the channel —
   so every heartbeat read as a gap.
2. Coinbase says `offer`, not `ask`. Getting that wrong drops every ask update,
   leaves a one-sided book — and a one-sided book can never cross, so the
   last-resort crossed-book detector never fires either.
3. Kraken's `status` frame has no `symbol`, which broke an eagerly-typed
   envelope on the counter whose entire job is signalling schema drift.

**From running the v2 depth audit against live venues:**

4. A guard band counted in *levels* is not a distance. The top 50 levels of
   Coinbase BTC-USD span **2.4 basis points**; five levels in is 0.2 bps — well
   inside the churn the guard existed to exclude.
5. "Two mismatching audits in a row" is not evidence. Roughly 2% of levels in
   the 1–10 bps band disagree at any instant, so an audit comparing hundreds of
   levels finds *something* wrong nearly every time — at a different price each
   time. The rule became "the same *price* wrong on consecutive audits": noise
   moves, a lost delta does not.

**From killing a live run:**

6. Hourly Parquet *partitioning* is not hourly *durability*. A Parquet file is
   unreadable until its footer is written, and the process only handled Ctrl-C —
   so `SIGTERM`, which is what an orchestrator sends on every deploy, discarded
   everything since the last roll.

**From replaying a three-minute tape while building v3:**

7. A matching checksum was being read as a *state transition*, because
   `Live` carries `last_verified` and comparing whole states sees it move.
   Kraken logged 1006 "book is live" lines for 1108 messages, and the "live
   for" clock reset on every one — on the single venue whose guarantee is
   strongest, and only there.

A fixture author writes the messages they are thinking about. A policy author
picks the units they are thinking in. Both are why
[`just record`](justfile) and [`just audit-probe`](justfile) exist.

---

## Things worth reading the code for

- **Backpressure with a stated policy.** The ingest channel is bounded and
  drops the *oldest* event, because a stale tick has negative value. This is
  the wrong answer for claims processing, and the code says so — the two places
  that take the opposite policy (the tape tee, and replay) are documented
  against it. Drops are counted; a silent drop policy is a bug.
- **Reconnect is a resync, not a pause.** Nothing resumes: Coinbase restarts
  its sequence numbers, Kraken resends a snapshot, Bitstamp sends nothing until
  a REST call lands. Session boundaries travel *in-band* so they land in the
  right place relative to the frames around them.
- **Detection and recovery live in different tasks.** The aggregator owns the
  books so only it can notice a gap; the ingest task owns the socket so only it
  can fix one. The signal between them is a `watch` counter, so requests
  coalesce instead of queueing into a self-inflicted reconnect storm.
- **Equal jitter, not full jitter.** Full jitter's best case is retrying
  immediately, and immediately is the wrong thing to do to a venue that just
  rate-limited you.
- **Two replay layers, neither replacing the other.** A raw-frame tape records
  *bytes* and can reproduce a parser bug; the Parquet archive records
  *normalised events*, covers hours, and is queryable — and still verifies
  rebuilt books against Kraken's own checksum.
- **`Decimal` everywhere, and prices serialise as JSON strings.** Kraken hashes
  the exact digits it sent, trailing zeros included. A JSON number would be an
  `f64` in every browser and would undo that at the last step; a test pins it.
- **Every rolling window states how much of itself it covers.** A "60-second
  high" over a book that spent twenty of those seconds `Desynced` is a
  40-second high wearing a 60-second label. Each reading carries `trusted_ms`
  beside `span_ms` and the weakest `Integrity` it sampled under, and a window
  with nothing in it is `null` rather than zero.
- **Sharding without consensus, and the two rules that make it safe.** Each
  node writes exactly one key — its own lease — so there is no
  compare-and-swap anywhere and a shared directory is a complete registry.
  Assignment is rendezvous hashing, so losing a node moves only that node's
  streams rather than reshuffling everything, and every reshuffle is a real
  disconnect against a venue that bans for reconnect storms. Safety comes from
  a lease enforced by its **holder**: a node that cannot reach the registry
  releases every stream `guard` before anyone could declare it dead, because a
  partitioned node's sockets are fine and nothing will ever tell it otherwise.
  A joining node serves a settling period for the mirror-image case.
- **The cross-venue touch is the most misreadable number here, and is built to
  resist it.** Highest bid and lowest ask across venues, where a negative
  spread is an *apparent* arbitrage. Untrusted books are excluded (a `Desynced`
  book keeps its last contents, and an unguarded `max` would read a frozen
  aggressive bid as a live one); so are stalled ones; and the reported
  `integrity_floor` is taken over the two legs actually used, not over the
  venues present. Every exclusion is published with its reason, because a
  consolidated touch that has quietly narrowed to one venue looks exactly like
  one drawn from three.

The reasoning lives in **[`docs/DESIGN.md`](docs/DESIGN.md)** — sequence
diagrams, the decisions table with what was rejected and why, and an operating
runbook that says what to do when each metric moves.

---

## Status

v1, v2 and v3 are complete: three venues, multi-symbol, full L2 depth,
reconnect with gap-fill, the periodic integrity audit, SSE and a page, metrics,
a Parquet archive the process can replay itself from, rolling indicators that
state their own coverage, a cross-venue consolidated touch, and streams sharded
across nodes by a lease coordinator.

**298 tests, clippy-clean at `-D warnings`, and the whole suite runs offline** —
including the multi-node cluster simulation, which steps several coordinators
through one registry and asserts that no two ever hold the same stream.

Deliberately unfinished, and stated rather than hidden:

- ~~S3 is written and has never been run against a bucket.~~ **Done.** An IAM
  user scoped to one prefix replaced the root credentials, and the store has
  been exercised end to end: archived to S3, flushed on `SIGTERM`, and replayed
  back out into three live checksum-verified books. The scoping is now
  *verified* rather than asserted — `S3Store::connect` lists the bucket outside
  its own prefix and refuses to start if that succeeds. What is still untested
  is the far tail: listing pagination, and file sizes this project does not yet
  produce.
- ~~No tape has been recorded across a real reconnect.~~ **Done.**
  `2026-08-09-btc-usd-reconnect.jsonl.gz` records three live venues dropping and
  resubscribing in turn, and `crates/ma-server/tests/reconnect.rs` replays it
  offline. What it proves is each venue's *resubscribe* behaviour in its own
  bytes, and that a book rebuilt from them is right — Kraken's verifiably so.
  What it does not prove is *detection*: the socket was closed by us, so the
  idle watchdog and mid-stream socket errors stay proven against the fake venue.

- ~~Symbol is a Parquet column, not a partition.~~ **Done.** The archive is
  now `events/symbol=X/date=D/hour=H/part-N.parquet`, so a query for one symbol
  prunes to one subtree. That broke an assumption the reader had never written
  down — key order stopped being time order — so it now merges one cursor per
  partition. Archives written under the old layout still read.

- **A cluster registry backed by S3 is not implemented.** The trait needs only
  `PutObject`, `ListObjects` and `DeleteObject` — deliberately no conditional
  write — so it is a small addition, and no longer a blocked one.
- **Nothing merges the nodes.** Each node serves its own page and its own share
  of the streams; `/cluster` says who has what. A gateway that re-consolidates
  across nodes inherits the whole cross-venue timing problem across a network
  hop instead of a socket, which makes it a separate piece of work rather than
  a flag.

Next: v4 — a cross-node view, and an S3-backed cluster registry.

## Non-goals

**No Kafka** (ops burden without payoff at single-node scale). **No equities**
(paywalled; crypto is free and public). **No trading** — read-only, and no API
key with trade permission exists. **No AI features** — the point is the
concurrency story.
