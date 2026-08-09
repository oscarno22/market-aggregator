# Real-Time Market Data Aggregator

Multi-venue crypto market data ingestion, normalization, and fan-out. Rust +
tokio. The point of this project is **coordination under async failure**, not
throughput benchmarks.

## Design constraints

- Long-lived websocket connections to multiple venues, one task per venue.
- All venue-specific wire formats normalize to a single internal event type at
  the edge. Nothing downstream knows which venue an event came from except by
  an explicit field.
- Bounded channels everywhere. Ingest must never block on a slow consumer.
- Drop policy for stale data is explicit and documented, not incidental.
- Everything that runs against a live feed must also run against a recorded
  replay. If a feature can't be tested in replay mode, it's not done.

## Architecture

```
venue ws ──┐
venue ws ──┼── normalize ──> mpsc ──> aggregator ──> broadcast ──> SSE clients
venue ws ──┘   (per-task)   (bounded)  (books,         (lagging      (browser)
                                        windows)        subs drop)
                                           │
                                           └──> writer ──> Parquet ──> S3
```

**Ingest tasks.** One `tokio` task per venue connection. Owns reconnect,
heartbeat/ping, and deserialization. Emits `MarketEvent` into the shared
bounded channel. Never touches shared state.

**Aggregator.** Single task owning all mutable state (order books, rolling
windows). Because it's single-owner, no locks. Reads from the mpsc, applies
updates, publishes snapshots to a `broadcast` channel on a tick.

**Fan-out.** `tokio::sync::broadcast` to per-client SSE tasks. Slow clients hit
`RecvError::Lagged`; correct handling is to skip forward to the latest snapshot,
not to error the connection.

**Persistence.** Separate consumer writing normalized events to Parquet, rolled
hourly, uploaded to S3. Feeds replay mode.

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

### 2. Backpressure

Bounded mpsc. When full, ingest drops the oldest event and increments a
counter, rather than awaiting send. Rationale: for market data, a stale tick
has negative value — delivering it late is worse than not delivering it. This
is a defensible position and should be written down in the code as a comment,
because it is the opposite of the right answer for the claims-processing case
where every message matters.

Expose drop counts as a metric. A silent drop policy is a bug.

### 3. Clock skew

Venue timestamps disagree, sometimes by seconds, and some venues lie. Every
event carries both `venue_ts` and `ingest_ts` (local monotonic + wall). All
windowing uses `ingest_ts`. Any cross-venue comparison surfaced in the UI must
label which clock it's using.

### 4. Replay

Replay reads Parquet and feeds the *same* aggregator through the *same*
channel, with an optional speed multiplier. This makes the whole pipeline
deterministically testable and lets the project demo with no network. Build
this early — it's the cheapest testing leverage in the project.

## Milestones

**v1 — the demonstrable core**
- 3 venues (Coinbase, Kraken, Binance), 1 symbol, top-of-book only
- Normalization, bounded channel, single-owner aggregator
- SSE endpoint + minimal chart page
- Reconnect with gap-fill and `Desynced` state
- Metrics: events/sec per venue, drop count, reconnect count, book age

**v2 — depth and durability**
- Full L2 order books with depth-limited pruning
- Parquet writer + S3 upload, hourly rolls
- Replay mode
- Multi-symbol

**v3 — only if v1 and v2 are solid**
- Shard venues across nodes with a coordination layer
- Rolling indicators over configurable windows
- Cross-venue spread / arbitrage view (careful with the clock caveat above)

## Non-goals

- **No Kafka.** At single-node scale it's ops burden without pedagogic payoff.
  Revisit only at v3 sharding.
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