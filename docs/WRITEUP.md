# Nine bugs, and the two that were in the ruler

A narrative account of what building [market-aggregator](../README.md) actually
taught, for a reader who is not going to clone it.

It is deliberately not a summary of the design. `docs/DESIGN.md` carries the
arguments at length and this defers to it rather than restating it — every
section ends with where to go for the reasoning. What is here is the part that
does not fit in a design document: which beliefs turned out to be wrong, how
they were found, and what the failures had in common.

---

## The question

The project is a multi-venue crypto market data pipeline in Rust and tokio —
three venues, any number of symbols, one process, websockets in and an order
book, a chart page and a Parquet archive out.

That description makes it sound like a throughput problem. It isn't. The
interesting property of a market data feed is that **an order book with a
missed delta is silently wrong**, and silently wrong is worse than obviously
down. A process that crashes gets noticed within a minute. A process that
serves confident prices from a book that quietly lost a message at 02:14 does
not, and everything downstream prices against a market that does not exist.

So the whole system is organised around one question:

> For every number we publish, can we say how much it should be believed?

Which is why the central type is not the book but its integrity. A book is
`Live`, `Desynced`, or `Uninitialized`, and the three are different claims:
"here is the market", "I have data I do not trust", "I have nothing". Most of
the design follows from refusing to collapse the middle one into either
neighbour — a desynced book is not an outage, and it is emphatically not a
price.

→ `docs/DESIGN.md` §1 for what the project refuses to be, §3 for the three
venues' very different guarantees and what the integrity model does with them.

---

## Nine bugs from live venues. Zero from the fixture suites.

The fixture suites were green throughout. All nine were found by pointing the
thing at a real venue, or by replaying a recording of one.

That number is the most useful thing the project produced, and the pattern in
it is sharper than "test against production". The bugs fall into three groups,
and each group has a different reason a fixture could never have contained it.

### A fixture author writes the messages they are thinking about

The first live recording — sixty seconds, three venues — found three parser
bugs in code that had passed hand-written fixtures for weeks.

Coinbase's `sequence_num` is scoped to the **connection**, not the channel, so
every heartbeat read as a gap in the book stream. Coinbase says `offer`, not
`ask` — and getting that wrong is worse than it sounds, because it drops every
ask update, leaves a one-sided book, and a one-sided book can never cross, so
the last-resort crossed-book detector never fires either. Kraken's `status`
frame carries no `symbol`, which broke an eagerly-typed envelope on the counter
whose entire job was signalling schema drift.

None of these is subtle. All three are the kind of thing you find in five
minutes with real traffic and never in a fixture, because a fixture contains
the messages its author already knew about. That is the whole argument for
recording raw frames *before* parsing, which is why `just record` exists and
why the tapes are committed.

### A policy author picks the units they are thinking in

Two venues of the three can prove nothing about the book you built from their
feed. Kraken checksums it on every message; Coinbase detects a lost message and
nothing else; Bitstamp detects nothing at all. So for two of three the only
independent evidence available is a periodic REST depth fetch — compare what
they say the book is against what we think it is.

The naive version of that is worse than not having it, because the REST
snapshot and the live book are from different instants and will disagree almost
every time. Two guards were supposed to fix it, and both were wrong in their
first implementation:

**A guard band counted in levels is not a distance.** The idea was to compare
only levels far enough from the touch that churn during the REST round trip
couldn't explain a difference. "Far enough" was written as a level count. But
the top fifty levels of Coinbase BTC-USD span **2.4 basis points**, and five
levels in is 0.2 bps — so the guard sat entirely inside the churn it existed to
exclude. It is now measured in basis points, and the REST requests ask for
enough depth to reach past it.

**"Two mismatching audits in a row" is not evidence.** Roughly 2% of levels in
the 1–10 bps band disagree at any given instant, so an audit comparing hundreds
of levels finds *something* wrong nearly every time — at a **different price**
each time. The rule became "the same price wrong on consecutive audits", which
is what the physical argument actually supports: noise moves, a lost delta does
not.

Both were found with `just audit-probe`, a diagnostic that prints the
disagreement profile by distance from the touch. Neither was reachable from a
fixture, because a fixture has no churn — the author picks the numbers, and
they hold still.

### A fixture is too short to contain the bug

The most quietly interesting one. Kraken publishes a CRC32 of the book on
every message, and a matching checksum advances a `last_verified` field inside
the `Live` state. Elsewhere, code comparing whole book states to decide whether
anything had changed therefore saw a **transition on every single message** —
1006 "book is live" log lines for 1108 messages, and the "live for" clock
resetting on each one.

Only Kraken publishes a checksum, so only Kraken showed it. And no fixture
could have, because the bug does not live in any message: it needs a *stream*
of consecutive **correct** ones. Every individual message in that stream was
handled perfectly. The fix is a comparison that asks about status rather than
about the whole state, which is a one-line change to code that was never wrong
about the market — only about whether it had just learned something new.

### An orchestrator sends SIGTERM

Hourly Parquet *partitioning* is not hourly *durability*. A Parquet file is
unreadable until its footer is written, and the process only handled Ctrl-C —
so `SIGTERM`, which is what every orchestrator sends on every deploy, discarded
everything since the last roll.

Found by killing a live run the way a deploy would. Fixed in two places:
`SIGTERM` is handled, and a part now closes every five minutes regardless, so
the hour decides the partition and that decides how much is ever at risk.

→ `docs/DESIGN.md` §5, §8, §9. The full numbered list is in the
[README](../README.md#what-live-data-taught-that-a-green-test-suite-did-not).

---

## The two that were in the ruler

The README's list stops at the system under test. The two most instructive
failures were not in it.

Replay is the foundation everything offline rests on: the same aggregator, the
same channel, the same books, fed from a recording instead of a socket. Every
claim the test suite makes about reconnects, windows and books is a claim about
what happened in replay. Twice, replay itself was wrong — and both times it
presented as a bug in the system it was measuring.

**Realtime replay drifted a timer tick per frame.** Pacing slept for the gap
between consecutive frames. A sleep may only overshoot, so the debt
accumulated: about seven seconds over a 5827-frame tape. Because replayed
frames carry reconstructed timestamps while the aggregator reads a real clock,
every book reported a seven-second age *while frames were arriving normally* —
which greyed every card on the page and made the cross-venue staleness guard
exclude every venue. It reads exactly like a system that has stopped receiving
data. It is now scheduled against a fixed origin.

**Replay had no clock of its own.** Frames carry `base + recorded_offset`, so
at `--speed n` they advance n times faster than the wall clock the aggregator
was reading. Book ages saturated to `0ms` and every rolling window read
*empty* — and an empty window renders identically to "this venue is sending
nothing". Invisible through the first two milestones because nothing before
then published a number derived from a duration. The fix is a clock that
advances with the tape.

The lesson is not "test your test harness", which nobody acts on. It is more
specific and more usable: **both bugs were in code that produces a duration,
and both were invisible in one of the two modes the harness supports.** So the
rule that came out of it is a procedure — after building anything that derives
a number from a duration, run the demo at speed 1.0 *and* at a multiplier, and
read the actual numbers. Bug one appears in only the first; bug two in only the
second.

It has already paid for itself once. A later test asserting on rolling windows
under full-speed replay would have re-created the second bug exactly —
full-speed replay keeps the system clock by design, so its window readings are
meaningless, and duration-derived assertions belong on a test clock.

→ `docs/DESIGN.md` §7.

---

## Sharding without a consensus system

Running the stream set across several nodes needs one property:

> **At most one node runs a given stream.**

Its dual — every stream running *somewhere* — is weaker in consequence, and
noticing that asymmetry is most of the design. An unowned stream is loudly
`uninitialized` on the page. A doubly-owned one looks completely fine until the
venue starts refusing connections. **Prefer the visible gap to the silent
duplicate**, which is the same instinct as the integrity model one layer down.

The obvious move is to reach for a consensus system. Kafka and etcd were both
revisited at this milestone, as planned, and declined — the problem is
*membership*, not a log, and membership by lease needs a clock and one writable
key per node. A consensus system would have been the largest operational
dependency in the project, bought to solve a problem a lease argument closes.

What it is instead:

**Assignment is rendezvous hashing over the live membership** — a pure
function, so every node computes the same answer with no leader and no
coordination. Not modulo, because modulo reshuffles two thirds of the
assignments when a node joins, and every reassignment is a disconnect and a
resync against a venue that bans for reconnect storms. The hash is hand-written
rather than `DefaultHasher`, because `DefaultHasher`'s keys are not stable
across processes, and two nodes disagreeing about the hash is two nodes
claiming one stream.

**Membership is a lease per node, with two rules that are each easy to get half
right.** The holder enforces its own expiry, releasing everything at
`ttl - guard` if it cannot complete a registry round trip — because readers
deciding who is alive is only safe if the dead node agrees, and a partitioned
node's sockets are all perfectly fine from where it sits. And a joining node
waits `ttl + guard` before acquiring, covering the mirror case where it sees
the new membership before the incumbent does.

Both halves are load-bearing, and that claim is checked the only way it can
honestly be checked: each was mutated separately and the disjointness assertion
confirmed to fail. A test that passes against a broken implementation is worse
than no test.

The consequence worth noticing: because each node writes only its own key,
there is no compare-and-swap anywhere in the registry interface — which is why
a shared directory is a *complete* implementation of it, and why the same code
runs against S3 with no new argument.

→ `docs/DESIGN.md` §13.

---

## A find, not a bug

The last recording made was two minutes of trades traffic, taken one commit
after the trade parsers landed, expecting to correct them. The parsers held.
What the tape caught instead was **Bitstamp's diff stream producing a genuinely
crossed book** — a lost or reordered diff upstream of our socket, on the one
venue where a dropped message leaves no trace in the protocol at all.

The crossed-book guard is Bitstamp's only loss signal, and it fired on real
data at recorded prices. Reconstructing the book from the tape's diffs alone
proves the trade frames sharing the socket had nothing to do with it. The
runbook row for `crossed book` used to describe a hypothetical; it now cites a
recording, and the whole episode is pinned in an offline test.

Two minutes. It is a good argument for recording more than you think you need.

→ `docs/DESIGN.md` §8.

---

## Where it stops

Five milestones, seven crates, 374 tests, all of them offline. There is a
recorded tape for the live path, a recorded tape for reconnects, a recorded
tape for trades, and a Parquet archive the process can replay itself out of.
`just demo` plays a real recording through the real pipeline with the network
unplugged.

It stops deliberately, and the things it does not do are stated rather than
hidden:

- **The gateway is a single point of failure.** It holds no state a node does
  not, so a second one is just a second process — but nothing elects one, and
  two gateways behind a load balancer would serve two views that differ by a
  tick. That is fine for a page and not fine for anything that alerts. It is
  not built, because nothing alerts, and building it would be `ma-coord`'s
  existing lease applied a second time rather than a new argument.
- **S3's far tail is half-proven.** Listing pagination now has an offline test.
  File sizes this project does not yet produce do not, and only a long run
  closes that honestly.
- **No Kafka, no etcd, no equities, no trading, no AI features.** Each declined
  for a stated reason rather than left as a gap.

The most transferable thing in the repository is not any of the code. It is the
habit that produced the list above: run it against reality early, record what
reality said, and treat a green suite as the start of verification rather than
the end.

→ `docs/DESIGN.md` §15.
