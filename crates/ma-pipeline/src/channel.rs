//! The bounded, drop-oldest channel between ingest and the aggregator.
//!
//! # Why drop the oldest, not block the sender
//!
//! When this channel is full, [`Sender::send`] evicts the oldest queued event
//! and inserts the new one. It never awaits, and it never blocks the ingest
//! task, which is what "ingest must never block on a slow consumer" (see
//! `CLAUDE.md`) actually requires in code rather than in prose.
//!
//! The justification is specific to market data: **a stale tick has negative
//! value.** A book update from three seconds ago is not a late-but-useful
//! version of the truth, it is actively misleading, because whoever reads it
//! will price against a book that no longer exists. Given a full buffer, the
//! right event to keep is the newest one, and the right thing to do with the
//! rest is throw them away immediately rather than deliver them late.
//!
//! **This is the opposite of the right answer for claims processing**, or
//! payments, or anything where every message is a fact that must eventually be
//! recorded. There, a full buffer means backpressure the producer must respect
//! — awaiting, retrying, or rejecting the write outright — because losing
//! message #4 to make room for #9 loses a fact, not a stale opinion. Silently
//! applying this policy there would be a bug, not a design decision. It is a
//! design decision here only because market data's freshness requirement makes
//! it one, and only because the drop is counted rather than silent.
//!
//! # Why a `Mutex<VecDeque>` and not something lock-free
//!
//! `CLAUDE.md` says the point of this project is coordination under async
//! failure, not throughput benchmarks. A [`std::sync::Mutex`] guarding a
//! [`VecDeque`] is held for O(1) work and is **never held across an `.await`**,
//! so it cannot deadlock with the async runtime and contributes no async
//! cancellation hazards. At the scale of three venues publishing top-of-book
//! ticks, lock contention is not a real cost; a reader six months from now
//! being able to see the whole invariant in one short critical section is.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::Notify;

struct Inner<T> {
    capacity: usize,
    queue: Mutex<VecDeque<T>>,
    notify: Notify,
    dropped: AtomicU64,
    sender_count: AtomicUsize,
    closed: AtomicBool,
}

// Written by hand rather than derived: `#[derive(Debug)]` would require
// `T: Debug` even though nothing here actually prints an item, which would
// force that bound onto `Sender<T>` and `Receiver<T>` for no real benefit.
impl<T> std::fmt::Debug for Inner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("capacity", &self.capacity)
            .field(
                "len",
                &self
                    .queue
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .len(),
            )
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .field("sender_count", &self.sender_count.load(Ordering::Relaxed))
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish()
    }
}

/// Point-in-time channel health, suitable for a metrics scrape.
///
/// `dropped` is the one CLAUDE.md calls out specifically: "a silent drop
/// policy is a bug." Wiring this into `/metrics` is what keeps it from being
/// one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ChannelMetrics {
    pub len: usize,
    pub capacity: usize,
    pub dropped: u64,
}

/// Create a bounded, drop-oldest, multi-producer single-consumer channel.
///
/// # Panics
/// If `capacity` is zero — a channel that can hold nothing would drop every
/// single event, which is never what a caller means to ask for.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(
        capacity > 0,
        "bounded channel capacity must be greater than zero"
    );
    let inner = Arc::new(Inner {
        capacity,
        queue: Mutex::new(VecDeque::with_capacity(capacity)),
        notify: Notify::new(),
        dropped: AtomicU64::new(0),
        sender_count: AtomicUsize::new(1),
        closed: AtomicBool::new(false),
    });
    (
        Sender {
            inner: Arc::clone(&inner),
        },
        Receiver { inner },
    )
}

/// What happened to the item just sent.
#[derive(Debug, PartialEq, Eq)]
pub enum SendOutcome<T> {
    /// Queued; the channel had room.
    Sent,
    /// The channel was full. This item was queued anyway, and the item
    /// returned here — the previously-oldest one — was evicted to make room.
    /// A caller that wants to log or count losses inspects this value; the
    /// running total is always available from [`Sender::metrics`].
    DroppedOldest(T),
    /// Every sender-side handle but this one has already been dropped, or
    /// [`Sender::close`] was called. Nothing was queued.
    Closed(T),
}

/// The producer half. One per ingest task; cloneable if a caller wants more.
#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    /// Queue `item`, evicting the oldest queued item if the channel is full.
    ///
    /// Synchronous and non-blocking by construction — there is no `.await` in
    /// this function's body, which is the property the whole module exists to
    /// provide. [`send_never_needs_a_runtime`](tests) below proves it by
    /// calling this from a plain `#[test]` with no async executor at all.
    pub fn send(&self, item: T) -> SendOutcome<T> {
        if self.inner.closed.load(Ordering::Acquire) {
            return SendOutcome::Closed(item);
        }

        let evicted = {
            let mut queue = self.lock();
            let evicted = if queue.len() >= self.inner.capacity {
                queue.pop_front()
            } else {
                None
            };
            queue.push_back(item);
            evicted
        };

        // Wake a waiting receiver outside the lock so it never contends with
        // the next sender for the mutex it's about to try to acquire.
        self.inner.notify.notify_one();

        match evicted {
            Some(old) => {
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                SendOutcome::DroppedOldest(old)
            }
            None => SendOutcome::Sent,
        }
    }

    /// Close the channel from this handle, as if every clone of it were
    /// dropped at once. Queued items remain available to [`Receiver::recv`];
    /// only new sends are refused after this.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    pub fn metrics(&self) -> ChannelMetrics {
        ChannelMetrics {
            len: self.lock().len(),
            capacity: self.inner.capacity,
            dropped: self.inner.dropped.load(Ordering::Relaxed),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<T>> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Closing on the last sender is what lets `Receiver::recv` return
        // `None` instead of hanging forever once nothing more can arrive.
        if self.inner.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.closed.store(true, Ordering::Release);
            self.inner.notify.notify_waiters();
        }
    }
}

/// The consumer half. Intended for a single aggregator task.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    /// Wait for the next item.
    ///
    /// Returns `None` once the channel is closed *and* drained — closing does
    /// not discard what was already queued.
    pub async fn recv(&self) -> Option<T> {
        loop {
            if let Some(item) = self.pop() {
                return Some(item);
            }
            if self.inner.closed.load(Ordering::Acquire) {
                // One more check: a send racing the close between our pop
                // above and this load must not be lost.
                return self.pop();
            }

            // Standard `Notify` pattern: obtain the future, then re-check the
            // condition before awaiting it, so a notification fired between
            // our first check and subscribing is not missed.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);

            if let Some(item) = self.pop() {
                return Some(item);
            }
            if self.inner.closed.load(Ordering::Acquire) {
                return self.pop();
            }

            notified.await;
        }
    }

    /// Take an item if one is queued, without waiting.
    ///
    /// The aggregator drains with this at the top of every loop turn, so a
    /// backlog is applied in one batch rather than one `select!` round trip
    /// per event. It also means nothing sits in the queue longer than the
    /// aggregator's tick, whatever happens with wakeups.
    pub fn try_recv(&self) -> Option<T> {
        self.pop()
    }

    pub fn metrics(&self) -> ChannelMetrics {
        ChannelMetrics {
            len: self.lock().len(),
            capacity: self.inner.capacity,
            dropped: self.inner.dropped.load(Ordering::Relaxed),
        }
    }

    fn pop(&self) -> Option<T> {
        self.lock().pop_front()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<T>> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn send_never_needs_a_runtime() {
        // No #[tokio::test], no runtime constructed anywhere in this test. If
        // `Sender::send` ever grew an `.await`, this test would not compile —
        // there is no executor here to poll it. That absence is the proof.
        let (tx, _rx) = bounded::<u32>(4);
        assert_eq!(tx.send(1), SendOutcome::Sent);
        assert_eq!(tx.send(2), SendOutcome::Sent);
    }

    #[test]
    fn items_are_delivered_fifo_under_capacity() {
        let (tx, rx) = bounded::<u32>(4);
        for i in 1..=3 {
            assert_eq!(tx.send(i), SendOutcome::Sent);
        }
        // recv() is async, but draining a non-empty queue never actually
        // suspends, so a synchronous block_on-free poll works in a plain test.
        let drained: Vec<u32> = std::iter::from_fn(|| rx.pop()).collect();
        assert_eq!(drained, vec![1, 2, 3]);
    }

    #[test]
    fn overflow_evicts_the_oldest_not_the_newest() {
        let (tx, rx) = bounded::<u32>(2);
        assert_eq!(tx.send(1), SendOutcome::Sent);
        assert_eq!(tx.send(2), SendOutcome::Sent);
        // Full. `3` must survive; `1`, the oldest, must be the one evicted.
        assert_eq!(tx.send(3), SendOutcome::DroppedOldest(1));

        let drained: Vec<u32> = std::iter::from_fn(|| rx.pop()).collect();
        assert_eq!(
            drained,
            vec![2, 3],
            "newest item was lost instead of oldest"
        );
    }

    #[test]
    fn dropped_counter_tracks_every_eviction_exactly() {
        let (tx, _rx) = bounded::<u32>(1);
        assert_eq!(tx.metrics().dropped, 0);

        assert_eq!(tx.send(1), SendOutcome::Sent);
        assert_eq!(
            tx.metrics().dropped,
            0,
            "first send filled the channel, nothing evicted yet"
        );

        for i in 2..=5u32 {
            tx.send(i);
        }
        assert_eq!(
            tx.metrics().dropped,
            4,
            "four overflowing sends into a capacity-1 channel should drop exactly four"
        );
    }

    #[test]
    fn metrics_reports_len_and_capacity() {
        let (tx, rx) = bounded::<u32>(3);
        assert_eq!(
            tx.metrics(),
            ChannelMetrics {
                len: 0,
                capacity: 3,
                dropped: 0
            }
        );

        tx.send(1);
        tx.send(2);
        assert_eq!(
            tx.metrics(),
            ChannelMetrics {
                len: 2,
                capacity: 3,
                dropped: 0
            }
        );

        rx.pop();
        assert_eq!(
            tx.metrics().len,
            1,
            "receiver and sender must observe the same queue"
        );
    }

    #[tokio::test]
    async fn recv_returns_none_after_close_once_drained() {
        let (tx, rx) = bounded::<u32>(4);
        tx.send(1);
        tx.close();

        // Draining what was already queued must still work after close...
        assert_eq!(rx.recv().await, Some(1));
        // ...and only then does the channel report exhausted.
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv())
                .await
                .expect("recv hung instead of returning None on a closed, drained channel"),
            None
        );
    }

    #[tokio::test]
    async fn last_sender_drop_closes_the_channel() {
        let (tx, rx) = bounded::<u32>(4);
        let tx2 = tx.clone();

        drop(tx);
        // A clone is still alive, so the channel must not be closed yet.
        let recv_fut = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(
            recv_fut.is_err(),
            "channel closed early while a sender clone was still live"
        );

        drop(tx2);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv())
                .await
                .expect("recv hung after the last sender was dropped"),
            None
        );
    }

    #[tokio::test]
    async fn recv_wakes_promptly_on_send_rather_than_polling() {
        let (tx, rx) = bounded::<u32>(4);

        let recv_task = tokio::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;
        tx.send(42);

        let result = tokio::time::timeout(Duration::from_millis(200), recv_task)
            .await
            .expect("recv did not wake up after a send")
            .expect("recv task panicked");
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn concurrent_producers_conserve_every_item_sent_or_counted_as_dropped() {
        // Property: nothing vanishes. Every item sent is either received or
        // accounted for in the dropped counter — never both, never neither.
        const PRODUCERS: u32 = 8;
        const PER_PRODUCER: u32 = 200;
        let (tx, rx) = bounded::<(u32, u32)>(16);

        let mut handles = Vec::new();
        for p in 0..PRODUCERS {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..PER_PRODUCER {
                    tx.send((p, i));
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let dropped = tx.metrics().dropped;
        drop(tx);

        let mut received = 0u64;
        while let Some(_item) = rx.recv().await {
            received += 1;
        }

        let total_sent = u64::from(PRODUCERS) * u64::from(PER_PRODUCER);
        assert_eq!(
            received + dropped,
            total_sent,
            "received ({received}) + dropped ({dropped}) must equal total sent ({total_sent})"
        );
    }

    #[test]
    #[should_panic(expected = "capacity must be greater than zero")]
    fn zero_capacity_is_rejected() {
        let _ = bounded::<u32>(0);
    }
}
