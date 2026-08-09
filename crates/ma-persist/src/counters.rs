//! Observability for the archive writer.
//!
//! `ma_persist::run` deliberately swallows write errors — the live book is the
//! primary product and a process that dies because S3 returned a 503 trades a
//! recoverable gap in the archive for a total outage. But swallowed and
//! *silent* are different failures: without these counters, a bucket rejecting
//! every write is indistinguishable from a healthy process on `/metrics`,
//! which is the project's own "a silent drop policy is a bug" rule broken in
//! the one place it wasn't enforced.
//!
//! One struct per process, not per stream: there is exactly one writer task,
//! and its files each hold one symbol already — the key names the partition,
//! so a per-stream label here would just restate the filesystem.
//!
//! These are *the* tallies, not a copy of tallies the writer keeps privately.
//! A safety control that exists twice will disagree with itself — the same
//! argument that made `ma-aws` one crate — so [`EventWriter`] holds an `Arc`
//! of this and owns no shadow counts.
//!
//! [`EventWriter`]: crate::EventWriter

use std::sync::atomic::{AtomicU64, Ordering};

/// Live counters for the one archive writer in a process.
///
/// Relaxed ordering throughout, for the reason `ma_pipeline::metrics` gives:
/// no counter here guards access to other memory. They are read for display,
/// never to make a decision.
#[derive(Debug, Default)]
pub struct ArchiveCounters {
    rows_written: AtomicU64,
    files_written: AtomicU64,
    bytes_written: AtomicU64,
    write_failures: AtomicU64,
    open_files: AtomicU64,
}

impl ArchiveCounters {
    /// One finished file reached the store.
    pub fn record_file(&self, rows: u64, bytes: u64) {
        self.files_written.fetch_add(1, Ordering::Relaxed);
        self.rows_written.fetch_add(rows, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// One write the archive could not complete: a failed append, or a file
    /// that could not be finished and uploaded. The event or file is gone —
    /// this counter is what keeps that from being silent.
    pub fn record_failure(&self) {
        self.write_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Gauge, not counter: how many Parquet files are currently open. Each one
    /// is an unwritten footer, i.e. data at risk until the next roll.
    pub fn set_open_files(&self, n: u64) {
        self.open_files.store(n, Ordering::Relaxed);
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }

    pub fn files_written(&self) -> u64 {
        self.files_written.load(Ordering::Relaxed)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    pub fn open_files(&self) -> u64 {
        self.open_files.load(Ordering::Relaxed)
    }
}
