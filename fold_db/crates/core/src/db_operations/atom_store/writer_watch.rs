//! Does another writer share the node with a background job?
//!
//! A background job that deletes in batches (the retention janitor, the blob dwell
//! pass) seals the write queue with each batch, so every writer that arrives waits
//! until the batch is durable. The job rests between batches for a share of the
//! time it kept writers waiting. When no other writer is there, the rest protects
//! nobody, and the job goes on at once.
//!
//! "No other writer" means that no other task has opened a write scope for a
//! quiet period (60 s by default, [`QUIET_SECS_ENV`]). The watch remembers the last
//! write scope of another task. A writer that writes less often than the pass
//! would otherwise find the pass flat out between two of its writes, and nearly
//! every write of it would meet a sealed batch (a flush for each write). So one
//! write scope of another task keeps the job in the duty-paced mode for the whole
//! quiet period. A long rest ends early only when the quiet period has passed.
//!
//! The watch counts the write scopes that the process opens
//! (`storage::disk_pack::transaction::blocked`). The scopes of the job itself are
//! taken off, so only another task makes the node "busy". A task of the node
//! itself (a sampler, a flush of telemetry) counts as another writer, because a
//! batch of the job makes it wait as well. Only write scopes are seen: a caller
//! that takes the write gate without a scope, and every read, are not.

use crate::storage::disk_pack::transaction::blocked::write_scopes;
use std::time::{Duration, Instant};

/// Environment name: seconds without a write scope of another task after which a
/// background job runs flat out. Zero means that only the step that saw the write
/// counts as busy.
pub const QUIET_SECS_ENV: &str = "LASTDB_MAINTENANCE_QUIET_SECS";
/// Default quiet period: 60 seconds.
pub const DEFAULT_QUIET_SECS: u64 = 60;

/// What a background job knows about the other writers of the node.
#[derive(Debug)]
pub struct WriterWatch {
    mark: u64,
    last_other: Option<Instant>,
    quiet: Duration,
}

impl WriterWatch {
    /// Measure only this task's native write scopes, including any sealed flush wait.
    pub async fn measure<F: std::future::Future>(work: F) -> (F::Output, Duration, u64) {
        let (output, cost) = crate::storage::disk_pack::transaction::blocked::measure(work).await;
        (output, cost.blocked, cost.scopes)
    }

    /// Start a watch at the current count. The quiet period is read from the
    /// environment.
    #[must_use]
    pub fn new() -> Self {
        Self {
            mark: write_scopes(),
            last_other: None,
            quiet: Duration::from_secs(env_flag::var_or(QUIET_SECS_ENV, DEFAULT_QUIET_SECS)),
        }
    }

    /// Look at the count of write scopes: a scope of another task since the last
    /// look is a writer seen now. `own` is the number of scopes the job opened in
    /// that time. Returns true when one was seen.
    fn look(&mut self, own: u64) -> bool {
        let now = write_scopes();
        let others = now.saturating_sub(self.mark).saturating_sub(own);
        self.mark = now;
        if others > 0 {
            self.last_other = Some(Instant::now());
        }
        others > 0
    }

    /// True when another task opened a write scope within the quiet period (the
    /// job rests the full time), false when none did (the job rests the minimum).
    /// `own` is the number of scopes that the job opened since the last call.
    pub fn writer_present(&mut self, own: u64) -> bool {
        let seen = self.look(own);
        seen || self.last_other.is_some_and(|at| at.elapsed() < self.quiet)
    }

    /// True when a long rest may end: no other task opened a write scope within the
    /// quiet period. The job opens no write scope while it rests.
    pub fn rest_may_end(&mut self) -> bool {
        !self.writer_present(0)
    }
}

impl Default for WriterWatch {
    fn default() -> Self {
        Self::new()
    }
}
