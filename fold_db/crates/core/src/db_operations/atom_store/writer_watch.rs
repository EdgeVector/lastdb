//! Does another writer share the node with a background job?
//!
//! A background job that deletes in batches (the retention janitor, the blob dwell
//! pass) seals the write queue with each batch, so every writer that arrives waits
//! until the batch is durable. The job rests between batches for a share of the
//! time it kept writers waiting. When no other writer opens a write scope while the
//! job works and rests, the rest protects nobody, and the job goes on at once. The
//! first write scope of another task ends that: the next rest is the full one.
//!
//! The watch counts the write scopes that the process opens
//! (`storage::disk_pack::transaction::blocked`). The scopes of the job itself are
//! taken off, so only another task makes the node "busy". A task of the node
//! itself (a sampler, a flush of telemetry) counts as another writer, because a
//! batch of the job makes it wait as well.
//!
//! A full rest can be long (up to 30 s). When the writers go away during it, the
//! rest ends early: [`QuietRest`] sees that no other task opened a write scope for
//! [`QUIET_WAKE`], and the job goes on.

use crate::storage::disk_pack::transaction::blocked::write_scopes;
use std::time::{Duration, Instant};

/// A rest ends early when no other task has opened a write scope for this long.
pub const QUIET_WAKE: Duration = Duration::from_secs(2);

/// The count of write scopes at the last decision of a job.
#[derive(Debug)]
pub struct WriterWatch {
    mark: u64,
}

impl WriterWatch {
    /// Start a watch at the current count.
    #[must_use]
    pub fn new() -> Self {
        Self {
            mark: write_scopes(),
        }
    }

    /// True when a task other than the job opened a write scope since the last
    /// call (or since the watch began). `own` is the number of scopes that the job
    /// opened in that time, from the cost of its step.
    pub fn others_wrote(&mut self, own: u64) -> bool {
        let now = write_scopes();
        let others = now.saturating_sub(self.mark).saturating_sub(own);
        self.mark = now;
        others > 0
    }
}

impl Default for WriterWatch {
    fn default() -> Self {
        Self::new()
    }
}

/// The watch of one rest: has any other task opened a write scope lately? The job
/// calls [`QuietRest::quiet`] between the slices of its sleep.
#[derive(Debug)]
pub struct QuietRest {
    mark: u64,
    since: Instant,
}

impl QuietRest {
    /// Start the watch of a rest that begins now.
    #[must_use]
    pub fn begin() -> Self {
        Self {
            mark: write_scopes(),
            since: Instant::now(),
        }
    }

    /// True when no write scope was opened for [`QUIET_WAKE`]. The job does not
    /// open one during its rest, so every scope counts.
    pub fn quiet(&mut self) -> bool {
        let now = write_scopes();
        if now != self.mark {
            self.mark = now;
            self.since = Instant::now();
        }
        self.since.elapsed() >= QUIET_WAKE
    }
}
