//! The safety net: find a bucket that holds entries and has no directory row.
//!
//! A reader lists the directory and walks the buckets in it. An entry in a
//! bucket without a directory row is never listed, so its work never runs: for
//! the retention queue, a kept conflict record, its references and its loser
//! atom stay for ever. A writer adds the entry and the row in one batch, and a
//! retirement deletes a row only while its token is unchanged, so no code of the
//! queue makes such a bucket. The net is for the case that something else did:
//! a delete that bypassed the queue, a partial restore, or an error in the
//! verdict "the bucket is empty".
//!
//! The net needs no scan. A queue puts an entry in the slice of its expiry,
//! and an expiry lies between a little before now (due entries that wait) and
//! one lifetime after now (entries that were just written). So the slices that
//! can hold a live entry are listed by arithmetic: from one lifetime and one
//! slice before now to one lifetime and one slice after it, about twice the
//! slices of a lifetime. For each slice the net reads the directory row by key.
//! Only a slice with no row needs a second read: one bounded range read of its
//! hash that stops at the first live key. A slice that holds a live key and has
//! no row is registered again with a new directory row, in one row commit.
//!
//! The directory covers every slice length that was ever in use, because a row
//! names the start of its bucket. The net covers the slicing of the current
//! lifetime only: after a change of the lifetime, the slices of the old
//! length do not line up with the new ones.

use super::keys::TimeQueue;
use crate::db_operations::atom_store::counter::RowChanges;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Range reads that a probe of one slice may make before it gives up. A slice
/// with a very long run of deleted keys needs more than one.
const MAX_PROBE_STEPS: usize = 256;

/// What one chunk of a sweep found.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct NetChunk {
    /// Slices that were probed.
    pub(crate) probed: usize,
    /// Buckets that held a live entry and had no directory row.
    pub(crate) unlisted: Vec<u64>,
    /// Slices whose probe gave up.
    pub(crate) unknown: usize,
}

/// The clock and the remaining slices of one safety-net sweep.
///
/// `set_every` always restarts the clock. `set_every_when_changed` keeps the
/// clock when the interval is the same. The caller supplies the slices when a
/// sweep starts, and the caller mends each chunk.
#[derive(Debug)]
pub(crate) struct NetSchedule {
    every: Option<Duration>,
    next_at: Instant,
    todo: VecDeque<u64>,
}

impl NetSchedule {
    pub(crate) fn new() -> Self {
        Self {
            every: None,
            next_at: Instant::now(),
            todo: VecDeque::new(),
        }
    }

    /// Record `every` and make the next sweep due now.
    pub(crate) fn set_every(&mut self, every: Option<Duration>) {
        self.every = every;
        self.next_at = Instant::now();
    }

    /// Record `every` only when it differs. The same interval keeps this clock.
    pub(crate) fn set_every_when_changed(&mut self, every: Option<Duration>) {
        if self.every != every {
            self.set_every(every);
        }
    }

    /// Slices for the next chunk, or `None` when no sweep is due.
    ///
    /// A new sweep stores `slices`, then runs `started`, then returns the first
    /// chunk. That chunk leaves this schedule before the caller mends it.
    /// [`Self::abandon`] drops whatever remains after a mend error.
    pub(crate) fn next_slices<Slices, Started>(
        &mut self,
        chunk_slices: usize,
        slices: Slices,
        started: Started,
    ) -> Option<Vec<u64>>
    where
        Slices: FnOnce() -> Vec<u64>,
        Started: FnOnce(),
    {
        if self.todo.is_empty() {
            let every = self.every?;
            if Instant::now() < self.next_at {
                return None;
            }
            self.next_at = Instant::now() + every;
            self.todo.extend(slices());
            started();
        }
        let take = self.todo.len().min(chunk_slices);
        Some(self.todo.drain(..take).collect())
    }

    /// Drop the rest of the current sweep.
    ///
    /// The next sweep waits for the clock, so a failed mend does not read again
    /// at once.
    pub(crate) fn abandon(&mut self) {
        self.todo.clear();
    }
}

impl TimeQueue {
    /// The slices that a sweep probes at `now`, for entries that live
    /// `lifetime_nanos`: one lifetime and one slice before now, to one lifetime
    /// and one slice after it.
    pub(crate) fn net_slices(&self, now: u64, lifetime_nanos: u64) -> Vec<u64> {
        let margin = lifetime_nanos.saturating_add(self.slice_nanos());
        self.slices(now.saturating_sub(margin), now.saturating_add(margin))
    }

    /// Probe `slices`, and register again every bucket that holds a live entry
    /// and has no directory row. `stamp` names the repair in the new token.
    /// One batch read of the rows, one bounded range read for each slice
    /// without a row, and one batch write when some bucket needs it.
    pub(crate) async fn mend_slices(
        &self,
        store: &AtomStore,
        slices: &[u64],
        stamp: u64,
    ) -> Result<NetChunk, SchemaError> {
        let chunk = self.find_unlisted(store, slices).await?;
        if !chunk.unlisted.is_empty() {
            self.register_buckets(store, &chunk.unlisted, stamp).await?;
        }
        Ok(chunk)
    }

    /// Probe `slices` and return the buckets that hold a live key and have no
    /// directory row (in `unlisted`, which the caller may register). It writes
    /// nothing; `verify-refs` and `recount` use it too.
    pub(crate) async fn find_unlisted(
        &self,
        store: &AtomStore,
        slices: &[u64],
    ) -> Result<NetChunk, SchemaError> {
        let mut chunk = NetChunk {
            probed: slices.len(),
            ..NetChunk::default()
        };
        if slices.is_empty() {
            return Ok(chunk);
        }
        let keys: Vec<Vec<u8>> = slices
            .iter()
            .map(|bucket| self.directory_key(*bucket).into_bytes())
            .collect();
        let rows = store
            .raw()
            .inner()
            .get_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read bucket rows: {error}")))?;
        for (bucket, row) in slices.iter().zip(rows) {
            if row.is_some() {
                continue;
            }
            match self.holds_live_key(store, *bucket).await? {
                Some(true) => chunk.unlisted.push(*bucket),
                Some(false) => {}
                None => chunk.unknown += 1,
            }
        }
        Ok(chunk)
    }

    /// True when the hash of `bucket` holds a live key. `None` when the probe
    /// gave up. The walk is a bounded range walk, so deleted keys cannot fail
    /// it; it stops at the first live key.
    async fn holds_live_key(
        &self,
        store: &AtomStore,
        bucket: u64,
    ) -> Result<Option<bool>, SchemaError> {
        let (start, end) = self.bucket_range(bucket);
        let mut walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), 1);
        for _ in 0..MAX_PROBE_STEPS {
            if walk.done() {
                return Ok(Some(false));
            }
            if !store.walk_step(&mut walk).await?.rows.is_empty() {
                return Ok(Some(true));
            }
        }
        Ok(None)
    }

    /// Write the directory rows of `buckets`, with a new token, in one row
    /// commit. A bucket that a writer registered in the meantime gets the
    /// token of this repair, which differs from every token of a writer, so a
    /// retirement that read the old token fails and starts the bucket again.
    pub(crate) async fn register_buckets(
        &self,
        store: &AtomStore,
        buckets: &[u64],
        stamp: u64,
    ) -> Result<(), SchemaError> {
        let rows = buckets
            .iter()
            .map(|bucket| self.directory_row(*bucket, &format!("net:{stamp:020}")))
            .collect();
        store
            .in_background_scope(async {
                store
                    .commit_rows(RowChanges {
                        rows,
                        ..RowChanges::default()
                    })
                    .await
                    .map(|_| ())
            })
            .await
    }
}
