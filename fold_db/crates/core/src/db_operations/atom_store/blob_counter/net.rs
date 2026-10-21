//! The safety net of the dwell queue.
//!
//! A reader of the queue lists the directory and walks the buckets in it. An
//! entry in a bucket that has no directory row is never listed, so its blob is
//! never deleted. A writer adds the entry and the row in one batch, and a
//! retirement deletes a row only while its token is unchanged, so no code of the
//! queue makes such a bucket. The net is for the case that something else did (a
//! delete that bypassed the queue, a partial restore, an error in the verdict
//! "the bucket is empty").
//!
//! The net is the generic one of the time queue (`TimeQueue::net_slices`,
//! `mend_slices`; see `time_queue/net.rs` and `docs/lastdb-time-queue.md`). It
//! needs no scan: it probes the slices that can hold a live entry, by arithmetic,
//! and registers again each bucket that holds a live key and has no directory
//! row. The slices of the blob queue are a few. An entry has the time of its
//! `zero_since` and lives until its dwell is over, so a live entry lies at most
//! one dwell and one slice before now (and none after it). The generic list
//! covers one dwell and one slice on each side of now. With the default dwell the
//! slice is one dwell, one hour, so a sweep probes about five slices.
//!
//! The dwell pass sweeps once at its start and then once in
//! `LASTDB_BLOB_DWELL_NET_SECS` (one hour by default; zero turns the net off).
//! A bucket that is older than one dwell and one slice is out of the net's reach:
//! `lastdb db verify-refs` finds it (`queue_entry_without_directory`) and
//! `lastdb db recount --execute` registers it again.

use super::dwell::DwellCursor;
use super::{blob_dwell_millis, BlobQueue};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction::hold::measure;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Slices that one net chunk probes. A chunk is one batch read and, for each
/// slice without a row, one bounded range read.
const NET_CHUNK_SLICES: usize = 16;

/// The schedule of the net, and the sweep in progress.
#[derive(Debug)]
pub(super) struct BlobNet {
    /// The time between two sweeps. `None` turns the net off.
    every: Option<Duration>,
    /// The next sweep starts at or after this time. The first one is at once.
    next_at: Instant,
    /// Slices of the sweep in progress that are not probed yet.
    todo: VecDeque<u64>,
}

impl BlobNet {
    pub(super) fn new() -> Self {
        Self {
            every: None,
            next_at: Instant::now(),
            todo: VecDeque::new(),
        }
    }
}

/// What one net chunk did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetStep {
    /// Slices the chunk probed.
    pub probed: usize,
    /// Buckets that held a live entry and had no directory row, and are
    /// registered again.
    pub repaired: u64,
    /// How long the chunk held the native write gate (zero when it repaired
    /// nothing).
    pub held: Duration,
}

impl DwellCursor {
    /// Sweep the slices of the queue for buckets without a directory row once in
    /// `every`, and now. `None` turns the net off. A call with the value the
    /// cursor has changes nothing, so a pass may call it each time.
    pub fn set_net_every(&mut self, every: Option<Duration>) {
        if self.net.every != every {
            self.net.every = every;
            self.net.next_at = Instant::now();
        }
    }
}

impl AtomStore {
    /// One chunk of the safety net: probe the next slices of the sweep, and
    /// register again each bucket that holds a live entry and has no directory
    /// row. A new sweep starts when the net is on and its time has come. `None`
    /// means no sweep is due. The registration is a batch of the planner.
    pub async fn blob_dwell_net_step(
        &self,
        cursor: &mut DwellCursor,
    ) -> Result<Option<NetStep>, SchemaError> {
        let (step, held) = measure(self.sweep_blob_chunk(cursor)).await;
        Ok(step?.map(|step| NetStep { held, ..step }))
    }

    /// The work of [`Self::blob_dwell_net_step`], without the measure of the gate
    /// hold.
    async fn sweep_blob_chunk(
        &self,
        cursor: &mut DwellCursor,
    ) -> Result<Option<NetStep>, SchemaError> {
        let queue = self.blob_events.queue();
        let now = BlobQueue::nanos(self.blob_clock_millis());
        if cursor.net.todo.is_empty() {
            let Some(every) = cursor.net.every else {
                return Ok(None);
            };
            if Instant::now() < cursor.net.next_at {
                return Ok(None);
            }
            cursor.net.next_at = Instant::now() + every;
            let dwell = BlobQueue::nanos(blob_dwell_millis());
            cursor
                .net
                .todo
                .extend(queue.layout().net_slices(now, dwell));
            self.blob_events.note_net_sweep();
        }
        let take = cursor.net.todo.len().min(NET_CHUNK_SLICES);
        let slices: Vec<u64> = cursor.net.todo.iter().take(take).copied().collect();
        let chunk = match queue.layout().mend_slices(self, &slices, now).await {
            Ok(chunk) => chunk,
            Err(error) => {
                // Drop the sweep. The next one starts at the next time of the
                // schedule, so a failing net does not read again at once.
                cursor.net.todo.clear();
                return Err(error);
            }
        };
        cursor.net.todo.drain(..take);
        for bucket in &chunk.unlisted {
            tracing::warn!(
                target: "fold_db::blob_dwell",
                bucket,
                "a blob queue bucket held live entries and had no directory row; it is registered again"
            );
        }
        let repaired = chunk.unlisted.len() as u64;
        self.blob_events.note_buckets_repaired(repaired);
        Ok(Some(NetStep {
            probed: chunk.probed,
            repaired,
            held: Duration::ZERO,
        }))
    }
}
