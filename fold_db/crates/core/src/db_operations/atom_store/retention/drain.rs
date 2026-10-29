//! The drain of an erased slot.
//!
//! An erase releases a bounded number of records in its own batch. For a slot
//! that still holds records it writes a drain marker, due at once, in the queue.
//! The janitor finds the marker and keeps a [`PendingDrain`] for it, up to
//! [`MAX_PENDING_DRAINS`]. Each drain batch reads the slot directories with the
//! cursors of the drains (a range walk that moves over the deleted keys of the
//! records it released), releases the records it found, and moves the cursors.
//! The batch that reaches the end of a slot directory deletes its marker. A drain
//! never reads from the start of its slot again, so a slot of any size drains in
//! batches of a fixed size, and many small slots share batches.

use super::entry::DrainMarker;
use super::keys;
use super::page::RELEASE_BATCH_RECORDS;
use crate::db_operations::atom_store::{AtomStore, RangeWalk};
use crate::schema::SchemaError;
use std::collections::VecDeque;

/// Drains the janitor holds in memory, at most. A marker that finds no room
/// stays in the queue, and the janitor reads it again later.
pub(super) const MAX_PENDING_DRAINS: usize = 1_024;

/// Drains one batch touches, at most. A drain that finds no record still costs a
/// range read and a marker delete, so the number of drains bounds the batch
/// beside the number of records.
const MAX_DRAINS_PER_BATCH: usize = 64;

/// A slot drain that the janitor has started.
#[derive(Debug)]
pub(crate) struct PendingDrain {
    pub(super) marker: DrainMarker,
    /// The bucket of the queue that holds the marker.
    pub(super) bucket: Option<u64>,
    /// The cursor: the directory of the slot, from the next key not read.
    walk: RangeWalk,
}

impl PendingDrain {
    pub(super) fn new(marker: DrainMarker, bucket: Option<u64>) -> Self {
        let (start, end) = keys::prefix_range(&marker.dir_prefix);
        let walk = RangeWalk::new(start.as_bytes(), end.as_bytes(), RELEASE_BATCH_RECORDS);
        Self {
            marker,
            bucket,
            walk,
        }
    }
}

/// What one drain batch did.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DrainOutcome {
    pub(crate) released: u64,
    pub(crate) skipped: u64,
    /// Drains whose slot directory the batch read to its end. It deleted their
    /// markers.
    pub(crate) finished: u64,
}

impl AtomStore {
    /// One drain batch: take the records of the pending drains in order, up to
    /// [`RELEASE_BATCH_RECORDS`] records and [`MAX_DRAINS_PER_BATCH`] drains,
    /// and release them in one native write scope.
    /// A slot with one record does not cost a batch of its own: the batch goes
    /// on to the next drain until it is full. Only records that were written at
    /// or before the erase of their slot count (`created <= erased_at`): a
    /// record of a later generation of the same key stays. A drain that the
    /// batch read to its end gives its marker to the delete of the batch.
    ///
    /// The cursors move after the batch is written. A read or write that fails
    /// leaves them where they were, and the next batch tries the same keys
    /// again. A drain whose read fails goes to the back of the line, and so does
    /// the first drain when the write fails, so one bad slot cannot hold back
    /// the others.
    pub(crate) async fn drain_batch(
        &self,
        pending: &mut VecDeque<PendingDrain>,
    ) -> Result<DrainOutcome, SchemaError> {
        let mut found = Vec::new();
        let mut markers = Vec::new();
        let mut moved: Vec<(usize, RangeWalk)> = Vec::new();
        let mut finished: Vec<usize> = Vec::new();
        let mut failed: Option<(usize, SchemaError)> = None;
        'drains: for (index, drain) in pending.iter().enumerate().take(MAX_DRAINS_PER_BATCH) {
            if found.len() >= RELEASE_BATCH_RECORDS {
                break;
            }
            let marker = &drain.marker;
            let mut walk = drain.walk.clone();
            while found.len() < RELEASE_BATCH_RECORDS && !walk.done() {
                walk.set_limit(RELEASE_BATCH_RECORDS - found.len());
                let step = match self.walk_step(&mut walk).await {
                    Ok(step) => step,
                    Err(error) => {
                        failed = Some((index, error));
                        break 'drains;
                    }
                };
                for (key, value) in &step.rows {
                    if let Some(entry) = self.slot_entry(key, value, &marker.slot, marker.erased_at)
                    {
                        if !marker.preserve_archives
                            || !super::super::atom_history::is_archive(&entry.history)
                        {
                            found.push(entry);
                        }
                    }
                }
            }
            if walk.done() {
                markers.push(marker.queue.as_bytes().to_vec());
                finished.push(index);
            }
            moved.push((index, walk));
        }
        if let Some((index, error)) = failed {
            if let Some(drain) = pending.remove(index) {
                pending.push_back(drain);
            }
            return Err(error);
        }
        let written = self
            .in_background_scope(async {
                let mut released = 0;
                let skipped = self.commit_releases(found, markers, &mut released).await?;
                Ok((released, skipped))
            })
            .await;
        let (released, skipped) = match written {
            Ok(done) => done,
            Err(error) => {
                pending.rotate_left(1);
                return Err(error);
            }
        };
        for (index, walk) in moved {
            pending[index].walk = walk;
        }
        for index in finished.iter().rev() {
            pending.remove(*index);
        }
        Ok(DrainOutcome {
            released,
            skipped,
            finished: finished.len() as u64,
        })
    }
}
