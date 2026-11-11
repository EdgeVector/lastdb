//! Ordered logical stream of accepted named-database writes.
//!
//! The byte-backup mutation journal retains encrypted run files and can
//! acknowledge a commit that never entered it. This stream records one
//! sealed logical entry per accepted user write before the caller ack.

mod body;
mod capture;
mod disk;
mod io;

pub(crate) use capture::{stage_records, stage_schema_drop, stage_schema_put};
pub(in crate::storage) use disk::Log;

use crate::storage::disk_pack::PackRuntime;
use crate::storage::error::{StorageError, StorageResult};

/// Why a stream read, ack, or snapshot note refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// An entry is missing or unreadable. Do not skip it.
    Gap,
    /// `after` is past the durable head.
    Ahead,
    /// `after` is behind the acknowledged cursor.
    Behind,
    /// The page limit is empty or above 90.
    Limit,
    /// The same mutation id was presented with a different body.
    Conflict,
    /// The durable spool would pass its cap.
    Spool,
    /// A snapshot still has to cover writes from before the stream.
    Snapshot,
    /// The disk call failed. This is not a skipped entry.
    Unavailable,
}

/// Cursor and coverage of one database stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    /// Last durable sequence. Zero means no entry.
    pub head: u64,
    /// Highest acknowledged sequence. Bodies at or below it may be gone.
    pub acked: u64,
    /// Native frontier when the stream started.
    pub opened_frontier: u64,
    /// Writes before the stream are not in this log.
    pub snapshot_required: bool,
    /// A hole is open. Reads and new commits refuse.
    pub gap: bool,
}

/// One sealed logical entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Sequence, starting at 1.
    pub sequence: u64,
    /// Caller mutation id, or the generated schema id.
    pub mutation_id: String,
    /// Content-key envelope of the canonical body.
    pub envelope: Vec<u8>,
    /// Sealed file row when the entry has an external file.
    pub file_ciphertext: Option<Vec<u8>>,
}

/// One exclusive page of the sealed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    /// Last sequence in this page, or `after` when the page is empty.
    pub through: u64,
    /// The same coverage flag as [`Status::snapshot_required`].
    pub snapshot_required: bool,
    /// Entries with sequence `after + 1` through `through`.
    pub items: Vec<Entry>,
}

pub(in crate::storage) fn storage(error: Error) -> StorageError {
    match error {
        Error::Spool => StorageError::WriteResourceLimit {
            detail: "accepted stream spool is full".into(),
        },
        Error::Conflict => {
            StorageError::InvalidOperation("accepted mutation id changed its body".into())
        }
        Error::Gap => StorageError::BackendError("accepted stream has a gap".into()),
        Error::Snapshot => {
            StorageError::InvalidOperation("accepted stream still needs its snapshot".into())
        }
        Error::Unavailable | Error::Ahead | Error::Behind | Error::Limit => {
            StorageError::BackendError("accepted stream is unavailable".into())
        }
    }
}

impl PackRuntime {
    /// Cursor of this home. An empty new home has head 0 and no snapshot flag.
    pub fn accepted_status(&self) -> Status {
        self.accepted.status()
    }

    /// Sealed entries with sequence greater than `after`, at most `limit`.
    pub fn accepted_prefix(&self, after: u64, limit: u32) -> Result<Page, Error> {
        self.accepted.prefix(after, limit)
    }

    /// Record that the consumer published every entry through `through`.
    pub fn acknowledge_accepted(&self, through: u64) -> Result<(), Error> {
        self.accepted.acknowledge(through)
    }

    /// Record that a snapshot covers the native frontier from before the stream.
    pub fn note_accepted_snapshot(&self, covered_frontier: u64) -> Result<(), Error> {
        self.accepted.note_snapshot(covered_frontier)
    }

    pub(in crate::storage) fn prepare_accepted(
        &self,
        frontier_before: u64,
        notes: &[disk::Note],
    ) -> StorageResult<()> {
        self.accepted
            .prepare(frontier_before, notes)
            .map_err(storage)
    }

    pub(in crate::storage) fn publish_accepted(
        &self,
        frontier_before: u64,
        frontier_after: u64,
    ) -> StorageResult<()> {
        self.accepted
            .publish(frontier_before, frontier_after)
            .map_err(storage)
    }

    pub(in crate::storage) fn fail_accepted(&self, frontier_before: u64) {
        let frontier = self.physical.current_root().frontier();
        if frontier == frontier_before {
            let _ = self.accepted.abort();
        } else {
            let _ = self.accepted.force_gap();
        }
    }

    pub(in crate::storage) fn mark_accepted_born(&self, frontier: u64) -> StorageResult<()> {
        self.accepted.mark_born(frontier).map_err(storage)
    }

    pub(in crate::storage) fn arm_accepted_split(&self) -> StorageResult<()> {
        self.accepted.arm_split().map_err(storage)
    }

    pub(in crate::storage) fn note_accepted_split(
        &self,
        frontier_before: u64,
    ) -> StorageResult<()> {
        self.accepted
            .note_split_committed(frontier_before)
            .map_err(storage)
    }
}
