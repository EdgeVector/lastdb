//! Ordered logical stream of accepted database writes: the cloud log.
//!
//! The byte-backup mutation journal retains encrypted run files and can
//! acknowledge a commit that never entered it. This stream records one
//! sealed logical entry per accepted user write before the caller ack.
//!
//! The code calls it the spool, and the status fields keep that name
//! (`spool_head`, `spool_confirmed_through`). It is the cloud log: it exists
//! only to send this device's signed stream to the cloud. The local node
//! writes its entries and never reads them back for a check. The sender's one
//! read of a finished entry is the upload itself. Only logs from other devices
//! are opened, when they arrive.

mod author;
mod body;
mod capture;
mod disk;

pub use author::{CaptureAuthor, LocalEntry, SignedLocal};
pub(crate) use capture::{capture_enabled, stage_records, stage_schema_drop, stage_schema_put};
pub use disk::upload::{UploadIdentity, UploadPosition};
pub(in crate::storage) use disk::{Change, Log, Note};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::disk_pack::{run_blocking, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};

/// The most notes one commit may put in the spool. See `CONTROL_MAX`.
pub(crate) const NOTES_MAX: usize = 1024;

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
    /// The first snapshot has not covered the writes from before this stream.
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
    /// Writes from before the stream are not in this log. No entry may be read for
    /// upload until a first snapshot covers them.
    pub snapshot_required: bool,
    /// A hole is open. Reads and new commits refuse.
    pub gap: bool,
}

/// What the spool of a home holds, for the status of the node.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct SpoolReport {
    /// Last captured write. Zero means none.
    pub head: u64,
    /// Last write the relay confirmed.
    pub acked: u64,
    /// The first snapshot of a home with older data is still to come.
    pub snapshot_required: bool,
    /// The spool is blocked until the node opens it again.
    pub gap: bool,
    /// Bytes of the entries still to send, with their sealed files.
    pub retained_bytes: u64,
    /// Id records of recent writes. The write path does not read them.
    pub id_records: u64,
    /// Last sequence number of this device's stream the relay confirmed.
    pub uploaded_writer_seq: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::storage) struct Frontiers {
    pub total: u64,
    pub main: u64,
}

/// One entry of the spool: a signed entry of the device stream, ready to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Sequence, starting at 1. It is the sequence number of the signed entry.
    pub sequence: u64,
    /// The signed entry, as the bytes the relay stores.
    pub signed: Vec<u8>,
    /// The sealed file that the entry names, when it names one.
    pub file: Option<EntryFile>,
}

/// The sealed file of a spool entry. The bytes stay on disk until they are sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryFile {
    /// Where the sealed bytes are.
    pub path: PathBuf,
    /// Their SHA-256, 64 lowercase hex characters. The relay names the blob by it.
    pub sha256: String,
    /// Their length.
    pub bytes: u64,
}

/// One exclusive page of the sealed stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    /// Last sequence in this page, or `after` when the page is empty.
    pub through: u64,
    /// Entries with sequence `after + 1` through `through`.
    pub items: Vec<Entry>,
}

/// Whether the home already captured its writes: it has the folder of sealed spool files, or the
/// spool folder of an older build (which the open then refuses with its upgrade step).
pub fn has_spool(data_dir: &Path) -> StorageResult<bool> {
    let run = data_dir.join(crate::storage::disk_pack::RUN_DIR);
    for name in [disk::FILES_DIR, disk::OLD_DIR] {
        match run.join(name).symlink_metadata() {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

pub(in crate::storage) fn storage(error: Error) -> StorageError {
    match error {
        Error::Spool => StorageError::WriteResourceLimit {
            detail: "accepted stream spool is full".into(),
        },
        Error::Conflict => {
            StorageError::InvalidOperation("mutation id was already accepted into the spool".into())
        }
        Error::Gap => StorageError::BackendError("accepted stream has a gap".into()),
        Error::Snapshot => {
            StorageError::InvalidOperation("accepted stream still needs its first snapshot".into())
        }
        Error::Unavailable | Error::Ahead | Error::Behind | Error::Limit => {
            StorageError::BackendError("accepted stream is unavailable".into())
        }
    }
}

impl PackRuntime {
    /// Whether this store records accepted writes for cloud sync.
    pub fn captures_accepted(&self) -> bool {
        self.accepted.is_some()
    }

    /// Give the store the author of its captured writes, once.
    pub fn name_capture_author(&self, author: Arc<dyn CaptureAuthor>) -> StorageResult<()> {
        let chain = crate::storage::format::metadata::chain_key(author.device());
        self.capture_author
            .set(author)
            .map_err(|_| StorageError::invalid("the store already has an author for its writes"))?;
        match &self.accepted {
            Some(log) => log.start_at_chain(&self.physical, &chain).map_err(storage),
            None => Ok(()),
        }
    }

    pub(crate) fn capture_author(&self) -> Option<&Arc<dyn CaptureAuthor>> {
        self.capture_author.get()
    }

    /// Apply already-published mutations to this store without recapturing them.
    pub async fn replay_accepted<T>(
        self: &std::sync::Arc<Self>,
        replay: impl std::future::Future<Output = T>,
    ) -> T {
        capture::REPLAY_STORE
            .scope(std::sync::Arc::clone(self), replay)
            .await
    }

    /// Cursor of this home. A home without capture has an empty cursor and does not block writes.
    pub fn accepted_status(&self) -> Status {
        self.accepted.as_ref().map_or(
            Status {
                head: 0,
                acked: 0,
                opened_frontier: 0,
                snapshot_required: false,
                gap: false,
            },
            Log::status,
        )
    }

    /// What the spool holds. `None` when this home captures nothing.
    pub fn accepted_report(&self) -> Option<SpoolReport> {
        self.accepted.as_ref().and_then(|log| log.report().ok())
    }

    /// Sealed entries with sequence greater than `after`, at most `limit`, for upload.
    ///
    /// A commit acknowledges a write after an ordered write, which does not force the drive's
    /// cache to the medium. Before any entry leaves this machine the whole drive is flushed, so
    /// the relay never holds a write that a power loss could still take from this home. A flush
    /// that already covered every entry up to the head is not repeated: the next pages of one
    /// turn, and a turn with no new write, cost no flush.
    pub fn accepted_prefix(&self, after: u64, limit: u32) -> Result<Page, Error> {
        let Some(log) = &self.accepted else {
            return idle_prefix(after, limit);
        };
        // Every entry up to this head was committed before the flush starts, so the flush
        // covers it. An entry committed during the flush waits for the next page.
        let flushed = log.status().head;
        if flushed > log.flushed_through() {
            laststore::flush_drive(self.physical.path()).map_err(|_| Error::Unavailable)?;
            log.note_flushed(flushed);
        }
        log.prefix(&self.physical, after, limit, flushed)
    }

    /// Read the upload checkpoint and finish an interrupted local acknowledgement.
    pub fn accepted_upload_position(
        &self,
        identity: &UploadIdentity,
    ) -> Result<Option<UploadPosition>, Error> {
        self.accepted
            .as_ref()
            .ok_or(Error::Unavailable)?
            .upload_position(identity)
    }

    /// Record the upload result and retire the entries it covers, in one commit.
    pub async fn confirm_accepted_upload(
        self: &Arc<Self>,
        identity: &UploadIdentity,
        after: u64,
        position: UploadPosition,
    ) -> Result<(), Error> {
        let identity = identity.clone();
        self.change_spool(move |log, physical| log.plan_ack(physical, &identity, after, position))
            .await
    }

    /// Record that a snapshot covers the native frontier from before the stream.
    pub async fn note_accepted_snapshot(
        self: &Arc<Self>,
        covered_frontier: u64,
    ) -> Result<(), Error> {
        if self.accepted.is_none() {
            return Ok(());
        }
        self.change_spool(move |log, _| log.note_snapshot(covered_frontier))
            .await
    }

    /// Empty the spool of a restored home. Its files carry the spool of the device that took the
    /// snapshot.
    pub(in crate::storage) async fn reset_accepted(
        self: &Arc<Self>,
        frontier: u64,
    ) -> Result<(), Error> {
        if self.accepted.is_none() {
            return Ok(());
        }
        self.change_spool(move |log, physical| log.reset(physical, frontier))
            .await
    }

    /// Plan a spool change and commit it under the write gate.
    async fn change_spool(
        self: &Arc<Self>,
        plan: impl FnOnce(&Log, &crate::storage::disk_pack::physical::V2Home) -> Result<Change, Error>
            + Send
            + 'static,
    ) -> Result<(), Error> {
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        let outcome = run_blocking(move || {
            let _gate = gate;
            let Some(log) = &runtime.accepted else {
                return Ok(Err(Error::Unavailable));
            };
            let change = match plan(log, &runtime.physical) {
                Ok(change) => change,
                Err(error) => return Ok(Err(error)),
            };
            log.apply(change, |ops| runtime.commit(ops)).map(Ok)
        })
        .await;
        outcome.unwrap_or(Err(Error::Unavailable))
    }

    /// The spool records that go into the commit of `notes`.
    pub(in crate::storage) fn prepare_accepted(
        &self,
        before: Frontiers,
        notes: &[disk::Note],
    ) -> StorageResult<Vec<laststore::disk_run::home::HomeMutation>> {
        let Some(log) = &self.accepted else {
            return Ok(Vec::new());
        };
        log.prepare(before, notes).map_err(storage)
    }

    pub(in crate::storage) fn publish_accepted(&self) -> StorageResult<()> {
        let Some(log) = &self.accepted else {
            return Ok(());
        };
        log.publish().map_err(storage)
    }

    /// The commit of [`Self::prepare_accepted`] failed. Its spool records are durable exactly
    /// when the main home moved past `before`.
    pub(in crate::storage) fn fail_accepted(&self, before: Frontiers) {
        let Some(log) = &self.accepted else {
            return;
        };
        let after = self.physical.current_root().frontiers();
        if after.main == before.main {
            let _ = log.abort();
        } else if after.main > before.main {
            let _ = log.publish();
        } else {
            let _ = log.force_gap();
        }
    }

    pub(in crate::storage) fn mark_accepted_born(&self, frontiers: Frontiers) -> StorageResult<()> {
        let Some(log) = &self.accepted else {
            return Ok(());
        };
        log.mark_born(frontiers).map_err(storage)
    }
}

fn idle_prefix(after: u64, limit: u32) -> Result<Page, Error> {
    if limit == 0 || limit > disk::MAX_PAGE {
        return Err(Error::Limit);
    }
    if after > 0 {
        return Err(Error::Ahead);
    }
    Ok(Page {
        through: after,
        items: Vec::new(),
    })
}
