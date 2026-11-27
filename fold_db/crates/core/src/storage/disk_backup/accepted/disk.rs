//! The spool, the cloud log: one signed entry per captured write. Its records live in the main
//! home and are written in the same commit as the write they belong to, so a write and its
//! entry become durable together and the spool needs no disk sync of its own. Sealed files stay
//! files, in one folder beside the home (see `files`).
//!
//! The cloud log exists only to send this device's signed stream to the cloud. The node writes
//! it and never reads it back for a check. The one read of a finished entry is the sender's
//! (`prefix`), and that read is the upload. Only logs from other devices are opened on arrival.

mod change;
mod files;
mod inner;
mod store;
mod types;
pub(super) mod upload;

pub(in crate::storage) use types::Note;

use super::{Error, Frontiers, Page, Status};
use crate::storage::disk_pack::physical::V2Home;
use crate::storage::error::StorageResult;
use inner::{fresh, read_page};
use laststore::disk_run::home::HomeMutation;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use types::{id_ok, sha_ok, spool_cap, State};
use upload::Checkpoint;

/// The folder of sealed files, beside the main home's runs.
pub(in crate::storage) const FILES_DIR: &str = "accepted-files";
/// The spool folder of builds before the spool moved into the store.
pub(in crate::storage) const OLD_DIR: &str = "accepted-v1";
pub(super) const MAX_PAGE: u32 = 90;
/// The id records of this many of the newest writes stay. No write path reads them: the id of
/// a captured write is new by construction (see `capture`).
const IDS_KEPT: u64 = 10_000;
/// How many acknowledged entries pass between two trims of the id records.
const TRIM_EVERY: u64 = 2_500;
/// Deletes in one commit of a reset, below the operation limit of a commit.
const CLEAR_PART: usize = 8_192;

const OLD_SPOOL: &str = "this home holds the spool of an older build (data/disk-run/accepted-v1); \
start that build until `lastdb status` shows every write acknowledged, stop it, move \
data/disk-run/accepted-v1 to the Trash, then start this build";

#[derive(Default)]
struct Inner {
    state: Option<State>,
    /// Last entry, or `acked` when the spool holds none.
    head: u64,
    /// Bytes of the entries the spool holds, with their sealed files.
    retained: u64,
    /// The home held data when it opened and has no stream yet.
    history: bool,
    blocked: bool,
    /// The state row in memory differs from the store. The next commit writes it.
    unsaved: bool,
    /// Acknowledged entries since the id records were last trimmed.
    since_trim: u64,
    /// Id records the store holds.
    ids: u64,
    planned: Option<Planned>,
}

/// What a commit in flight adds to the spool.
struct Planned {
    last: u64,
    notes: u64,
    added: u64,
    files: Vec<PathBuf>,
    saves_state: bool,
}

/// A change of the spool that its own commit writes. See [`Log::apply`].
pub(in crate::storage) struct Change {
    ops: Vec<HomeMutation>,
    state: State,
    released: u64,
    acknowledged: u64,
    trimmed: u64,
    did_trim: bool,
    remove: Vec<PathBuf>,
    clears: bool,
}

pub(in crate::storage) struct Log {
    files: PathBuf,
    inner: Mutex<Inner>,
    /// The spool head that a full drive flush of this run of the program covered: every entry
    /// at or below it is on the medium. It starts at zero, so the first upload flushes.
    flushed: AtomicU64,
}

impl Log {
    /// Open the spool of a captured home. Only a home that still has the spool folder of an
    /// older build is refused; any other fault blocks the spool and is reported in its status.
    pub(in crate::storage) fn open(
        physical: &V2Home,
        frontiers: Frontiers,
        user_data: impl FnOnce() -> StorageResult<bool>,
        initializing: bool,
    ) -> StorageResult<Self> {
        let home = physical.path();
        if home.join(OLD_DIR).symlink_metadata().is_ok() {
            return Err(crate::storage::error::StorageError::invalid(OLD_SPOOL));
        }
        let dir = home.join(FILES_DIR);
        let mut inner = Inner::default();
        if inner
            .load(physical, &dir, frontiers, user_data, initializing)
            .is_err()
        {
            inner.blocked = true;
        }
        Ok(Self {
            files: dir,
            inner: Mutex::new(inner),
            flushed: AtomicU64::new(0),
        })
    }

    /// The spool head that an earlier full drive flush covered.
    pub(in crate::storage::disk_backup) fn flushed_through(&self) -> u64 {
        self.flushed.load(Ordering::Acquire)
    }

    /// Record that a full drive flush that began after the commit of entry `head` returned.
    pub(in crate::storage::disk_backup) fn note_flushed(&self, head: u64) {
        self.flushed.fetch_max(head, Ordering::AcqRel);
    }

    pub(in crate::storage::disk_backup) fn status(&self) -> Status {
        self.inner.lock().map_or(
            Status {
                head: 0,
                acked: 0,
                opened_frontier: 0,
                snapshot_required: false,
                gap: true,
            },
            |inner| inner.status(),
        )
    }

    pub(in crate::storage) fn check_healthy(&self) -> Result<(), Error> {
        self.lock()?.refuse_gap()
    }

    /// Whether the spool takes `notes` after the queued, uncommitted writes: no gap, the id and
    /// sha formats, the sequence room, and the spool cap.
    ///
    /// It does not read the id records, and no write path does: the cloud log is not read back
    /// for a check. The id of a captured write is new by construction: a record write carries a
    /// fresh UUIDv4 that the node made, and a schema put or drop names its author millisecond,
    /// which is above that of every earlier entry (see `capture`). The caller compares the ids
    /// with the queued writes.
    pub(in crate::storage) fn check_admissible(
        &self,
        queued_notes: usize,
        queued_bytes: u64,
        notes: &[Note],
    ) -> Result<(), Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        let added_notes = queued_notes
            .checked_add(notes.len())
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(Error::Unavailable)?;
        inner
            .head
            .checked_add(added_notes)
            .ok_or(Error::Unavailable)?;
        let mut batch_bytes = 0u64;
        for note in notes {
            if !id_ok(&note.mutation_id) || !sha_ok(&note.plaintext_sha256) {
                return Err(Error::Conflict);
            }
            batch_bytes = batch_bytes
                .checked_add(note.byte_weight())
                .ok_or(Error::Spool)?;
        }
        let total = inner
            .retained
            .checked_add(queued_bytes)
            .and_then(|s| s.checked_add(batch_bytes))
            .ok_or(Error::Spool)?;
        (total <= spool_cap()).then_some(()).ok_or(Error::Spool)
    }

    /// The operations that put `notes` in the spool, for the commit that writes them. A state
    /// row that changed since the last commit goes with them. Sealed files are written now.
    ///
    /// This runs inside the commit turn and does not read the id records: the cloud log is not
    /// read back for a check, and the id of a captured write is new by construction (see
    /// [`Self::check_admissible`]). `precheck_accepted_queue` compared the ids with every queued,
    /// uncommitted write under both gates. The `seen` set below still refuses a repeat inside one
    /// turn.
    pub(in crate::storage) fn prepare(
        &self,
        before: Frontiers,
        notes: &[Note],
    ) -> Result<Vec<HomeMutation>, Error> {
        let mut inner = self.lock()?;
        // A commit without captured writes adds nothing, and is not refused by a blocked spool:
        // the reset of a restore is such a commit.
        if notes.is_empty() && (inner.blocked || !inner.unsaved) {
            return Ok(Vec::new());
        }
        inner.refuse_gap()?;
        if inner.planned.is_some() {
            return inner.fail(Error::Gap);
        }
        let mut ops = Vec::new();
        let mut planned = Planned {
            last: inner.head,
            notes: notes.len() as u64,
            added: 0,
            files: Vec::new(),
            saves_state: false,
        };
        if !notes.is_empty() {
            let mut seen = std::collections::BTreeSet::new();
            let mut added = 0u64;
            for note in notes {
                if !id_ok(&note.mutation_id)
                    || !sha_ok(&note.plaintext_sha256)
                    || !seen.insert(note.mutation_id.as_str())
                {
                    return Err(Error::Conflict);
                }
                added = added.checked_add(note.byte_weight()).ok_or(Error::Spool)?;
            }
            if inner
                .retained
                .checked_add(added)
                .is_none_or(|total| total > spool_cap())
            {
                return Err(Error::Spool);
            }
            if inner.state.is_none() {
                let history = inner.history;
                inner.state = Some(fresh(before.total, history));
                inner.unsaved = true;
            }
            let written = inner.plan_notes(&self.files, notes, &mut planned, &mut ops);
            if let Err(error) = written {
                for path in &planned.files {
                    let _ = files::remove(path);
                }
                return Err(error);
            }
            planned.added = added;
        }
        if inner.unsaved {
            let state = inner.state.as_ref().ok_or(Error::Gap)?;
            ops.push(store::state_op(state)?);
            planned.saves_state = true;
        }
        if !ops.is_empty() {
            inner.planned = Some(planned);
        }
        Ok(ops)
    }

    /// The commit of [`Self::prepare`] is durable.
    pub(in crate::storage) fn publish(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        let Some(planned) = inner.planned.take() else {
            return Ok(());
        };
        inner.head = planned.last;
        inner.retained = inner.retained.saturating_add(planned.added);
        inner.ids = inner.ids.saturating_add(planned.notes);
        if planned.saves_state {
            inner.unsaved = false;
            inner.history = false;
        }
        Ok(())
    }

    /// The commit of [`Self::prepare`] did not happen. Its sealed files are removed.
    pub(in crate::storage) fn abort(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        if let Some(planned) = inner.planned.take() {
            for path in &planned.files {
                files::remove(path)?;
            }
        }
        Ok(())
    }

    pub(super) fn force_gap(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.planned = None;
        inner.fail(Error::Gap)
    }

    /// Entries after `after`, at most `limit`, none past `flushed`: the head when the drive was
    /// last flushed to the medium.
    pub(super) fn prefix(
        &self,
        physical: &V2Home,
        after: u64,
        limit: u32,
        flushed: u64,
    ) -> Result<Page, Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        if limit == 0 || limit > MAX_PAGE {
            return Err(Error::Limit);
        }
        let view = inner.status();
        if after > view.head {
            return Err(Error::Ahead);
        }
        if after < view.acked {
            return Err(Error::Behind);
        }
        let last = after
            .saturating_add(u64::from(limit))
            .min(view.head)
            .min(flushed.max(after));
        match read_page(physical, &self.files, after, last) {
            Ok(items) => Ok(Page {
                through: items.last().map_or(after, |entry| entry.sequence),
                items,
            }),
            Err(Error::Gap) => inner.fail(Error::Gap),
            Err(error) => Err(error),
        }
    }

    /// What the spool holds, for the status of the node.
    pub(super) fn report(&self) -> Result<super::SpoolReport, Error> {
        let inner = self.lock()?;
        let status = inner.status();
        Ok(super::SpoolReport {
            head: status.head,
            acked: status.acked,
            snapshot_required: status.snapshot_required,
            gap: status.gap,
            retained_bytes: inner.retained,
            id_records: inner.ids,
            uploaded_writer_seq: inner
                .state
                .as_ref()
                .and_then(|state| state.upload.as_ref())
                .map(Checkpoint::writer_seq),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, Error> {
        self.inner.lock().map_err(|_| Error::Unavailable)
    }
}
