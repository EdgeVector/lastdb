//! The spool: one signed entry per captured write. Its records live in the main home and are
//! written in the same commit as the write they belong to, so a write and its entry become
//! durable together and the spool needs no disk sync of its own. Sealed files stay files, in
//! one folder beside the home (see `files`).

mod files;
mod store;
mod types;
pub(super) mod upload;

pub(in crate::storage) use types::Note;

use super::{Entry, EntryFile, Error, Frontiers, Page, Status};
use crate::storage::disk_pack::physical::V2Home;
use crate::storage::error::StorageResult;
use laststore::disk_run::home::HomeMutation;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use types::{id_ok, sha_ok, spool_cap, Meta, State, STATE_VERSION};
use upload::{Checkpoint, UploadIdentity, UploadPosition};

/// The folder of sealed files, beside the main home's runs.
pub(in crate::storage) const FILES_DIR: &str = "accepted-files";
/// The spool folder of builds before the spool moved into the store.
pub(in crate::storage) const OLD_DIR: &str = "accepted-v1";
pub(super) const MAX_PAGE: u32 = 90;
/// The id records of this many of the newest writes stay. A retry of a write older than that
/// is taken as a new write.
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
    remove: Vec<PathBuf>,
    clears: bool,
}

pub(in crate::storage) struct Log {
    files: PathBuf,
    inner: Mutex<Inner>,
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
        })
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

    pub(in crate::storage) fn check_admissible(
        &self,
        physical: &V2Home,
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
        let ids: Vec<String> = notes.iter().map(|note| note.mutation_id.clone()).collect();
        if store::read_ids(physical, &ids)?.iter().any(Option::is_some) {
            return Err(Error::Conflict);
        }
        let total = inner
            .retained
            .checked_add(queued_bytes)
            .and_then(|s| s.checked_add(batch_bytes))
            .ok_or(Error::Spool)?;
        (total <= spool_cap()).then_some(()).ok_or(Error::Spool)
    }

    pub(super) fn mutation_recorded(&self, physical: &V2Home, id: &str) -> Result<bool, Error> {
        if !id_ok(id) {
            return Ok(false);
        }
        Ok(store::read_ids(physical, &[id.to_owned()])?
            .into_iter()
            .next()
            .flatten()
            .is_some())
    }

    /// The operations that put `notes` in the spool, for the commit that writes them. A state
    /// row that changed since the last commit goes with them. Sealed files are written now.
    pub(in crate::storage) fn prepare(
        &self,
        physical: &V2Home,
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
            let ids: Vec<String> = notes.iter().map(|note| note.mutation_id.clone()).collect();
            if store::read_ids(physical, &ids)?.iter().any(Option::is_some) {
                return Err(Error::Conflict);
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

    pub(super) fn mark_born(&self, frontiers: Frontiers) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        inner.history = false;
        let state = match inner.state.clone() {
            Some(_) if inner.head > 0 => return Ok(()),
            Some(state) => State {
                snapshot_required: false,
                opened_frontier: frontiers.total,
                ..state
            },
            None => fresh(frontiers.total, false),
        };
        inner.state = Some(state);
        inner.unsaved = true;
        Ok(())
    }

    /// A spool that has recorded no write starts at the head of its device's chain: the entries up
    /// to it were sent by the spool that held them before (an older build's, before an upgrade).
    /// The same rule holds for a new home, whose chain is empty. No old spool file is read.
    pub(in crate::storage) fn start_at_chain(
        &self,
        physical: &V2Home,
        chain_key: &str,
    ) -> Result<(), Error> {
        let mut inner = self.lock()?;
        let started = inner
            .state
            .as_ref()
            .is_some_and(|state| state.acked != 0 || state.upload.is_some());
        if inner.blocked || inner.head != 0 || started {
            return Ok(());
        }
        let Some(seq) = store::chain_seq(physical, chain_key)?.filter(|&seq| seq > 0) else {
            return Ok(());
        };
        let opened = inner.state.as_ref().map_or_else(
            || physical.current_root().frontiers().total,
            |state| state.opened_frontier,
        );
        inner.state = Some(State {
            version: STATE_VERSION,
            opened_frontier: opened,
            acked: seq,
            snapshot_required: false,
            upload: None,
        });
        inner.head = seq;
        inner.history = false;
        inner.unsaved = true;
        Ok(())
    }

    /// The change that records that a snapshot covers the writes from before the stream.
    pub(super) fn note_snapshot(&self, covered: u64) -> Result<Change, Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        let state = match inner.state.clone() {
            Some(state) if covered < state.opened_frontier => return Err(Error::Snapshot),
            Some(state) => State {
                snapshot_required: false,
                ..state
            },
            None => fresh(covered, false),
        };
        inner.change(state, Vec::new())
    }

    /// The change that records the files at `covered` as the first snapshot's cut. The caller
    /// holds the write gate, no entry exists yet, and the cut is already durable.
    pub(in crate::storage) fn pin_initial_frontier(&self, covered: u64) -> Result<Change, Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        let state = match &inner.state {
            Some(state) if inner.head == 0 && state.snapshot_required => State {
                opened_frontier: covered,
                ..state.clone()
            },
            None if inner.history => fresh(covered, true),
            _ => return Err(Error::Snapshot),
        };
        inner.change(state, Vec::new())
    }

    /// The change that empties the spool of a restored home: the installed files carry the
    /// spool of the device that took the snapshot.
    pub(in crate::storage) fn reset(
        &self,
        physical: &V2Home,
        frontier: u64,
    ) -> Result<Change, Error> {
        let inner = self.lock()?;
        let ops = store::clear_ops(physical)?;
        let mut change = inner.change(fresh(frontier, false), ops)?;
        change.clears = true;
        Ok(change)
    }

    pub(super) fn upload_position(
        &self,
        identity: &UploadIdentity,
    ) -> Result<Option<UploadPosition>, Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        let Some(checkpoint) = inner.state.as_ref().and_then(|state| state.upload.as_ref()) else {
            return Ok(None);
        };
        let position = checkpoint.position(identity)?;
        let acked = inner.state.as_ref().map_or(0, |state| state.acked);
        if position.spool_through < acked {
            return Ok(None);
        }
        // The position and the acknowledgement are one write, so they cannot differ.
        if position.spool_through > acked {
            return Err(Error::Gap);
        }
        Ok(Some(position))
    }

    /// The change that confirms an upload through `position.spool_through` and drops those
    /// entries.
    pub(super) fn plan_ack(
        &self,
        physical: &V2Home,
        identity: &UploadIdentity,
        after: u64,
        position: UploadPosition,
    ) -> Result<Change, Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        let view = inner.status();
        if after != view.acked || position.spool_through < after {
            return Err(Error::Behind);
        }
        if position.spool_through > view.head || position.writer_seq == u64::MAX {
            return Err(Error::Ahead);
        }
        if view.snapshot_required && position.spool_through > after {
            return Err(Error::Snapshot);
        }
        let mut state = inner.state.clone().ok_or(Error::Gap)?;
        if let Some(previous) = &state.upload {
            let previous = previous.position(identity)?;
            if previous.spool_through > after || previous.writer_seq > position.writer_seq {
                return Err(Error::Conflict);
            }
        }
        let through = position.spool_through;
        let mut ops = Vec::new();
        let mut released = 0u64;
        let mut remove = Vec::new();
        for (meta, signed) in store::read_entries(physical, after + 1, through)? {
            released = released.saturating_add(meta.weight(signed.len()));
            if meta.file_sha256.is_some() {
                remove.push(files::path(&self.files, meta.sequence));
            }
            ops.push(store::delete_entry_op(meta.sequence));
        }
        let moved = through - after;
        let mut trimmed = 0;
        if inner.since_trim.saturating_add(moved) >= TRIM_EVERY && through > IDS_KEPT {
            let trim = store::trim_id_ops(physical, through - IDS_KEPT)?;
            trimmed = trim.len() as u64 / 2;
            ops.extend(trim);
        }
        state.acked = through;
        state.upload = Some(Checkpoint::new(identity, position));
        let mut change = inner.change(state, ops)?;
        change.released = released;
        change.acknowledged = moved;
        change.trimmed = trimmed;
        change.remove = remove;
        Ok(change)
    }

    /// Write `change` with `commit`, under the write gate, then adopt it. Its state row carries
    /// every state change still in memory, so the commit writes no other.
    pub(in crate::storage) fn apply(
        &self,
        change: Change,
        mut commit: impl FnMut(Vec<HomeMutation>) -> StorageResult<()>,
    ) -> StorageResult<()> {
        let mut ops = change.ops;
        // A reset deletes every spool record, more than one commit may hold. Its deletes go
        // first, in parts; the state row goes last. A crash between parts leaves the old state
        // row, and the restore runs the reset again.
        if change.clears {
            while ops.len() > CLEAR_PART {
                let rest = ops.split_off(CLEAR_PART);
                commit(std::mem::replace(&mut ops, rest))?;
            }
        }
        let unsaved = std::mem::replace(&mut self.lock().map_err(super::storage)?.unsaved, false);
        if let Err(error) = commit(ops) {
            if let Ok(mut inner) = self.lock() {
                inner.unsaved |= unsaved;
            }
            return Err(error);
        }
        let mut inner = self.lock().map_err(super::storage)?;
        inner.state = Some(change.state);
        inner.history = false;
        if change.clears {
            inner.head = 0;
            inner.retained = 0;
            inner.since_trim = 0;
            inner.ids = 0;
            inner.blocked = false;
        } else {
            inner.retained = inner.retained.saturating_sub(change.released);
            inner.ids = inner.ids.saturating_sub(change.trimmed);
            inner.since_trim = inner.since_trim.saturating_add(change.acknowledged);
            if inner.since_trim >= TRIM_EVERY {
                inner.since_trim = 0;
            }
            inner.head = inner.head.max(inner.state.as_ref().map_or(0, |s| s.acked));
        }
        let (acked, head) = (inner.state.as_ref().map_or(0, |s| s.acked), inner.head);
        drop(inner);
        // A crash before this removal leaves the files; the next open removes them.
        for path in &change.remove {
            let _ = files::remove(path);
        }
        if change.clears {
            let _ = files::sweep(&self.files, acked, head);
        }
        Ok(())
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

impl Inner {
    fn load(
        &mut self,
        physical: &V2Home,
        dir: &Path,
        frontiers: Frontiers,
        user_data: impl FnOnce() -> StorageResult<bool>,
        initializing: bool,
    ) -> Result<(), Error> {
        files::ensure_dir(dir)?;
        let state = store::read_state(physical)?;
        let entries = store::list_entries(physical)?;
        match state {
            None => {
                if !entries.is_empty() {
                    return Err(Error::Gap);
                }
                self.history = !initializing && user_data().map_err(|_| Error::Unavailable)?;
                if !self.history && !initializing {
                    // A home that captured from its birth and has not written its state row.
                    self.state = Some(fresh(frontiers.total, false));
                    self.unsaved = true;
                }
            }
            Some(state) => {
                let bad = state.version != STATE_VERSION;
                let acked = state.acked;
                self.state = Some(state);
                if bad {
                    return Err(Error::Gap);
                }
                for (offset, (sequence, meta, weight)) in entries.iter().enumerate() {
                    if *sequence != acked + 1 + offset as u64 {
                        return Err(Error::Gap);
                    }
                    if meta.file_sha256.is_some() {
                        files::check(&files::path(dir, *sequence), meta.file_bytes)?;
                    }
                    self.retained = self.retained.saturating_add(*weight);
                }
                self.head = entries.last().map_or(acked, |entry| entry.0);
                self.ids = store::count_ids(physical)?;
            }
        }
        let acked = self.state.as_ref().map_or(0, |state| state.acked);
        files::sweep(dir, acked, self.head)?;
        // Trim the id records at the first acknowledgement after a start.
        self.since_trim = TRIM_EVERY;
        Ok(())
    }

    fn status(&self) -> Status {
        let state = self.state.as_ref();
        Status {
            head: self.head,
            acked: state.map_or(0, |state| state.acked),
            opened_frontier: state.map_or(0, |state| state.opened_frontier),
            snapshot_required: state.map_or(self.history, |state| state.snapshot_required),
            gap: self.blocked,
        }
    }

    fn plan_notes(
        &self,
        dir: &Path,
        notes: &[Note],
        planned: &mut Planned,
        ops: &mut Vec<HomeMutation>,
    ) -> Result<(), Error> {
        for note in notes {
            let sequence = planned.last.checked_add(1).ok_or(Error::Unavailable)?;
            if note.seq != sequence {
                // The chain and the spool number a write differently. Storing it would send an
                // entry under the wrong position.
                return Err(Error::Gap);
            }
            let meta = Meta {
                sequence,
                mutation_id: note.mutation_id.clone(),
                plaintext_sha256: note.plaintext_sha256.clone(),
                file_sha256: note.file.as_deref().map(lastdb_model::hex::sha256_hex),
                file_bytes: note.file.as_ref().map_or(0, Vec::len) as u64,
            };
            if let Some(file) = note.file.as_deref() {
                planned.files.push(files::write(dir, sequence, file)?);
            }
            ops.push(store::entry_op(&meta, &note.envelope)?);
            ops.extend(store::id_ops(&meta));
            planned.last = sequence;
        }
        Ok(())
    }

    fn change(&self, state: State, mut ops: Vec<HomeMutation>) -> Result<Change, Error> {
        ops.push(store::state_op(&state)?);
        Ok(Change {
            ops,
            state,
            released: 0,
            acknowledged: 0,
            trimmed: 0,
            remove: Vec::new(),
            clears: false,
        })
    }

    fn refuse_gap(&self) -> Result<(), Error> {
        (!self.blocked).then_some(()).ok_or(Error::Gap)
    }

    /// Block the spool until the node opens it again. A real hole is still there at that open:
    /// the entries must be contiguous and every named file present.
    fn fail<T>(&mut self, error: Error) -> Result<T, Error> {
        self.blocked = true;
        Err(error)
    }
}

fn fresh(opened: u64, snapshot_required: bool) -> State {
    State {
        version: STATE_VERSION,
        opened_frontier: opened,
        acked: 0,
        snapshot_required,
        upload: None,
    }
}

/// Entries `after + 1 ..= last`, stopping before the next would pass one in-memory file cap.
fn read_page(physical: &V2Home, dir: &Path, after: u64, last: u64) -> Result<Vec<Entry>, Error> {
    let mut items = Vec::new();
    let mut held = 0u64;
    let budget = lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
    for (meta, signed) in store::read_entries(physical, after + 1, last)? {
        let weight = meta.weight(signed.len());
        if !items.is_empty() && held.saturating_add(weight) > budget {
            break;
        }
        let file = match meta.file_sha256 {
            Some(sha256) => {
                if !sha_ok(&sha256) {
                    return Err(Error::Gap);
                }
                let path = files::path(dir, meta.sequence);
                files::check(&path, meta.file_bytes)?;
                Some(EntryFile {
                    path,
                    sha256,
                    bytes: meta.file_bytes,
                })
            }
            None => None,
        };
        held = held.saturating_add(weight);
        items.push(Entry {
            sequence: meta.sequence,
            signed,
            file,
        });
    }
    Ok(items)
}
