//! The spool in memory: loading it from the store, and planning the records of a commit.

use super::super::{Entry, EntryFile, Error, Frontiers, Status};
use super::types::{sha_ok, Meta, Note, State, STATE_VERSION};
use super::{files, store, Change, Inner, Planned, TRIM_EVERY};
use crate::storage::disk_pack::physical::V2Home;
use crate::storage::error::StorageResult;
use laststore::disk_run::home::HomeMutation;
use std::path::Path;

impl Inner {
    pub(super) fn load(
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

    pub(super) fn status(&self) -> Status {
        let state = self.state.as_ref();
        Status {
            head: self.head,
            acked: state.map_or(0, |state| state.acked),
            opened_frontier: state.map_or(0, |state| state.opened_frontier),
            snapshot_required: state.map_or(self.history, |state| state.snapshot_required),
            gap: self.blocked,
        }
    }

    pub(super) fn plan_notes(
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

    pub(super) fn change(&self, state: State, mut ops: Vec<HomeMutation>) -> Result<Change, Error> {
        ops.push(store::state_op(&state)?);
        Ok(Change {
            ops,
            state,
            released: 0,
            acknowledged: 0,
            trimmed: 0,
            did_trim: false,
            remove: Vec::new(),
            clears: false,
        })
    }

    pub(super) fn refuse_gap(&self) -> Result<(), Error> {
        (!self.blocked).then_some(()).ok_or(Error::Gap)
    }

    /// Block the spool until the node opens it again. A real hole is still there at that open:
    /// the entries must be contiguous and every named file present.
    pub(super) fn fail<T>(&mut self, error: Error) -> Result<T, Error> {
        self.blocked = true;
        Err(error)
    }
}

pub(super) fn fresh(opened: u64, snapshot_required: bool) -> State {
    State {
        version: STATE_VERSION,
        opened_frontier: opened,
        acked: 0,
        snapshot_required,
        upload: None,
    }
}

/// Entries `after + 1 ..= last`, stopping before the next would pass one in-memory file cap.
pub(super) fn read_page(
    physical: &V2Home,
    dir: &Path,
    after: u64,
    last: u64,
) -> Result<Vec<Entry>, Error> {
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
