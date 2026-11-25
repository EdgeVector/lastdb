//! Changes of the spool that its own commit writes, and the start of a spool.

use super::super::{Error, Frontiers};
use super::types::{State, STATE_VERSION};
use super::upload::{Checkpoint, UploadIdentity, UploadPosition};
use super::{files, inner::fresh, store, Change, Log, CLEAR_PART, IDS_KEPT, TRIM_EVERY};
use crate::storage::disk_pack::physical::V2Home;
use crate::storage::error::StorageResult;
use laststore::disk_run::home::HomeMutation;

impl Log {
    pub(in crate::storage::disk_backup::accepted) fn mark_born(
        &self,
        frontiers: Frontiers,
    ) -> Result<(), Error> {
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
        let opened = inner.state.as_ref().map_or_else(
            || physical.current_root().frontiers().total,
            |state| state.opened_frontier,
        );
        let Some(seq) = store::chain_seq(physical, chain_key)?.filter(|&seq| seq > 0) else {
            if (inner.history || inner.state.as_ref().is_none_or(|s| s.snapshot_required))
                && store::has_any_stream_head(physical)?
            {
                inner.state = Some(fresh(opened, false));
                inner.history = false;
                inner.unsaved = true;
            }
            return Ok(());
        };
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
    pub(in crate::storage::disk_backup::accepted) fn note_snapshot(
        &self,
        covered: u64,
    ) -> Result<Change, Error> {
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

    pub(in crate::storage::disk_backup::accepted) fn upload_position(
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
    pub(in crate::storage::disk_backup::accepted) fn plan_ack(
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
        let mut did_trim = false;
        let trim_due = inner.since_trim.saturating_add(moved) >= TRIM_EVERY
            || inner.ids > IDS_KEPT + TRIM_EVERY;
        if trim_due && through > IDS_KEPT {
            let trim = store::trim_id_ops(physical, through - IDS_KEPT)?;
            trimmed = trim.len() as u64 / 2;
            did_trim = trimmed < store::TRIM_MAX as u64;
            ops.extend(trim);
        }
        state.acked = through;
        state.upload = Some(Checkpoint::new(identity, position));
        let mut change = inner.change(state, ops)?;
        change.released = released;
        change.acknowledged = moved;
        change.trimmed = trimmed;
        change.did_trim = did_trim;
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
        let unsaved = std::mem::replace(
            &mut self.lock().map_err(super::super::storage)?.unsaved,
            false,
        );
        if let Err(error) = commit(ops) {
            if let Ok(mut inner) = self.lock() {
                inner.unsaved |= unsaved;
            }
            return Err(error);
        }
        let mut inner = self.lock().map_err(super::super::storage)?;
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
            inner.since_trim = if change.did_trim {
                0
            } else {
                inner.since_trim.saturating_add(change.acknowledged)
            };
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
}
