//! Durable sequence beside the main run home. A gap blocks the next commit.

mod recover;
mod types;
pub(super) mod upload;

pub(in crate::storage) use types::Note;
pub(super) use types::{id_ok, sha_ok, spool_cap, Meta, Pending, PendingNote, State};

use super::{io, Error, Frontiers, Page, Status};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

pub(in crate::storage) const DIR: &str = "accepted-v1";
pub(super) const MAX_PAGE: u32 = 90;

#[derive(Default)]
struct Inner {
    state: Option<State>,
    frontier: u64,
    main_frontier: u64,
    blocked: bool,
    /// The home held data when it opened and has no stream yet.
    history: bool,
    /// Acknowledged entries since the id records were last trimmed.
    since_trim: u64,
}

pub(in crate::storage) struct Log {
    root: PathBuf,
    inner: Mutex<Inner>,
}

impl Log {
    pub(in crate::storage) fn has_state(home: &Path) -> bool {
        home.join(DIR)
            .join("state.json")
            .symlink_metadata()
            .is_ok_and(|meta| {
                meta.is_file() && crate::storage::disk_pack::admit::is_private_meta(&meta)
            })
    }

    pub(in crate::storage) fn open(
        home: &Path,
        frontiers: Frontiers,
        has_user_data: bool,
        initializing: bool,
    ) -> Self {
        let root = home.join(DIR);
        let mut inner = Inner {
            frontier: frontiers.total,
            main_frontier: frontiers.main,
            ..Inner::default()
        };
        let outcome = if root.exists() {
            inner.load(&root, has_user_data)
        } else if has_user_data {
            inner.history = true;
            Ok(())
        } else if initializing {
            Ok(())
        } else {
            inner.mark_born(&root, frontiers)
        };
        if let Err(error) = outcome {
            inner.block(&root, error);
        }
        Self {
            root,
            inner: Mutex::new(inner),
        }
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
        let inner = self.lock()?;
        inner.refuse_gap()?;
        if io::read_json::<Pending>(&self.root.join("pending.json"))?.is_some() {
            return Err(Error::Gap);
        }
        Ok(())
    }

    pub(in crate::storage) fn check_admissible(
        &self,
        queued_notes: usize,
        queued_bytes: u64,
        notes: &[Note],
    ) -> Result<(), Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        if io::read_json::<Pending>(&self.root.join("pending.json"))?.is_some() {
            return Err(Error::Gap);
        }
        let head = inner.state.as_ref().map_or(0, |s| s.head);
        let added_notes = queued_notes
            .checked_add(notes.len())
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(Error::Unavailable)?;
        head.checked_add(added_notes).ok_or(Error::Unavailable)?;
        let mut batch_bytes = 0u64;
        for note in notes {
            if !id_ok(&note.mutation_id) || !sha_ok(&note.plaintext_sha256) {
                return Err(Error::Conflict);
            }
            if recover::mutation_recorded(&self.root, &note.mutation_id)? {
                return Err(Error::Conflict);
            }
            batch_bytes = batch_bytes
                .checked_add(note.byte_weight())
                .ok_or(Error::Spool)?;
        }
        let retained = inner.state.as_ref().map_or(0, |s| s.retained_bytes);
        let total = retained
            .checked_add(queued_bytes)
            .and_then(|s| s.checked_add(batch_bytes))
            .ok_or(Error::Spool)?;
        (total <= spool_cap()).then_some(()).ok_or(Error::Spool)
    }

    pub(super) fn note_snapshot(&self, covered: u64) -> Result<(), Error> {
        self.lock()?.note_snapshot(&self.root, covered)
    }

    /// Record that the files at `covered` are the first snapshot's cut. The caller holds the
    /// write gate, no entry exists yet, and the checkpoint of those files is already durable.
    pub(in crate::storage) fn pin_initial_frontier(&self, covered: u64) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.refuse_gap()?;
        inner.frontier = covered;
        let state = match &inner.state {
            Some(state) if state.head == 0 && state.snapshot_required => State {
                opened_frontier: covered,
                ..state.clone()
            },
            None if inner.history => inner.fresh(covered, true),
            _ => return Err(Error::Snapshot),
        };
        inner.store(&self.root, state)
    }

    pub(super) fn prefix(&self, after: u64, limit: u32) -> Result<Page, Error> {
        self.lock()?.prefix(&self.root, after, limit)
    }

    pub(super) fn mark_born(&self, frontiers: Frontiers) -> Result<(), Error> {
        self.lock()?.mark_born(&self.root, frontiers)
    }

    pub(super) fn prepare(&self, before: Frontiers, notes: &[Note]) -> Result<(), Error> {
        self.lock()?.prepare(&self.root, before, notes)
    }

    pub(super) fn publish(&self, before: Frontiers, after: Frontiers) -> Result<(), Error> {
        self.lock()?.publish(&self.root, before, after)
    }

    pub(super) fn abort(&self) -> Result<(), Error> {
        let _inner = self.lock()?;
        recover::discard(&self.root)
    }

    pub(super) fn force_gap(&self) -> Result<(), Error> {
        self.lock()?.block(&self.root, Error::Gap);
        Err(Error::Gap)
    }

    pub(super) fn mutation_recorded(&self, id: &str) -> Result<bool, Error> {
        if !id_ok(id) {
            return Ok(false);
        }
        recover::mutation_recorded(&self.root, id)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, Error> {
        self.inner.lock().map_err(|_| Error::Unavailable)
    }
}

impl Inner {
    fn load(&mut self, root: &Path, has_user_data: bool) -> Result<(), Error> {
        io::require_dir(root)?;
        match io::read_json::<State>(&root.join("state.json"))? {
            Some(state) => {
                let bad = state.version != 1 || state.gap;
                self.state = Some(state);
                if bad {
                    return Err(Error::Gap);
                }
            }
            None => self.history = has_user_data,
        }
        recover::reconcile(root, self)?;
        recover::finish_pending(root, self)?;
        recover::trim_ids(root, self);
        Ok(())
    }

    fn status(&self) -> Status {
        let state = self.state.as_ref();
        Status {
            head: state.map_or(0, |state| state.head),
            acked: state.map_or(0, |state| state.acked),
            opened_frontier: state.map_or(0, |state| state.opened_frontier),
            snapshot_required: state.map_or(self.history, |state| state.snapshot_required),
            gap: self.blocked || state.is_some_and(|state| state.gap),
        }
    }

    fn prefix(&mut self, root: &Path, after: u64, limit: u32) -> Result<Page, Error> {
        self.refuse_gap()?;
        if limit == 0 || limit > MAX_PAGE {
            return Err(Error::Limit);
        }
        let view = self.status();
        if after > view.head {
            return Err(Error::Ahead);
        }
        if after < view.acked {
            return Err(Error::Behind);
        }
        let last = after.saturating_add(u64::from(limit)).min(view.head);
        let (through, items) = recover::read_page(root, self, after, last)?;
        Ok(Page { through, items })
    }

    fn acknowledge(&mut self, root: &Path, through: u64) -> Result<(), Error> {
        self.refuse_gap()?;
        let view = self.status();
        if view.snapshot_required && through > view.acked {
            return Err(Error::Snapshot);
        }
        if through > view.head {
            return Err(Error::Ahead);
        }
        if through <= view.acked {
            return Ok(());
        }
        let mut state = self.state.clone().ok_or(Error::Gap)?;
        let released = recover::retained_bytes(root, view.acked, through)?;
        state.retained_bytes = state
            .retained_bytes
            .checked_sub(released)
            .ok_or(Error::Gap)?;
        state.acked = through;
        io::write_json(&root.join("state.json"), &state)?;
        self.state = Some(state);
        recover::release_acked(root, view.acked, through);
        self.since_trim += through - view.acked;
        if self.since_trim >= recover::TRIM_EVERY {
            self.since_trim = 0;
            recover::trim_ids(root, self);
        }
        Ok(())
    }

    fn note_snapshot(&mut self, root: &Path, covered: u64) -> Result<(), Error> {
        self.refuse_gap()?;
        let needed = self
            .state
            .as_ref()
            .map_or(self.frontier, |s| s.opened_frontier);
        if covered < needed {
            return Err(Error::Snapshot);
        }
        self.history = false;
        let mut state = self
            .state
            .clone()
            .unwrap_or_else(|| self.fresh(needed, false));
        state.snapshot_required = false;
        self.store(root, state)
    }

    fn mark_born(&mut self, root: &Path, frontiers: Frontiers) -> Result<(), Error> {
        self.refuse_gap()?;
        self.frontier = frontiers.total;
        self.main_frontier = frontiers.main;
        self.history = false;
        if let Some(mut state) = self.state.clone() {
            if state.head > 0 {
                return Ok(());
            }
            state.snapshot_required = false;
            state.opened_frontier = frontiers.total;
            return self.store(root, state);
        }
        self.store(root, self.fresh(frontiers.total, false))
    }

    fn prepare(&mut self, root: &Path, before: Frontiers, notes: &[Note]) -> Result<(), Error> {
        self.refuse_gap()?;
        self.frontier = before.total;
        self.main_frontier = before.main;
        if notes.is_empty() {
            return Ok(());
        }
        if io::read_json::<Pending>(&root.join("pending.json"))?.is_some() {
            self.block(root, Error::Gap);
            return Err(Error::Gap);
        }
        let planned = recover::plan(root, self, notes)?;
        recover::write_pending(root, before, notes, &planned)
    }

    fn publish(&mut self, root: &Path, before: Frontiers, after: Frontiers) -> Result<(), Error> {
        let Some(pending) = io::read_json::<Pending>(&root.join("pending.json"))? else {
            self.frontier = after.total;
            self.main_frontier = after.main;
            return Ok(());
        };
        if after.main <= before.main
            || pending.frontier_before != before.total
            || pending.main_frontier_before != Some(before.main)
        {
            self.block(root, Error::Gap);
            return Err(Error::Gap);
        }
        self.frontier = after.total;
        self.main_frontier = after.main;
        recover::finish_pending(root, self)
    }

    fn refuse_gap(&self) -> Result<(), Error> {
        (!self.blocked && !self.state.as_ref().is_some_and(|s| s.gap))
            .then_some(())
            .ok_or(Error::Gap)
    }

    fn block(&mut self, root: &Path, _error: Error) {
        self.blocked = true;
        recover::persist_gap(root, self);
    }

    pub(super) fn fresh(&self, opened: u64, snapshot_required: bool) -> State {
        State {
            version: 1,
            opened_frontier: opened,
            head: 0,
            acked: 0,
            retained_bytes: 0,
            snapshot_required,
            gap: false,
            armed: false,
        }
    }

    pub(super) fn store(&mut self, root: &Path, state: State) -> Result<(), Error> {
        recover::ensure_tree(root)?;
        io::write_json(&root.join("state.json"), &state)?;
        self.state = Some(state);
        self.history = false;
        Ok(())
    }
}
