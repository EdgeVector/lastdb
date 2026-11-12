//! Durable sequence beside the main run home. A gap blocks the next commit.

mod recover;

use super::body;
use super::io;
use super::{Error, Page, Status};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

pub(in crate::storage) const DIR: &str = "accepted-v1";
pub(super) const MAX_PAGE: u32 = 90;

#[derive(Clone)]
pub(in crate::storage) struct Note {
    pub mutation_id: String,
    pub plaintext_sha256: String,
    pub envelope: Vec<u8>,
    pub file: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    version: u16,
    pub(super) opened_frontier: u64,
    pub(super) head: u64,
    pub(super) acked: u64,
    pub(super) retained_bytes: u64,
    pub(super) snapshot_required: bool,
    pub(super) gap: bool,
    pub(super) armed: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    version: u16,
    pub(super) frontier_before: u64,
    pub(super) notes: Vec<PendingNote>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PendingNote {
    pub(super) sequence: u64,
    pub(super) mutation_id: String,
    pub(super) plaintext_sha256: String,
    pub(super) envelope_sha256: String,
    pub(super) file_sha256: Option<String>,
    pub(super) envelope_bytes: u64,
    pub(super) file_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Meta {
    version: u16,
    pub(super) sequence: u64,
    pub(super) mutation_id: String,
    pub(super) plaintext_sha256: String,
    pub(super) envelope_sha256: String,
    pub(super) file_sha256: Option<String>,
    pub(super) envelope_bytes: u64,
    pub(super) file_bytes: u64,
}

struct Inner {
    state: Option<State>,
    frontier: u64,
    history: bool,
    blocked: bool,
}

pub(in crate::storage) struct Log {
    root: PathBuf,
    inner: Mutex<Inner>,
}

impl Log {
    pub(in crate::storage) fn open(home: &Path, frontier: u64) -> Self {
        let root = home.join(DIR);
        let mut inner = Inner {
            state: None,
            frontier,
            history: false,
            blocked: false,
        };
        if !root.exists() {
            inner.history = frontier > 0;
        } else if let Err(error) = inner.load(&root) {
            inner.block(&root, error);
        }
        Self {
            root,
            inner: Mutex::new(inner),
        }
    }

    pub(super) fn status(&self) -> Status {
        self.inner
            .lock()
            .map(|inner| inner.status())
            .unwrap_or(Status {
                head: 0,
                acked: 0,
                opened_frontier: 0,
                snapshot_required: true,
                gap: true,
            })
    }

    pub(super) fn prefix(&self, after: u64, limit: u32) -> Result<Page, Error> {
        let mut inner = self.lock()?;
        inner.prefix(&self.root, after, limit)
    }

    pub(super) fn acknowledge(&self, through: u64) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.acknowledge(&self.root, through)
    }

    pub(super) fn note_snapshot(&self, covered: u64) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.note_snapshot(&self.root, covered)
    }

    pub(super) fn mark_born(&self, frontier: u64) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.mark_born(&self.root, frontier)
    }

    pub(super) fn prepare(&self, frontier_before: u64, notes: &[Note]) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.prepare(&self.root, frontier_before, notes)
    }

    pub(super) fn publish(&self, frontier_before: u64, frontier_after: u64) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.publish(&self.root, frontier_before, frontier_after)
    }

    pub(super) fn abort(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        recover::discard(&self.root)?;
        recover::clear_armed(&self.root, &mut inner)
    }

    pub(super) fn force_gap(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.block(&self.root, Error::Gap);
        Err(Error::Gap)
    }

    pub(super) fn arm_split(&self) -> Result<(), Error> {
        let mut inner = self.lock()?;
        inner.arm_split(&self.root)
    }

    pub(super) fn note_split_committed(&self, frontier_before: u64) -> Result<(), Error> {
        let inner = self.lock()?;
        inner.refuse_gap()?;
        recover::mark_committed(&self.root, frontier_before)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, Error> {
        self.inner.lock().map_err(|_| Error::Unavailable)
    }
}

impl Inner {
    fn load(&mut self, root: &Path) -> Result<(), Error> {
        io::require_dir(root)?;
        match io::read_json::<State>(&root.join("state.json"))? {
            Some(state) => {
                let bad = state.version != 1 || state.gap;
                self.state = Some(state);
                if bad {
                    return Err(Error::Gap);
                }
            }
            None => self.history = self.frontier > 0,
        }
        recover::reconcile(root, self)?;
        recover::finish_pending(root, self)
    }

    fn status(&self) -> Status {
        let state = self.state.as_ref();
        Status {
            head: state.map(|state| state.head).unwrap_or(0),
            acked: state.map(|state| state.acked).unwrap_or(0),
            opened_frontier: state.map(|state| state.opened_frontier).unwrap_or(0),
            snapshot_required: state
                .map(|state| state.snapshot_required)
                .unwrap_or(self.history),
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
        Ok(Page {
            through,
            snapshot_required: view.snapshot_required,
            items,
        })
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
        state.acked = through;
        state.retained_bytes = recover::unacked_bytes(root, through, state.head)?;
        io::write_json(&root.join("state.json"), &state)?;
        self.state = Some(state);
        let _ = recover::release_acked(root, through);
        Ok(())
    }

    fn note_snapshot(&mut self, root: &Path, covered: u64) -> Result<(), Error> {
        self.refuse_gap()?;
        let needed = self
            .state
            .as_ref()
            .map(|state| state.opened_frontier)
            .unwrap_or(self.frontier);
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

    fn mark_born(&mut self, root: &Path, frontier: u64) -> Result<(), Error> {
        self.refuse_gap()?;
        self.frontier = frontier;
        self.history = false;
        if let Some(mut state) = self.state.clone() {
            if state.head > 0 {
                return Ok(());
            }
            state.snapshot_required = false;
            state.opened_frontier = frontier;
            state.armed = false;
            return self.store(root, state);
        }
        self.store(root, self.fresh(frontier, false))
    }

    fn prepare(&mut self, root: &Path, frontier_before: u64, notes: &[Note]) -> Result<(), Error> {
        self.refuse_gap()?;
        self.frontier = frontier_before;
        if notes.is_empty() {
            return Ok(());
        }
        if io::read_json::<Pending>(&root.join("pending.json"))?.is_some() {
            self.block(root, Error::Gap);
            return Err(Error::Gap);
        }
        let planned = recover::plan(root, self, notes)?;
        if planned.is_empty() {
            return Ok(());
        }
        recover::write_pending(root, frontier_before, notes, &planned)
    }

    fn publish(
        &mut self,
        root: &Path,
        frontier_before: u64,
        frontier_after: u64,
    ) -> Result<(), Error> {
        let Some(pending) = io::read_json::<Pending>(&root.join("pending.json"))? else {
            self.frontier = frontier_after;
            return recover::clear_armed(root, self);
        };
        if frontier_after <= frontier_before || pending.frontier_before != frontier_before {
            self.block(root, Error::Gap);
            return Err(Error::Gap);
        }
        self.frontier = frontier_after;
        recover::finish_pending(root, self)
    }

    pub(super) fn arm_split(&mut self, root: &Path) -> Result<(), Error> {
        recover::arm(root, self)
    }

    fn refuse_gap(&self) -> Result<(), Error> {
        if self.blocked || self.state.as_ref().is_some_and(|state| state.gap) {
            Err(Error::Gap)
        } else {
            Ok(())
        }
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

pub(super) fn spool_cap() -> u64 {
    lastdb_model::storage_policy::BACKUP_RESOURCES.durable_spool_bytes
}

pub(super) fn sha_ok(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn id_ok(id: &str) -> bool {
    body::mutation_id_ok(id)
}
