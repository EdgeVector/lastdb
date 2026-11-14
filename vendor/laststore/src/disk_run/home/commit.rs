//! Atomic run commits, lock/staging helpers, and background sync coordination.

use super::manifest::{Manifest, RunFile};
use super::{invalid, run_key, RunHome};
use crate::disk_run::{PreparedRun, RunWriter};
use crate::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

/// Complete native stored mutation. The home derives its physical group.
#[derive(Clone, Debug)]
pub struct HomeMutation {
    /// Exact namespace identity, never a filesystem path.
    pub collection: String,
    /// Complete encoded LastStore document id.
    pub key: String,
    /// Opaque stored body, or None for a tombstone.
    pub body: Option<Vec<u8>>,
}

impl HomeMutation {
    /// Build a single stored mutation or tombstone.
    pub fn new(col: impl Into<String>, key: impl Into<String>, body: Option<Vec<u8>>) -> Self {
        Self {
            collection: col.into(),
            key: key.into(),
            body,
        }
    }
}

/// One published commit.
#[derive(Debug, Clone)]
pub struct RunCommit {
    /// The immutable native file written by this commit.
    pub run_id: Uuid,
    /// Complete commits, this one included.
    pub frontier: u64,
    /// Size of the new run file.
    pub run_bytes: u64,
}

impl RunHome {
    pub(super) fn commit_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.commit_mutex.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_lock(&self) -> Result<MutexGuard<'_, ()>> {
        self.publish_mutex.lock().map_err(|_| self.poisoned_lock())
    }

    pub(super) fn publish_staged(&self, next: Manifest, staged: &Path) -> Result<Arc<Manifest>> {
        if let Err(err) = Manifest::replace(&self.root_dir, staged) {
            self.poisoned.store(true, Ordering::Release);
            return Err(err);
        }
        let next = Arc::new(next);
        *self.current.write().map_err(|_| self.poisoned_lock())? = Arc::clone(&next);
        Ok(next)
    }

    fn stage_new_run(&self, file: &Arc<RunFile>) -> Result<(Manifest, PathBuf)> {
        let mut next = self.next_manifest(true)?;
        if next.levels.is_empty() {
            next.levels.push(Vec::new());
        }
        next.levels[0].insert(0, Arc::clone(file));
        let path = next.stage(&self.root_dir, &self.key, None)?;
        Ok((next, path))
    }

    fn poisoned_lock(&self) -> Error {
        self.poisoned.store(true, Ordering::Release);
        Error::WriteCapacity(
            "disk run home: a lock was poisoned; restart the process to reopen the home".into(),
        )
    }

    fn install_prepared(&self, file: Arc<RunFile>, run: PreparedRun) -> Result<Arc<Manifest>> {
        let slot = Mutex::new(Some(run));
        let finish = || slot.lock().unwrap().take().unwrap().finish().map(|_| ());
        std::thread::scope(|scope| {
            let worker = std::thread::Builder::new()
                .name("laststore-sync".into())
                .spawn_scoped(scope, finish);
            let guard = self.publish_lock();
            let staged = guard.as_ref().ok().map(|_| self.stage_new_run(&file));
            let panicked = |_| Err(invalid("disk run: durability worker panicked"));
            let synced = worker.map_or_else(|_| finish(), |h| h.join().unwrap_or_else(panicked));
            let (_guard, (next, staged_path)) = (guard?, staged.unwrap()?);
            synced.inspect_err(|_| drop(fs::remove_file(&staged_path)))?;
            self.publish_staged(next, &staged_path)
        })
    }

    /// Write every operation into one new run, then publish it in a new CURRENT.
    pub fn commit(&self, operations: Vec<HomeMutation>) -> Result<RunCommit> {
        self.refuse_if_poisoned()?;
        if operations.is_empty() {
            return Err(invalid("disk run home: empty commit"));
        }
        let mut records: Vec<_> = operations
            .into_iter()
            .rev()
            .map(|op| Ok((run_key(&op.collection, op.key.as_bytes())?, op.body)))
            .collect::<Result<_>>()?;
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records.dedup_by(|later, first| later.0 == first.0);
        let _commit = self.commit_lock()?;
        let mut writer = RunWriter::open(&self.runs_dir, &self.key, None)?;
        for (key, body) in &records {
            writer.push(key, body.as_deref())?;
        }
        let prepared = writer.prepare_finish()?;
        let file = RunFile::new(&self.runs_dir, prepared.writer.meta.clone());
        let (run_id, run_bytes) = (file.meta.id, file.meta.file_bytes);
        let installed = self.install_prepared(Arc::clone(&file), prepared);
        if installed.is_err() && !self.is_poisoned() {
            file.retire();
        }
        Ok(RunCommit {
            run_id,
            frontier: installed?.frontier,
            run_bytes,
        })
    }
}
