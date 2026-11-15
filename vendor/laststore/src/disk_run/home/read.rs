//! Published roots, file-handle snapshots, stopped-home readers, and point reads.

use super::manifest::{Manifest, RunFile};
use super::{
    invalid, open_nofollow, read_descriptor, require_path, run_key, RunHome, CURRENT, LOCK, RUNS,
    RUN_HOME_MARKER,
};
use crate::disk_run::{Found, MetadataMemo, RunReader};
use crate::{Error, Result};
use rustix::fs::FlockOperation;
use std::fs::File;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_BATCH_KEYS: usize = 10_000;
pub(super) type ReadSlot = (Arc<RunFile>, Option<Arc<RunReader>>);

fn slots(manifest: &Manifest) -> Result<Vec<ReadSlot>> {
    let maximum = u64::MAX - manifest.retained_bytes();
    let (count, sz) = (manifest.newest_first().count(), size_of::<ReadSlot>());
    if count.checked_mul(sz).is_none_or(|b| b as u64 > maximum) {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut runs = Vec::new();
    runs.try_reserve_exact(count)
        .map_err(|_| Error::ReadBudgetExceeded)?;
    if (runs.capacity() * sz) as u64 > maximum {
        return Err(Error::ReadBudgetExceeded);
    }
    runs.extend(manifest.newest_first().map(|r| (Arc::clone(r), None)));
    Ok(runs)
}

/// One published state of a home: a handle to the CURRENT that a commit or a merge installed.
#[derive(Debug, Clone)]
pub struct RunRoot(pub(super) Arc<Manifest>);

impl RunRoot {
    /// CURRENT generation of this state.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    /// Complete commits in this state.
    pub fn frontier(&self) -> u64 {
        self.0.frontier
    }
}

/// A held run directory publication and the exact files it references.
pub struct RunSnapshot {
    root: RunRoot,
    descriptor: File,
    current: File,
}

impl RunHome {
    /// The state that the latest publish installed.
    pub fn current_root(&self) -> RunRoot {
        RunRoot(self.held())
    }

    /// One read call against exactly `root`, even when newer states exist.
    pub fn read_root(&self, root: &RunRoot, budget: ReadBudget) -> RunRead {
        RunRead::build(
            Arc::clone(&root.0),
            self.key,
            Arc::clone(&self.lock),
            budget,
        )
        .expect("unbounded read root")
    }

    /// Hold the current publication across subsequent writes and compactions.
    pub fn snapshot(&self) -> Result<RunSnapshot> {
        let _publish = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        let descriptor = open_nofollow(&self.path().join(RUN_HOME_MARKER))?;
        let current = open_nofollow(&self.path().join(CURRENT))?;
        Ok(RunSnapshot {
            root: self.current_root(),
            descriptor,
            current,
        })
    }
}

impl RunSnapshot {
    /// List relative paths and byte lengths in the held publication.
    pub fn files(&self) -> Result<Vec<(PathBuf, u64)>> {
        let mut out = vec![
            (RUN_HOME_MARKER.into(), self.descriptor.metadata()?.len()),
            (CURRENT.into(), self.current.metadata()?.len()),
        ];
        for run in self.root.0.newest_first() {
            let name = run
                .path
                .file_name()
                .ok_or_else(|| invalid("snapshot run name"))?;
            out.push((PathBuf::from(RUNS).join(name), run.meta.file_bytes));
        }
        Ok(out)
    }

    /// Open a file belonging to the held publication.
    pub fn open_file(&self, path: &Path) -> Result<File> {
        if path == Path::new(RUN_HOME_MARKER) {
            return Ok(self.descriptor.try_clone()?);
        }
        if path == Path::new(CURRENT) {
            return Ok(self.current.try_clone()?);
        }
        let name = path
            .strip_prefix(RUNS)
            .ok()
            .filter(|p| p.components().count() == 1)
            .and_then(Path::file_name)
            .ok_or_else(|| invalid("snapshot run name"))?;
        let run = self
            .root
            .0
            .newest_first()
            .find(|r| r.path.file_name() == Some(name))
            .ok_or_else(|| invalid("snapshot run absent"))?;
        Ok(open_nofollow(&run.path)?)
    }
}

/// Finite limits shared by all work in one read call. Zero is never unlimited.
#[derive(Clone, Copy, Debug)]
pub struct ReadBudget {
    /// Maximum cumulative file bytes, decoded bytes, and returned body bytes.
    pub bytes: u64,
    /// Maximum exact keys or range entries examined.
    pub keys: u64,
}

impl ReadBudget {
    /// Construct a finite read call budget.
    pub const fn new(bytes: u64, keys: u64) -> Self {
        Self { bytes, keys }
    }
}

/// Work one read call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunReadCosts {
    /// Exact keys and range records examined, tombstones included.
    pub examined_keys: u64,
    /// Key and body bytes charged to the budget.
    pub budget_bytes: u64,
}

/// One read call holding one CURRENT from start to end.
pub struct RunRead {
    pub(super) manifest: Arc<Manifest>,
    pub(super) key: [u8; 32],
    _lock: Arc<File>,
    pub(super) runs: Vec<ReadSlot>,
    pub(super) budget: ReadBudget,
    pub(super) costs: RunReadCosts,
    pub(super) metadata: MetadataMemo,
}

impl RunRead {
    pub(super) fn build(
        manifest: Arc<Manifest>,
        key: [u8; 32],
        lock: Arc<File>,
        budget: ReadBudget,
    ) -> Result<Self> {
        let runs = slots(&manifest)?;
        Ok(Self {
            manifest,
            key,
            _lock: lock,
            runs,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(budget.bytes),
        })
    }

    /// Open one existing CURRENT without the cleanup performed by `RunHome::open`.
    pub fn open_existing(root: &Path, key: &[u8; 32], budget: ReadBudget) -> Result<Self> {
        let root = root.canonicalize()?;
        let lock = open_nofollow(&root.join(LOCK))?;
        if !lock.metadata()?.is_file() {
            return Err(invalid("disk run snapshot: LOCK is not a file"));
        }
        match rustix::fs::flock(&lock, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => require_path(&root, &lock)?,
            Err(e) if e == rustix::io::Errno::WOULDBLOCK => {
                return Err(Error::HomeBusy(
                    "disk run snapshot: another owner holds the home".into(),
                ))
            }
            Err(e) => return Err(std::io::Error::from(e).into()),
        }
        let id = read_descriptor(&root.join(RUN_HOME_MARKER))?;
        let manifest = Arc::new(Manifest::load(&root, key, id)?);
        Self::build(manifest, *key, Arc::new(lock), budget)
    }

    /// Bytes retained by this call's manifest, reader slots and opened metadata.
    pub fn retained_bytes(&self) -> u64 {
        self.manifest.retained_bytes()
            + self.metadata.retained_bytes
            + (self.runs.capacity() * size_of::<ReadSlot>()) as u64
    }

    /// Generation of the held CURRENT.
    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    /// Complete commits in the held CURRENT.
    pub fn frontier(&self) -> u64 {
        self.manifest.frontier
    }

    /// The limits of this call.
    pub fn budget(&self) -> ReadBudget {
        self.budget
    }

    /// Work so far, including work before an error.
    pub fn costs(&self) -> RunReadCosts {
        self.costs
    }

    /// Charge a memory or pending-write result to this call's budget.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        let add = key_bytes
            .checked_add(body_bytes)
            .ok_or(Error::ReadBudgetExceeded)? as u64;
        self.examine(1).and_then(|()| self.charge(add))
    }

    pub(super) fn examine(&mut self, keys: u64) -> Result<()> {
        let next = self.costs.examined_keys.saturating_add(keys);
        (next <= self.budget.keys)
            .then(|| self.costs.examined_keys = next)
            .ok_or(Error::ReadBudgetExceeded)
    }

    pub(super) fn charge(&mut self, bytes: u64) -> Result<()> {
        let next = self.costs.budget_bytes.saturating_add(bytes);
        (next <= self.budget.bytes)
            .then(|| self.costs.budget_bytes = next)
            .ok_or(Error::ReadBudgetExceeded)
    }

    pub(super) fn reader(&mut self, slot: usize) -> Result<&Arc<RunReader>> {
        if self.runs[slot].1.is_none() {
            let base_from = self.runs.len() - self.manifest.base.len();
            if slot >= base_from {
                for (_, reader) in &mut self.runs[base_from..] {
                    *reader = None;
                }
            }
            let (file, reader) = &mut self.runs[slot];
            *reader = Some(Arc::new(RunReader::open(&file.path, &self.key)?));
        }
        Ok(self.runs[slot].1.as_ref().unwrap())
    }

    /// Batch read with one body limit for every slot.
    pub fn get_many_bounded(
        &mut self,
        coords: &[(String, String)],
        max: u64,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        if coords.len() > MAX_BATCH_KEYS {
            return Err(Error::ReadBudgetExceeded);
        }
        self.examine(coords.len() as u64)?;
        let mut wanted = Vec::with_capacity(coords.len());
        for (col, key) in coords {
            self.charge((col.len() + key.len()) as u64)?;
            wanted.push(run_key(col, key.as_bytes()));
        }
        let mut found: Vec<Option<Found>> = vec![None; coords.len()];
        for slot in 0..self.runs.len() {
            let meta = &self.runs[slot].0.meta;
            let open: Vec<usize> = (0..wanted.len())
                .filter(|&i| found[i].is_none() && matches!(&wanted[i], Ok(k) if meta.covers(k)))
                .collect();
            if !open.is_empty() {
                let keys: Vec<&[u8]> = open
                    .iter()
                    .map(|&i| wanted[i].as_ref().unwrap().as_slice())
                    .collect();
                let reader = Arc::clone(self.reader(slot)?);
                let hits =
                    reader.get_many_with(&keys, |rd, n, sc| self.metadata.read(rd, n, sc))?;
                for (i, hit) in open.into_iter().zip(hits) {
                    found[i] = hit;
                }
            }
        }
        wanted
            .into_iter()
            .zip(found)
            .map(|(k, hit)| match (k, hit) {
                (Err(e), _) => Ok(Err(e)),
                (Ok(_), Some(Found::Value(v))) if v.len() as u64 > max => {
                    Ok(Err(Error::ReadBudgetExceeded))
                }
                (Ok(_), Some(Found::Value(v))) => Ok(self.charge(v.len() as u64).map(|()| Some(v))),
                (Ok(_), _) => Ok(Ok(None)),
            })
            .collect()
    }
}
