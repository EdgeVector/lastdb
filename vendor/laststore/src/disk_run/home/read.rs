//! One read call against one held CURRENT, stopped-home readers, and point reads.

use super::manifest::{Manifest, RunFile};
use super::{
    invalid, open_nofollow, read_descriptor, require_path, run_key, RunHome, LOCK, RUN_HOME_MARKER,
};
use crate::disk_run::{Found, MetadataMemo, RunReader};
use crate::{Error, Result};
use rustix::fs::FlockOperation;
use std::fs::File;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const MAX_BATCH_KEYS: usize = 10_000;
pub(super) type ReadSlot = (Arc<RunFile>, Option<Arc<RunReader>>);

fn slots(manifest: &Manifest, maximum: u64) -> Result<Vec<ReadSlot>> {
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

/// Finite limits shared by all work in one read call. Zero is never unlimited.
#[derive(Clone, Copy, Debug)]
pub struct ReadBudget {
    /// Maximum distinct metadata pages read.
    pub pages: u64,
    /// Maximum distinct payload blocks decoded.
    pub blocks: u64,
    /// Maximum cumulative file bytes, decoded bytes, and returned body bytes.
    pub bytes: u64,
    /// Maximum exact keys or range entries examined.
    pub keys: u64,
}

impl ReadBudget {
    /// Construct a finite read call budget.
    pub const fn new(pages: u64, blocks: u64, bytes: u64, keys: u64) -> Self {
        Self {
            pages,
            blocks,
            bytes,
            keys,
        }
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

/// One read call holding one CURRENT from start to end.
pub struct RunRead {
    pub(super) bounded_streams: bool,
    pub(super) shared_metadata_bytes: u64,
    pub(super) manifest: Arc<Manifest>,
    pub(super) runs: Vec<ReadSlot>,
    pub(super) key: [u8; 32],
    pub(super) budget: ReadBudget,
    pub(super) costs: RunReadCosts,
    pub(super) metadata: MetadataMemo,
    pub(super) _owner_lock: Arc<File>,
}

impl RunHome {
    /// The state that the latest publish installed.
    pub fn current_root(&self) -> RunRoot {
        RunRoot(self.held())
    }

    /// One read call against exactly `root`, even when newer states exist.
    pub fn read_root(&self, root: &RunRoot, budget: ReadBudget) -> RunRead {
        let runs = root
            .0
            .newest_first()
            .map(|r| (Arc::clone(r), None))
            .collect();
        RunRead {
            bounded_streams: false,
            shared_metadata_bytes: 0,
            runs,
            manifest: Arc::clone(&root.0),
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(budget.bytes),
            _owner_lock: Arc::clone(&self.lock),
        }
    }
}

impl RunRead {
    fn build(
        manifest: Arc<Manifest>,
        key: [u8; 32],
        budget: ReadBudget,
        bounded: Option<(u64, Option<u64>)>,
        _owner_lock: Arc<File>,
    ) -> Result<Self> {
        let avail = bounded.map_or_else(|| u64::MAX - manifest.retained_bytes(), |(a, _)| a);
        let runs = slots(&manifest, avail)?;
        let slot_mem = (runs.capacity() * size_of::<ReadSlot>()) as u64;
        Ok(Self {
            bounded_streams: bounded.is_some(),
            shared_metadata_bytes: bounded.and_then(|(_, b)| b).map_or(0, |b| b + slot_mem),
            runs,
            manifest,
            key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(if bounded.is_some() { 0 } else { budget.bytes }),
            _owner_lock,
        })
    }

    /// Fork the exact held root with preallocation-bounded backup reads enabled.
    pub fn fork_bounded(&self, budget: ReadBudget) -> Result<Self> {
        let retained = self.shared_metadata_bytes.max(self.retained_bytes());
        let avail = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .ok_or(Error::ReadBudgetExceeded)?;
        let (m, l) = (Arc::clone(&self.manifest), Arc::clone(&self._owner_lock));
        Self::build(m, self.key, budget, Some((avail, Some(retained))), l)
    }

    /// Open one existing CURRENT without the cleanup performed by `RunHome::open`.
    pub fn open_existing(root: &Path, key: &[u8; 32], budget: ReadBudget) -> Result<Self> {
        Self::open_inner(root, key, budget, None, false)
    }

    /// Open an inactive root with explicit combined metadata-memory admission.
    pub fn open_existing_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: u64,
    ) -> Result<Self> {
        Self::open_inner(root, key, budget, Some(maximum), false)
    }

    /// Open an existing root alongside other readers while excluding writers.
    pub fn open_existing_shared_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: u64,
    ) -> Result<Self> {
        Self::open_inner(root, key, budget, Some(maximum), true)
    }

    fn open_inner(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        maximum: Option<u64>,
        shared: bool,
    ) -> Result<Self> {
        let root = root.canonicalize()?;
        let lock = open_nofollow(&root.join(LOCK))?;
        if !lock.metadata()?.is_file() {
            return Err(invalid("disk run snapshot: LOCK is not a file"));
        }
        let op = [
            FlockOperation::NonBlockingLockExclusive,
            FlockOperation::NonBlockingLockShared,
        ][usize::from(shared)];
        rustix::fs::flock(&lock, op)
            .map_err(|_| invalid("disk run snapshot: another owner holds the home"))?;
        require_path(&root, &lock)?;
        let m = Arc::new(Manifest::load(
            &root,
            key,
            read_descriptor(&root.join(RUN_HOME_MARKER))?,
            maximum,
        )?);
        let bounded = maximum
            .map(|b| {
                b.checked_sub(m.retained_bytes())
                    .map(|a| (a, None))
                    .ok_or(Error::ReadBudgetExceeded)
            })
            .transpose()?;
        Self::build(m, *key, budget, bounded, Arc::new(lock))
    }

    /// Bytes retained by this call's manifest, reader slots and opened metadata.
    pub fn retained_bytes(&self) -> u64 {
        self.manifest.retained_bytes()
            + self.metadata.bytes
            + (self.runs.capacity() * size_of::<ReadSlot>()) as u64
    }

    /// Admit the paired capture metadata shared by subsequent bounded calls.
    pub fn reserve_metadata(&mut self, bytes: u64) -> Result<()> {
        let max = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        (self.retained_bytes()..=max)
            .contains(&bytes)
            .then(|| self.shared_metadata_bytes = bytes)
            .ok_or(Error::ReadBudgetExceeded)
    }

    /// Identity authenticated by this call's selected CURRENT.
    pub fn home_id(&self) -> Uuid {
        self.manifest.home_id
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

    /// Every immutable run named by this exact CURRENT, newest first.
    pub fn referenced_runs(&self) -> impl Iterator<Item = (Uuid, u64)> + '_ {
        self.manifest
            .newest_first()
            .map(|r| (r.meta.run_id, r.meta.file_bytes))
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
                for (_, r) in &mut self.runs[base_from..] {
                    *r = None;
                }
            }
            let (file, reader) = &mut self.runs[slot];
            *reader = Some(Arc::new(RunReader::open(&file.path, &self.key)?));
        }
        Ok(self.runs[slot].1.as_ref().unwrap())
    }

    /// The value of one key; a body over `maximum` fails the call's budget.
    pub fn get_bounded(&mut self, col: &str, key: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        self.get_many_bounded(&[(col.into(), key.into())], maximum)?
            .pop()
            .unwrap()
    }

    /// Batch read. Results keep the input order.
    pub fn get_many(
        &mut self,
        coords: &[(String, String)],
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        self.get_many_bounded(coords, u64::MAX)
    }

    /// Batch read with one body limit for every slot.
    pub fn get_many_bounded(
        &mut self,
        coords: &[(String, String)],
        maximum: u64,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        if coords.len() > MAX_BATCH_KEYS {
            return Err(Error::ReadBudgetExceeded);
        }
        if self.bounded_streams {
            return Ok(coords
                .iter()
                .map(|(c, k)| self.streamed_value(c, k, maximum))
                .collect());
        }
        self.examine(coords.len() as u64)?;
        let mut wanted = Vec::with_capacity(coords.len());
        for (c, k) in coords {
            self.charge((c.len() + k.len()) as u64)?;
            wanted.push(run_key(c, k.as_bytes()));
        }
        let mut found: Vec<Option<Found>> = vec![None; coords.len()];
        for slot in 0..self.runs.len() {
            let open: Vec<usize> = (0..wanted.len())
                .filter(|&i| {
                    found[i].is_none()
                        && matches!(&wanted[i], Ok(k) if self.runs[slot].0.meta.covers(k))
                })
                .collect();
            if !open.is_empty() {
                let keys: Vec<&[u8]> = open
                    .iter()
                    .map(|&i| wanted[i].as_ref().unwrap().as_slice())
                    .collect();
                let r = Arc::clone(self.reader(slot)?);
                for (i, hit) in open
                    .into_iter()
                    .zip(r.get_many_with(&keys, &mut self.metadata)?)
                {
                    found[i] = hit;
                }
            }
        }
        wanted
            .into_iter()
            .zip(found)
            .map(|(k, hit)| match (k, hit) {
                (Err(e), _) => Ok(Err(e)),
                (Ok(_), Some(Found::Value(v))) if v.len() as u64 > maximum => {
                    Ok(Err(Error::ReadBudgetExceeded))
                }
                (Ok(_), Some(Found::Value(v))) => Ok(self.charge(v.len() as u64).map(|()| Some(v))),
                (Ok(_), _) => Ok(Ok(None)),
            })
            .collect()
    }
}
