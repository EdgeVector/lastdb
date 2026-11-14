//! One read call against one held CURRENT, stopped-home readers, and point reads.

use super::manifest::{Manifest, RunFile};
use super::{
    invalid, open_nofollow, read_descriptor, require_path, run_key, LOCK, RUN_HOME_MARKER,
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
    pub(super) streamed: bool,
    pub(super) reserved_metadata_bytes: u64,
    pub(super) manifest: Arc<Manifest>,
    pub(super) key: [u8; 32],
    lock: Arc<File>,
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
        bounded: Option<(u64, Option<u64>)>,
    ) -> Result<Self> {
        let limit = bounded.map_or_else(|| u64::MAX - manifest.retained_bytes(), |(a, _)| a);
        let runs = slots(&manifest, limit)?;
        let slot_mem = (runs.capacity() * size_of::<ReadSlot>()) as u64;
        let streamed = bounded.is_some();
        let reserved_metadata_bytes = bounded.and_then(|(_, b)| b).map_or(0, |v| v + slot_mem);
        let metadata = MetadataMemo::new(if streamed { 0 } else { budget.bytes });
        Ok(Self {
            streamed,
            reserved_metadata_bytes,
            manifest,
            key,
            lock,
            runs,
            budget,
            costs: RunReadCosts::default(),
            metadata,
        })
    }

    /// Fork the exact held root with preallocation-bounded backup reads enabled.
    pub fn fork_bounded(&self, budget: ReadBudget) -> Result<Self> {
        let ret = self.reserved_metadata_bytes.max(self.retained_bytes());
        let avail = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(ret)
            .ok_or(Error::ReadBudgetExceeded)?;
        Self::build(
            Arc::clone(&self.manifest),
            self.key,
            Arc::clone(&self.lock),
            budget,
            Some((avail, Some(ret))),
        )
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
        max_bytes: u64,
    ) -> Result<Self> {
        Self::open_inner(root, key, budget, Some(max_bytes), false)
    }

    /// Open an existing root alongside other readers while excluding writers.
    pub fn open_existing_shared_bounded(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        max_bytes: u64,
    ) -> Result<Self> {
        Self::open_inner(root, key, budget, Some(max_bytes), true)
    }

    fn open_inner(
        root: &Path,
        key: &[u8; 32],
        budget: ReadBudget,
        max: Option<u64>,
        shared: bool,
    ) -> Result<Self> {
        let root = root.canonicalize()?;
        let lock = open_nofollow(&root.join(LOCK))?;
        if !lock.metadata()?.is_file() {
            return Err(invalid("disk run snapshot: LOCK is not a file"));
        }
        let op = if shared {
            FlockOperation::NonBlockingLockShared
        } else {
            FlockOperation::NonBlockingLockExclusive
        };
        match rustix::fs::flock(&lock, op) {
            Ok(()) => require_path(&root, &lock)?,
            Err(e) if e == rustix::io::Errno::WOULDBLOCK => {
                return Err(Error::HomeBusy(
                    "disk run snapshot: another owner holds the home".into(),
                ))
            }
            Err(e) => return Err(std::io::Error::from(e).into()),
        }
        let id = read_descriptor(&root.join(RUN_HOME_MARKER))?;
        let manifest = Arc::new(Manifest::load(&root, key, id, max)?);
        let bounded = max
            .map(|lim| {
                lim.checked_sub(manifest.retained_bytes())
                    .map(|r| (r, None))
                    .ok_or(Error::ReadBudgetExceeded)
            })
            .transpose()?;
        Self::build(manifest, *key, Arc::new(lock), budget, bounded)
    }

    /// Bytes retained by this call's manifest, reader slots and opened metadata.
    pub fn retained_bytes(&self) -> u64 {
        self.manifest.retained_bytes()
            + self.metadata.retained_bytes
            + (self.runs.capacity() * size_of::<ReadSlot>()) as u64
    }

    /// Admit the paired capture metadata shared by subsequent bounded calls.
    pub fn reserve_metadata(&mut self, bytes: u64) -> Result<()> {
        let max = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        (self.retained_bytes()..=max)
            .contains(&bytes)
            .then(|| self.reserved_metadata_bytes = bytes)
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
            .map(|r| (r.meta.id, r.meta.file_bytes))
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

    /// The value of one key; a body over `maximum` fails the call's budget.
    pub fn get_bounded(&mut self, col: &str, key: &str, maximum: u64) -> Result<Option<Vec<u8>>> {
        self.get_many_bounded(&[(col.into(), key.into())], maximum)?
            .pop()
            .unwrap()
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
        if self.streamed {
            return Ok(coords
                .iter()
                .map(|(c, k)| self.streamed_value(c, k, max))
                .collect());
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
