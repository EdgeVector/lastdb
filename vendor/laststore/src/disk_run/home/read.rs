//! One read call against one held CURRENT.

use super::keys::run_key;
use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::disk_pack::ReadBudget;
use crate::disk_run::{Found, RunReader};
use crate::{Error, Result};
use std::sync::Arc;

/// Most coordinates one batch read accepts.
const MAX_BATCH_KEYS: usize = 10_000;

/// Work one read call did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunReadCosts {
    /// Exact keys and range records examined, tombstones included.
    pub examined_keys: u64,
    /// Key and body bytes charged to the budget.
    pub budget_bytes: u64,
    /// Run files this call opened.
    pub runs_opened: u64,
}

/// One published state of a home: a handle to the CURRENT that a commit or
/// a merge installed. A holder can read exactly that state later, and the
/// state's run files stay on disk while any handle or read holds it.
#[derive(Debug, Clone)]
pub struct RunRoot(pub(super) Arc<Manifest>);

impl RunRoot {
    /// CURRENT generation of this state.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    /// Complete commits in this state. A merge changes the generation but
    /// not the frontier, and two roots with one frontier hold the same data.
    pub fn frontier(&self) -> u64 {
        self.0.frontier
    }
}

/// One read call. It holds one CURRENT from start to end, so every get,
/// batch, and range in the call sees the same complete commit boundary.
///
/// The call opens a run file the first time it needs it and keeps that
/// reader only until the call ends. The base runs do not overlap, so the
/// call keeps one base reader at a time: a long base costs one open file.
pub struct RunRead {
    pub(super) manifest: Arc<Manifest>,
    /// Every run, newest first, with its reader once opened.
    pub(super) runs: Vec<(Arc<RunFile>, Option<RunReader>)>,
    pub(super) key: [u8; 32],
    pub(super) budget: ReadBudget,
    pub(super) costs: RunReadCosts,
    /// Keeps the owner lock while the read names run files.
    _owner_lock: Arc<std::fs::File>,
}

impl RunHome {
    /// The state that the latest publish installed.
    pub fn current_root(&self) -> RunRoot {
        RunRoot(self.held())
    }

    /// Capture the current CURRENT and one budget for a whole read call.
    pub fn read(&self, budget: ReadBudget) -> RunRead {
        self.read_root(&self.current_root(), budget)
    }

    /// One read call against exactly `root`, even when newer states exist.
    pub fn read_root(&self, root: &RunRoot, budget: ReadBudget) -> RunRead {
        let manifest = Arc::clone(&root.0);
        let runs = manifest
            .newest_first()
            .map(|run| (Arc::clone(run), None))
            .collect();
        RunRead {
            manifest,
            runs,
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::clone(&self.lock),
        }
    }
}

impl RunRead {
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
        self.examine(1)?;
        let bytes = key_bytes
            .checked_add(body_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        self.charge(bytes as u64)
    }

    pub(super) fn examine(&mut self, keys: u64) -> Result<()> {
        let next = self.costs.examined_keys.saturating_add(keys);
        if next > self.budget.keys {
            return Err(Error::ReadBudgetExceeded);
        }
        self.costs.examined_keys = next;
        Ok(())
    }

    pub(super) fn charge(&mut self, bytes: u64) -> Result<()> {
        let next = self.costs.budget_bytes.saturating_add(bytes);
        if next > self.budget.bytes {
            return Err(Error::ReadBudgetExceeded);
        }
        self.costs.budget_bytes = next;
        Ok(())
    }

    /// The reader of run `slot`, opened on first use. Opening a base reader
    /// closes the other base readers.
    pub(super) fn reader(&mut self, slot: usize) -> Result<&RunReader> {
        if self.runs[slot].1.is_none() {
            let base_from = self.runs.len() - self.manifest.base.len();
            if slot >= base_from {
                for (_, reader) in &mut self.runs[base_from..] {
                    *reader = None;
                }
            }
            let (file, reader) = &mut self.runs[slot];
            *reader = Some(RunReader::open(&file.path, &self.key)?);
            self.costs.runs_opened += 1;
        }
        Ok(self.runs[slot].1.as_ref().expect("reader opened"))
    }

    /// The value of one complete storage key.
    pub fn get(&mut self, collection: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.get_bounded(collection, key, u64::MAX)
    }

    /// The value of one key; a body over `maximum` fails the call's budget.
    pub fn get_bounded(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.examine(1)?;
        self.charge((collection.len() + key.len()) as u64)?;
        let wanted = run_key(collection, key.as_bytes())?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            match self.reader(slot)?.get(&wanted)? {
                Some(Found::Value(value)) => return self.accept(value, maximum).map(Some),
                Some(Found::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    fn accept(&mut self, value: Vec<u8>, maximum: u64) -> Result<Vec<u8>> {
        if value.len() as u64 > maximum {
            return Err(Error::ReadBudgetExceeded);
        }
        self.charge(value.len() as u64)?;
        Ok(value)
    }

    /// Batch read. Results keep the input order; one slot's error does not
    /// fail the others. Collect every coordinate before the call.
    pub fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        self.get_many_bounded(coordinates, u64::MAX)
    }

    /// Batch read with one body limit for every slot.
    pub fn get_many_bounded(
        &mut self,
        coordinates: &[(String, String)],
        maximum: u64,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        if coordinates.len() > MAX_BATCH_KEYS {
            return Err(Error::ReadBudgetExceeded);
        }
        self.examine(coordinates.len() as u64)?;
        let mut wanted = Vec::with_capacity(coordinates.len());
        for (collection, key) in coordinates {
            self.charge((collection.len() + key.len()) as u64)?;
            wanted.push(run_key(collection, key.as_bytes()));
        }
        let mut found: Vec<Option<Found>> = vec![None; coordinates.len()];
        for slot in 0..self.runs.len() {
            let open: Vec<usize> = (0..wanted.len())
                .filter(|&at| found[at].is_none())
                .filter(|&at| matches!(&wanted[at], Ok(k) if self.runs[slot].0.meta.covers(k)))
                .collect();
            if open.is_empty() {
                continue;
            }
            let keys: Vec<&[u8]> = open
                .iter()
                .map(|&at| wanted[at].as_ref().expect("checked").as_slice())
                .collect();
            let hits = self.reader(slot)?.get_many(&keys)?;
            for (at, hit) in open.into_iter().zip(hits) {
                found[at] = hit;
            }
        }
        let mut out = Vec::with_capacity(coordinates.len());
        for (key, hit) in wanted.into_iter().zip(found) {
            out.push(match (key, hit) {
                (Err(error), _) => Err(error),
                (Ok(_), Some(Found::Value(value))) => self.accept(value, maximum).map(Some),
                (Ok(_), _) => Ok(None),
            });
        }
        Ok(out)
    }
}
