//! One read call against one held CURRENT.

use super::manifest::{Manifest, RunFile};
use super::open::{slots, ReadSlot};
use super::{run_key, RunHome};
use crate::disk_run::{Found, MetadataMemo, RunReader};
use crate::{Error, Result};
use std::sync::Arc;

const MAX_BATCH_KEYS: usize = 10_000;

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
    pub(super) runs: Vec<(Arc<RunFile>, Option<RunReader>)>,
    pub(super) key: [u8; 32],
    pub(super) budget: ReadBudget,
    pub(super) costs: RunReadCosts,
    pub(super) metadata: MetadataMemo,
    pub(super) _owner_lock: Arc<std::fs::File>,
}

impl RunHome {
    /// The state that the latest publish installed.
    pub fn current_root(&self) -> RunRoot {
        RunRoot(self.held())
    }

    /// One read call against exactly `root`, even when newer states exist.
    pub fn read_root(&self, root: &RunRoot, budget: ReadBudget) -> RunRead {
        let manifest = Arc::clone(&root.0);
        RunRead {
            bounded_streams: false,
            shared_metadata_bytes: 0,
            runs: manifest
                .newest_first()
                .map(|r| (Arc::clone(r), None))
                .collect(),
            manifest,
            key: self.key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(budget.bytes),
            _owner_lock: Arc::clone(&self.lock),
        }
    }
}

impl RunRead {
    pub(super) fn new_bounded(
        manifest: Arc<Manifest>,
        key: [u8; 32],
        budget: ReadBudget,
        maximum: u64,
        lock: Arc<std::fs::File>,
    ) -> Result<Self> {
        let available = maximum
            .checked_sub(manifest.retained_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(Self {
            bounded_streams: true,
            shared_metadata_bytes: 0,
            runs: slots(&manifest, available)?,
            manifest,
            key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(0),
            _owner_lock: lock,
        })
    }

    pub(super) fn with_retained(
        manifest: Arc<Manifest>,
        key: [u8; 32],
        budget: ReadBudget,
        retained: u64,
        lock: Arc<std::fs::File>,
    ) -> Result<Self> {
        let available = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .ok_or(Error::ReadBudgetExceeded)?;
        let runs = slots(&manifest, available)?;
        Ok(Self {
            bounded_streams: true,
            shared_metadata_bytes: retained
                + (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64,
            runs,
            manifest,
            key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(0),
            _owner_lock: lock,
        })
    }

    /// Bytes retained by this call's manifest, reader slots and opened metadata.
    pub fn retained_bytes(&self) -> u64 {
        self.manifest.retained_bytes()
            + self.metadata.retained_bytes()
            + (self.runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64
    }

    /// Admit the paired capture metadata shared by subsequent bounded calls.
    pub fn reserve_metadata(&mut self, bytes: u64) -> Result<()> {
        if bytes < self.retained_bytes()
            || bytes > lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes
        {
            return Err(Error::ReadBudgetExceeded);
        }
        self.shared_metadata_bytes = bytes;
        Ok(())
    }

    /// Identity authenticated by this call's selected CURRENT.
    pub fn home_id(&self) -> uuid::Uuid {
        self.manifest.home_id
    }

    /// Every immutable run named by this exact CURRENT, newest first.
    pub fn referenced_runs(&self) -> impl Iterator<Item = (uuid::Uuid, u64)> + '_ {
        self.manifest
            .newest_first()
            .map(|r| (r.meta.run_id, r.meta.file_bytes))
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
        self.examine(1)?;
        self.charge(
            key_bytes
                .checked_add(body_bytes)
                .ok_or(Error::ReadBudgetExceeded)? as u64,
        )
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
        }
        Ok(self.runs[slot].1.as_ref().expect("reader opened"))
    }

    /// The value of one key; a body over `maximum` fails the call's budget.
    pub fn get_bounded(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.get_many_bounded(&[(collection.to_owned(), key.to_owned())], maximum)?
            .pop()
            .expect("one coordinate")
    }

    /// Batch read. Results keep the input order.
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
        if self.bounded_streams {
            return Ok(coordinates
                .iter()
                .map(|(collection, key)| self.streamed_value(collection, key, maximum))
                .collect());
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
                .filter(|&at| {
                    found[at].is_none()
                        && matches!(&wanted[at], Ok(k) if self.runs[slot].0.meta.covers(k))
                })
                .collect();
            if open.is_empty() {
                continue;
            }
            let keys: Vec<&[u8]> = open
                .iter()
                .map(|&at| wanted[at].as_ref().expect("checked").as_slice())
                .collect();
            self.reader(slot)?;
            let hits = self.runs[slot]
                .1
                .as_ref()
                .expect("reader opened")
                .get_many_with(&keys, &mut self.metadata)?;
            for (at, hit) in open.into_iter().zip(hits) {
                found[at] = hit;
            }
        }
        wanted
            .into_iter()
            .zip(found)
            .map(|(key, hit)| match (key, hit) {
                (Err(error), _) => Ok(Err(error)),
                (Ok(_), Some(Found::Value(v))) if v.len() as u64 > maximum => {
                    Ok(Err(Error::ReadBudgetExceeded))
                }
                (Ok(_), Some(Found::Value(v))) => Ok(self.charge(v.len() as u64).map(|()| Some(v))),
                (Ok(_), _) => Ok(Ok(None)),
            })
            .collect()
    }
}
