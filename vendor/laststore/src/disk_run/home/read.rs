//! One read call against one held CURRENT, including memory-admitted point reads.

use super::manifest::Manifest;
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
    pub(super) runs: Vec<ReadSlot>,
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
        RunRead {
            bounded_streams: false,
            shared_metadata_bytes: 0,
            runs: root
                .0
                .newest_first()
                .map(|r| (Arc::clone(r), None))
                .collect(),
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
    pub(super) fn bounded_from(
        manifest: Arc<Manifest>,
        key: [u8; 32],
        budget: ReadBudget,
        available: u64,
        shared_base: Option<u64>,
        _owner_lock: Arc<std::fs::File>,
    ) -> Result<Self> {
        let runs = slots(&manifest, available)?;
        let slot_mem = (runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64;
        Ok(Self {
            bounded_streams: true,
            shared_metadata_bytes: shared_base.map_or(0, |b| b + slot_mem),
            runs,
            manifest,
            key,
            budget,
            costs: RunReadCosts::default(),
            metadata: MetadataMemo::new(0),
            _owner_lock,
        })
    }

    /// Bytes retained by this call's manifest, reader slots and opened metadata.
    pub fn retained_bytes(&self) -> u64 {
        self.manifest.retained_bytes()
            + self.metadata.bytes
            + (self.runs.capacity() * std::mem::size_of::<ReadSlot>()) as u64
    }

    /// Admit the paired capture metadata shared by subsequent bounded calls.
    pub fn reserve_metadata(&mut self, bytes: u64) -> Result<()> {
        let max = lastdb_model::storage_policy::BACKUP_RESOURCES.working_memory_bytes;
        if bytes < self.retained_bytes() || bytes > max {
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
        let add = key_bytes
            .checked_add(body_bytes)
            .ok_or(Error::ReadBudgetExceeded)? as u64;
        self.examine(1)?;
        self.charge(add)
            .inspect_err(|_| self.costs.examined_keys -= 1)
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
                self.runs[base_from..]
                    .iter_mut()
                    .for_each(|(_, r)| *r = None);
            }
            let (file, reader) = &mut self.runs[slot];
            *reader = Some(Arc::new(RunReader::open(&file.path, &self.key)?));
        }
        Ok(self.runs[slot].1.as_ref().expect("reader opened"))
    }

    pub(super) fn admit_stream(&self, extra: u64) -> Result<(u64, u64)> {
        let res = lastdb_model::storage_policy::BACKUP_RESOURCES;
        let logical = self
            .budget
            .bytes
            .checked_sub(self.costs.budget_bytes)
            .ok_or(Error::ReadBudgetExceeded)?;
        let reserved = self
            .shared_metadata_bytes
            .max(self.retained_bytes())
            .checked_add(self.costs.budget_bytes)
            .and_then(|n| n.checked_add(extra))
            .ok_or(Error::ReadBudgetExceeded)?;
        let global = res
            .working_memory_bytes
            .checked_sub(reserved)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok((logical.min(global).min(res.decoded_values_bytes), reserved))
    }

    pub(super) fn open_stream(&self, slot: usize, extra: u64) -> Result<(RunReader, u64, u64)> {
        let (admission, reserved) = self.admit_stream(extra)?;
        let reader =
            RunReader::open_bounded(&self.runs[slot].0.path, &self.key, admission, reserved)?;
        Ok((reader, admission, reserved))
    }

    pub(super) fn streamed_value(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.account_memory_record(collection.len(), key.len())?;
        let buffers = 2 * lastdb_model::storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        let pre = (collection.len() as u64 + key.len() as u64 + 2)
            .checked_add(buffers)
            .ok_or(Error::ReadBudgetExceeded)?;
        self.admit_stream(pre)?;
        let wanted = run_key(collection, key.as_bytes())?;
        let post = (wanted.capacity() as u64)
            .checked_add(buffers)
            .ok_or(Error::ReadBudgetExceeded)?;
        self.admit_stream(post)?;
        for slot in 0..self.runs.len() {
            if !self.runs[slot].0.meta.covers(&wanted) {
                continue;
            }
            let (reader, admission, reserved) = self.open_stream(slot, wanted.len() as u64)?;
            if let Some(hit) = reader.stream_body(&wanted, admission, reserved, maximum)? {
                if let Some(ref v) = hit {
                    self.charge(v.len() as u64)?;
                }
                return Ok(hit);
            }
        }
        Ok(None)
    }

    /// The value of one key; a body over `maximum` fails the call's budget.
    pub fn get_bounded(
        &mut self,
        collection: &str,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        self.get_many_bounded(&[(collection.into(), key.into())], maximum)?
            .pop()
            .expect("one")
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
                .map(|(c, k)| self.streamed_value(c, k, maximum))
                .collect());
        }
        self.examine(coordinates.len() as u64)?;
        let mut wanted = Vec::with_capacity(coordinates.len());
        for (c, k) in coordinates {
            self.charge((c.len() + k.len()) as u64)?;
            wanted.push(run_key(c, k.as_bytes()));
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
                .map(|&at| wanted[at].as_ref().unwrap().as_slice())
                .collect();
            let reader = Arc::clone(self.reader(slot)?);
            for (at, hit) in open
                .into_iter()
                .zip(reader.get_many_with(&keys, &mut self.metadata)?)
            {
                found[at] = hit;
            }
        }
        wanted
            .into_iter()
            .zip(found)
            .map(|(key, hit)| match (key, hit) {
                (Err(err), _) => Ok(Err(err)),
                (Ok(_), Some(Found::Value(v))) if v.len() as u64 > maximum => {
                    Ok(Err(Error::ReadBudgetExceeded))
                }
                (Ok(_), Some(Found::Value(v))) => Ok(self.charge(v.len() as u64).map(|()| Some(v))),
                (Ok(_), _) => Ok(Ok(None)),
            })
            .collect()
    }
}
