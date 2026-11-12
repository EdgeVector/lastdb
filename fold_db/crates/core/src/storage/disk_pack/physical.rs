//! The physical home under one node store: the disk format v2 run homes.

use super::*;
use laststore::disk_run::home::{RunHome, RunRead, RunReadCosts, RunRoot};
use std::sync::PoisonError;

pub use crate::storage::format::CAS_COLLECTION;

/// The v2 run homes (main and CAS) and their background merge thread.
pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    pub(in crate::storage) cas: Arc<RunHome>,
    pub(in crate::storage) merging: Arc<AtomicBool>,
    pub(in crate::storage) stop: Arc<AtomicBool>,
    pub(in crate::storage) merger: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(in crate::storage) retry_after: Arc<Mutex<Option<std::time::Instant>>>,
    pub(in crate::storage) cas_delete_recovery: Option<super::cas_delete::CasDeleteRecovery>,
}

impl V2Home {
    pub(in crate::storage) fn new(home: Arc<RunHome>, cas: Arc<RunHome>) -> Self {
        Self {
            home,
            cas,
            merging: Arc::default(),
            stop: Arc::default(),
            merger: Mutex::new(None),
            retry_after: Arc::default(),
            cas_delete_recovery: None,
        }
    }

    pub(in crate::storage) fn current_root(&self) -> V2Root {
        V2Root {
            main: self.home.current_root(),
            cas: self.cas.current_root(),
        }
    }

    pub(in crate::storage) fn read(&self, root: &V2Root, write_prep: bool) -> V2Read {
        let mut limits = budget();
        if write_prep {
            limits.keys = 40_000;
        }
        V2Read {
            main: self.home.read_root(&root.main, limits),
            cas: self.cas.read_root(&root.cas, limits),
        }
    }

    pub(in crate::storage) fn identity(&self) -> uuid::Uuid {
        self.home.identity()
    }

    pub(in crate::storage) fn path(&self) -> &Path {
        self.home.path()
    }
}

/// The published roots of the two v2 run homes.
#[derive(Clone)]
pub(in crate::storage) struct V2Root {
    pub(in crate::storage) main: RunRoot,
    pub(in crate::storage) cas: RunRoot,
}

impl V2Root {
    pub(in crate::storage) fn generation(&self) -> u64 {
        self.main.generation() + self.cas.generation()
    }

    pub(in crate::storage) fn frontier(&self) -> u64 {
        self.main.frontier() + self.cas.frontier()
    }
}

/// One v2 read call: the main home and the CAS home at one held root.
pub(in crate::storage) struct V2Read {
    pub(in crate::storage) main: RunRead,
    pub(in crate::storage) cas: RunRead,
}

impl V2Read {
    pub(in crate::storage::disk_pack) fn home_for(&mut self, collection: &str) -> &mut RunRead {
        if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        }
    }

    /// Batch read across the record and file homes while preserving coordinate order.
    pub(in crate::storage) fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coordinates.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.main.get_many(coordinates);
        }
        let (mut records, mut files) = (Vec::new(), Vec::new());
        for (slot, (col, _)) in coordinates.iter().enumerate() {
            if col == CAS_COLLECTION {
                files.push(slot);
            } else {
                records.push(slot);
            }
        }
        let mut out: Vec<Option<laststore::Result<Option<Vec<u8>>>>> =
            (0..coordinates.len()).map(|_| None).collect();
        for (slots, read) in [(records, &mut self.main), (files, &mut self.cas)] {
            if !slots.is_empty() {
                let wanted: Vec<_> = slots.iter().map(|&s| coordinates[s].clone()).collect();
                for (slot, found) in slots.into_iter().zip(read.get_many(&wanted)?) {
                    out[slot] = Some(found);
                }
            }
        }
        let (costs, limit) = (self.costs(), self.main.budget());
        if costs.examined_keys > limit.keys || costs.budget_bytes > limit.bytes {
            return Err(laststore::Error::ReadBudgetExceeded);
        }
        Ok(out
            .into_iter()
            .map(|found| found.expect("each slot is read once"))
            .collect())
    }

    pub(in crate::storage) fn costs(&self) -> RunReadCosts {
        let (main, cas) = (self.main.costs(), self.cas.costs());
        RunReadCosts {
            examined_keys: main.examined_keys + cas.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes,
        }
    }
}

impl PackRuntime {
    pub(crate) fn record_read_stage(&self, _stage: &'static str, _items: usize) {}

    pub(in crate::storage::disk_pack) fn raw_many(
        &self,
        context: &Arc<read::ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let result = (|| {
            context.validate_owner(pending)?;
            let max = if context.owns_write(pending) {
                40_000
            } else {
                10_000
            };
            if coordinates.len() > max {
                return Err(StorageError::ReadResourceLimit {
                    detail: format!("native batch exceeds {max} keys"),
                });
            }
            let mut call = context.call.lock().unwrap_or_else(PoisonError::into_inner);
            context.require_room(&call, coordinates.len() as u64, 0)?;
            let mut out = Vec::with_capacity(coordinates.len());
            for chunk in coordinates.chunks(10_000) {
                out.extend(self.read_chunk(context, &mut call, chunk, pending)?);
            }
            Ok(out)
        })();
        context.remembered(result)
    }

    fn read_chunk(
        &self,
        context: &read::ReadContext,
        call: &mut V2Read,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let (mut out, mut misses) = (vec![None; coordinates.len()], Vec::new());
        for (slot, coord) in coordinates.iter().enumerate() {
            let (col, key) = coord;
            if let Some(rec) = context.overlay_record(pending, col, key) {
                let len = rec.body.as_ref().map_or(0, Vec::len);
                context.account_memory_record(call, key.len(), len)?;
                out[slot] = rec.body.clone();
            } else if let Some(val) = context.reuse_disk_value(call, coord)? {
                out[slot] = val;
            } else {
                misses.push(slot);
            }
        }
        let wanted: Vec<_> = misses.iter().map(|&s| coordinates[s].clone()).collect();
        let results = call.get_many(&wanted).map_err(|e| call_error(e, call))?;
        for (slot, res) in misses.into_iter().zip(results) {
            let val = res.map_err(|e| call_error(e, call))?;
            context.remember_disk_value(&coordinates[slot], val.as_deref());
            out[slot] = val;
        }
        Ok(out)
    }
}

pub(super) fn call_error(error: laststore::Error, call: &V2Read) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (costs, limits) = (call.costs(), call.main.budget());
    StorageError::ReadResourceLimit {
        detail: format!(
            "native call: examined_keys={}/{}, charged_bytes={}/{}",
            costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
        ),
    }
}
