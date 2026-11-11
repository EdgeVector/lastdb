//! The physical home under one node store: the disk format v2 run homes.

use super::*;
pub(in crate::storage) use laststore::disk_run::home::RunReadCosts as PhysicalCosts;
use laststore::disk_run::home::{RangeBounds, RunHashRange, RunHome, RunRead, RunRoot};

pub use crate::storage::format::CAS_COLLECTION;

pub(in crate::storage) const V2_BATCH_OPERATIONS: usize = 16_384;

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

#[derive(Clone, Copy)]
pub(in crate::storage) enum ReadMode {
    Ordinary(ReadBudget),
    WritePreparation,
}

pub(in crate::storage) type PhysicalRoot = V2Root;
pub(in crate::storage) type PhysicalRead = V2Read;

impl V2Read {
    fn home_for(&mut self, collection: &str) -> &mut RunRead {
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
        for (slot, (collection, _)) in coordinates.iter().enumerate() {
            if collection == CAS_COLLECTION {
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

    pub(in crate::storage) fn costs(&self) -> PhysicalCosts {
        let (main, cas) = (self.main.costs(), self.cas.costs());
        PhysicalCosts {
            examined_keys: main.examined_keys + cas.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes,
        }
    }

    pub(in crate::storage) fn budget(&self) -> ReadBudget {
        self.main.budget()
    }

    pub(in crate::storage) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        self.main.account_memory_record(key_bytes, body_bytes)
    }

    pub(in crate::storage) fn hash_range(
        &mut self,
        collection: &str,
        prefix: &str,
        bounds: RangeBounds,
    ) -> laststore::Result<RunHashRange<'_>> {
        self.home_for(collection)
            .hash_range(collection, prefix, bounds)
    }

    pub(in crate::storage) fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> laststore::Result<RunHashRange<'_>> {
        self.home_for(collection)
            .administrative_range(collection, start, end)
    }
}

impl PackRuntime {
    pub(in crate::storage) fn physical_read(
        &self,
        root: &PhysicalRoot,
        mode: ReadMode,
    ) -> PhysicalRead {
        let limits = match mode {
            ReadMode::Ordinary(limits) => limits,
            ReadMode::WritePreparation => ReadBudget {
                keys: 40_000,
                ..budget()
            },
        };
        V2Read {
            main: self.physical.home.read_root(&root.main, limits),
            cas: self.physical.cas.read_root(&root.cas, limits),
        }
    }

    pub(in crate::storage) fn home_identity(&self) -> uuid::Uuid {
        self.physical.identity()
    }

    pub(crate) fn record_read_stage(&self, stage: &'static str, items: usize) {
        if let Some(context) = read::ReadContext::captured(self) {
            context.record_stage(stage, items);
        }
    }

    pub(in crate::storage::disk_pack) fn raw_get(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        key: &str,
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .raw_many(
                context,
                &[(collection.to_string(), key.to_string())],
                pending,
            )?
            .pop()
            .expect("one native coordinate"))
    }

    pub(in crate::storage::disk_pack) fn raw_many(
        &self,
        context: &Arc<read::ReadContext>,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        trace_families(coordinates);
        let result = (|| {
            context.validate_owner(pending)?;
            let maximum = if context.owns_write(pending) {
                40_000
            } else {
                10_000
            };
            if coordinates.len() > maximum {
                return Err(StorageError::ReadResourceLimit {
                    detail: format!("native batch exceeds {maximum} keys"),
                });
            }
            let mut call = context
                .call
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        call: &mut PhysicalRead,
        coordinates: &[(String, String)],
        pending: Option<&Arc<transaction::PendingBatch>>,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let (mut out, mut misses) = (vec![None; coordinates.len()], Vec::new());
        for (slot, coordinate) in coordinates.iter().enumerate() {
            let (collection, key) = coordinate;
            if let Some(record) = context.overlay_record(pending, collection, key) {
                context.account_memory_record(
                    call,
                    key.len(),
                    record.body.as_ref().map_or(0, Vec::len),
                )?;
                out[slot] = record.body.clone();
            } else if let Some(value) = context.reuse_disk_value(call, coordinate)? {
                out[slot] = value;
            } else {
                misses.push(slot);
            }
        }
        let addresses: Vec<_> = misses.iter().map(|&s| coordinates[s].clone()).collect();
        let results = call.get_many(&addresses).map_err(|e| call_error(e, call))?;
        for (slot, result) in misses.into_iter().zip(results) {
            let value = result.map_err(|e| call_error(e, call))?;
            context.remember_disk_value(&coordinates[slot], value.as_deref());
            out[slot] = value;
        }
        Ok(out)
    }
}

pub(super) fn call_error(error: laststore::Error, call: &PhysicalRead) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let (costs, limits) = (call.costs(), call.budget());
    StorageError::ReadResourceLimit {
        detail: format!(
            "native call: examined_keys={}/{}, charged_bytes={}/{}",
            costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
        ),
    }
}

fn trace_families(coordinates: &[(String, String)]) {
    if !tracing::enabled!(target: "lastdb::native_read_family", tracing::Level::DEBUG) {
        return;
    }
    let mut counts = std::collections::BTreeMap::new();
    for (collection, key) in coordinates {
        let family = match collection.as_str() {
            crate::storage::format::SCHEMAS_NAMESPACE => "schema",
            crate::storage::format::SCHEMA_STATES_NAMESPACE => "schema_state",
            _ => match native_routing::decode(key)
                .ok()
                .and_then(|raw| String::from_utf8(raw).ok())
                .as_deref()
                .and_then(|k| k.split([':', '\0']).next())
            {
                Some("rk") => "record",
                Some("ak") => "alternate_key",
                _ => "other",
            },
        };
        *counts.entry(family).or_insert(0usize) += 1;
    }
    for (family, keys) in counts {
        tracing::debug!(target: "lastdb::native_read_family", family, keys, "native exact read families");
    }
}
