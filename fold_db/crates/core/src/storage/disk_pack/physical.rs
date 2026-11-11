//! The physical home under one node store: the disk format v2 run homes.
//!
//! A node keeps the CAS files (`cas_blobs`) in their own run home so backups
//! can route them separately from the rest of the data.

use super::*;
use laststore::disk_run::home::RangeBounds;
use laststore::disk_run::home::{RunHashRange, RunHome, RunRead, RunRoot};

pub use crate::storage::format::CAS_COLLECTION;

/// Most operations in one v2 commit.
pub(in crate::storage) const V2_BATCH_OPERATIONS: usize = 16_384;

/// The v2 run homes (main and CAS) and their background merge thread.
pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    /// The CAS files only.
    pub(in crate::storage) cas: Arc<RunHome>,
    /// Set while a merge thread runs; at most one runs at a time.
    pub(in crate::storage) merging: Arc<AtomicBool>,
    /// Set when the node store closes; the thread stops after its step.
    pub(in crate::storage) stop: Arc<AtomicBool>,
    pub(in crate::storage) merger: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// After a failed merge, no new merge thread starts before this time.
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

    /// The roots that reads use now.
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

/// The costs that the node store reads from a read call.
#[derive(Clone, Copy)]
pub(in crate::storage) struct PhysicalCosts {
    pub(in crate::storage) examined_keys: u64,
    pub(in crate::storage) budget_bytes: u64,
}

/// The limits of a new read call.
#[derive(Clone, Copy)]
pub(in crate::storage) enum ReadMode {
    Ordinary(ReadBudget),
    WritePreparation,
}

/// One published state of the physical home.
pub(in crate::storage) type PhysicalRoot = V2Root;

/// One read call against one held root.
pub(in crate::storage) type PhysicalRead = V2Read;

impl V2Read {
    pub(in crate::storage::disk_pack) fn legacy_home(&mut self, collection: &str) -> &mut RunRead {
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
        let mut records = Vec::new();
        let mut files = Vec::new();
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
            if slots.is_empty() {
                continue;
            }
            let wanted: Vec<_> = slots
                .iter()
                .map(|slot| coordinates[*slot].clone())
                .collect();
            for (slot, found) in slots.into_iter().zip(read.get_many(&wanted)?) {
                out[slot] = Some(found);
            }
        }
        let costs = self.costs();
        let limit = self.main.budget();
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
    ) -> laststore::Result<PhysicalCursor<'_>> {
        self.legacy_home(collection)
            .hash_range(collection, prefix, bounds)
            .map(PhysicalCursor)
    }

    pub(in crate::storage) fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> laststore::Result<PhysicalCursor<'_>> {
        self.legacy_home(collection)
            .administrative_range(collection, start, end)
            .map(PhysicalCursor)
    }
}

/// One live record of a range page.
pub(in crate::storage) struct PhysicalRecord {
    pub(in crate::storage) key: String,
    pub(in crate::storage) body: Vec<u8>,
}

pub(in crate::storage) struct PhysicalPage {
    pub(in crate::storage) records: Vec<PhysicalRecord>,
    pub(in crate::storage) exhausted: bool,
    pub(in crate::storage) resume: Option<Vec<u8>>,
}

/// An ordered range under one held read call.
pub(in crate::storage) struct PhysicalCursor<'a>(RunHashRange<'a>);

impl PhysicalCursor<'_> {
    pub(in crate::storage) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        self.0.account_memory_record(key_bytes, body_bytes)
    }

    pub(in crate::storage) fn next_page(
        &mut self,
        limit: usize,
    ) -> laststore::Result<PhysicalPage> {
        self.0.next_page(limit).map(page)
    }

    pub(in crate::storage) fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> laststore::Result<PhysicalPage> {
        self.0
            .next_page_yielding(limit, held_rows, keys_per_row)
            .map(page)
    }
}

fn page(page: laststore::disk_run::home::RunRangePage) -> PhysicalPage {
    PhysicalPage {
        records: page
            .records
            .into_iter()
            .map(|(key, body)| PhysicalRecord { key, body })
            .collect(),
        exhausted: page.exhausted,
        resume: page.resume.map(String::into_bytes),
    }
}

impl PackRuntime {
    /// One read call against `root`. A write preparation call permits 40,000
    /// examined keys with the ordinary node limits for the rest.
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
}
