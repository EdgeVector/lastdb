//! The physical home under one node store: the disk format v2 run homes.
//!
//! The logical half (queue, warm set, schema keys, logical paths) reaches the
//! files only through the types here.
//!
//! A node keeps the CAS files (the `cas_blobs` collection) in their own run
//! home, so they are separate files that a backup can send to a different
//! store than the rest of the data.

pub(super) use super::physical_v2::{V2Home, V2Read, V2Root, CAS_COLLECTION, V2_FEATURES};
use super::*;
use laststore::disk_run::home::RangeBounds;
use laststore::disk_run::home::RunHashRange;

/// Most operations in one v2 commit.
pub(super) const V2_BATCH_OPERATIONS: usize = 16_384;

impl V2Home {
    pub(super) fn identity(&self) -> uuid::Uuid {
        self.home.identity()
    }

    pub(super) fn path(&self) -> &Path {
        self.home.path()
    }
}

/// The costs that the node store reads from a read call.
#[derive(Clone, Copy)]
pub(super) struct PhysicalCosts {
    pub(super) examined_keys: u64,
    pub(super) budget_bytes: u64,
    pub(super) payload_blocks: u64,
    pub(super) metadata_pages: u64,
}

/// The limits of a new read call.
#[derive(Clone, Copy)]
pub(super) enum ReadMode {
    Ordinary(ReadBudget),
    WritePreparation,
}

/// One published state of the physical home.
pub(super) type PhysicalRoot = V2Root;

/// One read call against one held root.
pub(super) type PhysicalRead = V2Read;

impl V2Read {
    pub(super) fn costs(&self) -> PhysicalCosts {
        let (main, cas) = (self.main.costs(), self.cas.costs());
        PhysicalCosts {
            examined_keys: main.examined_keys + cas.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes,
            payload_blocks: 0,
            metadata_pages: 0,
        }
    }

    pub(super) fn budget(&self) -> ReadBudget {
        self.main.budget()
    }

    pub(super) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        self.main.account_memory_record(key_bytes, body_bytes)
    }

    pub(super) fn hash_range(
        &mut self,
        collection: &str,
        prefix: &str,
        bounds: RangeBounds,
    ) -> laststore::Result<PhysicalCursor<'_>> {
        self.home(collection)
            .hash_range(collection, prefix, bounds)
            .map(PhysicalCursor)
    }

    pub(super) fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> laststore::Result<PhysicalCursor<'_>> {
        self.home(collection)
            .administrative_range(collection, start, end)
            .map(PhysicalCursor)
    }
}

/// One live record of a range page.
pub(super) struct PhysicalRecord {
    pub(super) key: String,
    pub(super) body: Vec<u8>,
}

pub(super) struct PhysicalPage {
    pub(super) records: Vec<PhysicalRecord>,
    pub(super) exhausted: bool,
    pub(super) resume: Option<Vec<u8>>,
}

/// An ordered range under one held read call.
pub(super) struct PhysicalCursor<'a>(RunHashRange<'a>);

impl PhysicalCursor<'_> {
    pub(super) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        self.0.account_memory_record(key_bytes, body_bytes)
    }

    pub(super) fn next_page(&mut self, limit: usize) -> laststore::Result<PhysicalPage> {
        self.0.next_page(limit).map(page)
    }

    pub(super) fn next_page_yielding(
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
    pub(super) fn physical_read(&self, root: &PhysicalRoot, mode: ReadMode) -> PhysicalRead {
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

    pub(super) fn home_identity(&self) -> uuid::Uuid {
        self.physical.identity()
    }
}
