//! The physical home under one node store: a v1 pack home or a v2 run home.
//!
//! The logical half (queue, warm set, schema keys, logical paths) is the same
//! for both formats. It reaches the files only through the types here. Work
//! that exists only in format v1 (parts, maintenance, byte backup, restore,
//! file dependencies) takes the v1 home through [`PackRuntime::v1`], which
//! refuses a v2 home.
//!
//! A v2 node keeps the CAS files (the `cas_blobs` collection) in their own
//! run home, so they are separate files that a backup can send to a
//! different store than the rest of the data.

pub(super) use super::physical_v2::{V2Home, V2Read, V2Root, CAS_COLLECTION, V2_FEATURES};
use super::*;
use laststore::disk_pack::{HashRangeCursor, ReadCosts};
use laststore::disk_run::home::RangeBounds;
use laststore::disk_run::home::RunHashRange;

/// Most operations in one v2 commit; the same ceiling as a v1 native home.
const V2_BATCH_OPERATIONS: usize = 16_384;

/// The v1 pack home and its exclusive snapshot writer.
pub(super) struct V1Home {
    pub(super) home: Arc<DiskHome>,
    pub(super) writer: Mutex<HomeSnapshot>,
    pub(super) read_lease: laststore::disk_pack::lifetime::WriterReadLease,
    pub(super) capture_cleanup: laststore::disk_pack::snapshot::CaptureCleanup,
}

pub(super) enum Physical {
    V1(Box<V1Home>),
    V2(V2Home),
}

impl Physical {
    pub(super) fn identity(&self) -> uuid::Uuid {
        match self {
            Self::V1(v1) => v1.home.descriptor().identity,
            Self::V2(v2) => v2.home.identity(),
        }
    }

    pub(super) fn path(&self) -> &Path {
        match self {
            Self::V1(v1) => v1.home.path(),
            Self::V2(v2) => v2.home.path(),
        }
    }

    /// Release idle capture holds after a write; v2 has none.
    pub(super) fn release_idle(&self) {
        if let Self::V1(v1) = self {
            v1.capture_cleanup.release_idle();
        }
    }
}

/// One published state of the physical home.
#[derive(Clone)]
pub(super) enum PhysicalRoot {
    V1(Current),
    V2(V2Root),
}

impl PhysicalRoot {
    pub(super) fn generation(&self) -> u64 {
        match self {
            Self::V1(current) => current.generation,
            Self::V2(root) => root.generation(),
        }
    }

    pub(super) fn frontier(&self) -> u64 {
        match self {
            Self::V1(current) => current.frontier,
            Self::V2(root) => root.frontier(),
        }
    }

    pub(super) fn batch_operation_limit(&self) -> usize {
        match self {
            Self::V1(current) => current.batch_operation_limit(),
            Self::V2(_) => V2_BATCH_OPERATIONS,
        }
    }

    pub(super) fn required_features(&self) -> u64 {
        match self {
            Self::V1(current) => current.required_features,
            Self::V2(_) => V2_FEATURES,
        }
    }
}

pub(super) fn not_in_v2() -> StorageError {
    StorageError::InvalidOperation("this operation is not in format v2".into())
}

/// The costs that the node store reads from either read call.
#[derive(Clone, Copy)]
pub(super) struct PhysicalCosts {
    pub(super) examined_keys: u64,
    pub(super) budget_bytes: u64,
    pub(super) payload_blocks: u64,
    pub(super) metadata_pages: u64,
}

impl From<ReadCosts> for PhysicalCosts {
    fn from(costs: ReadCosts) -> Self {
        Self {
            examined_keys: costs.examined_keys,
            budget_bytes: costs.budget_bytes,
            payload_blocks: costs.payload_blocks,
            metadata_pages: costs.directory_pages
                + costs.locator_pages
                + costs.overflow_pages
                + costs.map_pages,
        }
    }
}

/// The limits of a new read call.
#[derive(Clone, Copy)]
pub(super) enum ReadMode {
    Ordinary(ReadBudget),
    WritePreparation,
}

/// One read call against one held root of either format.
pub(super) enum PhysicalRead {
    V1(Box<laststore::disk_pack::home::HomeRead>),
    V2(Box<V2Read>),
}

impl PhysicalRead {
    pub(super) fn costs(&self) -> PhysicalCosts {
        match self {
            Self::V1(read) => read.costs().into(),
            Self::V2(read) => {
                let (main, cas) = (read.main.costs(), read.cas.costs());
                PhysicalCosts {
                    examined_keys: main.examined_keys + cas.examined_keys,
                    budget_bytes: main.budget_bytes + cas.budget_bytes,
                    payload_blocks: 0,
                    metadata_pages: 0,
                }
            }
        }
    }

    pub(super) fn budget(&self) -> ReadBudget {
        match self {
            Self::V1(read) => read.budget(),
            Self::V2(read) => read.main.budget(),
        }
    }

    pub(super) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        match self {
            Self::V1(read) => read.account_memory_record(key_bytes, body_bytes),
            Self::V2(read) => read.main.account_memory_record(key_bytes, body_bytes),
        }
    }

    pub(super) fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        match self {
            Self::V1(read) => read.get_many(coordinates),
            Self::V2(read) => read.get_many(coordinates),
        }
    }

    pub(super) fn hash_range(
        &mut self,
        collection: &str,
        prefix: &str,
        bounds: RangeBounds,
    ) -> laststore::Result<PhysicalCursor<'_>> {
        match self {
            Self::V1(read) => read
                .hash_range(collection, prefix, bounds)
                .map(PhysicalCursor::V1),
            Self::V2(read) => read
                .home(collection)
                .hash_range(collection, prefix, bounds)
                .map(PhysicalCursor::V2),
        }
    }

    pub(super) fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> laststore::Result<PhysicalCursor<'_>> {
        match self {
            Self::V1(read) => read
                .administrative_range(collection, start, end)
                .map(PhysicalCursor::V1),
            Self::V2(read) => read
                .home(collection)
                .administrative_range(collection, start, end)
                .map(PhysicalCursor::V2),
        }
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

/// An ordered range under one held read call of either format.
pub(super) enum PhysicalCursor<'a> {
    V1(HashRangeCursor<'a>),
    V2(RunHashRange<'a>),
}

impl PhysicalCursor<'_> {
    pub(super) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        match self {
            Self::V1(cursor) => cursor.account_memory_record(key_bytes, body_bytes),
            Self::V2(cursor) => cursor.account_memory_record(key_bytes, body_bytes),
        }
    }

    pub(super) fn next_page(&mut self, limit: usize) -> laststore::Result<PhysicalPage> {
        match self {
            Self::V1(cursor) => cursor.next_page(limit).map(v1_page),
            Self::V2(cursor) => cursor.next_page(limit).map(v2_page),
        }
    }

    pub(super) fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> laststore::Result<PhysicalPage> {
        match self {
            Self::V1(cursor) => cursor
                .next_page_yielding(limit, held_rows, keys_per_row)
                .map(v1_page),
            Self::V2(cursor) => cursor
                .next_page_yielding(limit, held_rows, keys_per_row)
                .map(v2_page),
        }
    }
}

fn v1_page(page: laststore::disk_pack::RangePage) -> PhysicalPage {
    PhysicalPage {
        records: page
            .records
            .into_iter()
            .map(|record| PhysicalRecord {
                key: record.key,
                body: record.body,
            })
            .collect(),
        exhausted: page.exhausted,
        resume: page.resume,
    }
}

fn v2_page(page: laststore::disk_run::home::RunRangePage) -> PhysicalPage {
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
    /// The v1 pack home, or a refusal for work that does not exist in format v2.
    pub(super) fn v1(&self) -> StorageResult<&V1Home> {
        match &self.physical {
            Physical::V1(v1) => Ok(v1.as_ref()),
            Physical::V2(_) => Err(not_in_v2()),
        }
    }

    /// One read call against `root`. A write preparation call permits 40,000
    /// examined keys with the ordinary node limits for the rest.
    pub(super) fn physical_read(
        &self,
        root: &PhysicalRoot,
        mode: ReadMode,
    ) -> StorageResult<PhysicalRead> {
        match (&self.physical, root, mode) {
            (Physical::V1(v1), PhysicalRoot::V1(current), ReadMode::WritePreparation) => v1
                .home
                .read_for_write_preparation(current.clone(), &v1.read_lease)
                .map(|read| PhysicalRead::V1(Box::new(read)))
                .map_err(map),
            (Physical::V1(v1), PhysicalRoot::V1(current), ReadMode::Ordinary(limits)) => v1
                .home
                .read_from_writer(current.clone(), limits, &v1.read_lease)
                .map(|read| PhysicalRead::V1(Box::new(read)))
                .map_err(map),
            (Physical::V2(v2), PhysicalRoot::V2(root), mode) => {
                let limits = match mode {
                    ReadMode::Ordinary(limits) => limits,
                    ReadMode::WritePreparation => ReadBudget {
                        keys: 40_000,
                        ..budget()
                    },
                };
                Ok(PhysicalRead::V2(Box::new(V2Read {
                    main: v2.home.read_root(&root.main, limits),
                    cas: v2.cas.read_root(&root.cas, limits),
                })))
            }
            _ => Err(StorageError::BackendError(
                "native root and physical home use different formats".into(),
            )),
        }
    }

    pub(super) fn home_identity(&self) -> uuid::Uuid {
        self.physical.identity()
    }

    /// True when this node store uses a v2 run home.
    pub fn is_disk_format_v2(&self) -> bool {
        matches!(self.physical, Physical::V2(_))
    }
}
