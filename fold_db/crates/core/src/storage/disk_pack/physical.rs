//! The physical home under one node store: the disk format v2 run homes.
//!
//! A node keeps the CAS files (`cas_blobs`) in their own run home so backups
//! can route them separately from the rest of the data.

use super::shared_layout::range::MergedRange;
use super::shared_layout::{Lane, Scope, SharedHome};
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
    pub(in crate::storage::disk_pack) shared: Option<SharedHome>,
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
            shared: None,
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
            private: self
                .shared
                .as_ref()
                .map(|shared| shared.private.current_root()),
            scope: self
                .shared
                .as_ref()
                .map(|shared| Arc::clone(&shared.scope.read().expect("shared disk scope"))),
        }
    }

    pub(in crate::storage) fn private_home(&self) -> Option<&Arc<RunHome>> {
        self.shared.as_ref().map(|shared| &shared.private)
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
    pub(in crate::storage::disk_pack) private: Option<RunRoot>,
    pub(in crate::storage::disk_pack) scope: Option<Arc<Scope>>,
}

impl V2Root {
    pub(in crate::storage) fn generation(&self) -> u64 {
        self.main.generation()
            + self.cas.generation()
            + self.private.as_ref().map_or(0, RunRoot::generation)
    }

    pub(in crate::storage) fn frontier(&self) -> u64 {
        self.main.frontier()
            + self.cas.frontier()
            + self.private.as_ref().map_or(0, RunRoot::frontier)
    }
}

/// One v2 read call: the main home and the CAS home at one held root.
pub(in crate::storage) struct V2Read {
    pub(in crate::storage) main: RunRead,
    pub(in crate::storage) cas: RunRead,
    pub(in crate::storage::disk_pack) private: Option<RunRead>,
    pub(in crate::storage::disk_pack) scope: Option<Arc<Scope>>,
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

    pub(in crate::storage::disk_pack) fn lane(&mut self, lane: Lane) -> &mut RunRead {
        match lane {
            Lane::Records => &mut self.main,
            Lane::Files => &mut self.cas,
            Lane::Private => self.private.as_mut().expect("shared private reader"),
        }
    }

    /// Batch read across physical lanes while preserving coordinate order.
    pub(in crate::storage) fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if self.scope.is_none() && !coordinates.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.main.get_many(coordinates);
        }
        let mut batches = [Vec::new(), Vec::new(), Vec::new()];
        for (slot, (collection, key)) in coordinates.iter().enumerate() {
            let lane = self.scope.as_ref().map_or_else(
                || {
                    if collection == CAS_COLLECTION {
                        Lane::Files
                    } else {
                        Lane::Records
                    }
                },
                |scope| scope.lane(collection, key),
            );
            batches[lane as usize].push(slot);
        }
        let mut out: Vec<Option<laststore::Result<Option<Vec<u8>>>>> =
            (0..coordinates.len()).map(|_| None).collect();
        for (lane, slots) in [Lane::Records, Lane::Files, Lane::Private]
            .into_iter()
            .zip(batches)
        {
            if slots.is_empty() {
                continue;
            }
            let wanted: Vec<_> = slots
                .iter()
                .map(|slot| coordinates[*slot].clone())
                .collect();
            for (slot, found) in slots.into_iter().zip(self.lane(lane).get_many(&wanted)?) {
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
        let private = self
            .private
            .as_ref()
            .map(RunRead::costs)
            .unwrap_or_default();
        PhysicalCosts {
            examined_keys: main.examined_keys + cas.examined_keys + private.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes + private.budget_bytes,
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
        let read = match self.scope.as_ref() {
            Some(scope)
                if matches!(
                    collection,
                    crate::storage::format::RECORDS_COLLECTION | CAS_COLLECTION
                ) =>
            {
                let lane = scope.lane(collection, prefix);
                self.lane(lane)
            }
            _ => self.legacy_home(collection),
        };
        read.hash_range(collection, prefix, bounds)
            .map(PhysicalCursor::Single)
    }

    pub(in crate::storage) fn administrative_range(
        &mut self,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> laststore::Result<PhysicalCursor<'_>> {
        let Some(scope) = &self.scope else {
            return self
                .legacy_home(collection)
                .administrative_range(collection, start, end)
                .map(PhysicalCursor::Single);
        };
        if let Some(lane) = scope.bounded_range_lane(collection, start, end) {
            return self
                .lane(lane)
                .administrative_range(collection, start, end)
                .map(PhysicalCursor::Single);
        }
        let public = if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        };
        let private = self.private.as_mut().expect("shared private reader");
        let public = public.administrative_range(collection, start, end)?;
        let private = private.administrative_range(collection, start, end)?;
        Ok(PhysicalCursor::Merged(Box::new(MergedRange::new(
            public, private,
        ))))
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
pub(in crate::storage) enum PhysicalCursor<'a> {
    Single(RunHashRange<'a>),
    Merged(Box<MergedRange<'a>>),
}

impl PhysicalCursor<'_> {
    pub(in crate::storage) fn account_memory_record(
        &mut self,
        key_bytes: usize,
        body_bytes: usize,
    ) -> laststore::Result<()> {
        match self {
            Self::Single(cursor) => cursor.account_memory_record(key_bytes, body_bytes),
            Self::Merged(cursor) => cursor.account_memory_record(key_bytes, body_bytes),
        }
    }

    pub(in crate::storage) fn next_page(
        &mut self,
        limit: usize,
    ) -> laststore::Result<PhysicalPage> {
        match self {
            Self::Single(cursor) => cursor.next_page(limit),
            Self::Merged(cursor) => cursor.page(limit, None),
        }
        .map(page)
    }

    pub(in crate::storage) fn next_page_yielding(
        &mut self,
        limit: usize,
        held_rows: usize,
        keys_per_row: usize,
    ) -> laststore::Result<PhysicalPage> {
        match self {
            Self::Single(cursor) => cursor.next_page_yielding(limit, held_rows, keys_per_row),
            Self::Merged(cursor) => cursor.page(limit, Some((held_rows, keys_per_row))),
        }
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
            private: self
                .physical
                .shared
                .as_ref()
                .zip(root.private.as_ref())
                .map(|(shared, root)| shared.private.read_root(root, limits)),
            scope: root.scope.clone(),
        }
    }

    pub(in crate::storage) fn home_identity(&self) -> uuid::Uuid {
        self.physical.identity()
    }
}
