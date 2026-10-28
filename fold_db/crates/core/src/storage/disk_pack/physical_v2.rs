//! The format v2 half of the physical home: the two run homes (the records,
//! and the CAS files apart), their published roots, and one read call across
//! both. A read or a write routes by collection: `cas_blobs` goes to the CAS
//! home, each other collection to the main home.

use super::*;
use laststore::disk_run::home::{RunHome, RunRead, RunRoot};

/// Logical format features that every v2 home has from its creation.
pub(super) const V2_FEATURES: u64 = laststore::disk_run::FEATURE_SCHEMA_RECORD_REFS
    | laststore::disk_run::FEATURE_LARGE_BATCHES
    | laststore::disk_run::FEATURE_ATOM_RECORDS;
/// The collection that holds the CAS files. In format v2 it has its own run home.
pub(super) const CAS_COLLECTION: &str = "cas_blobs";

/// The v2 run homes (main and CAS) and their background merge thread.
pub(super) struct V2Home {
    pub(super) range_order: schema_types::key_comparison::KeyOrderVersion,
    pub(super) home: Arc<RunHome>,
    /// The CAS files only.
    pub(super) cas: Arc<RunHome>,
    /// Set while a merge thread runs; at most one runs at a time.
    pub(super) merging: Arc<AtomicBool>,
    /// Set when the node store closes; the thread stops after its step.
    pub(super) stop: Arc<AtomicBool>,
    pub(super) merger: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// After a failed merge, no new merge thread starts before this time.
    pub(super) retry_after: Arc<Mutex<Option<std::time::Instant>>>,
    pub(super) cas_delete_recovery: Option<super::cas_delete::CasDeleteRecovery>,
}

impl V2Home {
    pub(super) fn new(
        home: Arc<RunHome>,
        cas: Arc<RunHome>,
        range_order: schema_types::key_comparison::KeyOrderVersion,
    ) -> Self {
        Self {
            range_order,
            home,
            cas,
            merging: Arc::default(),
            stop: Arc::default(),
            merger: Mutex::new(None),
            retry_after: Arc::default(),
            cas_delete_recovery: None,
        }
    }
}

impl V2Home {
    /// The roots that reads use now.
    pub(super) fn current_root(&self) -> V2Root {
        V2Root {
            main: self.home.current_root(),
            cas: self.cas.current_root(),
        }
    }
}

/// The published roots of the two v2 run homes. Generation and frontier are
/// the sums of the two homes: each commit or merge raises at least one, and
/// neither home goes back.
#[derive(Clone)]
pub(super) struct V2Root {
    pub(super) main: RunRoot,
    pub(super) cas: RunRoot,
}

impl V2Root {
    pub(super) fn generation(&self) -> u64 {
        self.main.generation() + self.cas.generation()
    }

    pub(super) fn frontier(&self) -> u64 {
        self.main.frontier() + self.cas.frontier()
    }
}

/// One v2 read call: the main home and the CAS home at one held root.
pub(super) struct V2Read {
    pub(super) main: RunRead,
    pub(super) cas: RunRead,
}

impl V2Read {
    pub(super) fn home(&mut self, collection: &str) -> &mut RunRead {
        if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        }
    }

    /// Batch read across both homes; results keep the input order.
    pub(super) fn get_many(
        &mut self,
        coordinates: &[(String, String)],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coordinates.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.main.get_many(coordinates);
        }
        let (cas, main): (Vec<_>, Vec<_>) = coordinates
            .iter()
            .enumerate()
            .partition(|(_, (collection, _))| collection == CAS_COLLECTION);
        let limit = self.main.budget();
        let mut out: Vec<Option<laststore::Result<Option<Vec<u8>>>>> =
            (0..coordinates.len()).map(|_| None).collect();
        for (read, part) in [(&mut self.main, main), (&mut self.cas, cas)] {
            if part.is_empty() {
                continue;
            }
            let wanted: Vec<_> = part.iter().map(|(_, c)| (*c).clone()).collect();
            for ((slot, _), found) in part.iter().zip(read.get_many(&wanted)?) {
                out[*slot] = Some(found);
            }
        }
        // Each home enforces its own limit; the call as a whole has one.
        let (main, cas) = (self.main.costs(), self.cas.costs());
        if main.examined_keys + cas.examined_keys > limit.keys
            || main.budget_bytes + cas.budget_bytes > limit.bytes
        {
            return Err(laststore::Error::ReadBudgetExceeded);
        }
        Ok(out
            .into_iter()
            .map(|found| found.expect("each slot is read once"))
            .collect())
    }
}
