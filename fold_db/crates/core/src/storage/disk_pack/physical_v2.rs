//! The format v2 half of the physical home: the two run homes (the records,
//! and the CAS files apart), their published roots, and one read call across
//! both. A read or a write routes by collection: `cas_blobs` goes to the CAS
//! home, each other collection to the main home.

use super::shared_layout::{Lane, Scope, SharedHome};
use super::*;
use laststore::disk_run::home::{RunHome, RunRead, RunRoot};

pub use crate::storage::format::CAS_COLLECTION;

/// The v2 run homes (main and CAS) and their background merge thread.
pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    /// The CAS files only.
    pub(in crate::storage) cas: Arc<RunHome>,
    pub(super) shared: Option<SharedHome>,
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
}

impl V2Home {
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
}

/// The published roots of the two v2 run homes. Generation and frontier are
/// the sums of the two homes: each commit or merge raises at least one, and
/// neither home goes back.
#[derive(Clone)]
pub(in crate::storage) struct V2Root {
    pub(in crate::storage) main: RunRoot,
    pub(in crate::storage) cas: RunRoot,
    pub(super) private: Option<RunRoot>,
    pub(super) scope: Option<Arc<Scope>>,
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
    pub(super) private: Option<RunRead>,
    pub(super) scope: Option<Arc<Scope>>,
}

impl V2Read {
    pub(super) fn legacy_home(&mut self, collection: &str) -> &mut RunRead {
        if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        }
    }

    pub(super) fn lane(&mut self, lane: Lane) -> &mut RunRead {
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
}
