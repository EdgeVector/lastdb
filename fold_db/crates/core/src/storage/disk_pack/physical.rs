//! Physical v2 run homes, status/diagnostics, and background run merges.

use super::read::Coordinate;
use super::*;
use crate::resident::CoreWarmByteStats;
use laststore::disk_run::home::{RunHome, RunHomeDiagnostics, RunRead, RunReadCosts, RunRoot};
use std::sync::{TryLockError, Weak};
use std::time::{Duration, Instant};

pub use crate::storage::format::CAS_COLLECTION;

type MergeCtl = (AtomicBool, AtomicBool, Mutex<Option<Instant>>);

#[derive(serde::Serialize)]
pub struct NativeDiskStatus {
    pub cleanup: NativeCleanupStatus,
    pub generation: u64,
    pub frontier: u64,
    pub accepted_batch: bool,
    pub accepted_batches: usize,
    pub accepted_sequence: u64,
    pub completed_sequence: u64,
    pub writer_failed: bool,
    pub cas_delete_recovery: Option<CasDeleteRecovery>,
    pub warm_records: usize,
    pub core_warm_held: usize,
    pub core_warm_hits: u64,
    pub core_warm_misses: u64,
    pub core_warm_purged: u64,
    pub core_warm_bytes: CoreWarmByteStats,
}

#[derive(serde::Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeMemoryPurgeReport {
    pub records_purged: u64,
    pub target_met: bool,
}

pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    pub(in crate::storage) cas: Arc<RunHome>,
    merge: (Arc<MergeCtl>, Mutex<Option<std::thread::JoinHandle<()>>>),
    pub(in crate::storage) cas_delete_recovery: Option<CasDeleteRecovery>,
}

impl V2Home {
    pub(in crate::storage) fn new(home: Arc<RunHome>, cas: Arc<RunHome>) -> Self {
        Self {
            home,
            cas,
            merge: (Arc::default(), Mutex::new(None)),
            cas_delete_recovery: None,
        }
    }

    pub(in crate::storage) fn current_root(&self) -> V2Root {
        V2Root {
            main: self.home.current_root(),
            cas: self.cas.current_root(),
        }
    }

    pub(super) fn read(&self, root: &V2Root, write_prep: bool) -> V2Read {
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

impl Drop for V2Home {
    fn drop(&mut self) {
        self.merge.0 .1.store(true, Ordering::Release);
        if let Ok(Some(handle)) = self.merge.1.get_mut().map(Option::take) {
            let _ = handle.join();
        }
    }
}

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

pub(in crate::storage::disk_pack) struct V2Read {
    pub(super) main: RunRead,
    pub(super) cas: RunRead,
}

impl V2Read {
    pub(super) fn home_for(&mut self, collection: &str) -> &mut RunRead {
        if collection == CAS_COLLECTION {
            &mut self.cas
        } else {
            &mut self.main
        }
    }

    pub(super) fn get_many(
        &mut self,
        coordinates: &[Coordinate],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coordinates.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.main.get_many(coordinates);
        }
        let mut out: Vec<_> = (0..coordinates.len()).map(|_| Ok(None)).collect();
        for (is_cas, read) in [(false, &mut self.main), (true, &mut self.cas)] {
            let idx: Vec<_> = (0..coordinates.len())
                .filter(|&i| (coordinates[i].0 == CAS_COLLECTION) == is_cas)
                .collect();
            if !idx.is_empty() {
                let wanted: Vec<_> = idx.iter().map(|&i| coordinates[i].clone()).collect();
                for (slot, found) in idx.into_iter().zip(read.get_many(&wanted)?) {
                    out[slot] = found;
                }
            }
        }
        let (costs, limit) = (self.costs(), self.main.budget());
        if costs.examined_keys > limit.keys || costs.budget_bytes > limit.bytes {
            return Err(laststore::Error::ReadBudgetExceeded);
        }
        Ok(out)
    }

    pub(super) fn costs(&self) -> RunReadCosts {
        let (main, cas) = (self.main.costs(), self.cas.costs());
        RunReadCosts {
            examined_keys: main.examined_keys + cas.examined_keys,
            budget_bytes: main.budget_bytes + cas.budget_bytes,
        }
    }
}

impl PackRuntime {
    pub fn status_native(&self) -> NativeDiskStatus {
        let q = self.queue.lock().expect("native accepted queue");
        let (generation, frontier) = self.view.try_read().map_or_else(
            |_| {
                let r = self.physical.current_root();
                (r.generation(), r.frontier())
            },
            |v| (v.0.generation(), v.0.frontier()),
        );
        let accepted = q.requests.len();
        let (core, core_warm_bytes) = {
            let mut c = self.core.lock().expect("native core status");
            (c.stats(), c.byte_stats())
        };
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation,
            frontier,
            accepted_batch: accepted > 0,
            accepted_batches: accepted,
            accepted_sequence: q.next_sequence,
            completed_sequence: *self.completed.borrow(),
            writer_failed: self.requires_recovery(),
            cas_delete_recovery: self.physical.cas_delete_recovery,
            warm_records: core.records,
            core_warm_held: core.held,
            core_warm_hits: core.hits,
            core_warm_misses: core.misses,
            core_warm_purged: core.purged,
            core_warm_bytes,
        }
    }

    pub async fn diagnostics_native(
        &self,
        include_inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        if include_inventory && !self.queue.lock().expect("native queue").requests.is_empty() {
            return Err(StorageError::invalid(
                "storage inventory requires a checkpoint without accepted writes",
            ));
        }
        let (main, cas) = (
            Arc::clone(&self.physical.home),
            Arc::clone(&self.physical.cas),
        );
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            Ok(Some(NativeStorageDiagnostics {
                main: main.diagnostics(include_inventory, deadline).map_err(map)?,
                cas: cas.diagnostics(include_inventory, deadline).map_err(map)?,
            }))
        })
        .await?
    }

    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let err = || StorageError::backend("native warm set lock is unavailable");
        let _view = self.view.read().map_err(|_| err())?;
        let (purged, _, after) = self
            .core
            .lock()
            .map_err(|_| err())?
            .purge_clean_to_bytes(usize::try_from(target_bytes).unwrap_or(usize::MAX));
        Ok(NativeMemoryPurgeReport {
            records_purged: purged as u64,
            target_met: after as u64 <= target_bytes,
        })
    }

    pub(super) fn spawn_merges(&self) {
        let (v2, ctl) = (&self.physical, &self.physical.merge.0);
        if (!v2.home.needs_merge() && !v2.cas.needs_merge())
            || ctl
                .2
                .lock()
                .expect("disk run merge retry")
                .is_some_and(|at| Instant::now() < at)
            || ctl.0.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
        let (view, ctl_c, policy) = (
            Arc::clone(&self.view),
            Arc::clone(ctl),
            self.space_policy.clone(),
        );
        match std::thread::Builder::new()
            .name("lastdb-run-merge".into())
            .spawn(move || merge_until_settled(&homes, &view, &ctl_c, &policy))
        {
            Ok(h) => {
                if let Some(prev) = v2.merge.1.lock().expect("disk run merge thread").replace(h) {
                    let _ = prev.join();
                }
            }
            Err(error) => {
                ctl.0.store(false, Ordering::Release);
                tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
            }
        }
    }

    pub(in crate::storage) fn adopt_merged_root(&self) -> StorageResult<()> {
        match self.view.try_write() {
            Ok(mut view) => drop(adopt(&mut view, self.physical.current_root())),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Poisoned(_)) => {
                self.failed.store(true, Ordering::Release);
                let msg =
                    "native read view lock is poisoned; restart the process to reopen the home";
                tracing::error!(msg);
                return Err(StorageError::backend(msg));
            }
        }
        Ok(())
    }
}

fn adopt(view: &mut NativeReadView, latest: V2Root) -> Option<V2Root> {
    (view.0.frontier() == latest.frontier() && view.0.generation() < latest.generation())
        .then(|| std::mem::replace(&mut view.0, latest))
}

fn merge_until_settled(
    homes: &[Weak<RunHome>; 2],
    view: &RwLock<NativeReadView>,
    ctl: &MergeCtl,
    policy: &SpacePolicy,
) {
    let cas_policy = SpacePolicy::default();
    loop {
        'steps: while !ctl.1.load(Ordering::Acquire) {
            let mut merged = false;
            for (idx, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                match home.merge_step_with_space(if idx == 0 { policy } else { &cas_policy }) {
                    Ok(true) => {
                        drop(home);
                        if let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) {
                            let latest = V2Root {
                                main: main.current_root(),
                                cas: cas.current_root(),
                            };
                            drop(view.write().ok().and_then(|mut v| adopt(&mut v, latest)));
                        }
                        let (until, mut limit) = (Instant::now() + Duration::from_secs(2), None);
                        while let Some(h) = weak.upgrade() {
                            let cap =
                                *limit.get_or_insert_with(|| (h.run_totals().2 / 16).max(1 << 20));
                            if h.retired_bytes() <= cap
                                || ctl.1.load(Ordering::Acquire)
                                || Instant::now() >= until
                            {
                                break;
                            }
                            drop(h);
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        merged = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, "disk run merge failed");
                        *ctl.2.lock().expect("disk run merge retry") =
                            Some(Instant::now() + Duration::from_secs(30));
                        ctl.0.store(false, Ordering::Release);
                        return;
                    }
                }
            }
            if !merged {
                break;
            }
        }
        ctl.0.store(false, Ordering::Release);
        let needed = !ctl.1.load(Ordering::Acquire)
            && homes
                .iter()
                .filter_map(Weak::upgrade)
                .any(|h| h.needs_merge());
        if !needed || ctl.0.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}
