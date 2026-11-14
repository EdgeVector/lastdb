//! Physical v2 run homes, status reporting, and background run merges.

use super::read::{unpoison, Coordinate};
use super::*;
use crate::resident::CoreWarmByteStats;
use laststore::disk_run::home::{RunHome, RunRead, RunReadCosts, RunRoot};
use std::path::Path;
use std::sync::Weak;
use std::time::{Duration, Instant};

pub(crate) use crate::storage::format::CAS_COLLECTION;

#[derive(Default)]
struct MergeControl {
    running: AtomicBool,
    stopped: AtomicBool,
    backoff_until: Mutex<Option<Instant>>,
}

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

pub(in crate::storage) struct V2Home {
    pub(in crate::storage) home: Arc<RunHome>,
    pub(in crate::storage) cas: Arc<RunHome>,
    merge_ctl: Arc<MergeControl>,
    merge_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(in crate::storage) cas_delete_recovery: Option<CasDeleteRecovery>,
}

impl V2Home {
    pub(in crate::storage) fn new(home: Arc<RunHome>, cas: Arc<RunHome>) -> Self {
        Self {
            home,
            cas,
            merge_ctl: Arc::default(),
            merge_handle: Mutex::new(None),
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
        let mut lim = budget();
        if write_prep {
            lim.keys = 40_000;
        }
        V2Read {
            main: self.home.read_root(&root.main, lim),
            cas: self.cas.read_root(&root.cas, lim),
        }
    }

    pub(in crate::storage) fn path(&self) -> &Path {
        self.home.path()
    }
}

impl Drop for V2Home {
    fn drop(&mut self) {
        self.merge_ctl.stopped.store(true, Ordering::Release);
        if let Ok(Some(handle)) = self.merge_handle.get_mut().map(Option::take) {
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
        coords: &[Coordinate],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coords.iter().any(|(col, _)| col == CAS_COLLECTION) {
            return self.main.get_many_bounded(coords, u64::MAX);
        }
        let mut out: Vec<_> = (0..coords.len()).map(|_| Ok(None)).collect();
        for (is_cas, read) in [(false, &mut self.main), (true, &mut self.cas)] {
            let indices: Vec<_> = (0..coords.len())
                .filter(|&i| (coords[i].0 == CAS_COLLECTION) == is_cas)
                .collect();
            if !indices.is_empty() {
                let requests: Vec<_> = indices.iter().map(|&i| coords[i].clone()).collect();
                for (slot, res) in indices
                    .into_iter()
                    .zip(read.get_many_bounded(&requests, u64::MAX)?)
                {
                    out[slot] = res;
                }
            }
        }
        let (costs, limit) = (self.costs(), self.main.budget());
        (costs.examined_keys <= limit.keys && costs.budget_bytes <= limit.bytes)
            .then_some(out)
            .ok_or(laststore::Error::ReadBudgetExceeded)
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
    pub fn subscribe_durable_writes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.completed.subscribe()
    }

    pub fn status_native(&self) -> NativeDiskStatus {
        let queue = self.queue.lock().expect("native accepted queue");
        let root = self
            .view
            .try_read()
            .map_or_else(|_| self.physical.current_root(), |view| view.root.clone());
        let accepted = queue.requests.len();
        let mut core_guard = unpoison(&self.core);
        let (core, core_warm_bytes) = (core_guard.stats(), core_guard.byte_stats());
        drop(core_guard);
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation: root.generation(),
            frontier: root.frontier(),
            accepted_batch: accepted > 0,
            accepted_batches: accepted,
            accepted_sequence: queue.next_sequence,
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

    pub(super) fn spawn_merges(&self) {
        let (v2, ctl) = (&self.physical, &self.physical.merge_ctl);
        if (!v2.home.needs_merge() && !v2.cas.needs_merge())
            || unpoison(&ctl.backoff_until).is_some_and(|until| Instant::now() < until)
            || ctl.running.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
        let (view, ctl_clone, policy) = (
            Arc::clone(&self.view),
            Arc::clone(ctl),
            self.space_policy.clone(),
        );
        let thread = std::thread::Builder::new().name("lastdb-run-merge".into());
        match thread.spawn(move || merge_until_settled(&homes, &view, &ctl_clone, &policy)) {
            Ok(handle) => drop(
                unpoison(&v2.merge_handle)
                    .replace(handle)
                    .map(std::thread::JoinHandle::join),
            ),
            Err(error) => {
                ctl.running.store(false, Ordering::Release);
                tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
            }
        }
    }
}

pub(super) fn adopt(view: &mut NativeReadView, latest: V2Root) -> Option<V2Root> {
    (view.root.frontier() == latest.frontier() && view.root.generation() < latest.generation())
        .then(|| std::mem::replace(&mut view.root, latest))
}

fn merge_until_settled(
    homes: &[Weak<RunHome>; 2],
    view: &RwLock<NativeReadView>,
    ctl: &MergeControl,
    policy: &SpacePolicy,
) {
    let cas_policy = SpacePolicy::default();
    loop {
        'steps: while !ctl.stopped.load(Ordering::Acquire) {
            let mut merged = false;
            for (idx, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                match home.merge_step_with_space(if idx == 0 { policy } else { &cas_policy }) {
                    Ok(true) => {
                        drop(home);
                        if let (Some(main), Some(cas)) = (homes[0].upgrade(), homes[1].upgrade()) {
                            let root = V2Root {
                                main: main.current_root(),
                                cas: cas.current_root(),
                            };
                            drop(view.write().ok().and_then(|mut v| adopt(&mut v, root)));
                        }
                        wait_for_retired_drain(weak, ctl);
                        merged = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(%error, "disk run merge failed");
                        *unpoison(&ctl.backoff_until) =
                            Some(Instant::now() + Duration::from_secs(30));
                        ctl.running.store(false, Ordering::Release);
                        return;
                    }
                }
            }
            if !merged {
                break;
            }
        }
        ctl.running.store(false, Ordering::Release);
        let more = !ctl.stopped.load(Ordering::Acquire)
            && homes
                .iter()
                .any(|weak| weak.upgrade().is_some_and(|home| home.needs_merge()));
        if !more || ctl.running.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

fn wait_for_retired_drain(weak: &Weak<RunHome>, ctl: &MergeControl) {
    let (until, mut limit) = (Instant::now() + Duration::from_secs(2), None);
    while let Some(home) = weak.upgrade() {
        let cap = *limit.get_or_insert_with(|| (home.run_totals().2 / 16).max(1 << 20));
        if home.retired_bytes() <= cap
            || ctl.stopped.load(Ordering::Acquire)
            || Instant::now() >= until
        {
            break;
        }
        drop(home);
        std::thread::sleep(Duration::from_millis(2));
    }
}
