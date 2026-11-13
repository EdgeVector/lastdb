//! Physical v2 run homes, status, background run merges, and metadata publications.

use super::read::{unpoison, Coordinate, ReadContext};
use super::transaction::PendingBatch;
use super::*;
use crate::resident::CoreWarmByteStats;
use laststore::disk_run::home::{RunHome, RunRead, RunReadCosts, RunRoot};
use std::path::Path;
use std::sync::Weak;
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
        let mut lim = budget();
        if write_prep {
            lim.keys = 40_000;
        }
        V2Read(
            self.home.read_root(&root.main, lim),
            self.cas.read_root(&root.cas, lim),
        )
    }
    pub(in crate::storage) fn path(&self) -> &Path {
        self.home.path()
    }
}

impl Drop for V2Home {
    fn drop(&mut self) {
        self.merge.0 .1.store(true, Ordering::Release);
        if let Ok(Some(h)) = self.merge.1.get_mut().map(Option::take) {
            let _ = h.join();
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

pub(in crate::storage::disk_pack) struct V2Read(pub(super) RunRead, pub(super) RunRead);

impl V2Read {
    pub(super) fn home_for(&mut self, col: &str) -> &mut RunRead {
        if col == CAS_COLLECTION {
            &mut self.1
        } else {
            &mut self.0
        }
    }
    pub(super) fn get_many(
        &mut self,
        coords: &[Coordinate],
    ) -> laststore::Result<Vec<laststore::Result<Option<Vec<u8>>>>> {
        if !coords.iter().any(|(c, _)| c == CAS_COLLECTION) {
            return self.0.get_many_bounded(coords, u64::MAX);
        }
        let mut out: Vec<_> = (0..coords.len()).map(|_| Ok(None)).collect();
        for (is_cas, read) in [(false, &mut self.0), (true, &mut self.1)] {
            let idx: Vec<_> = (0..coords.len())
                .filter(|&i| (coords[i].0 == CAS_COLLECTION) == is_cas)
                .collect();
            if !idx.is_empty() {
                let req: Vec<_> = idx.iter().map(|&i| coords[i].clone()).collect();
                for (slot, res) in idx.into_iter().zip(read.get_many_bounded(&req, u64::MAX)?) {
                    out[slot] = res;
                }
            }
        }
        let (c, l) = (self.costs(), self.0.budget());
        (c.examined_keys <= l.keys && c.budget_bytes <= l.bytes)
            .then_some(out)
            .ok_or(laststore::Error::ReadBudgetExceeded)
    }
    pub(super) fn costs(&self) -> RunReadCosts {
        let (m, c) = (self.0.costs(), self.1.costs());
        RunReadCosts {
            examined_keys: m.examined_keys + c.examined_keys,
            budget_bytes: m.budget_bytes + c.budget_bytes,
        }
    }
}

impl PackRuntime {
    pub fn subscribe_durable_writes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.completed.subscribe()
    }
    pub fn status_native(&self) -> NativeDiskStatus {
        let q = self.queue.lock().expect("native accepted queue");
        let root = self
            .view
            .try_read()
            .map_or_else(|_| self.physical.current_root(), |v| v.0.clone());
        let accepted = q.requests.len();
        let mut c = unpoison(&self.core);
        let (core, core_warm_bytes) = (c.stats(), c.byte_stats());
        drop(c);
        NativeDiskStatus {
            cleanup: self.status_cleanup(),
            generation: root.generation(),
            frontier: root.frontier(),
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
    pub(super) fn spawn_merges(&self) {
        let (v2, ctl) = (&self.physical, &self.physical.merge.0);
        if (!v2.home.needs_merge() && !v2.cas.needs_merge())
            || unpoison(&ctl.2).is_some_and(|t| Instant::now() < t)
            || ctl.0.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let homes = [Arc::downgrade(&v2.home), Arc::downgrade(&v2.cas)];
        let (view, ctl_c, pol) = (
            Arc::clone(&self.view),
            Arc::clone(ctl),
            self.space_policy.clone(),
        );
        let thread = std::thread::Builder::new().name("lastdb-run-merge".into());
        match thread.spawn(move || merge_until_settled(&homes, &view, &ctl_c, &pol)) {
            Ok(h) => drop(
                unpoison(&v2.merge.1)
                    .replace(h)
                    .map(std::thread::JoinHandle::join),
            ),
            Err(error) => {
                ctl.0.store(false, Ordering::Release);
                tracing::warn!(%error, "disk run merge thread did not start; the next commit retries");
            }
        }
    }
}

pub(super) fn adopt(view: &mut NativeReadView, latest: V2Root) -> Option<V2Root> {
    (view.0.frontier() == latest.frontier() && view.0.generation() < latest.generation())
        .then(|| std::mem::replace(&mut view.0, latest))
}

fn merge_until_settled(
    homes: &[Weak<RunHome>; 2],
    view: &RwLock<NativeReadView>,
    ctl: &MergeCtl,
    policy: &SpacePolicy,
) {
    let cas_pol = SpacePolicy::default();
    loop {
        'steps: while !ctl.1.load(Ordering::Acquire) {
            let mut merged = false;
            for (idx, weak) in homes.iter().enumerate() {
                let Some(home) = weak.upgrade() else {
                    break 'steps;
                };
                match home.merge_step_with_space(if idx == 0 { policy } else { &cas_pol }) {
                    Ok(true) => {
                        drop(home);
                        if let (Some(m), Some(c)) = (homes[0].upgrade(), homes[1].upgrade()) {
                            let r = V2Root {
                                main: m.current_root(),
                                cas: c.current_root(),
                            };
                            drop(view.write().ok().and_then(|mut v| adopt(&mut v, r)));
                        }
                        let (until, mut lim) = (Instant::now() + Duration::from_secs(2), None);
                        while let Some(h) = weak.upgrade() {
                            let cap =
                                *lim.get_or_insert_with(|| (h.run_totals().2 / 16).max(1 << 20));
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
                        *unpoison(&ctl.2) = Some(Instant::now() + Duration::from_secs(30));
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
        let more = !ctl.1.load(Ordering::Acquire)
            && homes
                .iter()
                .any(|w| w.upgrade().is_some_and(|h| h.needs_merge()));
        if !more || ctl.0.swap(true, Ordering::AcqRel) {
            return;
        }
    }
}

pub(crate) mod publication {
    use super::{
        unpoison, Arc, Mutex, PackRuntime, PendingBatch, ReadContext, StorageError, StorageResult,
    };
    use std::any::Any;
    use std::collections::BTreeMap;

    type Action = Box<dyn FnOnce() -> StorageResult<()> + Send>;
    type Value = Arc<dyn Any + Send + Sync>;
    type ChangeMap = (BTreeMap<(usize, String), (Value, Action)>, usize);

    #[derive(Default)]
    pub(in crate::storage::disk_pack) struct PendingPublications(Mutex<ChangeMap>);

    impl PendingPublications {
        pub(in crate::storage::disk_pack) fn is_empty(&self) -> StorageResult<bool> {
            Ok(unpoison(&self.0).0.is_empty())
        }
        pub(in crate::storage::disk_pack) fn finish(&self) -> StorageResult<()> {
            let mut err = None;
            for (_, (_, act)) in std::mem::take(&mut *unpoison(&self.0)).0 {
                if let Err(e) = act() {
                    err.get_or_insert(e);
                }
            }
            err.map_or(Ok(()), Err)
        }
    }

    pub(crate) fn publish<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
        name: String,
        val: T,
        act: impl FnOnce() -> StorageResult<()> + Send + 'static,
    ) -> StorageResult<()> {
        let Some(p) = PendingBatch::current(rt) else {
            return act();
        };
        let mut c = unpoison(&p.publications.0);
        if c.1 >= 4096 {
            return Err(StorageError::invalid(
                "native mutation exceeds 4096 metadata publications",
            ));
        }
        c.0.insert((cache, name), (Arc::new(val), Box::new(act)));
        c.1 += 1;
        Ok(())
    }

    fn cast<T: Any + Send + Sync>(val: &Value) -> StorageResult<Arc<T>> {
        Arc::clone(val)
            .downcast::<T>()
            .map_err(|_| StorageError::backend("metadata cache type differs"))
    }

    pub(crate) fn get<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
        name: &str,
    ) -> StorageResult<Option<Arc<T>>> {
        let key = (cache, name.to_string());
        for p in write_views(rt).into_iter().rev() {
            if let Some((val, _)) = unpoison(&p.publications.0).0.get(&key) {
                return cast(val).map(Some);
            }
        }
        Ok(None)
    }

    pub(crate) fn entries<T: Any + Send + Sync>(
        rt: &PackRuntime,
        cache: usize,
    ) -> StorageResult<Vec<(String, Arc<T>)>> {
        let mut out = BTreeMap::new();
        for p in write_views(rt) {
            for ((owner, name), (val, _)) in &unpoison(&p.publications.0).0 {
                if *owner == cache {
                    out.insert(name.clone(), cast(val)?);
                }
            }
        }
        Ok(out.into_iter().collect())
    }

    fn write_views(rt: &PackRuntime) -> Vec<Arc<PendingBatch>> {
        let Some(p) = PendingBatch::current(rt) else {
            return Vec::new();
        };
        let mut v = ReadContext::captured(rt).map_or_else(Vec::new, |c| (*c.1).clone());
        v.push(p);
        v
    }
}
