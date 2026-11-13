//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use crate::resident::CoreWarmSet;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::CREATION_INTENT;
use lastdb_model::storage_policy::native_read::{EAGER_BLOCKS, EAGER_WORK_BYTES};
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHomeDiagnostics};
use laststore::space::{SpacePolicy, SpaceReservation};
use serde::Serialize;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};

mod kv;
pub(in crate::storage) use kv::native_routing;
pub(in crate::storage) mod physical;
mod read;
mod run_commit;
pub(in crate::storage) mod run_node;
pub(crate) mod transaction;

pub use super::disk_backup::{
    byte_backup, mutation_log, run_restore, snapshot, NativeCloudWorkGuard,
};
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupPhase, NativeCleanupStatus,
};
pub use crate::storage::format::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
pub use physical::{NativeDiskStatus, CAS_COLLECTION};
pub use run_commit::NativeFlushReceipt;
pub(crate) use run_node::record_warm;
pub use run_node::{CAS_DIR, RUN_DIR};
pub use transaction::PackReadScope;

#[derive(Serialize)]
pub struct NativeStorageDiagnostics {
    pub main: RunHomeDiagnostics,
    pub cas: RunHomeDiagnostics,
}

#[derive(Clone, Copy, Debug)]
pub struct NativeMemoryPurgeReport {
    pub records_purged: u64,
    pub target_met: bool,
}

pub(in crate::storage) mod cas_delete {
    use super::physical::V2Home;
    use super::{map, HomeMutation, StorageError, StorageResult, CAS_COLLECTION};
    use laststore::disk_run::home::{CheckpointKind::CasDelete, MAX_CAS_DELETE_CHECKPOINT_BYTES};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Copy, Debug, Serialize)]
    pub struct CasDeleteRecovery {
        pub outcome: CasDeleteRecoveryOutcome,
        pub intent_keys: u64,
        pub main_frontier: u64,
        pub cas_frontier_before: u64,
        pub cas_frontier_after: u64,
        pub intent_released: bool,
    }

    #[derive(Clone, Copy, Debug, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub enum CasDeleteRecoveryOutcome {
        MainNotPublished,
        Completed,
        AlreadyComplete,
    }

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Intent {
        version: u16,
        main_home: uuid::Uuid,
        cas_home: uuid::Uuid,
        main_before: u64,
        main_after: u64,
        cas_before: u64,
        cas_after_add: u64,
        keys: Vec<String>,
    }

    pub(in crate::storage::disk_pack) fn reserved_space(
        ops: &[HomeMutation],
        prev: Option<&[HomeMutation]>,
    ) -> StorageResult<u64> {
        let mut bytes = 0u64;
        for op in ops.iter().chain(prev.into_iter().flatten()) {
            if op.collection == CAS_COLLECTION && op.body.is_none() {
                let add = (op.key.len() as u64).saturating_mul(6) + 4;
                bytes = bytes
                    .checked_add(add)
                    .ok_or_else(|| StorageError::invalid("CAS size overflow"))?;
            }
        }
        if bytes == 0 {
            return Ok(0);
        }
        let res = bytes
            .checked_add(8192)
            .ok_or_else(|| StorageError::invalid("CAS size overflow"))?;
        (res <= MAX_CAS_DELETE_CHECKPOINT_BYTES as u64)
            .then_some(res)
            .ok_or_else(|| StorageError::write_limit("CAS deletion intent exceeds its byte bound"))
    }

    pub(in crate::storage::disk_pack) fn save(
        v2: &V2Home,
        puts: bool,
        main: bool,
        deletes: &[HomeMutation],
    ) -> StorageResult<()> {
        if deletes.is_empty() {
            return Ok(());
        }
        require_clear(v2)?;
        let cur = v2.current_root();
        let (mb, cb) = (cur.main.frontier(), cur.cas.frontier());
        let step = |f: u64, inc: bool, m: &str| {
            f.checked_add(u64::from(inc))
                .ok_or_else(|| StorageError::invalid(m))
        };
        let intent = Intent {
            version: 1,
            main_home: v2.home.identity(),
            cas_home: v2.cas.identity(),
            main_before: mb,
            main_after: step(mb, main, "main frontier overflow")?,
            cas_before: cb,
            cas_after_add: step(cb, puts, "CAS frontier overflow")?,
            keys: deletes.iter().map(|op| op.key.clone()).collect(),
        };
        let b = serde_json::to_vec(&intent)
            .map_err(|_| StorageError::invalid("CAS intent encoding"))?;
        v2.home.save_checkpoint(CasDelete, &b).map_err(map)
    }

    pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
        let none = v2.home.load_checkpoint(CasDelete).map_err(map)?.is_none();
        none.then_some(())
            .ok_or_else(|| StorageError::invalid("pending CAS deletion requires native recovery"))
    }

    pub(in crate::storage::disk_pack) fn recover(
        v2: &V2Home,
    ) -> StorageResult<Option<CasDeleteRecovery>> {
        let Some(b) = v2.home.load_checkpoint(CasDelete).map_err(map)? else {
            return Ok(None);
        };
        let i: Intent =
            serde_json::from_slice(&b).map_err(|_| StorageError::invalid("CAS intent encoding"))?;
        let cur = v2.current_root();
        let (main, cas) = (cur.main.frontier(), cur.cas.frontier());
        let del = i
            .cas_after_add
            .checked_add(1)
            .ok_or_else(|| StorageError::invalid("CAS frontier overflow"))?;
        let step_ok = |a: u64, b: u64| matches!(a.checked_sub(b), Some(0 | 1));
        if i.version != 1
            || (i.main_home, i.cas_home) != (v2.home.identity(), v2.cas.identity())
            || i.keys.is_empty()
            || i.keys.windows(2).any(|w| w[0] >= w[1])
            || !step_ok(i.main_after, i.main_before)
            || !step_ok(i.cas_after_add, i.cas_before)
            || ![i.main_before, i.main_after].contains(&main)
            || !(i.cas_before..=del).contains(&cas)
            || (main != i.main_after && cas > i.cas_after_add)
        {
            return Err(StorageError::invalid(
                "CAS deletion intent identity or frontier mismatch",
            ));
        }
        let committed = main == i.main_after && cas >= i.cas_after_add;
        let intent_keys = i.keys.len() as u64;
        let outcome = if committed && cas != del {
            let ops = i
                .keys
                .into_iter()
                .map(|k| HomeMutation::new(CAS_COLLECTION, k, None))
                .collect();
            v2.cas.commit(ops).map_err(map)?;
            CasDeleteRecoveryOutcome::Completed
        } else if committed {
            CasDeleteRecoveryOutcome::AlreadyComplete
        } else {
            CasDeleteRecoveryOutcome::MainNotPublished
        };
        v2.home.clear_checkpoint(CasDelete).map_err(map)?;
        let cas_frontier_after = v2.cas.current_root().frontier();
        Ok(Some(CasDeleteRecovery {
            outcome,
            intent_keys,
            main_frontier: main,
            cas_frontier_before: cas,
            cas_frontier_after,
            intent_released: true,
        }))
    }
}

#[derive(Clone)]
pub(in crate::storage) struct NativeReadView(
    pub(in crate::storage::disk_pack) physical::V2Root,
    pub(in crate::storage::disk_pack) u64,
);

pub(in crate::storage) async fn run_blocking<T: Send + 'static>(
    work: impl FnOnce() -> StorageResult<T> + Send + 'static,
) -> StorageResult<T> {
    if Handle::try_current().is_ok_and(|h| matches!(h.runtime_flavor(), RuntimeFlavor::MultiThread))
    {
        tokio::task::block_in_place(work)
    } else {
        tokio::task::spawn_blocking(work).await?
    }
}

pub struct PackRuntime {
    pub(in crate::storage) physical: physical::V2Home,
    pub(in crate::storage) cleanup_job: Arc<CleanupJob>,
    pub(in crate::storage) mutation_log: mutation_log::MutationLog,
    pub(in crate::storage) accepted: Option<crate::storage::disk_backup::accepted::Log>,
    pub(in crate::storage) view: Arc<RwLock<NativeReadView>>,
    pub(in crate::storage::disk_pack) prepare_gate: Arc<tokio::sync::Mutex<()>>,
    pub(in crate::storage::disk_pack) write_gate: Arc<tokio::sync::Mutex<()>>,
    pub(in crate::storage) queue: Mutex<run_commit::WriteQueue>,
    pub(in crate::storage::disk_pack) completed: tokio::sync::watch::Sender<u64>,
    pub(in crate::storage) core: Arc<Mutex<CoreWarmSet>>,
    pub(in crate::storage) failed: AtomicBool,
    pub(in crate::storage::disk_pack) initializing: AtomicBool,
    pub(in crate::storage) serving: AtomicBool,
    pub(in crate::storage::disk_pack) failed_write: Mutex<Option<SpaceReservation>>,
    pub(in crate::storage) space_policy: SpacePolicy,
}

impl PackRuntime {
    pub fn requires_recovery(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    pub(crate) fn batch_operation_limit(&self) -> usize {
        16_384
    }
    pub(crate) fn record_read_stage(&self, _stage: &'static str, _items: usize) {}
    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }
    pub async fn diagnostics_native(
        &self,
        inv: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        if inv && !self.queue.lock().expect("native queue").requests.is_empty() {
            return Err(StorageError::invalid(
                "inventory requires a checkpoint without accepted writes",
            ));
        }
        let main = Arc::clone(&self.physical.home);
        let cas = Arc::clone(&self.physical.cas);
        tokio::task::spawn_blocking(move || {
            let dl = Instant::now() + Duration::from_secs(60);
            Ok(Some(NativeStorageDiagnostics {
                main: main.diagnostics(inv, dl).map_err(map)?,
                cas: cas.diagnostics(inv, dl).map_err(map)?,
            }))
        })
        .await?
    }
    pub fn purge_clean_memory(&self, target_bytes: u64) -> StorageResult<NativeMemoryPurgeReport> {
        let err = || StorageError::backend("native warm set lock is unavailable");
        let _view = self.view.read().map_err(|_| err())?;
        let mut core = self.core.lock().map_err(|_| err())?;
        let target = usize::try_from(target_bytes).unwrap_or(usize::MAX);
        let (purged, _, after) = core.purge_clean_to_bytes(target);
        Ok(NativeMemoryPurgeReport {
            records_purged: purged as u64,
            target_met: after as u64 <= target_bytes,
        })
    }
    pub(crate) async fn finish_fresh_initialization(self: &Arc<Self>) -> StorageResult<()> {
        if !self.fresh_initialization_pending() {
            return Ok(());
        }
        if transaction::PendingBatch::current(self).is_some() {
            return Err(creation_refusal(
                "current creation needs its own durable transaction",
            ));
        }
        let (gate, rt) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            if rt.requires_recovery() {
                return Err(creation_refusal(
                    "current creation requires a snapshot reopen",
                ));
            }
            if rt.fresh_initialization_pending() {
                rt.mark_accepted_born(rt.physical.current_root().frontier())?;
                let node = rt
                    .physical
                    .path()
                    .parent()
                    .ok_or_else(|| StorageError::invalid("no node home"))?;
                let rm = fs::remove_file(node.join(CREATION_INTENT));
                rm.or_else(|e| (e.kind() == ErrorKind::NotFound).then_some(()).ok_or(e))?;
                File::open(node)?.sync_all()?;
                rt.initializing.store(false, Ordering::Release);
            }
            Ok(())
        })
        .await
    }
    pub(in crate::storage::disk_pack) async fn mutate(
        self: &Arc<Self>,
        ops: Vec<HomeMutation>,
    ) -> StorageResult<()> {
        if let Some(pending) = transaction::PendingBatch::current(self) {
            return pending.stage(ops);
        }
        let (gate, rt) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            rt.commit(ops)
        })
        .await
    }
}

pub(in crate::storage) fn creation_refusal(message: &str) -> StorageError {
    StorageError::invalid(format!("native home creation refused: {message}"))
}

#[derive(Clone)]
pub struct PackNamespacedStore {
    pub(in crate::storage) runtime: Arc<PackRuntime>,
}

#[derive(Clone)]
pub(crate) struct PackKvStore(pub(super) Arc<PackRuntime>, pub(super) String);

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    use laststore::Error as E;
    let detail = error.to_string();
    match error {
        E::Io(io) if StorageError::is_storage_full_io(&io) => StorageError::StorageFull { detail },
        E::Io(io) => StorageError::IoError(io),
        E::ReadBudgetExceeded => StorageError::read_limit(detail),
        E::WriteCapacity(d) => StorageError::write_limit(d),
        _ => StorageError::backend(detail),
    }
}

pub(in crate::storage) fn budget() -> ReadBudget {
    ReadBudget::new(4096, EAGER_BLOCKS, EAGER_WORK_BYTES, 10000)
}
