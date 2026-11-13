//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use crate::resident::CoreWarmSet;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::{CREATION_INTENT, NAMESPACES, REGISTRY};
use lastdb_model::storage_policy::native_read::{EAGER_BLOCKS, EAGER_WORK_BYTES};
use laststore::disk_run::home::{HomeMutation, ReadBudget};
use laststore::space::{SpacePolicy, SpaceReservation};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::{Handle, RuntimeFlavor};

pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
mod kv;
pub(in crate::storage) use kv::native_routing;
pub(in crate::storage) mod physical;
mod queue;
mod read;
mod run_commit;
pub(crate) use run_commit::record_warm;
pub(in crate::storage) mod run_node;
pub(crate) mod transaction;

pub use crate::storage::disk_backup::{
    byte_backup, mutation_log, run_restore, NativeCloudWorkGuard,
};
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupPhase, NativeCleanupStatus,
};
pub use crate::storage::format::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
pub use physical::{
    NativeDiskStatus, NativeMemoryPurgeReport, NativeStorageDiagnostics, CAS_COLLECTION,
};
pub use queue::NativeFlushReceipt;
pub use run_node::{CAS_DIR, RUN_DIR};
pub use transaction::PackReadScope;

pub(in crate::storage) mod cas_delete {
    use super::physical::{V2Home, CAS_COLLECTION};
    use super::{map, HomeMutation, StorageError, StorageResult};
    use laststore::disk_run::home::{CheckpointKind, MAX_CAS_DELETE_CHECKPOINT_BYTES};
    use serde::{Deserialize, Serialize};

    fn inv(m: &str) -> StorageError {
        StorageError::InvalidOperation(m.into())
    }

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
        let bytes = ops
            .iter()
            .chain(prev.into_iter().flatten())
            .filter(|op| op.collection == CAS_COLLECTION && op.body.is_none())
            .try_fold(0u64, |b, op| {
                b.checked_add((op.key.len() as u64).saturating_mul(6) + 4)
            })
            .ok_or_else(|| inv("CAS deletion intent size overflow"))?;
        if bytes == 0 {
            return Ok(0);
        }
        let res = bytes
            .checked_add(8192)
            .ok_or_else(|| inv("CAS deletion intent size overflow"))?;
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
        let step = |f: u64, inc: bool, m: &str| f.checked_add(u64::from(inc)).ok_or_else(|| inv(m));
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
        let b = serde_json::to_vec(&intent).map_err(|_| inv("CAS deletion intent encoding"))?;
        v2.home
            .save_checkpoint(CheckpointKind::CasDelete, &b)
            .map_err(map)
    }

    pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
        v2.home
            .load_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?
            .is_none()
            .then_some(())
            .ok_or_else(|| inv("pending CAS deletion requires native recovery before backup"))
    }

    pub(in crate::storage::disk_pack) fn recover(
        v2: &V2Home,
    ) -> StorageResult<Option<CasDeleteRecovery>> {
        let Some(b) = v2
            .home
            .load_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?
        else {
            return Ok(None);
        };
        let i: Intent =
            serde_json::from_slice(&b).map_err(|_| inv("CAS deletion intent encoding"))?;
        let cur = v2.current_root();
        let (main, cas) = (cur.main.frontier(), cur.cas.frontier());
        let del = i
            .cas_after_add
            .checked_add(1)
            .ok_or_else(|| inv("CAS frontier overflow"))?;
        if i.version != 1
            || (i.main_home, i.cas_home) != (v2.home.identity(), v2.cas.identity())
            || i.keys.is_empty()
            || i.keys.windows(2).any(|w| w[0] >= w[1])
            || !matches!(i.main_after.checked_sub(i.main_before), Some(0 | 1))
            || !matches!(i.cas_after_add.checked_sub(i.cas_before), Some(0 | 1))
            || ![i.main_before, i.main_after].contains(&main)
            || !(i.cas_before..=del).contains(&cas)
            || (main != i.main_after && cas > i.cas_after_add)
        {
            return Err(inv("CAS deletion intent identity or frontier mismatch"));
        }
        let (committed, intent_keys) = (
            main == i.main_after && cas >= i.cas_after_add,
            i.keys.len() as u64,
        );
        let outcome = if committed && cas != del {
            let ops = i
                .keys
                .into_iter()
                .map(|key| HomeMutation::new(CAS_COLLECTION, key, None))
                .collect();
            v2.cas.commit(ops).map_err(map)?;
            CasDeleteRecoveryOutcome::Completed
        } else if committed {
            CasDeleteRecoveryOutcome::AlreadyComplete
        } else {
            CasDeleteRecoveryOutcome::MainNotPublished
        };
        v2.home
            .clear_checkpoint(CheckpointKind::CasDelete)
            .map_err(map)?;
        Ok(Some(CasDeleteRecovery {
            outcome,
            intent_keys,
            main_frontier: main,
            cas_frontier_before: cas,
            cas_frontier_after: v2.cas.current_root().frontier(),
            intent_released: true,
        }))
    }
}

#[derive(Clone)]
pub(in crate::storage::disk_pack) struct NativeReadView(
    pub(in crate::storage::disk_pack) physical::V2Root,
    pub(in crate::storage::disk_pack) u64,
);

pub(in crate::storage) async fn run_blocking<T, F>(work: F) -> StorageResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> StorageResult<T> + Send + 'static,
{
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
    pub(in crate::storage::disk_pack) view: Arc<RwLock<NativeReadView>>,
    pub(in crate::storage::disk_pack) prepare_gate: Arc<tokio::sync::Mutex<()>>,
    pub(in crate::storage::disk_pack) write_gate: Arc<tokio::sync::Mutex<()>>,
    pub(in crate::storage::disk_pack) queue: Mutex<queue::WriteQueue>,
    pub(in crate::storage::disk_pack) completed: tokio::sync::watch::Sender<u64>,
    pub(in crate::storage::disk_pack) core: Arc<Mutex<CoreWarmSet>>,
    pub(in crate::storage) failed: AtomicBool,
    pub(in crate::storage::disk_pack) initializing: AtomicBool,
    pub(in crate::storage) serving: AtomicBool,
    pub(in crate::storage::disk_pack) failed_write: Mutex<Option<SpaceReservation>>,
    pub(in crate::storage) space_policy: SpacePolicy,
}

impl PackRuntime {
    pub fn native_home_id(&self) -> uuid::Uuid {
        self.physical.identity()
    }
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
                let node = rt.physical.path().parent().ok_or_else(|| {
                    StorageError::invalid("a format v2 run home has no node home")
                })?;
                if let Err(e) = std::fs::remove_file(node.join(CREATION_INTENT)) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        return Err(e.into());
                    }
                }
                std::fs::File::open(node)?.sync_all()?;
                rt.initializing.store(false, Ordering::Release);
            }
            Ok(())
        })
        .await
    }

    pub(in crate::storage::disk_pack) async fn mutate(
        self: &Arc<Self>,
        operations: Vec<HomeMutation>,
    ) -> StorageResult<()> {
        if let Some(pending) = transaction::PendingBatch::current(self) {
            return pending.stage(operations);
        }
        let (gate, rt) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            rt.commit(operations)
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
pub(crate) struct PackKvStore {
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) namespace: String,
}

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    use laststore::Error as E;
    match error {
        E::Io(io) if StorageError::is_storage_full_io(&io) => StorageError::StorageFull {
            detail: io.to_string(),
        },
        E::Io(io) => StorageError::IoError(io),
        E::ReadBudgetExceeded => StorageError::read_limit(error.to_string()),
        E::WriteCapacity(detail) => StorageError::write_limit(detail),
        _ => StorageError::backend(error.to_string()),
    }
}

pub(in crate::storage) fn budget() -> ReadBudget {
    ReadBudget::new(4096, EAGER_BLOCKS, EAGER_WORK_BYTES, 10000)
}
