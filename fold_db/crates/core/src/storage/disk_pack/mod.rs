//! Native disk backend with complete-request admission, CAS deletion recovery, and captured read roots.

use super::error::{StorageError, StorageResult};
use crate::resident::CoreWarmSet;
use crate::storage::{disk_cleanup::CleanupJob, format::CREATION_INTENT};
use lastdb_model::storage_policy::native_read::EAGER_WORK_BYTES;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHomeDiagnostics};
use laststore::space::{SpacePolicy, SpaceReservation};
use serde::Serialize;
use std::fs::{self, File};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::{io::ErrorKind, time::Duration, time::Instant};
use tokio::runtime::{Handle, RuntimeFlavor};

mod kv;
pub(in crate::storage) use kv::native_routing;
pub(in crate::storage) mod physical;
mod read;
pub(crate) mod record_warm;
mod run_commit;
pub(in crate::storage) mod run_node;
pub(in crate::storage) use run_node as admit;
pub(crate) mod transaction;
mod user_data;

pub use super::disk_backup::{snapshot, NativeCloudWorkGuard};
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupPhase, NativeCleanupStatus,
};
pub use crate::storage::format::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
pub(crate) use physical::CAS_COLLECTION;
pub use {
    physical::NativeDiskStatus, run_commit::NativeFlushReceipt, run_node::CAS_DIR,
    run_node::RUN_DIR, transaction::PackReadScope,
};

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

#[derive(Clone)]
pub(in crate::storage) struct NativeReadView {
    pub(in crate::storage::disk_pack) root: physical::V2Root,
    pub(in crate::storage::disk_pack) revision: u64,
}

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
    pub(in crate::storage) accepted: Option<crate::storage::disk_backup::accepted::Log>,
    /// The author that signs the captured writes of this store.
    pub(in crate::storage) capture_author:
        std::sync::OnceLock<Arc<dyn crate::storage::disk_backup::accepted::CaptureAuthor>>,
    pub(in crate::storage) local_file_refs_lock: Mutex<()>,
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

    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub async fn diagnostics_native(
        &self,
        inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        let timing = std::env::var("LASTDB_SNAPSHOT_TIMING");
        if !timing.is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on")) {
            return Ok(None);
        }
        if inventory && !self.queue.lock().expect("native queue").requests.is_empty() {
            let msg = "inventory requires a checkpoint without accepted writes";
            return Err(StorageError::invalid(msg));
        }
        let (main, cas) = (
            Arc::clone(&self.physical.home),
            Arc::clone(&self.physical.cas),
        );
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            Ok(Some(NativeStorageDiagnostics {
                main: main.diagnostics(inventory, deadline).map_err(map)?,
                cas: cas.diagnostics(inventory, deadline).map_err(map)?,
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
            let msg = "current creation needs its own durable transaction";
            return Err(creation_refusal(msg));
        }
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            if runtime.requires_recovery() {
                let msg = "current creation requires a snapshot reopen";
                return Err(creation_refusal(msg));
            }
            if runtime.fresh_initialization_pending() {
                runtime.mark_accepted_born(runtime.physical.current_root().frontiers())?;
                let path = runtime.physical.path();
                let node = path
                    .parent()
                    .ok_or_else(|| StorageError::invalid("no node home"))?;
                let removed = fs::remove_file(node.join(CREATION_INTENT));
                removed.or_else(|e| (e.kind() == ErrorKind::NotFound).then_some(()).ok_or(e))?;
                File::open(node)?.sync_all()?;
                runtime.initializing.store(false, Ordering::Release);
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
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            runtime.commit(ops)
        })
        .await
    }
}

pub(in crate::storage) fn creation_refusal(message: &str) -> StorageError {
    StorageError::invalid(format!("native home creation refused: {message}"))
}

#[derive(Clone)]
pub(crate) struct PackNamespacedStore {
    pub(in crate::storage) runtime: Arc<PackRuntime>,
}

#[derive(Clone)]
pub(crate) struct PackKvStore {
    runtime: Arc<PackRuntime>,
    namespace: String,
}

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    StorageError::from_laststore(error)
}

pub(in crate::storage) fn budget() -> ReadBudget {
    ReadBudget::new(EAGER_WORK_BYTES, 10_000)
}

pub(in crate::storage) mod cas_delete {
    use super::physical::V2Home;
    use super::{map, HomeMutation, StorageError, StorageResult, CAS_COLLECTION};
    use laststore::disk_run::home::CheckpointSpec;
    use serde::{Deserialize, Serialize};

    const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 << 20;

    pub(in crate::storage::disk_pack) const CAS_DELETE_CHECKPOINT: CheckpointSpec =
        CheckpointSpec {
            file_name: "CAS_DELETE",
            magic: b"LDRCAS1\0",
            max_bytes: MAX_CAS_DELETE_CHECKPOINT_BYTES,
        };

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
        let reserved = bytes
            .checked_add(8192)
            .ok_or_else(|| StorageError::invalid("CAS size overflow"))?;
        (reserved <= MAX_CAS_DELETE_CHECKPOINT_BYTES as u64)
            .then_some(reserved)
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
        let current = v2.current_root();
        let (main_before, cas_before) = (current.main.frontier(), current.cas.frontier());
        let step = |frontier: u64, inc: bool, msg: &str| {
            frontier
                .checked_add(u64::from(inc))
                .ok_or_else(|| StorageError::invalid(msg))
        };
        let intent = Intent {
            version: 1,
            main_home: v2.home.identity(),
            cas_home: v2.cas.identity(),
            main_before,
            main_after: step(main_before, main, "main frontier overflow")?,
            cas_before,
            cas_after_add: step(cas_before, puts, "CAS frontier overflow")?,
            keys: deletes.iter().map(|op| op.key.clone()).collect(),
        };
        let bytes = serde_json::to_vec(&intent)
            .map_err(|_| StorageError::invalid("CAS intent encoding"))?;
        v2.home
            .save_checkpoint(CAS_DELETE_CHECKPOINT, &bytes)
            .map_err(map)
    }

    pub(in crate::storage) fn require_clear(v2: &V2Home) -> StorageResult<()> {
        if v2
            .home
            .load_checkpoint(CAS_DELETE_CHECKPOINT)
            .map_err(map)?
            .is_some()
        {
            return Err(StorageError::invalid(
                "pending CAS deletion requires native recovery",
            ));
        }
        Ok(())
    }

    pub(in crate::storage::disk_pack) fn recover(
        v2: &V2Home,
    ) -> StorageResult<Option<CasDeleteRecovery>> {
        let home = &v2.home;
        let Some(bytes) = home.load_checkpoint(CAS_DELETE_CHECKPOINT).map_err(map)? else {
            return Ok(None);
        };
        let intent: Intent = serde_json::from_slice(&bytes)
            .map_err(|_| StorageError::invalid("CAS intent encoding"))?;
        let current = v2.current_root();
        let (main, cas) = (current.main.frontier(), current.cas.frontier());
        let after_del = intent
            .cas_after_add
            .checked_add(1)
            .ok_or_else(|| StorageError::invalid("CAS frontier overflow"))?;
        let step_ok = |after: u64, before: u64| matches!(after.checked_sub(before), Some(0 | 1));
        if intent.version != 1
            || (intent.main_home, intent.cas_home) != (home.identity(), v2.cas.identity())
            || intent.keys.is_empty()
            || intent.keys.windows(2).any(|pair| pair[0] >= pair[1])
            || !step_ok(intent.main_after, intent.main_before)
            || !step_ok(intent.cas_after_add, intent.cas_before)
            || ![intent.main_before, intent.main_after].contains(&main)
            || !(intent.cas_before..=after_del).contains(&cas)
            || (main != intent.main_after && cas > intent.cas_after_add)
        {
            return Err(StorageError::invalid(
                "CAS deletion intent identity or frontier mismatch",
            ));
        }
        let committed = main == intent.main_after && cas >= intent.cas_after_add;
        let intent_keys = intent.keys.len() as u64;
        let outcome = if committed && cas != after_del {
            let ops = intent
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
        home.clear_checkpoint(CAS_DELETE_CHECKPOINT).map_err(map)?;
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
