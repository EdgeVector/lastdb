//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use crate::resident::CoreWarmSet;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::CREATION_INTENT;
use lastdb_model::storage_policy::native_read::EAGER_WORK_BYTES;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHomeDiagnostics};
use laststore::space::{SpacePolicy, SpaceReservation};
use serde::Serialize;
use std::fs::{self, File};
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};

pub(in crate::storage) mod admit;
pub(in crate::storage) mod cas_delete;
mod kv;
pub(in crate::storage) use kv::native_routing;
pub(in crate::storage) mod physical;
pub(crate) mod publication;
mod read;
pub(crate) mod record_warm;
mod run_commit;
pub(in crate::storage) mod run_node;
pub(crate) mod transaction;

pub use super::disk_backup::{snapshot, NativeCloudWorkGuard};
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupPhase, NativeCleanupStatus,
};
pub use crate::storage::format::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
pub use physical::NativeDiskStatus;
pub(crate) use physical::CAS_COLLECTION;
pub use run_commit::NativeFlushReceipt;
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

    pub(crate) fn record_read_stage(&self, _stage: &'static str, _items: usize) {}

    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub async fn diagnostics_native(
        &self,
        inventory: bool,
    ) -> StorageResult<Option<NativeStorageDiagnostics>> {
        if !laststore::diagnostics_enabled() {
            return Ok(None);
        }
        if inventory && !self.queue.lock().expect("native queue").requests.is_empty() {
            return Err(StorageError::invalid(
                "inventory requires a checkpoint without accepted writes",
            ));
        }
        let main = Arc::clone(&self.physical.home);
        let cas = Arc::clone(&self.physical.cas);
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
            return Err(creation_refusal(
                "current creation needs its own durable transaction",
            ));
        }
        let (gate, runtime) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let _gate = gate;
            if runtime.requires_recovery() {
                return Err(creation_refusal(
                    "current creation requires a snapshot reopen",
                ));
            }
            if runtime.fresh_initialization_pending() {
                runtime.mark_accepted_born(runtime.physical.current_root().frontier())?;
                let node = runtime
                    .physical
                    .path()
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
    pub(super) runtime: Arc<PackRuntime>,
    pub(super) namespace: String,
}

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    StorageError::from_laststore(error)
}

pub(in crate::storage) fn budget() -> ReadBudget {
    ReadBudget::new(EAGER_WORK_BYTES, 10_000)
}
