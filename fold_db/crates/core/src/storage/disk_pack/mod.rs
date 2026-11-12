//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use crate::resident::CoreWarmSet;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::{NAMESPACES, REGISTRY};
use lastdb_model::storage_policy::native_read::{EAGER_BLOCKS, EAGER_WORK_BYTES};
use laststore::disk_run::home::{HomeMutation, ReadBudget};
use laststore::space::{SpacePolicy, SpaceReservation};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::runtime::{Handle, RuntimeFlavor};

mod blob_backup;
pub use blob_backup::{blob_path_of_key, BlobRow};
pub(in crate::storage) mod cas_delete;
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
mod kv;
pub(in crate::storage) use kv::native_routing;
pub(in crate::storage) mod physical;
mod queue;
mod read;
mod rows;
mod run_commit;
pub(in crate::storage) mod run_node;
pub(crate) mod transaction;
mod view;
pub(crate) use view::record_warm;

pub use crate::storage::disk_backup::{
    byte_backup, mutation_log, run_restore, NativeCloudWorkGuard,
};
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus,
};
pub use crate::storage::format::{HOME_FORMAT, NODE_MARKER, RECORD_NODE_BYTES};
pub use physical::CAS_COLLECTION;
pub use queue::NativeFlushReceipt;
pub(in crate::storage) use queue::WriteGuard;
pub use read::PackReadScope;
pub use run_node::{CAS_DIR, RUN_DIR};
pub use view::{
    NativeDiskStatus, NativeMemoryPurgeReport, NativeMemoryStatus, NativeStorageDiagnostics,
};

/// Run synchronous native disk work from an async caller.
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

/// Native v2 run homes and their complete published root.
pub struct PackRuntime {
    pub(in crate::storage) physical: physical::V2Home,
    pub(in crate::storage) cleanup_job: Arc<CleanupJob>,
    pub(in crate::storage) mutation_log: mutation_log::MutationLog,
    pub(in crate::storage) accepted: Option<crate::storage::disk_backup::accepted::Log>,
    view: Arc<RwLock<view::NativeReadView>>,
    prepare_gate: Arc<tokio::sync::Mutex<()>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    core: Arc<Mutex<CoreWarmSet>>,
    pub(in crate::storage) failed: AtomicBool,
    initializing: AtomicBool,
    pub(in crate::storage) serving: AtomicBool,
    failed_write: Mutex<Option<SpaceReservation>>,
    pub(in crate::storage) space_policy: SpacePolicy,
    last_root: Arc<view::LastRoot>,
}

impl PackRuntime {
    pub(crate) fn batch_operation_limit(&self) -> usize {
        16_384
    }

    pub(crate) fn fresh_initialization_pending(&self) -> bool {
        self.initializing.load(Ordering::Acquire)
    }

    pub(in crate::storage) fn require_initialized(&self) -> StorageResult<()> {
        (!self.fresh_initialization_pending())
            .then_some(())
            .ok_or_else(|| {
                creation_refusal("current creation requires its durable key proofs first")
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
            if rt.failed.load(Ordering::Acquire) {
                return Err(creation_refusal(
                    "current creation requires a snapshot reopen",
                ));
            }
            if rt.fresh_initialization_pending() {
                rt.mark_accepted_born(rt.physical.current_root().frontier())?;
                run_node::complete_initialization(rt.physical.path())?;
                rt.initializing.store(false, Ordering::Release);
            }
            Ok(())
        })
        .await
    }

    pub(in crate::storage::disk_pack) async fn ensure_namespace(
        self: &Arc<Self>,
        name: &str,
    ) -> StorageResult<()> {
        let key = format!("{NAMESPACES}{name}");
        let ctx = read::ReadContext::current(self).await?;
        let pending = transaction::PendingBatch::current(self);
        let coord = [(REGISTRY.to_string(), key.clone())];
        if self
            .raw_many(&ctx, &coord, pending.as_ref())?
            .pop()
            .flatten()
            .is_none()
        {
            self.mutate(vec![HomeMutation {
                collection: REGISTRY.into(),
                key,
                body: Some(vec![1]),
            }])
            .await?;
        }
        Ok(())
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

pub(in crate::storage::disk_pack) fn creation_refusal(message: &str) -> StorageError {
    StorageError::InvalidOperation(format!("native home creation refused: {message}"))
}

/// All namespaces on one native disk home.
#[derive(Clone)]
pub struct PackNamespacedStore {
    runtime: Arc<PackRuntime>,
}

#[derive(Clone)]
pub(crate) struct PackKvStore {
    runtime: Arc<PackRuntime>,
    namespace: String,
}

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    match error {
        laststore::Error::Io(io) if StorageError::is_storage_full_io(&io) => {
            StorageError::StorageFull {
                detail: io.to_string(),
            }
        }
        laststore::Error::Io(io) => StorageError::IoError(io),
        laststore::Error::ReadBudgetExceeded => StorageError::ReadResourceLimit {
            detail: error.to_string(),
        },
        laststore::Error::WriteCapacity(detail) => StorageError::WriteResourceLimit { detail },
        _ => StorageError::BackendError(error.to_string()),
    }
}

pub(in crate::storage) fn budget() -> ReadBudget {
    ReadBudget {
        pages: 4096,
        blocks: EAGER_BLOCKS,
        bytes: EAGER_WORK_BYTES,
        keys: 10000,
    }
}

impl PackNamespacedStore {
    pub fn open_keyed(
        path: &Path,
        node_key: Option<&[u8; 32]>,
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        if run_node::is_new_home(path)? || run_node::creation_stopped(path)? {
            return Self::create_run(path, run_node::require_key(node_key)?, capture_accepted);
        }
        Self::open_run(path, node_key, capture_accepted)
    }

    fn assemble(
        physical: physical::V2Home,
        current: physical::V2Root,
        initializing: bool,
        space_policy: SpacePolicy,
        capture_accepted: bool,
    ) -> Self {
        let last_root = Arc::new(view::LastRoot::new(&current));
        let cleanup_job = Arc::new(CleanupJob::open(&physical));
        let mutation_log = mutation_log::MutationLog::new(&physical);
        let accepted = capture_accepted.then(|| {
            crate::storage::disk_backup::accepted::Log::open(physical.path(), current.frontier())
        });
        Self {
            runtime: Arc::new(PackRuntime {
                physical,
                cleanup_job,
                mutation_log,
                accepted,
                view: Arc::new(RwLock::new(view::NativeReadView {
                    current,
                    revision: 0,
                })),
                prepare_gate: Arc::default(),
                write_gate: Arc::default(),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                core: Arc::new(Mutex::new(CoreWarmSet::default())),
                failed: AtomicBool::new(false),
                serving: AtomicBool::new(false),
                initializing: AtomicBool::new(initializing),
                failed_write: Mutex::new(None),
                space_policy,
                last_root,
            }),
        }
    }

    pub(crate) async fn open_namespace(&self, name: &str) -> StorageResult<PackKvStore> {
        if name.len() > 65531 || name == REGISTRY {
            return Err(StorageError::InvalidOperation(
                "namespace exceeds format limit or uses the reserved registry identity".into(),
            ));
        }
        if read::ReadContext::captured(&self.runtime).is_none() {
            self.runtime.ensure_namespace(name).await?;
        }
        Ok(PackKvStore {
            runtime: Arc::clone(&self.runtime),
            namespace: name.to_string(),
        })
    }

    pub fn disk_pack_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }
}
