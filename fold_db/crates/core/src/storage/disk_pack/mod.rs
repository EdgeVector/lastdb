//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use super::traits::{KvStore, NamespacedStore};
use crate::resident::PersistLaneMetrics;
use laststore::disk_run::home::{HomeMutation, ReadBudget};
use laststore::space::SpaceReservation;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod blocking;
pub(in crate::storage) mod cas_delete;
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
mod diagnostics;
pub use diagnostics::NativeStorageDiagnostics;
mod initialization;
mod kv;
pub mod logical_capture;
pub(crate) mod logical_paths;
mod memory;
mod namespace_delete;
pub(in crate::storage) mod native_routing;
pub(in crate::storage) mod physical;
pub(in crate::storage) mod physical_v2;
mod queue;
mod read;
pub(crate) mod record_warm;
pub(crate) mod restore_safety;
mod rows;
mod rows_yielding;
pub(in crate::storage) mod run_admission;
mod run_commit;
pub(in crate::storage) mod run_node;
pub(in crate::storage) mod space;
mod timing;
pub(crate) mod transaction;
mod view;
mod write;
pub(crate) use crate::resident::CoreMemorySource;
pub use crate::resident::CoreMemoryValue;
pub use crate::storage::disk_backup::byte_backup;
pub use crate::storage::disk_backup::file_dependencies;
pub use crate::storage::disk_backup::mutation_log;
pub use crate::storage::disk_backup::run_restore;
pub use crate::storage::disk_backup::NativeCloudWorkGuard;
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus,
};
pub(in crate::storage) use blocking::run_blocking;
pub use memory::{NativeMemoryPurgeReport, NativeMemoryStatus};
pub(crate) use read::memory::{
    core_memory_get, core_memory_get_for_write, core_memory_insert, core_memory_sources,
};
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get};
pub use read::PackReadScope;
pub(crate) use rows::WHOLE_COLLECTION_END;
pub use view::NativeDiskStatus;
pub use write::NativeFlushReceipt;

pub(in crate::storage) const NODE_MARKER: &str = "laststore-pack-node-v1";
const REGISTRY: &str = "__native_home";
const NAMESPACES: &str = "namespaces\0";

/// Shared v2 run homes and their complete published root.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    pub(in crate::storage) physical: physical::V2Home,
    pub(in crate::storage) cleanup_job: Arc<crate::storage::disk_cleanup::CleanupJob>,
    pub(in crate::storage) mutation_log: mutation_log::MutationLog,
    /// Shared with the v2 merge thread, which installs merged roots.
    view: Arc<RwLock<view::NativeReadView>>,
    prepare_gate: Arc<tokio::sync::Mutex<()>>,
    restore_gate: Arc<tokio::sync::Mutex<()>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    persist_metrics: Arc<PersistLaneMetrics>,
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    pub(in crate::storage) failed: AtomicBool,
    initializing: AtomicBool,
    /// True for a format 7 home: the record model. Only such a home uses the
    /// `rk:` record keys and mints a record space key bundle for a schema.
    record_home: bool,
    restore_blocked: AtomicBool,
    pub(in crate::storage) serving: AtomicBool,
    // A failed commit retains its space charge until this runtime drops.
    failed_write: Mutex<Option<SpaceReservation>>,
    pub(in crate::storage) space_policy: laststore::space::SpacePolicy,
    /// The root that the last commit or merge installed, for a status read
    /// that does not wait for the view lock.
    last_root: Arc<view::LastRoot>,
}

/// All namespaces on one native disk home.
#[derive(Clone)]
pub struct PackNamespacedStore {
    runtime: Arc<PackRuntime>,
}

struct PackKvStore {
    runtime: Arc<PackRuntime>,
    namespace: String,
}

pub(in crate::storage) fn map(error: laststore::Error) -> StorageError {
    match error {
        laststore::Error::NativeProof(code) => StorageError::NativeProof {
            stage: crate::storage::error::NativeProofStage::NativeReader,
            cause: crate::storage::error::NativeProofCause::Static(code),
        },
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
        blocks: lastdb_storage_policy::native_read::EAGER_BLOCKS,
        bytes: lastdb_storage_policy::native_read::EAGER_WORK_BYTES,
        keys: 10000,
    }
}

impl PackNamespacedStore {
    /// Open a node store with the node encryption key. An absent or empty
    /// home becomes a new v2 home, and a v2 creation that stopped before its
    /// node marker is created again. A v2 home needs the key: its run home
    /// keys derive from it. Any other home is refused: this program opens
    /// only disk format v2.
    pub fn open_keyed(path: &Path, node_key: Option<&[u8; 32]>) -> StorageResult<Self> {
        if run_node::is_run_node(path)? {
            return Self::open_run(path, node_key);
        }
        if run_node::is_new_home(path)? || run_node::creation_stopped(path)? {
            return Self::create_run(path, run_node::require_key(node_key)?);
        }
        Err(run_node::not_v2(path))
    }

    /// Open an existing v2 node home for an offline tool. The home must be
    /// complete: it refuses a home whose creation has not written its
    /// key-format proofs. It never creates a home or finishes a creation;
    /// the node does those when it starts.
    pub fn open_existing(path: &Path, node_key: &[u8; 32]) -> StorageResult<Self> {
        Self::open_complete_existing(path, node_key, true)
    }

    /// A stopped-home backup owns both native homes without a logical database
    /// boot or merge worker. Capture may write only its normal backup metadata.
    pub fn open_existing_for_backup(path: &Path, node_key: &[u8; 32]) -> StorageResult<Self> {
        Self::open_complete_existing(path, node_key, false)
    }

    fn open_complete_existing(
        path: &Path,
        node_key: &[u8; 32],
        resume_merges: bool,
    ) -> StorageResult<Self> {
        if !path.join(NODE_MARKER).try_exists()? || run_node::creation_pending(path)? {
            return Err(StorageError::InvalidOperation(format!(
                "an offline tool needs a complete node home at {}; start the node once, then \
                 stop it",
                path.display()
            )));
        }
        if !run_node::is_run_node(path)? {
            return Err(run_node::not_v2(path));
        }
        Self::open_run_with_merges(path, Some(node_key), resume_merges)
    }

    /// One runtime over an opened physical home and its published root.
    fn assemble(
        physical: physical::V2Home,
        current: physical::V2Root,
        initializing: bool,
        record_home: bool,
        space_policy: laststore::space::SpacePolicy,
    ) -> Self {
        let metrics = Arc::new(PersistLaneMetrics::new());
        let last_root = Arc::new(view::LastRoot::new(&current));
        let cleanup_job = Arc::new(crate::storage::disk_cleanup::CleanupJob::open(&physical));
        let mutation_log = mutation_log::MutationLog::new(&physical);
        Self {
            runtime: Arc::new(PackRuntime {
                physical,
                cleanup_job,
                mutation_log,
                view: Arc::new(RwLock::new(view::NativeReadView {
                    current,
                    revision: 0,
                })),
                prepare_gate: Arc::new(tokio::sync::Mutex::new(())),
                restore_gate: Arc::new(tokio::sync::Mutex::new(())),
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                persist_metrics: metrics,
                core: Arc::new(Mutex::new(crate::resident::CoreWarmSet::default())),
                failed: AtomicBool::new(false),
                restore_blocked: AtomicBool::new(false),
                serving: AtomicBool::new(false),
                initializing: AtomicBool::new(initializing),
                record_home,
                failed_write: Mutex::new(None),
                space_policy,
                last_root,
            }),
        }
    }
}

impl PackRuntime {
    /// Share one process counter owner with DbOperations and its background work.
    pub(crate) fn persist_metrics(&self) -> Arc<PersistLaneMetrics> {
        Arc::clone(&self.persist_metrics)
    }

    /// True for a format 7 home: the record model.
    pub(crate) const fn record_home(&self) -> bool {
        self.record_home
    }
}

#[async_trait::async_trait]
impl NamespacedStore for PackNamespacedStore {
    async fn open_namespace(&self, name: &str) -> StorageResult<Arc<dyn KvStore>> {
        if name.len() > 65531 || name == REGISTRY {
            return Err(StorageError::InvalidOperation(
                "namespace exceeds format limit or uses the reserved registry identity".into(),
            ));
        }
        // A query may inspect an absent namespace. Its held root stays read-only;
        // a later mutation registers the namespace in the same complete batch.
        if !read::ReadContext::active(&self.runtime) {
            self.runtime.ensure_namespace(name).await?;
        }
        Ok(Arc::new(PackKvStore {
            runtime: Arc::clone(&self.runtime),
            namespace: name.to_string(),
        }))
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<String>> {
        let context = read::ReadContext::current(&self.runtime).await?;
        let pending = transaction::PendingBatch::current(&self.runtime);
        let runtime = Arc::clone(&self.runtime);
        run_blocking(move || {
            let (rows, complete) = runtime.raw_rows(
                &context,
                REGISTRY,
                NAMESPACES.as_bytes(),
                b"namespaces\x01",
                10000,
                pending.as_ref(),
            )?;
            if !complete {
                return Err(StorageError::ReadResourceLimit {
                    detail: "native namespace list exceeds its call limit".into(),
                });
            }
            rows.into_iter()
                .map(|(key, body)| {
                    if body != [1] {
                        return Err(StorageError::BackendError(
                            "invalid native namespace registry value".into(),
                        ));
                    }
                    Ok(key[NAMESPACES.len()..].to_string())
                })
                .collect()
        })
        .await
    }

    async fn delete_namespace(&self, name: &str) -> StorageResult<bool> {
        self.runtime.delete_namespace(name).await
    }

    async fn restore_durability_barrier(&self) -> StorageResult<()> {
        self.runtime.flush().await
    }

    fn disk_pack_runtime(&self) -> Option<Arc<PackRuntime>> {
        Some(Arc::clone(&self.runtime))
    }
}
