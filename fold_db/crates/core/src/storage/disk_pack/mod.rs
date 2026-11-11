//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use laststore::disk_run::home::{HomeMutation, ReadBudget};
use laststore::space::SpaceReservation;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod blob_backup;
pub use blob_backup::{blob_path_of_key, BlobRow};
mod blocking;
pub(in crate::storage) mod cas_delete;
pub use cas_delete::{CasDeleteRecovery, CasDeleteRecoveryOutcome};
mod diagnostics;
pub use diagnostics::NativeStorageDiagnostics;
mod initialization;
mod kv;
pub(crate) mod logical_paths;
mod memory;
pub(in crate::storage) mod native_routing;
pub(in crate::storage) mod physical;
mod queue;
mod read;
pub(crate) mod record_warm;
mod rows;
mod rows_yielding;
pub(in crate::storage) mod run_admission;
mod run_commit;
pub(in crate::storage) mod run_node;
mod shared_layout;
pub(in crate::storage) mod space;
pub(crate) mod transaction;
mod view;
mod write;
pub use crate::storage::disk_backup::byte_backup;
pub use crate::storage::disk_backup::mutation_log;
pub use crate::storage::disk_backup::run_restore;
pub use crate::storage::disk_backup::NativeCloudWorkGuard;
pub use crate::storage::disk_cleanup::{
    NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus,
};
pub(in crate::storage) use blocking::run_blocking;
pub use memory::{NativeMemoryPurgeReport, NativeMemoryStatus};
pub use physical::CAS_COLLECTION;
pub use read::PackReadScope;
pub use run_admission::{HOME_FORMAT, RECORD_NODE_BYTES};
pub use run_node::{prepare_shared_restored_run, CAS_DIR, RUN_DIR};
pub use view::NativeDiskStatus;
pub use write::NativeFlushReceipt;
pub(in crate::storage) use write::WriteGuard;

use crate::storage::format::{NAMESPACES, REGISTRY};
pub use crate::storage::format::{NODE_MARKER, SHARED_NODE_BYTES};

/// Shared v2 run homes and their complete published root.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    pub(in crate::storage) physical: physical::V2Home,
    pub(in crate::storage) cleanup_job: Arc<crate::storage::disk_cleanup::CleanupJob>,
    pub(in crate::storage) mutation_log: mutation_log::MutationLog,
    /// Shared with the v2 merge thread, which installs merged roots.
    view: Arc<RwLock<view::NativeReadView>>,
    prepare_gate: Arc<tokio::sync::Mutex<()>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    pub(in crate::storage) failed: AtomicBool,
    initializing: AtomicBool,
    pub(in crate::storage) serving: AtomicBool,
    // A failed commit retains its space charge until this runtime drops.
    failed_write: Mutex<Option<SpaceReservation>>,
    pub(in crate::storage) space_policy: laststore::space::SpacePolicy,
    /// The root that the last commit or merge installed, for a status read
    /// that does not wait for the view lock.
    last_root: Arc<view::LastRoot>,
}

impl PackRuntime {
    pub(in crate::storage) fn require_ordinary_backup_home(&self) -> StorageResult<()> {
        if self.physical.shared.is_some() {
            return Err(shared_layout::invalid(
                "dedicated shared homes use the shared snapshot command",
            ));
        }
        Ok(())
    }

    /// True when this runtime serves a dedicated shared database home.
    pub fn is_dedicated_shared_home(&self) -> bool {
        self.physical.shared.is_some()
    }

    pub fn require_direct_shared_home(&self) -> StorageResult<()> {
        if self.physical.shared.is_none() {
            return Err(shared_layout::invalid(
                "direct snapshot requires a dedicated shared disk home",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_staged_shared_rejoin(&self) -> StorageResult<()> {
        self.require_direct_shared_home()?;
        if self.serving.load(Ordering::Acquire) {
            return Err(shared_layout::invalid(
                "shared rejoin requires a staged home",
            ));
        }
        Ok(())
    }
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
        blocks: lastdb_model::storage_policy::native_read::EAGER_BLOCKS,
        bytes: lastdb_model::storage_policy::native_read::EAGER_WORK_BYTES,
        keys: 10000,
    }
}

impl PackNamespacedStore {
    /// Open a node store with the node encryption key. An absent or empty
    /// home becomes a new v2 home, and a v2 creation that stopped before its
    /// node marker is created again. A v2 home needs the key: its run home
    /// keys derive from it. The node marker check admits only [`HOME_FORMAT`].
    pub fn open_keyed(path: &Path, node_key: Option<&[u8; 32]>) -> StorageResult<Self> {
        if run_node::is_new_home(path)? || run_node::creation_stopped(path)? {
            return Self::create_run(path, run_node::require_key(node_key)?);
        }
        Self::open_run(path, node_key)
    }

    /// One runtime over an opened physical home and its published root.
    fn assemble(
        physical: physical::V2Home,
        current: physical::V2Root,
        initializing: bool,
        space_policy: laststore::space::SpacePolicy,
    ) -> Self {
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
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                core: Arc::new(Mutex::new(crate::resident::CoreWarmSet::default())),
                failed: AtomicBool::new(false),
                serving: AtomicBool::new(false),
                initializing: AtomicBool::new(initializing),
                failed_write: Mutex::new(None),
                space_policy,
                last_root,
            }),
        }
    }
}

impl PackNamespacedStore {
    pub(crate) async fn open_namespace(&self, name: &str) -> StorageResult<PackKvStore> {
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
        Ok(PackKvStore {
            runtime: Arc::clone(&self.runtime),
            namespace: name.to_string(),
        })
    }

    pub fn disk_pack_runtime(&self) -> Arc<PackRuntime> {
        Arc::clone(&self.runtime)
    }
}
