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
pub mod byte_backup;
mod cleanup;
mod cloud_work;
mod diagnostics;
pub use cleanup::{
    NativeCleanupBoundary, NativeCleanupHome, NativeCleanupPhase, NativeCleanupStatus,
};
pub mod file_dependencies;
pub mod file_recovery;
pub use cloud_work::NativeCloudWorkGuard;
pub use diagnostics::NativeStorageDiagnostics;
mod initialization;
mod kv;
pub(crate) mod logical_paths;
mod memory;
mod namespace_delete;
pub(in crate::storage) mod native_routing;
mod physical;
mod physical_v2;
mod queue;
pub mod range_inventory;
mod read;
pub(crate) mod restore_safety;
mod rows;
mod rows_yielding;
mod run_admission;
mod run_commit;
mod run_node;
pub mod run_restore;
pub(crate) mod schema_keys;
mod space;
mod timing;
pub(crate) mod transaction;
mod view;
mod write;
pub(crate) use crate::resident::CoreMemorySource;
pub use crate::resident::CoreMemoryValue;
pub(in crate::storage) use blocking::run_blocking;
pub use memory::{NativeMemoryPurgeReport, NativeMemoryStatus};
pub(crate) use read::memory::{core_memory_get, core_memory_insert, core_memory_sources};
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
pub use read::PackReadScope;
pub use view::NativeDiskStatus;
pub use write::NativeFlushReceipt;

const NODE_MARKER: &str = "laststore-pack-node-v1";
const REGISTRY: &str = "__native_home";
const NAMESPACES: &str = "namespaces\0";

/// Shared v2 run homes and their complete published root.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    physical: physical::V2Home,
    cleanup_job: Arc<cleanup::CleanupJob>,
    /// Shared with the v2 merge thread, which installs merged roots.
    view: Arc<RwLock<view::NativeReadView>>,
    prepare_gate: Arc<tokio::sync::Mutex<()>>,
    restore_gate: Arc<tokio::sync::Mutex<()>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    persist_metrics: Arc<PersistLaneMetrics>,
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    schema_keys: schema_keys::SchemaKeys,
    failed: AtomicBool,
    initializing: AtomicBool,
    restore_blocked: AtomicBool,
    serving: AtomicBool,
    // A failed commit retains its space charge until this runtime drops.
    failed_write: Mutex<Option<SpaceReservation>>,
    space_policy: laststore::space::SpacePolicy,
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

fn map(error: laststore::Error) -> StorageError {
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

fn budget() -> ReadBudget {
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
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
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
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
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
        Self::open_run(path, Some(node_key))
    }

    /// One runtime over an opened physical home and its published root.
    fn assemble(
        physical: physical::V2Home,
        current: physical::V2Root,
        initializing: bool,
        space_policy: laststore::space::SpacePolicy,
    ) -> Self {
        let metrics = Arc::new(PersistLaneMetrics::new());
        let last_root = Arc::new(view::LastRoot::new(&current));
        let cleanup_job = Arc::new(cleanup::CleanupJob::open(&physical));
        Self {
            runtime: Arc::new(PackRuntime {
                physical,
                cleanup_job,
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
                schema_keys: schema_keys::SchemaKeys::new(),
                failed: AtomicBool::new(false),
                restore_blocked: AtomicBool::new(false),
                serving: AtomicBool::new(false),
                initializing: AtomicBool::new(initializing),
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
