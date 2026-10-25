//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use super::traits::{KvStore, NamespacedStore};
use crate::resident::PersistLaneMetrics;
use laststore::disk_pack::home::{DiskHome, HomeMutation, HomeSnapshot};
use laststore::disk_pack::maintenance::SpaceReservation;
use laststore::disk_pack::{Current, ReadBudget};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod backup;
pub mod backup_capture;
mod boundary;
pub mod byte_backup;
mod cloud_work;
mod convert_envelopes;
mod convert_v2;
mod convert_v2_copy;
mod convert_v2_switch;
pub mod file_dependencies;
pub mod file_recovery;
pub use cloud_work::NativeCloudWorkGuard;
pub use convert_envelopes::convert_transferred_node;
pub use convert_v2::{kept_v1_path, DiskConversionReport};
mod initialization;
mod kv;
pub(crate) mod logical_paths;
mod maintenance;
mod memory;
mod namespace_delete;
mod parts;
mod physical;
mod physical_v2;
mod queue;
mod read;
mod restore;
pub(crate) mod restore_safety;
mod rows;
mod rows_yielding;
mod run_commit;
mod run_node;
pub(crate) mod schema_keys;
mod space;
mod timing;
pub(crate) mod transaction;
mod view;
mod write;
pub(crate) use crate::resident::CoreMemorySource;
pub use crate::resident::CoreMemoryValue;
pub use boundary::CoreNativeBackupBoundary;
pub use maintenance::{NativeMaintenanceOptions, NativeMaintenanceReport};
pub use memory::{NativeMemoryPurgeReport, NativeMemoryStatus};
pub use parts::NativePartsStatus;
pub(crate) use read::memory::{core_memory_get, core_memory_insert, core_memory_sources};
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
pub use read::PackReadScope;
pub use restore::{
    prepare_restored_node, validate_restored_atom_key, validate_restored_node, RestoredNodeGuard,
};
pub use view::NativeDiskStatus;
pub use write::NativeFlushReceipt;

const NODE_MARKER: &str = "laststore-pack-node-v1";
const REGISTRY: &str = "__native_home";
const NAMESPACES: &str = "namespaces\0";

/// Shared native home, complete published root, and exclusive snapshot writer.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    physical: physical::Physical,
    /// Shared with the v2 merge thread, which installs merged roots.
    view: Arc<RwLock<view::NativeReadView>>,
    prepare_gate: Arc<tokio::sync::Mutex<()>>,
    restore_gate: Arc<tokio::sync::Mutex<()>>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    compaction_slots: Arc<tokio::sync::Semaphore>,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    native_backup_boundary: tokio::sync::watch::Sender<CoreNativeBackupBoundary>,
    persist_metrics: Arc<PersistLaneMetrics>,
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    schema_keys: schema_keys::SchemaKeys,
    failed: AtomicBool,
    initializing: AtomicBool,
    restore_blocked: AtomicBool,
    serving: AtomicBool,
    node_home: Option<initialization::NodeHome>,
    // Failed snapshot publication retains its space charge until this runtime drops.
    failed_write: Mutex<Option<SpaceReservation>>,
    space_policy: laststore::disk_pack::maintenance::SpacePolicy,
    parts: Arc<parts::PartsLedger>,
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
    /// Open a native node store, or create the native format in an empty home.
    /// The caller must reject cloud sync until its native transfer gate is wired.
    pub fn open(path: &Path) -> StorageResult<Self> {
        Self::open_keyed(path, None)
    }

    /// Open a node store with the node encryption key. A v2 run home needs
    /// the key: its home key derives from it. With the key and
    /// `LASTDB_DISK_FORMAT=2`, a v1 home is converted to v2 once first.
    pub fn open_keyed(path: &Path, node_key: Option<&[u8; 32]>) -> StorageResult<Self> {
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
        let space_policy = space::policy_from_env()?;
        if let Some(node_key) = node_key {
            convert_v2::resume_or_convert(path, node_key)?;
        }
        if path.join("laststore-pack-home-v1").try_exists()? {
            return Self::open_home(path);
        }
        if run_node::is_run_node(path)? {
            return Self::open_run(path, node_key);
        }
        if run_node::wants_new_run_node(path)? {
            return Self::create_run(path, run_node::require_key(node_key)?);
        }
        let node_home = initialization::NodeHome::open(path, &space_policy)?;
        Self::open_physical(&path.join("disk-pack"), Some(node_home))
    }

    /// Open the native physical home beneath the node format fence.
    pub(crate) fn open_home(path: &Path) -> StorageResult<Self> {
        initialization::refuse_in_progress_parent(path)?;
        Self::open_physical(path, None)
    }

    fn open_physical(
        path: &Path,
        node_home: Option<initialization::NodeHome>,
    ) -> StorageResult<Self> {
        let space_policy = space::policy_from_env()?;
        let home = Arc::new(DiskHome::open(path).map_err(map)?);
        let writer = home.snapshot().map_err(map)?;
        let restore_blocked = restore_safety::pending(&home, path)?;
        let read_lease = writer.read_lease();
        let capture_cleanup = writer.capture_cleanup();
        let current = writer.current().clone();
        let native_backup_boundary = tokio::sync::watch::channel(CoreNativeBackupBoundary {
            source_home: home.descriptor().identity,
            frontier: current.frontier,
        })
        .0;
        let parts = parts::PartsLedger::from_env(
            writer.dependency_parts(),
            current.generation,
            current.frontier,
        );
        let physical = physical::Physical::V1(Box::new(physical::V1Home {
            home,
            writer: Mutex::new(writer),
            read_lease,
            capture_cleanup,
        }));
        Ok(Self::assemble(
            physical,
            physical::PhysicalRoot::V1(current),
            native_backup_boundary,
            parts,
            restore_blocked,
            node_home,
            space_policy,
        ))
    }

    /// One runtime over an opened physical home and its published root.
    fn assemble(
        physical: physical::Physical,
        current: physical::PhysicalRoot,
        native_backup_boundary: tokio::sync::watch::Sender<CoreNativeBackupBoundary>,
        parts: Arc<parts::PartsLedger>,
        restore_blocked: bool,
        node_home: Option<initialization::NodeHome>,
        space_policy: laststore::disk_pack::maintenance::SpacePolicy,
    ) -> Self {
        let metrics = Arc::new(PersistLaneMetrics::new());
        Self {
            runtime: Arc::new(PackRuntime {
                physical,
                view: Arc::new(RwLock::new(view::NativeReadView {
                    current,
                    revision: 0,
                })),
                prepare_gate: Arc::new(tokio::sync::Mutex::new(())),
                restore_gate: Arc::new(tokio::sync::Mutex::new(())),
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                compaction_slots: Arc::new(tokio::sync::Semaphore::new(2)),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                native_backup_boundary,
                persist_metrics: metrics,
                core: Arc::new(Mutex::new(crate::resident::CoreWarmSet::default())),
                schema_keys: schema_keys::SchemaKeys::new(),
                failed: AtomicBool::new(false),
                restore_blocked: AtomicBool::new(restore_blocked),
                serving: AtomicBool::new(false),
                initializing: AtomicBool::new(
                    node_home.as_ref().is_some_and(|home| home.initializing),
                ),
                node_home,
                failed_write: Mutex::new(None),
                space_policy,
                parts,
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
        super::laststore::LastStoreKvStore::run_blocking(move || {
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

    async fn flush_written_keys(&self, _keys: &[laststore::ShardKey]) -> StorageResult<()> {
        self.runtime.flush().await
    }

    fn disk_pack_runtime(&self) -> Option<Arc<PackRuntime>> {
        Some(Arc::clone(&self.runtime))
    }
}
