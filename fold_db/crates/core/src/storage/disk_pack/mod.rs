//! Native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use super::traits::{KvStore, NamespacedStore};
use crate::resident::PersistLaneMetrics;
use laststore::disk_pack::home::{DiskHome, HomeJournal, HomeMutation};
use laststore::disk_pack::maintenance::SpaceReservation;
use laststore::disk_pack::{Current, ReadBudget};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod backup;
mod initialization;
mod kv;
pub(crate) mod logical_paths;
mod maintenance;
mod memory;
mod namespace_delete;
mod queue;
mod read;
mod rows;
pub(crate) mod schema_keys;
mod space;
pub(crate) mod transaction;
mod view;
mod write;
pub(crate) use crate::resident::CoreMemorySource;
pub use crate::resident::CoreMemoryValue;
pub use maintenance::{NativeMaintenanceOptions, NativeMaintenanceReport};
pub use memory::{NativeMemoryPurgeReport, NativeMemoryStatus};
pub(crate) use read::memory::{core_memory_get, core_memory_insert, core_memory_sources};
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
pub use read::PackReadScope;
pub use view::NativeDiskStatus;
pub use write::NativeFlushReceipt;

const NODE_MARKER: &str = "laststore-pack-node-v1";
const REGISTRY: &str = "__native_home";
const NAMESPACES: &str = "namespaces\0";

/// Shared native home, complete published root, and exclusive journal owner.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    home: Arc<DiskHome>,
    view: RwLock<view::NativeReadView>,
    journal: Mutex<HomeJournal>,
    read_lease: laststore::disk_pack::lifetime::WriterReadLease,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    flush_notify: tokio::sync::Notify,
    queue: Mutex<queue::WriteQueue>,
    completed: tokio::sync::watch::Sender<u64>,
    persist_metrics: Arc<PersistLaneMetrics>,
    core: Arc<Mutex<crate::resident::CoreWarmSet>>,
    schema_keys: schema_keys::SchemaKeys,
    failed: AtomicBool,
    initializing: AtomicBool,
    node_home: Option<initialization::NodeHome>,
    // Failed journal publication retains its space charge until this runtime drops.
    failed_write: Mutex<Option<SpaceReservation>>,
    space_policy: laststore::disk_pack::maintenance::SpacePolicy,
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
        blocks: 16384,
        bytes: 256 * 1024 * 1024,
        keys: 10000,
    }
}

impl PackNamespacedStore {
    /// Open a native node store, or create the native format in an empty home.
    /// The caller must reject cloud sync until its native transfer gate is wired.
    pub fn open(path: &Path) -> StorageResult<Self> {
        crate::db_operations::AtomStore::validate_boot_key_encoding_settings()?;
        let space_policy = space::policy_from_env()?;
        if path.join("laststore-pack-home-v1").try_exists()? {
            return Self::open_home(path);
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
        let journal = home.journal().map_err(map)?;
        let read_lease = journal.read_lease();
        let current = journal.current().clone();
        let metrics = Arc::new(PersistLaneMetrics::new());
        Ok(Self {
            runtime: Arc::new(PackRuntime {
                home,
                view: RwLock::new(view::NativeReadView {
                    current,
                    accepted: Arc::new(Vec::new()),
                    revision: 0,
                    sequence: 0,
                }),
                journal: Mutex::new(journal),
                read_lease,
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                flush_notify: tokio::sync::Notify::new(),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                persist_metrics: metrics,
                core: Arc::new(Mutex::new(crate::resident::CoreWarmSet::default())),
                schema_keys: schema_keys::SchemaKeys::new(),
                failed: AtomicBool::new(false),
                initializing: AtomicBool::new(
                    node_home.as_ref().is_some_and(|home| home.initializing),
                ),
                node_home,
                failed_write: Mutex::new(None),
                space_policy,
            }),
        })
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
