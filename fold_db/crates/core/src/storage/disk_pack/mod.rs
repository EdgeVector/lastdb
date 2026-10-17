//! Explicit native disk backend with complete-request admission and captured read roots.

use super::error::{StorageError, StorageResult};
use super::traits::{KvStore, NamespacedStore};
use crate::resident::{LogicalResidentSet, ResidentMetrics};
use laststore::disk_pack::home::{
    DiskHome, HomeBuilder, HomeDescriptor, HomeJournal, HomeMutation,
};
use laststore::disk_pack::{Current, ReadBudget};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

mod admission;
mod backup;
mod kv;
pub(crate) mod logical_paths;
mod maintenance;
mod namespace_delete;
mod queue;
mod read;
mod rows;
mod space;
pub(crate) mod transaction;
mod validity;
mod view;
mod write;
pub use logical_paths::warm::{remember_wire_query, warm_wire_query, WarmQueryFields};
pub use maintenance::{NativeMaintenanceOptions, NativeMaintenanceReport};
pub(crate) use read::note_molecule_header;
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
pub use read::PackReadScope;
pub(crate) use transaction::note_header_write;
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
    validity: Arc<Mutex<validity::Validity>>,
    logical: Arc<Mutex<LogicalResidentSet>>,
    published: AtomicU64,
    failed: AtomicBool,
    fresh_empty: AtomicBool,
    failed_write: Mutex<Option<Arc<admission::WriteProtection>>>,
    failed_requests: Mutex<Vec<Arc<transaction::PendingBatch>>>,
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

pub(crate) fn requested(path: &Path) -> bool {
    path.join(NODE_MARKER).exists()
        || path.join("laststore-pack-home-v1").exists()
        || path.join("laststore-native-source-sealed-v1").exists()
        || env_flag::var_parse("LASTDB_DISK_PACK").unwrap_or(false)
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
    /// Open an explicitly fenced native node store, or create an empty requested home.
    /// The caller must reject cloud sync until its native transfer gate is wired.
    pub fn open(path: &Path) -> StorageResult<Self> {
        let space_policy = space::policy_from_env()?;
        if path.join("laststore-pack-home-v1").try_exists()? {
            return Self::open_home(path);
        }
        space::validate_resident_policy()?;
        use std::io::{Read, Write};
        let expected = b"format=1\nhome=disk-pack\n";
        let marker = path.join(NODE_MARKER);
        let exists = marker.try_exists()?;
        let created = !exists;
        if exists {
            let mut fence = std::fs::File::open(path.join("laststore-layout-v1"))?;
            let expected_fence = laststore::disk_pack::home::LEGACY_FENCE_BYTES;
            if fence.metadata()?.len() != expected_fence.len() as u64 {
                return Err(StorageError::ConfigurationError(
                    "native node downgrade fence differs from its format".into(),
                ));
            }
            let mut bytes = vec![0; expected_fence.len()];
            fence.read_exact(&mut bytes)?;
            if bytes != expected_fence {
                return Err(StorageError::ConfigurationError(
                    "native node downgrade fence is invalid".into(),
                ));
            }
            let mut bytes = Vec::new();
            std::fs::File::open(&marker)?
                .take(128)
                .read_to_end(&mut bytes)?;
            if bytes != expected {
                return Err(StorageError::BackendError(
                    "invalid native node home fence".into(),
                ));
            }
        } else {
            if !env_flag::var_parse("LASTDB_DISK_PACK").unwrap_or(false) {
                return Err(StorageError::InvalidOperation(
                    "native creation requires explicit selection".into(),
                ));
            }
            std::fs::create_dir_all(path)?;
            if std::fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::InvalidOperation(
                    "native creation refuses a nonempty legacy home".into(),
                ));
            }
            // Empty construction has no data-dependent output: one empty tree,
            // descriptor, current and journal-tail metadata plus directories.
            // Reserve a conservative MiB before writing any format fence.
            let _initial_space =
                laststore::disk_pack::maintenance::reserve_space(path, &space_policy, 1024 * 1024)
                    .map_err(map)?;
            laststore::disk_pack::home::write_legacy_fence(path).map_err(map)?;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&marker)?;
            file.write_all(expected)?;
            file.sync_all()?;
            std::fs::File::open(path)?.sync_all()?;
            HomeBuilder::create_empty_with_schema_record_refs(
                &path.join("disk-pack"),
                // New native homes match the plain LastStore fanout. Existing
                // homes always retain the fanout in their saved descriptor.
                HomeDescriptor::new(1, 0).map_err(map)?,
            )
            .map_err(map)?;
        }
        let store = Self::open_home(&path.join("disk-pack"))?;
        store.runtime.fresh_empty.store(created, Ordering::Release);
        Ok(store)
    }

    /// Open a completed converted or restored native home with its original node identity.
    pub(crate) fn open_home(path: &Path) -> StorageResult<Self> {
        let space_policy = space::policy_from_env()?;
        space::validate_resident_policy()?;
        let home = Arc::new(DiskHome::open(path).map_err(map)?);
        let journal = home.journal().map_err(map)?;
        let read_lease = journal.read_lease();
        let current = journal.current().clone();
        let frontier = current.frontier;
        Ok(Self {
            runtime: Arc::new(PackRuntime {
                home,
                view: RwLock::new(view::NativeReadView {
                    current,
                    accepted: Arc::new(Vec::new()),
                    revision: 0,
                }),
                journal: Mutex::new(journal),
                read_lease,
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                flush_notify: tokio::sync::Notify::new(),
                queue: Mutex::new(queue::WriteQueue::default()),
                completed: tokio::sync::watch::channel(0).0,
                validity: Arc::new(Mutex::new(validity::Validity::default())),
                logical: Arc::new(Mutex::new(
                    LogicalResidentSet::new().with_metrics(Arc::new(ResidentMetrics::new())),
                )),
                published: AtomicU64::new(frontier),
                failed: AtomicBool::new(false),
                fresh_empty: AtomicBool::new(false),
                failed_write: Mutex::new(None),
                failed_requests: Mutex::new(Vec::new()),
                space_policy,
            }),
        })
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

    fn logical_resident_set(&self) -> Option<Arc<Mutex<LogicalResidentSet>>> {
        Some(Arc::clone(&self.runtime.logical))
    }

    fn disk_pack_runtime(&self) -> Option<Arc<PackRuntime>> {
        Some(Arc::clone(&self.runtime))
    }

    fn plaintext_read_cache(&self) -> Option<Arc<Mutex<LogicalResidentSet>>> {
        // The native cache holds exactly the stored bytes. A second plaintext
        // cache would double admission and could mix committed generations.
        None
    }
}
