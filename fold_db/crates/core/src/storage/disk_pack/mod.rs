//! Explicit native disk backend. Activation remains gated until its complete write surface is wired.

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
mod kv;
mod maintenance;
mod read;
mod rows;
pub(crate) mod transaction;
mod view;
mod write;
pub use maintenance::{NativeMaintenanceOptions, NativeMaintenanceReport};
pub(crate) use read::snapshots::{read_snapshot_active, read_snapshot_get, read_snapshot_insert};
pub use read::PackReadScope;

const NODE_MARKER: &str = "laststore-pack-node-v1";
const REGISTRY: &str = "__native_home";
const NAMESPACES: &str = "namespaces\0";

/// Shared native home, complete published root, and exclusive journal owner.
/// Only the complete root advances the readable frontier and write protection.
pub struct PackRuntime {
    home: Arc<DiskHome>,
    view: RwLock<view::NativeReadView>,
    journal: Mutex<HomeJournal>,
    write_gate: Arc<tokio::sync::Mutex<()>>,
    logical: Arc<Mutex<LogicalResidentSet>>,
    published: AtomicU64,
    failed: AtomicBool,
    failed_write: Mutex<Option<admission::WriteProtection>>,
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
    /// The caller must reject cloud sync and memory acknowledgements until their native gates are wired.
    pub fn open(path: &Path) -> StorageResult<Self> {
        if path.join("laststore-pack-home-v1").try_exists()? {
            return Self::open_home(path);
        }
        if crate::resident::ResidentPolicy::from_env()
            .mode
            .reads_resident_first()
        {
            return Err(StorageError::ConfigurationError(
                "native disk resident routing requires complete read-root and pre-apply admission gates".into(),
            ));
        }
        use std::io::{Read, Write};
        let expected = b"format=1\nhome=disk-pack\n";
        let marker = path.join(NODE_MARKER);
        if marker.try_exists()? {
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
            laststore::disk_pack::home::write_legacy_fence(path).map_err(map)?;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&marker)?;
            file.write_all(expected)?;
            file.sync_all()?;
            std::fs::File::open(path)?.sync_all()?;
            HomeBuilder::create(
                &path.join("disk-pack"),
                HomeDescriptor::new(16, 0).map_err(map)?,
                0,
            )
            .map_err(map)?
            .finish()
            .map_err(map)?;
        }
        Self::open_home(&path.join("disk-pack"))
    }

    /// Open a completed converted or restored native home with its original node identity.
    pub(crate) fn open_home(path: &Path) -> StorageResult<Self> {
        if crate::resident::ResidentPolicy::from_env()
            .mode
            .reads_resident_first()
        {
            return Err(StorageError::ConfigurationError(
                "native resident routing requires complete logical record ownership".into(),
            ));
        }
        let home = Arc::new(DiskHome::open(path).map_err(map)?);
        let journal = home.journal().map_err(map)?;
        let current = journal.current().clone();
        let frontier = current.frontier;
        Ok(Self {
            runtime: Arc::new(PackRuntime {
                home,
                view: RwLock::new(view::NativeReadView {
                    current,
                    accepted: None,
                }),
                journal: Mutex::new(journal),
                write_gate: Arc::new(tokio::sync::Mutex::new(())),
                logical: Arc::new(Mutex::new(
                    LogicalResidentSet::new().with_metrics(Arc::new(ResidentMetrics::new())),
                )),
                published: AtomicU64::new(frontier),
                failed: AtomicBool::new(false),
                failed_write: Mutex::new(None),
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

    async fn delete_namespace(&self, _name: &str) -> StorageResult<bool> {
        Err(StorageError::InvalidOperation(
            "native namespace removal requires a complete named-key plan".into(),
        ))
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
