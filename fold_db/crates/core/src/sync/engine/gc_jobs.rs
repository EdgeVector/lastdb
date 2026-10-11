//! Local GC receipts. These are sidecars, not replicated application records.
//!
//! Acceptance and status touch only fixed index / exact job keys. The index
//! contains at most 16 nonterminal jobs; object receipts use numeric point keys.
//! A receipt is durable before dispatch. Recovery never replays a keep set or
//! a DELETE: an unfinished dispatch remains unknown and the job is interrupted.
//! This does not supply the cloud-side cancellation/instance fence required by
//! the later cloud protocol. In particular a DELETE 2xx is an acknowledgement,
//! not proof of deleted-now bytes or reconciled billing.

use super::{BackupOrphanGcReport, SyncEngine};
use crate::clock::unix_millis;
use crate::sync::error::{SyncError, SyncResult};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod receipts;
pub use self::receipts::*;
mod persist;
pub(super) use self::persist::*;

/// Newest receipt format this build writes. A receipt with no version 2
/// field still serializes as version 1, byte-for-byte as P1 wrote it.
pub const GC_RECEIPT_VERSION: u32 = 2;
/// Oldest receipt format this build reads. Reads accept 1 and 2 only.
pub const GC_RECEIPT_MIN_VERSION: u32 = 1;
/// Named error prefix for a receipt whose version is outside the read range.
pub const GC_RECEIPT_VERSION_UNSUPPORTED: &str = "GC_RECEIPT_VERSION_UNSUPPORTED";
const MAX_ACTIVE: usize = 16;
const MAX_RETAINED_JOBS: u64 = 4096;
const MAX_RESERVED_OBJECT_RECEIPTS: u64 = 1_048_576;
const RECEIPT_RESERVATION_BATCH: u64 = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Index {
    /// Absent in P1 archives, which read as version 1. Written as the newest
    /// version so a later migration tool can stamp what it found.
    #[serde(default = "index_version_default")]
    version: u32,
    active: Vec<String>,
    latest: Option<String>,
    #[serde(default)]
    retained_jobs: u64,
    #[serde(default)]
    reserved_object_receipts: u64,
}

impl Default for Index {
    fn default() -> Self {
        Self {
            version: GC_RECEIPT_VERSION,
            active: Vec::new(),
            latest: None,
            retained_jobs: 0,
            reserved_object_receipts: 0,
        }
    }
}

pub(crate) struct GcJobManager {
    shared: Arc<GcJobStore>,
    /// Status-only handles never register a new engine or revoke its lease.
    engine_epoch: Option<u64>,
}

pub(crate) struct GcJobStore {
    root: Option<PathBuf>,
    // Serializes local metadata only. Never hold this during network or a
    // publication/executor lock wait.
    index: Mutex<Option<Index>>,
    live: Mutex<HashSet<String>>,
    engines: AtomicUsize,
    epoch: AtomicU64,
    executor: Arc<tokio::sync::Mutex<()>>,
    publication: Arc<tokio::sync::Mutex<()>>,
}

impl std::ops::Deref for GcJobManager {
    type Target = GcJobStore;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl Drop for GcJobManager {
    fn drop(&mut self) {
        if self.engine_epoch.is_some() {
            // The last engine can disappear while a status handle still owns
            // the store. Invalidate runtime liveness, not durable receipts.
            let mut index = self
                .shared
                .index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.engines.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.live
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
                *index = None;
            }
        }
    }
}

/// A local-only boot can still retrieve old GC results without credentials.
/// The registry also retains detached engines after Host removes its slot.
/// A status handle shares their liveness; it never declares those jobs lost.
pub fn backup_gc_receipts_without_engine(
    sidecar_dir: PathBuf,
    id: Option<&str>,
    after: u64,
    limit: usize,
) -> SyncResult<(Option<GcJobReceipt>, Vec<GcObjectReceipt>)> {
    if !sidecar_dir.join("backup_gc_jobs/index.json").exists() && id.is_none() {
        return Ok((None, Vec::new()));
    }
    let manager = GcJobManager::open(Some(sidecar_dir), false);
    let job = manager.get(id)?;
    let objects = job
        .as_ref()
        .map(|job| manager.objects(&job.job_id, after, limit))
        .transpose()?
        .unwrap_or_default();
    Ok((job, objects))
}

impl GcJobManager {
    pub(crate) fn new(sidecar_dir: Option<PathBuf>) -> Self {
        Self::open(sidecar_dir, true)
    }

    fn open(sidecar_dir: Option<PathBuf>, engine: bool) -> Self {
        type Registry = Mutex<HashMap<PathBuf, Weak<GcJobStore>>>;
        static STORES: OnceLock<Registry> = OnceLock::new();
        // Resolve aliases before choosing the shared owner. An unavailable
        // sidecar fails closed, including legacy startup without a source.
        let root = sidecar_dir
            .and_then(|dir| std::fs::canonicalize(dir).ok())
            .map(|dir| dir.join("backup_gc_jobs"));
        let mut registry = STORES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|_, store| store.strong_count() > 0);
        let shared = root
            .as_ref()
            .and_then(|root| registry.get(root))
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Arc::new(GcJobStore {
                    root: root.clone(),
                    index: Mutex::new(None),
                    live: Mutex::new(HashSet::new()),
                    engines: AtomicUsize::new(0),
                    epoch: AtomicU64::new(0),
                    executor: Arc::new(tokio::sync::Mutex::new(())),
                    publication: Arc::new(tokio::sync::Mutex::new(())),
                })
            });
        if let Some(root) = root {
            registry.insert(root, Arc::downgrade(&shared));
        }
        let engine_epoch = engine.then(|| {
            let _index = shared
                .index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            shared.engines.fetch_add(1, Ordering::SeqCst);
            shared.epoch.fetch_add(1, Ordering::SeqCst) + 1
        });
        Self {
            shared,
            engine_epoch,
        }
    }

    pub(crate) fn executor_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.executor)
    }

    pub(crate) fn publication_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.publication)
    }

    /// Call under the shared publication turn before selection and every
    /// DELETE. Replacement revokes old engines permanently. While two engine
    /// objects coexist, neither GC can omit the other's held publication cut.
    pub(crate) fn require_current_engine(&self) -> SyncResult<()> {
        if self.engines.load(Ordering::SeqCst) != 1
            || self.engine_epoch != Some(self.epoch.load(Ordering::SeqCst))
        {
            return Err(io_error("GC engine replaced or overlapping; no DELETE"));
        }
        Ok(())
    }

    fn directory_barrier(&self, path: &Path, fail: bool) -> SyncResult<()> {
        if fail {
            return Err(io_error("injected post-persist directory sync failure"));
        }
        std::fs::File::open(path)
            .and_then(|dir| dir.sync_all())
            .map_err(io_error)
    }

    fn commit_index(&self, cache: &mut Option<Index>, index: Index) -> SyncResult<()> {
        let failure: Option<u8> = None;
        let result = (|| {
            if failure == Some(0) {
                return Err(io_error("injected index persistence failure"));
            }
            self.persist_index(&index)?;
            self.directory_barrier(self.root()?, failure == Some(1))?;
            if let Some(parent) = self.root()?.parent() {
                self.directory_barrier(parent, failure == Some(2))?;
            }
            Ok(())
        })();
        // A failed rename barrier can still leave the new file visible. Never
        // overwrite its counters from a stale cache. Reload under the same
        // lock, and preserve runtime liveness only for prior accepted jobs.
        *cache = if result.is_ok() { Some(index) } else { None };
        result
    }

    fn persist_index(&self, index: &Index) -> SyncResult<()> {
        put(&self.root()?.join("index.json"), index)
    }

    fn root(&self) -> SyncResult<&Path> {
        self.root
            .as_deref()
            .ok_or_else(|| io_error("durable sidecar directory unavailable"))
    }

    fn job_path(&self, id: &str) -> SyncResult<PathBuf> {
        uuid::Uuid::parse_str(id).map_err(|_| io_error("job_id must be a UUID"))?;
        Ok(self.root()?.join(id).join("summary.json"))
    }

    fn object_path(&self, id: &str, sequence: u64) -> SyncResult<PathBuf> {
        Ok(self
            .job_path(id)?
            .with_file_name(format!("{sequence:020}.json")))
    }

    fn index(&self) -> SyncResult<std::sync::MutexGuard<'_, Option<Index>>> {
        let mut guard = self
            .index
            .lock()
            .map_err(|_| io_error("metadata lock poisoned"))?;
        if guard.is_none() {
            let path = self.root()?.join("index.json");
            let mut index: Index = match std::fs::read(&path) {
                Ok(bytes) => {
                    let value: serde_json::Value =
                        serde_json::from_slice(&bytes).map_err(io_error)?;
                    // P1 wrote no index version; that absence reads as 1.
                    if let Some(version) = value.get("version") {
                        check_receipt_version(version.as_u64())?;
                    }
                    serde_json::from_value(value).map_err(io_error)?
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Index::default(),
                Err(e) => return Err(io_error(e)),
            };
            index.version = GC_RECEIPT_VERSION;
            if index.active.len() > MAX_ACTIVE {
                return Err(io_error("invalid active index bound"));
            }
            let live = self
                .live
                .lock()
                .map_err(|_| io_error("liveness lock poisoned"))?;
            for id in &index.active {
                if live.contains(id) {
                    continue;
                }
                let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
                if !job.state.terminal() {
                    for pending in job.in_flight_objects() {
                        // Outcome is written before the summary. Reconcile the
                        // exact key if the process died between those writes.
                        let mut object: GcObjectReceipt =
                            read(&self.object_path(id, pending.sequence)?)?;
                        if object.outcome == GcObjectOutcome::Intent {
                            object.outcome = GcObjectOutcome::Unknown;
                            put_object(&self.object_path(id, object.sequence)?, &mut object)?;
                        }
                        Self::apply_outcome(&mut job, &object)?;
                    }
                    job.state = GcJobState::Interrupted;
                    job.phase = "terminal".into();
                    job.stop_reason = Some("daemon_lost; no stale keep set or DELETE was replayed; unknown dispatch may still complete remotely".into());
                    job.stop_code = Some(GcStopCode::DaemonLost);
                    job.progress_unix_ms = unix_millis();
                    put_job(&self.job_path(id)?, &mut job)?;
                }
            }
            index.active.retain(|id| live.contains(id));
            put(&path, &index)?;
            *guard = Some(index);
        }
        Ok(guard)
    }
}

mod dispatch;
mod engine_api;
mod lifecycle;
