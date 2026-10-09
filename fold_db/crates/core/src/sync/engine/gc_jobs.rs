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

    /// Persist the job and bounded discovery index before returning its ID.
    /// A caller-supplied UUID makes a lost acceptance response safe to retry.
    pub(crate) fn accept(
        &self,
        trigger: &str,
        dry_run: bool,
        id: Option<&str>,
    ) -> SyncResult<(GcJobReceipt, bool)> {
        let mut guard = self.index()?;
        let index = guard.as_mut().expect("index initialized");
        let id = id.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
        let path = self.job_path(&id)?;
        if path.exists() {
            let mut job: GcJobReceipt = read(&path)?;
            if job.trigger != trigger || job.dry_run != dry_run {
                return Err(io_error("request ID reused with different GC parameters"));
            }
            if !job.state.terminal() && !index.active.contains(&id) {
                job.state = GcJobState::Interrupted;
                job.phase = "terminal".into();
                job.stop_reason =
                    Some("acceptance interrupted before index commit; no replay".into());
                job.stop_code = Some(GcStopCode::AcceptanceInterrupted);
                job.progress_unix_ms = unix_millis();
                put_job(&path, &mut job)?;
            }
            return Ok((job, false));
        }
        if index.active.len() >= MAX_ACTIVE {
            return Err(io_error("GC job queue full; attach to an existing job"));
        }
        if index.retained_jobs >= MAX_RETAINED_JOBS {
            return Err(io_error("GC_METADATA_CAPACITY_REQUIRED: terminal job archive is full; export and explicit migration are required; no evidence was pruned"));
        }
        let mut job = GcJobReceipt {
            version: GC_RECEIPT_MIN_VERSION,
            job_id: id.clone(),
            trigger: trigger.into(),
            dry_run,
            state: GcJobState::Queued,
            phase: "queued".into(),
            created_unix_ms: unix_millis(),
            progress_unix_ms: unix_millis(),
            stop_reason: None,
            stop_code: None,
            report: None,
            selection: None,
            objects_reconciled: 0,
            receipt_capacity: 0,
            delete_acknowledged: 0,
            acknowledged_bytes: 0,
            failed_before_dispatch: 0,
            failed_bytes: 0,
            unknown: 0,
            unknown_bytes: 0,
            dispositions: None,
            pending: None,
            pending_batch: Vec::new(),
        };
        // Reserve first: a failed admission may leave an unindexed summary,
        // and that evidence must consume bounded archive capacity too.
        let mut reserved = index.clone();
        reserved.retained_jobs += 1;
        self.commit_index(&mut guard, reserved)?;
        // Summary before active index. An unindexed summary cannot run.
        // A retry of that exact ID becomes Interrupted, never a silent replay.
        put_job(&path, &mut job)?;
        let mut committed = guard.as_ref().expect("reserved index").clone();
        committed.active.push(id.clone());
        committed.latest = Some(id);
        self.commit_index(&mut guard, committed)?;
        self.live
            .lock()
            .map_err(|_| io_error("liveness lock poisoned"))?
            .insert(job.job_id.clone());
        Ok((job, true))
    }

    pub(crate) fn get(&self, id: Option<&str>) -> SyncResult<Option<GcJobReceipt>> {
        let guard = self.index()?;
        let id = id.or_else(|| guard.as_ref().and_then(|index| index.latest.as_deref()));
        id.map(|id| {
            let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
            if !job.state.terminal()
                && !guard
                    .as_ref()
                    .expect("index initialized")
                    .active
                    .iter()
                    .any(|active| active == id)
            {
                job.state = GcJobState::Interrupted;
                job.phase = "terminal".into();
                job.stop_reason = Some("unindexed acceptance; no automatic replay".into());
                job.stop_code = Some(GcStopCode::UnindexedAcceptance);
                put_job(&self.job_path(id)?, &mut job)?;
            }
            Ok(job)
        })
        .transpose()
    }

    pub(crate) fn objects(
        &self,
        id: &str,
        after: u64,
        limit: usize,
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        let job = self.get(Some(id))?.ok_or_else(|| io_error("job missing"))?;
        let end = job.objects_reconciled + job.in_flight_len();
        (after.saturating_add(1)..=end.min(after.saturating_add(limit.min(256) as u64)))
            .map(|sequence| read(&self.object_path(id, sequence)?))
            .collect()
    }

    pub(crate) fn update(
        &self,
        id: &str,
        change: impl FnOnce(&mut GcJobReceipt),
    ) -> SyncResult<()> {
        let mut guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
        if job.state.terminal() {
            return Err(io_error("job is terminal"));
        }
        change(&mut job);
        job.progress_unix_ms = unix_millis();
        put_job(&self.job_path(id)?, &mut job)?;
        if job.state.terminal() {
            let index = guard.as_mut().expect("index initialized");
            let mut committed = index.clone();
            committed.active.retain(|active| active != id);
            self.live
                .lock()
                .map_err(|_| io_error("liveness lock poisoned"))?
                .remove(id);
            self.commit_index(&mut guard, committed)?;
        }
        Ok(())
    }

    pub(crate) fn phase(&self, id: &str, phase: &str) -> SyncResult<()> {
        self.update(id, |job| {
            job.state = GcJobState::Active;
            job.phase = phase.into();
        })
    }

    fn apply_outcome(job: &mut GcJobReceipt, object: &GcObjectReceipt) -> SyncResult<()> {
        let pending = job
            .in_flight_objects()
            .into_iter()
            .next()
            .ok_or_else(|| io_error("no dispatch intent"))?;
        if pending.sequence != object.sequence
            || pending.key != object.key
            || object.job_id != job.job_id
            || object.sequence != job.objects_reconciled + 1
            || pending.listed_bytes != object.listed_bytes
        {
            return Err(io_error("object receipt does not match dispatch intent"));
        }
        let bytes = object.listed_bytes;
        // Version 1 counters keep their meaning. A precise version 2
        // disposition also feeds the version 1 counter it refines:
        // deleted_now and already_absent are both provider acknowledgements;
        // uncertain_write is a DELETE that may still complete remotely.
        match object.outcome {
            GcObjectOutcome::DeleteAcknowledged => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
            }
            GcObjectOutcome::FailedBeforeDispatch => {
                job.failed_before_dispatch += 1;
                job.failed_bytes += bytes;
            }
            GcObjectOutcome::Unknown => {
                job.unknown += 1;
                job.unknown_bytes += bytes;
            }
            GcObjectOutcome::DeletedNow => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.deleted_now += 1;
                counts.deleted_now_bytes += bytes;
            }
            GcObjectOutcome::AlreadyAbsent => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.already_absent += 1;
                counts.already_absent_bytes += bytes;
            }
            GcObjectOutcome::Failed => {
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.failed += 1;
                counts.failed_bytes += bytes;
            }
            GcObjectOutcome::Protected => {
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.protected += 1;
                counts.protected_bytes += bytes;
            }
            GcObjectOutcome::UncertainWrite => {
                job.unknown += 1;
                job.unknown_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.uncertain_write += 1;
                counts.uncertain_write_bytes += bytes;
            }
            GcObjectOutcome::Intent => return Err(io_error("intent is not an outcome")),
        }
        let rest: Vec<GcObjectReceipt> = job
            .in_flight_objects()
            .into_iter()
            .filter(|pending| pending.sequence != object.sequence)
            .collect();
        job.set_in_flight(rest);
        job.objects_reconciled = object.sequence;
        if let Some(report) = job.report.as_mut() {
            report.deleted = job.delete_acknowledged as usize;
            let failed_after_dispatch = job.dispositions.map_or(0, |counts| counts.failed);
            report.failed =
                (job.failed_before_dispatch + job.unknown + failed_after_dispatch) as usize;
        }
        Ok(())
    }

    pub(crate) fn outcome(
        &self,
        mut object: GcObjectReceipt,
        outcome: GcObjectOutcome,
    ) -> SyncResult<()> {
        let guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(&object.job_id)?)?;
        object.outcome = outcome;
        Self::apply_outcome(&mut job, &object)?;
        put_object(
            &self.object_path(&object.job_id, object.sequence)?,
            &mut object,
        )?;
        job.progress_unix_ms = unix_millis();
        put_job(&self.job_path(&object.job_id)?, &mut job)?;
        drop(guard);
        Ok(())
    }

    /// Persist N intents, then one directory fsync. No DELETE until this
    /// returns. Sequence order matches `shas`.
    pub(crate) fn intent_batch(
        &self,
        id: &str,
        shas: &[(String, u64)],
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        if shas.is_empty() {
            return Ok(Vec::new());
        }
        if shas
            .iter()
            .any(|(sha, _)| !super::backup_keys::is_sha256_hex(sha))
        {
            return Err(io_error(
                "GC_INTENT_NOT_A_V1_CHUNK_DIGEST: legacy_backup_chunk receipts take a 64-hex sha; no DELETE was dispatched",
            ));
        }
        let mut guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
        if job.state.terminal() || job.in_flight_len() != 0 {
            return Err(io_error("invalid dispatch transition"));
        }
        let needed = job.objects_reconciled.saturating_add(shas.len() as u64);
        while job.receipt_capacity < needed {
            let index = guard.as_mut().expect("index initialized");
            if index.reserved_object_receipts >= MAX_RESERVED_OBJECT_RECEIPTS {
                return Err(io_error("GC_METADATA_CAPACITY_REQUIRED: object receipt archive is full; no evidence was pruned and no DELETE was dispatched"));
            }
            let mut committed = index.clone();
            committed.reserved_object_receipts += RECEIPT_RESERVATION_BATCH;
            self.commit_index(&mut guard, committed)?;
            job.receipt_capacity += RECEIPT_RESERVATION_BATCH;
        }
        let mut objects = Vec::with_capacity(shas.len());
        for (offset, (sha, bytes)) in shas.iter().enumerate() {
            let object = GcObjectReceipt {
                version: GC_RECEIPT_MIN_VERSION,
                job_id: id.into(),
                sequence: job.objects_reconciled + offset as u64 + 1,
                object_class: "legacy_backup_chunk".into(),
                key: format!("{}{sha}", super::backup_keys::BACKUP_CHUNKS_PREFIX),
                listed_bytes: *bytes,
                outcome: GcObjectOutcome::Intent,
                instance_id: None,
                provider_version: None,
            };
            put_file(&self.object_path(id, object.sequence)?, &object)?;
            objects.push(object);
        }
        let job_path = self.job_path(id)?;
        sync_parent(&self.object_path(id, objects[0].sequence)?)?;
        job.set_in_flight(objects.clone());
        job.phase = "delete".into();
        job.progress_unix_ms = unix_millis();
        put(&job_path, &job)?;
        drop(guard);
        Ok(objects)
    }

    pub(crate) fn outcome_batch(
        &self,
        items: Vec<(GcObjectReceipt, GcObjectOutcome)>,
    ) -> SyncResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let job_id = items[0].0.job_id.clone();
        let guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(&job_id)?)?;
        for (mut object, outcome) in items {
            object.outcome = outcome;
            Self::apply_outcome(&mut job, &object)?;
            put_file(&self.object_path(&job_id, object.sequence)?, &object)?;
        }
        sync_parent(&self.object_path(&job_id, job.objects_reconciled)?)?;
        job.progress_unix_ms = unix_millis();
        put(&self.job_path(&job_id)?, &job)?;
        drop(guard);
        Ok(())
    }

    pub(crate) fn finish(
        &self,
        id: &str,
        result: &SyncResult<Option<BackupOrphanGcReport>>,
    ) -> SyncResult<()> {
        if let Some(job) = self.get(Some(id))? {
            for pending in job.in_flight_objects() {
                let object: GcObjectReceipt = read(&self.object_path(id, pending.sequence)?)?;
                let outcome = if object.outcome == GcObjectOutcome::Intent {
                    GcObjectOutcome::Unknown
                } else {
                    object.outcome.clone()
                };
                self.outcome(object, outcome)?;
            }
        }
        self.update(id, |job| {
            let (state, reason, code) = match result {
                Ok(Some(report)) => {
                    job.report = Some(report.clone());
                    if report.superseded {
                        (
                            GcJobState::Superseded,
                            Some("publication_state_changed".into()),
                            Some(GcStopCode::PublicationStateChanged),
                        )
                    } else if report.failed > 0 {
                        (
                            GcJobState::PartialFailure,
                            Some("object_failures; inspect receipts".into()),
                            Some(GcStopCode::ObjectFailures),
                        )
                    } else {
                        (GcJobState::Completed, None, None)
                    }
                }
                Ok(None) => (
                    GcJobState::Superseded,
                    Some("newer_generation".into()),
                    Some(GcStopCode::NewerGeneration),
                ),
                Err(error) => {
                    let text = error.to_string();
                    (
                        GcJobState::Failed,
                        Some(crate::sync::error::redact_sync_error_text(&text)),
                        Some(GcStopCode::from_failure_text(&text)),
                    )
                }
            };
            job.state = state;
            job.stop_reason = reason;
            job.stop_code = code;
            job.phase = "terminal".into();
        })
    }
}

impl SyncEngine {
    pub fn backup_gc_job(&self, id: Option<&str>) -> SyncResult<Option<GcJobReceipt>> {
        self.backup_gc_jobs.get(id)
    }

    pub fn backup_gc_objects(
        &self,
        id: &str,
        after: u64,
        limit: usize,
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        self.backup_gc_jobs.objects(id, after, limit)
    }

    /// Detached daemon work: acceptance never reads a manifest, lists cloud
    /// objects, or waits for the publication/single-executor locks.
    pub fn start_backup_gc(
        self: &Arc<Self>,
        dry_run: bool,
        request_id: Option<&str>,
    ) -> SyncResult<GcJobReceipt> {
        let (job, fresh) = self.backup_gc_jobs.accept("manual", dry_run, request_id)?;
        if fresh {
            let engine = Arc::clone(self);
            let id = job.job_id.clone();
            // lint:spawn-bare-ok daemon-owned durable GC, not request-scoped
            tokio::spawn(async move {
                use futures::FutureExt;
                let result = std::panic::AssertUnwindSafe(async {
                    engine.backup_gc_jobs.phase(&id, "resolve_keep")?;
                    let manifests = engine.resolve_admin_backup_gc_keep_set().await?;
                    engine
                        .gc_orphan_backup_chunks_for_job(&manifests, dry_run, &id)
                        .await
                        .map(Some)
                })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(io_error("GC task panic; no automatic replay")));
                if let Err(error) = engine.backup_gc_jobs.finish(&id, &result) {
                    tracing::error!(target: "fold_db::sync::backup", job_id = %id, error = %error, "GC terminal receipt write failed; durable intent remains");
                }
            });
        }
        Ok(job)
    }
}
