//! Explicit resumable local compaction and retired-format guards.

use crate::crypto::clock::unix_secs;
use crate::storage::disk_pack::{
    cas_delete, creation_refusal, map, physical, run_blocking, transaction, PackRuntime,
};
use crate::storage::error::{StorageError, StorageResult};
use laststore::disk_run::home::CheckpointKind::NodeCleanup;
use laststore::space::{reserve_space, SpacePolicy};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;
use NativeCleanupPhase::*;

pub(in crate::storage) mod retired {
    use crate::storage::current_keys::{JOURNAL_RETIRED, META_PFX};
    use crate::storage::disk_pack::{map, native_routing::collection_for_namespace};
    use crate::storage::error::{StorageError, StorageResult};
    use crate::storage::format::{
        MAIN_NAMESPACE, METADATA_NAMESPACE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
    };
    use laststore::disk_run::home::RunRead;
    use std::path::Path;

    pub(in crate::storage) fn refuse_shared_home(root: &Path, marker: &[u8]) -> StorageResult<()> {
        let dir =
            std::fs::symlink_metadata(root.join(RETIRED_SHARED_DIR)).is_ok_and(|m| m.is_dir());
        let ok = marker != RETIRED_SHARED_NODE_BYTES && !dir;
        ok.then_some(()).ok_or(StorageError::RetiredFormat(
            "the dedicated shared home format is retired",
        ))
    }

    pub(in crate::storage) fn refuse_journal(main: &mut RunRead) -> StorageResult<()> {
        let prefixes = META_PFX.iter().map(|&p| (METADATA_NAMESPACE, p));
        for (ns, pfx) in prefixes.chain([(MAIN_NAMESPACE, "ade:")]) {
            let mut end = pfx.as_bytes().to_vec();
            if let Some(b) = end.last_mut() {
                *b += 1;
            }
            let (col, b) = (collection_for_namespace(ns), pfx.as_bytes());
            let mut r = main.administrative_range(col, b, &end).map_err(map)?;
            if !r.next_page(1).map_err(map)?.records.is_empty() {
                return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCleanupPhase {
    #[default]
    Idle,
    Pending,
    Running,
    WaitingForReads,
    Retrying,
    RecoveryRequired,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupBoundary {
    pub main_frontier: u64,
    pub cas_frontier: u64,
    pub main_generation: u64,
    pub cas_generation: u64,
}

impl NativeCleanupBoundary {
    fn capture(v2: &physical::V2Home) -> Self {
        let (m, c) = (v2.home.current_root(), v2.cas.current_root());
        let (mf, cf, mg, cg) = (m.frontier(), c.frontier(), m.generation(), c.generation());
        Self {
            main_frontier: mf,
            cas_frontier: cf,
            main_generation: mg,
            cas_generation: cg,
        }
    }
    fn covers(self, e: Self) -> bool {
        let (s, o) = (self, e);
        (s.main_frontier >= o.main_frontier && s.cas_frontier >= o.cas_frontier)
            && (s.main_generation >= o.main_generation && s.cas_generation >= o.cas_generation)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupHome {
    pub tiered_runs: usize,
    pub merging_runs: usize,
    pub held_retired_bytes: u64,
    pub removed_files: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupStatus {
    pub version: u32,
    pub request_id: Option<String>,
    pub phase: NativeCleanupPhase,
    pub complete: bool,
    pub scope: String,
    pub main_home: String,
    pub cas_home: String,
    pub requested_at: Option<u64>,
    pub target: Option<NativeCleanupBoundary>,
    pub completed_at: Option<u64>,
    pub completed_through: Option<NativeCleanupBoundary>,
    pub merge_steps_since_resume: u64,
    pub main: NativeCleanupHome,
    pub cas: NativeCleanupHome,
    pub last_error: Option<String>,
    pub backup_copies_excluded: bool,
    pub live_cas_orphan_repair_included: bool,
}

impl NativeCleanupStatus {
    fn idle(v2: &physical::V2Home) -> Self {
        Self {
            version: 1,
            scope: "already_deleted_records_in_active_main_and_cas_stores".into(),
            main_home: v2.home.identity().to_string(),
            cas_home: v2.cas.identity().to_string(),
            backup_copies_excluded: true,
            ..Self::default()
        }
    }
    fn needs_merge(&self) -> bool {
        let (m, c) = (&self.main, &self.cas);
        (m.tiered_runs | m.merging_runs | c.tiered_runs | c.merging_runs) != 0
    }
    fn active(&self) -> bool {
        self.request_id.is_some() && !matches!(self.phase, Complete | RecoveryRequired)
    }
}

type OwnerLock = Arc<tokio::sync::Mutex<()>>;
type CleanRes = StorageResult<NativeCleanupStatus>;
pub(in crate::storage) struct CleanupJob(OwnerLock, AtomicBool, Mutex<NativeCleanupStatus>);

impl CleanupJob {
    pub(in crate::storage) fn open(v2: &physical::V2Home) -> Self {
        let s = load(v2).unwrap_or_else(|e| NativeCleanupStatus {
            phase: RecoveryRequired,
            last_error: Some(err_str(&e)),
            ..NativeCleanupStatus::idle(v2)
        });
        Self(Arc::default(), AtomicBool::new(false), Mutex::new(s))
    }
    fn lock_s(&self) -> MutexGuard<'_, NativeCleanupStatus> {
        self.2.lock().unwrap_or_else(PoisonError::into_inner)
    }
    fn get(&self) -> NativeCleanupStatus {
        self.lock_s().clone()
    }
    fn set(&self, status: NativeCleanupStatus) {
        *self.lock_s() = status;
    }
}

fn err_str(e: &impl std::fmt::Display) -> String {
    e.to_string().chars().take(1024).collect()
}

fn load(v2: &physical::V2Home) -> CleanRes {
    let idle = NativeCleanupStatus::idle(v2);
    let Some(b) = v2.home.load_checkpoint(NodeCleanup).map_err(map)? else {
        return Ok(idle);
    };
    let s: NativeCleanupStatus = serde_json::from_slice(&b)
        .map_err(|_| StorageError::invalid("native cleanup checkpoint encoding is invalid"))?;
    let cur = NativeCleanupBoundary::capture(v2);
    let done_ok = if s.complete {
        matches!((s.completed_at, s.completed_through, s.target), (Some(_), Some(f), Some(t)) if f.covers(t) && cur.covers(f))
    } else {
        (s.completed_at, s.completed_through) == (None, None)
    };
    let same_id = (s.version, &s.scope, &s.main_home, &s.cas_home)
        == (idle.version, &idle.scope, &idle.main_home, &idle.cas_home);
    let req_ok = s
        .request_id
        .as_deref()
        .is_some_and(|id| Uuid::parse_str(id).is_ok());
    let flags = same_id && s.backup_copies_excluded && !s.live_cas_orphan_repair_included;
    let phase_ok =
        s.complete == (s.phase == Complete) && s.phase != Idle && s.requested_at.is_some();
    let valid = flags && done_ok && req_ok && phase_ok && s.target.is_some_and(|t| cur.covers(t));
    valid
        .then_some(s)
        .ok_or_else(|| StorageError::invalid("invalid cleanup checkpoint"))
}

impl PackRuntime {
    pub fn status_cleanup(&self) -> NativeCleanupStatus {
        self.cleanup_job.get()
    }
    pub async fn request_native_cleanup(self: &Arc<Self>, req_id: Option<Uuid>) -> CleanRes {
        if transaction::PendingBatch::current(self).is_some() {
            return Err(StorageError::invalid(
                "native cleanup requires its own request scope",
            ));
        }
        let req = req_id.as_ref().map(Uuid::to_string);
        let owner = Arc::clone(&self.cleanup_job.0).lock_owned().await;
        let prior = self.cleanup_job.get();
        if prior.active() || (req.is_some() && prior.request_id == req) {
            if req.is_some() && prior.request_id != req {
                return Err(StorageError::invalid("another cleanup request is active"));
            }
            drop(owner);
            self.resume_cleanup();
            return Ok(prior);
        }
        let (gate, rt) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            rt.require_cleanup_safe()?;
            let s = NativeCleanupStatus {
                request_id: Some(req.unwrap_or_else(|| Uuid::new_v4().to_string())),
                requested_at: Some(unix_secs()),
                target: Some(NativeCleanupBoundary::capture(&rt.physical)),
                phase: Pending,
                ..NativeCleanupStatus::idle(&rt.physical)
            };
            rt.save_cleanup(&s)?;
            rt.cleanup_job.set(s.clone());
            if rt.serving.load(Ordering::Acquire) {
                rt.resume_cleanup();
            }
            Ok(s)
        })
        .await
    }
    pub(crate) fn begin_serving(self: &Arc<Self>) {
        self.serving.store(true, Ordering::Release);
        self.resume_cleanup();
    }
    fn resume_cleanup(self: &Arc<Self>) {
        if self.cleanup_job.get().active() && !self.cleanup_job.1.swap(true, Ordering::AcqRel) {
            let job = Arc::clone(&self.cleanup_job);
            tokio::spawn(worker_loop(Arc::downgrade(self), job).in_current_span());
        }
    }
    async fn cleanup_tick(self: &Arc<Self>) -> CleanRes {
        let owner = Arc::clone(&self.cleanup_job.0).lock_owned().await;
        let rt = Arc::clone(self);
        let (s, owner) = run_blocking(move || {
            rt.require_cleanup_safe()?;
            let mut s = rt.cleanup_job.get();
            if !s.active() {
                return Ok((s, owner));
            }
            (s.phase, s.last_error) = (Running, None);
            rt.cleanup_job.set(s.clone());
            let c_pol = SpacePolicy::default();
            for (h, pol) in [
                (&rt.physical.home, &rt.space_policy),
                (&rt.physical.cas, &c_pol),
            ] {
                rt.adopt_merged_root()?;
                if h.retired_bytes() > 0 {
                    s.phase = WaitingForReads;
                } else if h.cleanup_step(pol).map_err(map)? {
                    s.merge_steps_since_resume = s.merge_steps_since_resume.saturating_add(1);
                    rt.cleanup_job.set(s.clone());
                    rt.adopt_merged_root()?;
                }
            }
            rt.refresh(&mut s, 0, 0);
            Ok((s, owner))
        })
        .await?;
        if !s.active() || s.needs_merge() {
            return Ok(s);
        }
        let (gate, rt) = (self.enter_write().await, Arc::clone(self));
        run_blocking(move || {
            let (_owner, _gate) = (owner, gate);
            rt.require_cleanup_safe()?;
            cas_delete::require_clear(&rt.physical)?;
            rt.adopt_merged_root()?;
            let mut s = rt.cleanup_job.get();
            rt.refresh(&mut s, 0, 0);
            if s.needs_merge() {
                return Ok(s);
            }
            let (m_held, m_rem) = rt.physical.home.cleanup_files().map_err(map)?;
            let (c_held, c_rem) = rt.physical.cas.cleanup_files().map_err(map)?;
            rt.adopt_merged_root()?;
            rt.refresh(&mut s, m_rem, c_rem);
            if (s.main.held_retired_bytes | s.cas.held_retired_bytes | m_held | c_held) != 0 {
                s.phase = WaitingForReads;
            } else {
                let boundary = NativeCleanupBoundary::capture(&rt.physical);
                if !s.target.is_some_and(|t| boundary.covers(t)) {
                    return Err(StorageError::invalid(
                        "cleanup target or frontier regressed",
                    ));
                }
                (s.phase, s.complete, s.completed_at, s.completed_through) =
                    (Complete, true, Some(unix_secs()), Some(boundary));
                rt.save_cleanup(&s)?;
            }
            rt.cleanup_job.set(s.clone());
            Ok(s)
        })
        .await
    }
    fn refresh(&self, s: &mut NativeCleanupStatus, dm: u64, dc: u64) {
        for (dst, h, add) in [
            (&mut s.main, &self.physical.home, dm),
            (&mut s.cas, &self.physical.cas, dc),
        ] {
            let (tiered_runs, merging_runs, _) = h.run_totals();
            let (held, removed) = (h.retired_bytes(), dst.removed_files.saturating_add(add));
            *dst = NativeCleanupHome {
                tiered_runs,
                merging_runs,
                held_retired_bytes: held,
                removed_files: removed,
            };
        }
        self.cleanup_job.set(s.clone());
    }
    fn save_cleanup(&self, status: &NativeCleanupStatus) -> StorageResult<()> {
        let b = serde_json::to_vec(status).map_err(|_| StorageError::invalid("cleanup encode"))?;
        let p = self.physical.home.path();
        let _s = reserve_space(p, &self.space_policy, b.len() as u64 + 4096).map_err(map)?;
        self.physical
            .home
            .save_checkpoint(NodeCleanup, &b)
            .map_err(map)
    }
    fn is_healthy(&self) -> bool {
        let p = &self.physical;
        !(self.requires_recovery() || p.home.is_poisoned() || p.cas.is_poisoned())
    }
    fn require_cleanup_safe(&self) -> StorageResult<()> {
        if self.fresh_initialization_pending() {
            return Err(creation_refusal(
                "creation requires durable key proofs first",
            ));
        }
        self.is_healthy().then_some(()).ok_or_else(|| {
            StorageError::invalid("native cleanup requires a successful home reopen")
        })
    }
}

async fn worker_loop(weak: Weak<PackRuntime>, job: Arc<CleanupJob>) {
    while let Some(rt) = weak.upgrade() {
        let delay = match rt.cleanup_tick().await {
            Ok(s) if !s.active() => break,
            Ok(s) if s.phase == WaitingForReads => Duration::from_millis(250),
            Ok(_) => Duration::from_millis(25),
            Err(e) => {
                let (healthy, mut s) = (rt.is_healthy(), job.get());
                (s.complete, s.last_error) = (false, Some(err_str(&e)));
                s.phase = if healthy { Retrying } else { RecoveryRequired };
                job.set(s);
                if !healthy {
                    break;
                }
                Duration::from_secs(30)
            }
        };
        drop(rt);
        tokio::time::sleep(delay).await;
    }
    job.1.store(false, Ordering::Release);
    if let Some(rt) = weak.upgrade().filter(|_| job.get().active()) {
        rt.resume_cleanup();
    }
}
