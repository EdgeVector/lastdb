//! Explicit resumable local compaction, with an honest completion receipt.

use crate::clock::unix_secs;
use crate::storage::disk_pack::{
    cas_delete, map, physical, run_blocking, transaction, PackRuntime,
};
use crate::storage::error::{StorageError, StorageResult};
use laststore::disk_run::home::CheckpointKind;
use laststore::space::{reserve_space, SpacePolicy};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;
use NativeCleanupPhase::*;

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
        Self {
            main_frontier: m.frontier(),
            cas_frontier: c.frontier(),
            main_generation: m.generation(),
            cas_generation: c.generation(),
        }
    }

    fn covers(self, e: Self) -> bool {
        self.main_frontier >= e.main_frontier
            && self.cas_frontier >= e.cas_frontier
            && self.main_generation >= e.main_generation
            && self.cas_generation >= e.cas_generation
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
        [&self.main, &self.cas]
            .into_iter()
            .any(|h| h.tiered_runs != 0 || h.merging_runs != 0)
    }

    fn active(&self) -> bool {
        self.request_id.is_some() && !matches!(self.phase, Complete | RecoveryRequired)
    }
}

pub(super) struct CleanupJob {
    gate: Arc<tokio::sync::Mutex<()>>,
    worker: AtomicBool,
    status: Mutex<NativeCleanupStatus>,
}

impl CleanupJob {
    pub(super) fn open(v2: &physical::V2Home) -> Self {
        let s = load(v2).unwrap_or_else(|e| NativeCleanupStatus {
            phase: RecoveryRequired,
            last_error: Some(e.to_string().chars().take(1024).collect()),
            ..NativeCleanupStatus::idle(v2)
        });
        Self {
            gate: Arc::default(),
            worker: AtomicBool::new(false),
            status: Mutex::new(s),
        }
    }

    fn get(&self) -> NativeCleanupStatus {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set(&self, status: NativeCleanupStatus) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = status;
    }
}

fn load(v2: &physical::V2Home) -> StorageResult<NativeCleanupStatus> {
    let idle = NativeCleanupStatus::idle(v2);
    let Some(b) = v2
        .home
        .load_checkpoint(CheckpointKind::NodeCleanup)
        .map_err(map)?
    else {
        return Ok(idle);
    };
    let s: NativeCleanupStatus = serde_json::from_slice(&b)
        .map_err(|_| invalid("native cleanup checkpoint encoding is invalid"))?;
    let cur = NativeCleanupBoundary::capture(v2);
    let done_ok = if s.complete {
        s.completed_at.is_some()
            && s.completed_through
                .zip(s.target)
                .is_some_and(|(f, t)| f.covers(t) && cur.covers(f))
    } else {
        s.completed_at.is_none() && s.completed_through.is_none()
    };
    let req_ok = s
        .request_id
        .as_deref()
        .is_some_and(|id| Uuid::parse_str(id).is_ok());
    let valid = (s.version, &s.scope, &s.main_home, &s.cas_home)
        == (idle.version, &idle.scope, &idle.main_home, &idle.cas_home)
        && (s.backup_copies_excluded && !s.live_cas_orphan_repair_included && done_ok && req_ok)
        && (s.complete, s.phase != Idle, s.requested_at.is_some())
            == (s.phase == Complete, true, true)
        && s.target.is_some_and(|t| cur.covers(t));
    valid
        .then_some(s)
        .ok_or_else(|| invalid("native cleanup checkpoint identity or boundary is invalid"))
}

impl PackRuntime {
    pub fn status_cleanup(&self) -> NativeCleanupStatus {
        self.cleanup_job.get()
    }

    pub async fn request_native_cleanup(
        self: &Arc<Self>,
        request_id: Option<Uuid>,
    ) -> StorageResult<NativeCleanupStatus> {
        if transaction::PendingBatch::current(self).is_some() {
            return Err(invalid("native cleanup requires its own request scope"));
        }
        let req = request_id.map(|id| id.to_string());
        let owner = Arc::clone(&self.cleanup_job.gate).lock_owned().await;
        let prior = self.cleanup_job.get();
        if prior.active() || (req.is_some() && prior.request_id == req) {
            if req.is_some() && prior.request_id != req {
                return Err(invalid("another cleanup request is active"));
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
        if self.cleanup_job.get().active() && !self.cleanup_job.worker.swap(true, Ordering::AcqRel)
        {
            let fut = worker_loop(Arc::downgrade(self), Arc::clone(&self.cleanup_job));
            tokio::spawn(fut.in_current_span());
        }
    }

    fn finish_cleanup(&self) -> StorageResult<NativeCleanupStatus> {
        self.require_cleanup_safe()?;
        cas_delete::require_clear(&self.physical)?;
        self.adopt_merged_root()?;
        let mut s = self.cleanup_job.get();
        self.refresh(&mut s, 0, 0);
        if s.needs_merge() {
            return Ok(s);
        }
        let (m_held, m_rem) = self.physical.home.cleanup_files().map_err(map)?;
        let (c_held, c_rem) = self.physical.cas.cleanup_files().map_err(map)?;
        self.adopt_merged_root()?;
        self.refresh(&mut s, m_rem, c_rem);
        if (s.main.held_retired_bytes | s.cas.held_retired_bytes | m_held | c_held) != 0 {
            s.phase = WaitingForReads;
        } else {
            let boundary = NativeCleanupBoundary::capture(&self.physical);
            if !s.target.is_some_and(|t| boundary.covers(t)) {
                return Err(invalid("cleanup target missing or frontier went backwards"));
            }
            (s.phase, s.complete) = (Complete, true);
            (s.completed_at, s.completed_through) = (Some(unix_secs()), Some(boundary));
            self.save_cleanup(&s)?;
        }
        self.cleanup_job.set(s.clone());
        Ok(s)
    }

    fn refresh(&self, s: &mut NativeCleanupStatus, dm: u64, dc: u64) {
        for (dst, home, add) in [
            (&mut s.main, &self.physical.home, dm),
            (&mut s.cas, &self.physical.cas, dc),
        ] {
            let (tiered_runs, merging_runs, _) = home.run_totals();
            *dst = NativeCleanupHome {
                tiered_runs,
                merging_runs,
                held_retired_bytes: home.retired_bytes(),
                removed_files: dst.removed_files.saturating_add(add),
            };
        }
        self.cleanup_job.set(s.clone());
    }

    fn save_cleanup(&self, status: &NativeCleanupStatus) -> StorageResult<()> {
        let b = serde_json::to_vec(status).map_err(|_| invalid("cleanup checkpoint encode"))?;
        let home = &self.physical.home;
        let _s =
            reserve_space(home.path(), &self.space_policy, b.len() as u64 + 4096).map_err(map)?;
        home.save_checkpoint(CheckpointKind::NodeCleanup, &b)
            .map_err(map)
    }

    fn is_healthy(&self) -> bool {
        !(self.requires_recovery()
            || self.physical.home.is_poisoned()
            || self.physical.cas.is_poisoned())
    }

    fn require_cleanup_safe(&self) -> StorageResult<()> {
        self.require_initialized()?;
        self.is_healthy()
            .then_some(())
            .ok_or_else(|| invalid("native cleanup requires a successful home reopen"))
    }
}

async fn worker_loop(weak: Weak<PackRuntime>, job: Arc<CleanupJob>) {
    while let Some(rt) = weak.upgrade() {
        let tick = async {
            let owner = Arc::clone(&rt.cleanup_job.gate).lock_owned().await;
            let r1 = Arc::clone(&rt);
            let (s, owner) = run_blocking(move || {
                r1.require_cleanup_safe()?;
                let mut s = r1.cleanup_job.get();
                if !s.active() {
                    return Ok((s, owner));
                }
                (s.phase, s.last_error) = (Running, None);
                r1.cleanup_job.set(s.clone());
                let (p, c_pol) = (&r1.physical, SpacePolicy::default());
                for (h, pol) in [(&p.home, &r1.space_policy), (&p.cas, &c_pol)] {
                    r1.adopt_merged_root()?;
                    if h.retired_bytes() > 0 {
                        s.phase = WaitingForReads;
                    } else if h.cleanup_step(pol, false).map_err(map)? {
                        s.merge_steps_since_resume = s.merge_steps_since_resume.saturating_add(1);
                        r1.cleanup_job.set(s.clone());
                        r1.adopt_merged_root()?;
                    }
                }
                r1.refresh(&mut s, 0, 0);
                Ok((s, owner))
            })
            .await?;
            if !s.active() || s.needs_merge() {
                return Ok(s);
            }
            let (gate, r2) = (rt.enter_write().await, Arc::clone(&rt));
            run_blocking(move || {
                let (_owner, _gate) = (owner, gate);
                r2.finish_cleanup()
            })
            .await
        };
        let delay = match tick.await {
            Ok(s) if !s.active() => break,
            Ok(s) if s.phase == WaitingForReads => Duration::from_millis(250),
            Ok(_) => Duration::from_millis(25),
            Err(e) => {
                let (fatal, mut s) = (!rt.is_healthy(), job.get());
                (s.complete, s.last_error) =
                    (false, Some(e.to_string().chars().take(1024).collect()));
                s.phase = if fatal { RecoveryRequired } else { Retrying };
                job.set(s);
                if fatal {
                    break;
                }
                Duration::from_secs(30)
            }
        };
        drop(rt);
        tokio::time::sleep(delay).await;
    }
    job.worker.store(false, Ordering::Release);
    if let Some(rt) = weak.upgrade().filter(|_| job.get().active()) {
        rt.resume_cleanup();
    }
}

fn invalid(msg: &str) -> StorageError {
    StorageError::InvalidOperation(msg.into())
}
