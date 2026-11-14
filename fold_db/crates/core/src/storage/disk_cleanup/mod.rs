//! Explicit resumable local compaction status and checkpoint model.

mod worker;

use crate::storage::disk_pack::{map, physical};
use crate::storage::error::{StorageError, StorageResult};
use laststore::disk_run::home::CheckpointSpec;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use uuid::Uuid;
use NativeCleanupPhase::*;

pub(super) const NODE_CLEANUP_CHECKPOINT: CheckpointSpec = CheckpointSpec {
    file_name: "NODE_CLEANUP",
    magic: b"LDRCLN1\0",
    max_bytes: 16 * 1024,
};

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
    pub(super) fn capture(v2: &physical::V2Home) -> Self {
        let (main, cas) = (v2.home.current_root(), v2.cas.current_root());
        Self {
            main_frontier: main.frontier(),
            cas_frontier: cas.frontier(),
            main_generation: main.generation(),
            cas_generation: cas.generation(),
        }
    }

    pub(super) fn covers(self, earlier: Self) -> bool {
        self.main_frontier >= earlier.main_frontier
            && self.cas_frontier >= earlier.cas_frontier
            && self.main_generation >= earlier.main_generation
            && self.cas_generation >= earlier.cas_generation
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
    pub(super) fn idle(v2: &physical::V2Home) -> Self {
        Self {
            version: 1,
            scope: "already_deleted_records_in_active_main_and_cas_stores".into(),
            main_home: v2.home.identity().to_string(),
            cas_home: v2.cas.identity().to_string(),
            backup_copies_excluded: true,
            ..Self::default()
        }
    }

    pub(super) fn needs_merge(&self) -> bool {
        (self.main.tiered_runs
            | self.main.merging_runs
            | self.cas.tiered_runs
            | self.cas.merging_runs)
            != 0
    }

    pub(super) fn active(&self) -> bool {
        self.request_id.is_some() && !matches!(self.phase, Complete | RecoveryRequired)
    }
}

pub(in crate::storage) struct CleanupJob {
    pub(super) owner_lock: Arc<tokio::sync::Mutex<()>>,
    pub(super) running: AtomicBool,
    status: Mutex<NativeCleanupStatus>,
}

impl CleanupJob {
    pub(in crate::storage) fn open(v2: &physical::V2Home) -> Self {
        let status = load(v2).unwrap_or_else(|error| NativeCleanupStatus {
            phase: RecoveryRequired,
            last_error: Some(truncate_error(&error)),
            ..NativeCleanupStatus::idle(v2)
        });
        Self {
            owner_lock: Arc::default(),
            running: AtomicBool::new(false),
            status: Mutex::new(status),
        }
    }

    fn lock_status(&self) -> MutexGuard<'_, NativeCleanupStatus> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn get(&self) -> NativeCleanupStatus {
        self.lock_status().clone()
    }

    pub(super) fn set(&self, status: NativeCleanupStatus) {
        *self.lock_status() = status;
    }
}

pub(super) fn truncate_error(error: &impl std::fmt::Display) -> String {
    error.to_string().chars().take(1024).collect()
}

fn load(v2: &physical::V2Home) -> StorageResult<NativeCleanupStatus> {
    let idle = NativeCleanupStatus::idle(v2);
    let Some(bytes) = v2
        .home
        .load_checkpoint(NODE_CLEANUP_CHECKPOINT)
        .map_err(map)?
    else {
        return Ok(idle);
    };
    let status: NativeCleanupStatus = serde_json::from_slice(&bytes)
        .map_err(|_| StorageError::invalid("native cleanup checkpoint encoding is invalid"))?;
    let current = NativeCleanupBoundary::capture(v2);
    let done_ok = if status.complete {
        matches!(
            (status.completed_at, status.completed_through, status.target),
            (Some(_), Some(finished), Some(target)) if finished.covers(target) && current.covers(finished)
        )
    } else {
        (status.completed_at, status.completed_through) == (None, None)
    };
    let same_id = (
        status.version,
        &status.scope,
        &status.main_home,
        &status.cas_home,
    ) == (idle.version, &idle.scope, &idle.main_home, &idle.cas_home);
    let request_ok = status
        .request_id
        .as_deref()
        .is_some_and(|id| Uuid::parse_str(id).is_ok());
    let flags_ok =
        same_id && status.backup_copies_excluded && !status.live_cas_orphan_repair_included;
    let phase_ok = status.complete == (status.phase == Complete)
        && status.phase != Idle
        && status.requested_at.is_some();
    let valid = flags_ok
        && done_ok
        && request_ok
        && phase_ok
        && status.target.is_some_and(|target| current.covers(target));
    valid
        .then_some(status)
        .ok_or_else(|| StorageError::invalid("invalid cleanup checkpoint"))
}
