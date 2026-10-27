//! The last explicit cleanup request and its completed local boundary.

use serde::{Deserialize, Serialize};

pub(super) const SCOPE: &str = "already_deleted_records_in_active_main_and_cas_stores";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCleanupPhase {
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCleanupHome {
    pub tiered_runs: usize,
    pub merging_runs: usize,
    pub held_retired_bytes: u64,
    pub removed_files: u64,
}

/// A completion receipt covers a past local boundary, never future writes or
/// retained backup copies. Live CAS rows are not an orphan-GC candidate here.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub(super) fn idle(v2: &super::super::physical::V2Home) -> Self {
        Self {
            version: 1,
            request_id: None,
            phase: NativeCleanupPhase::Idle,
            complete: false,
            scope: SCOPE.into(),
            main_home: v2.home.identity().to_string(),
            cas_home: v2.cas.identity().to_string(),
            requested_at: None,
            target: None,
            completed_at: None,
            completed_through: None,
            merge_steps_since_resume: 0,
            main: NativeCleanupHome::default(),
            cas: NativeCleanupHome::default(),
            last_error: None,
            backup_copies_excluded: true,
            live_cas_orphan_repair_included: false,
        }
    }

    pub(super) fn active(&self) -> bool {
        self.request_id.is_some()
            && !matches!(
                self.phase,
                NativeCleanupPhase::Complete | NativeCleanupPhase::RecoveryRequired
            )
    }
}
