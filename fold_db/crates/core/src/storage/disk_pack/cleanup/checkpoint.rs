//! Durable request identity and a past completion boundary, never user bodies.

use super::*;

pub(super) fn load(v2: &physical::V2Home) -> StorageResult<NativeCleanupStatus> {
    let Some(bytes) = v2.home.load_cleanup_checkpoint().map_err(map)? else {
        return Ok(NativeCleanupStatus::idle(v2));
    };
    let status: NativeCleanupStatus = serde_json::from_slice(&bytes)
        .map_err(|_| invalid("native cleanup checkpoint encoding is invalid"))?;
    let current = NativeCleanupBoundary {
        main_frontier: v2.home.status().frontier,
        cas_frontier: v2.cas.status().frontier,
    };
    let target_ok = status.target.is_some_and(|target| {
        target.main_frontier <= current.main_frontier && target.cas_frontier <= current.cas_frontier
    });
    let finish_ok = status.completed_through.is_some_and(|finish| {
        status.target.is_some_and(|target| {
            finish.main_frontier >= target.main_frontier
                && finish.cas_frontier >= target.cas_frontier
        }) && finish.main_frontier <= current.main_frontier
            && finish.cas_frontier <= current.cas_frontier
    });
    if status.version != 1
        || status.scope != model::SCOPE
        || status.main_home != v2.home.identity().to_string()
        || status.cas_home != v2.cas.identity().to_string()
        || !status.backup_copies_excluded
        || status.live_cas_orphan_repair_included
        || status
            .request_id
            .as_deref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
            .is_none()
        || !target_ok
        || status.requested_at.is_none()
        || status.complete != (status.phase == NativeCleanupPhase::Complete)
        || (status.complete && (!finish_ok || status.completed_at.is_none()))
        || (!status.complete
            && (status.completed_at.is_some() || status.completed_through.is_some()))
        || status.phase == NativeCleanupPhase::Idle
    {
        return Err(invalid(
            "native cleanup checkpoint identity or boundary is invalid",
        ));
    }
    Ok(status)
}

impl PackRuntime {
    pub(super) fn save_cleanup(&self, status: &NativeCleanupStatus) -> StorageResult<()> {
        let bytes = serde_json::to_vec(status)
            .map_err(|_| invalid("native cleanup checkpoint encoding failed"))?;
        let _space = laststore::space::reserve_space(
            self.physical.home.path(),
            &self.space_policy,
            (bytes.len() as u64).saturating_add(4096),
        )
        .map_err(map)?;
        self.physical
            .home
            .save_cleanup_checkpoint(&bytes)
            .map_err(map)
    }
}
