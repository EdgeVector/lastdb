//! Scoped readers retain the exact target used to authorize the download.

use super::*;

pub(super) async fn target(
    engine: &SyncEngine,
    record: &PinLogRecord,
    scope_target: Option<&SyncTarget>,
) -> SyncResult<Option<SyncTarget>> {
    if let Some(target) = scope_target {
        if record.target_prefix != target.prefix {
            return Err(crate::sync::cloud_share::held(
                "the foreign log record crossed its signed target",
            ));
        }
        return Ok(Some(target.clone()));
    }
    if record.target_prefix.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        engine
            .pin_log
            .sync_target_by_prefix(&record.target_prefix)
            .await
            .map_err(SyncError::Storage)?,
    ))
}

impl SyncEngine {
    pub(in crate::sync::engine) async fn replay_foreign_share_segments(
        &self,
        target: &SyncTarget,
        segments: &[MutationLogSegment],
        frontier: &Frontier,
    ) -> SyncResult<MutationLogReplayReport> {
        if target.read_binding.is_none() {
            return Err(crate::sync::cloud_share::held(
                "this log replay has no foreign receiver binding",
            ));
        }
        super::replay::replay_mutation_log_segments_with_progress(
            self,
            segments,
            frontier,
            None,
            &target.crypto,
            Some(target),
        )
        .await
    }
}
