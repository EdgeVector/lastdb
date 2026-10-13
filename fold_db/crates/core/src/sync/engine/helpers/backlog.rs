use crate::sync::engine::types::{
    CloudSyncBacklogSnapshot, CloudSyncFailureKey, CloudSyncFailureStats,
    CLOUD_SYNC_BACKLOG_THRESHOLDS, MAX_PRESIGN_BATCH,
};
use crate::sync::error::SyncError;

pub(crate) fn crossed_cloud_sync_backlog_threshold(
    pending_count: usize,
    max_pending: usize,
) -> Option<u8> {
    if pending_count == 0 || max_pending == 0 {
        return None;
    }
    let percent = pending_count.saturating_mul(100) / max_pending;
    CLOUD_SYNC_BACKLOG_THRESHOLDS
        .iter()
        .copied()
        .rev()
        .find(|threshold| percent >= *threshold as usize)
}

pub(crate) fn cloud_sync_failure_class(operation: &str, err: &SyncError) -> String {
    let kind = match err {
        SyncError::Network(_) => "network",
        SyncError::S3(_) => "s3",
        SyncError::Auth(_) => "auth",
        SyncError::Banned(_) => "auth_banned",
        SyncError::QuotaExceeded(_) => "quota_exceeded",
        SyncError::CloudPurgeHeld(_) => "cloud_purge_held",
        SyncError::BackupSnapshotInProgress { .. }
        | SyncError::BackupSnapshotVerifyPending { .. } => "snapshot_in_progress",
        SyncError::Storage(_) => "storage",
        SyncError::DeviceLocked { .. } => "device_locked",
        SyncError::CorruptEntry { .. } | SyncError::PoisonEntry { .. } => "replay",
        // Distinct from "replay" (a bad cloud object) and from the "storage"
        // class this used to fall into: the entry is fine and the store is
        // fine, but this build cannot apply the entry, so the cursor is pinned
        // and every upload queues behind it. During the 2026-08-16 outage this
        // reported as the generic `download_storage`, indistinguishable from
        // ordinary disk trouble.
        SyncError::ReplayApplyFailed { .. } => "replay_apply_failed",
        SyncError::CorruptProofObject { .. } => "proof_object",
        SyncError::KeyProofFailed { .. } => "key_mismatch",
        SyncError::BackupBootstrapBlocked { .. } => "backup_bootstrap_blocked",
        SyncError::MissingSyncTarget { .. } => "missing_sync_target",
        SyncError::UnsupportedEnvelope { .. } => "unsupported_envelope",
        SyncError::UnsupportedBackupFormat { .. } => "unsupported_backup_format",
        SyncError::SequenceGap { .. } => "sequence_gap",
        SyncError::Crypto(_) | SyncError::WrongKey => "crypto",
        SyncError::Serialization(_) => "serialization",
        SyncError::Io(_) => "io",
        SyncError::SnapshotTooLarge { .. } => "snapshot_too_large",
        SyncError::MissingSnapshot { .. } => "missing_snapshot",
    };
    format!("{operation}_{kind}")
}

pub(crate) fn emit_cloud_sync_backlog_incident(
    key: &CloudSyncFailureKey,
    stats: &CloudSyncFailureStats,
    snapshot: CloudSyncBacklogSnapshot,
    sync_concurrency: usize,
) {
    tracing::error!(
        target: "fold_db::sync::backlog",
        sync_target = %key.sync_target,
        failure_class = %key.failure_class,
        failure_count = stats.count,
        pending_count = snapshot.pending_count,
        max_pending = snapshot.max_pending,
        pending_threshold_percent = snapshot.threshold_percent,
        oldest_pending_age_secs = snapshot.oldest_pending_age_secs,
        sync_concurrency = sync_concurrency,
        chunk_size = MAX_PRESIGN_BATCH,
        last_success_age_secs = snapshot.last_success_age_secs,
        error = %stats.last_error,
        "cloud sync backlog incident: repeated transfer failures crossed pending-depth threshold"
    );
}
