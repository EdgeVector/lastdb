//! Quarantine of the cloud log entry that blocks replay.

use super::super::super::error::{SyncError, SyncResult};
use super::*;

impl SyncEngine {
    /// Check the request names the active blocker; return the quarantine mode.
    async fn validate_replay_blocker_request(
        &self,
        target: &str,
        seq: u64,
    ) -> SyncResult<&'static str> {
        let blocker = self
            .replay_blocker
            .lock()
            .await
            .clone()
            .ok_or_else(|| SyncError::Storage("no replay blocker is active".to_string()))?;
        let mode = match blocker.code.as_str() {
            "cloud_replay_corrupt_entry" => "delete",
            "cloud_replay_apply_failed" => "skip_local",
            other => {
                return Err(SyncError::Storage(format!(
                    "unsupported replay blocker code '{other}'"
                )));
            }
        };
        if blocker.target != target || blocker.seq != seq {
            return Err(SyncError::Storage(format!(
                "replay blocker mismatch: active target='{}' seq={}, requested target='{target}' seq={seq}",
                blocker.target, blocker.seq
            )));
        }
        Ok(mode)
    }

    async fn replay_blocker_sync_target(&self, target: &str) -> SyncResult<SyncTarget> {
        self.targets
            .lock()
            .await
            .iter()
            .find(|candidate| candidate.label == target)
            .cloned()
            .ok_or_else(|| {
                SyncError::Storage(format!(
                    "active replay blocker target '{target}' is not configured"
                ))
            })
    }

    /// Delete one corrupt cloud log object and confirm metering (best effort).
    async fn delete_replay_blocker_log_object(
        &self,
        sync_target: &SyncTarget,
        seq: u64,
    ) -> SyncResult<()> {
        let urls = self
            .auth
            .presign_log_delete_target(sync_target, &[seq])
            .await?;
        if urls.len() != 1 {
            return Err(SyncError::Auth(format!(
                "presign_log_delete '{}': expected 1 url, got {}",
                sync_target.label,
                urls.len()
            )));
        }
        self.s3.delete(&urls[0]).await?;
        if let Err(e) = self
            .auth
            .confirm_log_delete_object_keys(sync_target, &[seq], &[])
            .await
        {
            tracing::warn!(
                target = %sync_target.label,
                seq,
                error = %e,
                "quarantine: log delete metering confirm failed (non-fatal)"
            );
        }
        Ok(())
    }

    /// Clear the exact cloud log entry currently blocking replay.
    ///
    /// Callers must provide the target and seq from `SyncStatus.replay_blocker`
    /// (no wildcards). Two codes are supported, with different safety:
    ///
    /// - `cloud_replay_corrupt_entry` — **delete** the cloud object, write a
    ///   local tombstone, leave the cursor for the next cycle to skip the 404.
    ///   The object was unreadable; destroying it is correct for every device.
    /// - `cloud_replay_apply_failed` — **skip-with-tombstone only**: write a
    ///   local skip tombstone, advance this device's download cursor past the
    ///   seq, do **not** delete the cloud object. Peers / later builds can still
    ///   replay it. Deleting here would be irreversible shared-data loss for a
    ///   mere local apply refusal.
    pub async fn quarantine_current_replay_blocker(
        &self,
        target: &str,
        seq: u64,
    ) -> SyncResult<ReplayBlockerQuarantineOutcome> {
        if *self.state.lock().await == SyncState::Syncing {
            return Err(SyncError::Storage(
                "cannot quarantine replay blocker while sync is running".to_string(),
            ));
        }

        let mode = self.validate_replay_blocker_request(target, seq).await?;
        let sync_target = self.replay_blocker_sync_target(target).await?;
        let deleted_log_objects = if mode == "delete" {
            self.delete_replay_blocker_log_object(&sync_target, seq)
                .await?;
            1
        } else {
            0
        };

        // Local tombstone: corrupt path needs it so a listed-but-404 object
        // advances; apply-failed path needs it so a still-present object is
        // skipped without re-applying on the next cycle (defense in depth —
        // we also advance the cursor below for skip_local).
        self.save_replay_quarantine_tombstone(&sync_target.prefix, seq)
            .await?;

        if mode == "skip_local" {
            // Advance past the pinned seq so the next cycle can upload again
            // without re-hitting the un-applicable entry.
            self.checkpoint_download_cursor_after_replay(&sync_target, seq)
                .await;
        }

        let msg = if mode == "delete" {
            format!(
                "Quarantined (deleted) cloud replay blocker target='{}' seq={seq}; retry sync to advance replay",
                sync_target.label
            )
        } else {
            format!(
                "Skipped local cloud replay blocker target='{}' seq={seq} without deleting the cloud object; next cycle may upload",
                sync_target.label
            )
        };
        *self.replay_blocker.lock().await = None;
        *self.last_error.lock().await = Some(msg.clone());
        *self.last_error_at.lock().await = Some(crate::clock::unix_secs());
        self.set_state(SyncState::Dirty, Some(&msg)).await;

        Ok(ReplayBlockerQuarantineOutcome {
            target: sync_target.label,
            seq,
            deleted_log_objects,
            mode: mode.to_string(),
        })
    }
}
