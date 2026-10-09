//! Purge personal cloud log + snapshots for this device prefix.

use super::super::error::{SyncError, SyncResult};
use super::*;

impl SyncEngine {
    /// Delete every cloud-side log entry and snapshot for the personal
    /// target, leaving the prefix empty.
    ///
    /// Used by `fold_db_node`'s `reset-database` flow to make local wipe
    /// actually stick: without this, the next sync cycle would re-bootstrap
    /// from `latest.enc` and replay the full personal log, undoing the
    /// reset.
    ///
    /// Org targets (`targets[1..]`) are intentionally untouched. Org logs
    /// are shared state across members; leaving an org is a separate flow
    /// the user must perform explicitly.
    ///
    /// The device write lock is acquired for the duration of the purge so
    /// no other device can race uploads against us. The lock is released
    /// best-effort on the way out — a stuck lock self-heals via TTL.
    pub async fn purge_personal_log(&self) -> SyncResult<PurgeOutcome> {
        self.acquire_lock().await?;
        let result = self.purge_personal_log_inner().await;
        if let Err(e) = self.release_lock().await {
            tracing::warn!("purge_personal_log: failed to release device lock (non-fatal): {e}");
        }
        result
    }

    pub(crate) async fn purge_personal_log_inner(&self) -> SyncResult<PurgeOutcome> {
        let personal = self.targets.lock().await[0].clone();
        if !personal.prefix.is_empty() {
            // Defense-in-depth: targets[0] is always personal (empty
            // prefix) by construction. If this ever changes upstream we
            // want to fail loud, not nuke the wrong prefix.
            return Err(SyncError::Auth(
                "purge_personal_log: targets[0] is not the personal target".to_string(),
            ));
        }

        let mut outcome = PurgeOutcome::default();

        // 1) Delete every log object: list → presign DELETE in chunks of
        //    1000 (Lambda cap) → DELETE each presigned URL.
        let log_objects = self.auth.list_log_objects(&personal).await?;
        // Fail loud on unparseable log keys (mirrors the snapshot loop below).
        // Silently dropping them via `filter_map` could leave objects behind
        // after a "purge", letting the next bootstrap replay stale data back in.
        let mut log_keys: Vec<(u64, String)> = Vec::with_capacity(log_objects.len());
        for obj in &log_objects {
            let Some((_, seq)) = parse_mutation_log_object_key(&obj.key) else {
                return Err(SyncError::Auth(format!(
                    "purge_personal_log: log key '{}' did not parse as a mutation-log key \
                     (list_log_objects contract violation)",
                    obj.key,
                )));
            };
            let relative = relative_mutation_log_key(&obj.key)
                .unwrap_or(obj.key.as_str())
                .to_string();
            log_keys.push((seq, relative));
        }

        for chunk in log_keys.chunks(MAX_PRESIGN_BATCH) {
            let seqs: Vec<u64> = chunk.iter().map(|(seq, _)| *seq).collect();
            let keys: Vec<String> = chunk.iter().map(|(_, key)| key.clone()).collect();
            let urls = self
                .auth
                .presign_log_delete_object_keys(&personal, &seqs, &keys)
                .await?;
            for url in &urls {
                self.s3.delete(url).await?;
            }
            outcome.deleted_log_objects += urls.len();
            // Debit the meter for the purged objects (best effort; the
            // daily storage audit heals a missed debit).
            if let Err(e) = self
                .auth
                .confirm_log_delete_object_keys(&personal, &seqs, &keys)
                .await
            {
                tracing::warn!(error = %e, "purge_personal_log: log delete metering confirm failed");
            }
        }

        // 2) Delete every snapshot under `snapshots/`. Includes
        //    `latest.enc` plus any compacted `{seq}.enc` left behind by
        //    prior compactions. Listing first (rather than only attempting
        //    `latest.enc`) ensures the prefix is fully empty after reset.
        let snapshot_objects = self.auth.list_snapshot_objects(&personal).await?;
        for obj in snapshot_objects {
            // The Lambda strips the scope prefix on list responses, so
            // keys come back as `snapshots/{name}` — we want the bare
            // `{name}` for the delete request.
            let Some(name) = obj.key.strip_prefix("snapshots/") else {
                // The Lambda's list_snapshot_objects contract is that every
                // returned key starts with `snapshots/`. A non-`snapshots/`
                // key here means the contract was violated upstream — fail
                // loud rather than silently skipping (and possibly leaving
                // stale snapshots behind on a "purge" call). Mirrors the
                // defense-in-depth check on `targets[0]` above.
                return Err(SyncError::Auth(format!(
                    "purge_personal_log: snapshot key '{}' did not start with 'snapshots/' \
                     (Lambda list_snapshot_objects contract violation)",
                    obj.key,
                )));
            };
            let url = self.auth.presign_snapshot_delete(name).await?;
            self.s3.delete(&url).await?;
            outcome.deleted_snapshots += 1;
            if let Err(e) = self
                .auth
                .confirm_snapshot_delete_for_target(&personal, name)
                .await
            {
                tracing::warn!(
                    snapshot = %name,
                    error = %e,
                    "purge_personal_log: snapshot delete metering confirm failed"
                );
            }
        }

        tracing::info!(
            "purge_personal_log: deleted {} log objects, {} snapshots",
            outcome.deleted_log_objects,
            outcome.deleted_snapshots,
        );
        Ok(outcome)
    }

    // =========================================================================
    // Non-personal sync configuration (cross-user shares)
    // =========================================================================
}
