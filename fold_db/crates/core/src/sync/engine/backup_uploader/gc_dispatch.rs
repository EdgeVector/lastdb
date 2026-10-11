//! Effect-aware backup deletion after publication admission and durable intent.

use super::*;

impl SyncEngine {
    pub(super) async fn dispatch_backup_gc_deletes(
        &self,
        job_id: &str,
        specs: &[(String, u64)],
        report: &mut BackupOrphanGcReport,
    ) -> SyncResult<Vec<String>> {
        if specs.is_empty() {
            return Ok(Vec::new());
        }
        // A protocol presign can perform the delete itself. Record every
        // exact intent before the authenticated call, not just before S3 IO.
        let intents = self.backup_gc_jobs.intent_batch(job_id, specs)?;
        let mut known_present = self.backup_known_present.lock().await;
        let results = join_all(specs.iter().map(|(sha, _)| async move {
            let receipt = self.auth.presign_backup_chunk_delete(sha).await?;
            self.s3.delete(&receipt).await
        }))
        .await;
        let mut deleted = Vec::new();
        let mut outcomes = Vec::with_capacity(intents.len());
        for ((sha, _), (intent, result)) in specs.iter().zip(intents.into_iter().zip(results)) {
            match result {
                Ok(()) => {
                    report.deleted += 1;
                    known_present.remove(sha);
                    deleted.push(sha.clone());
                    outcomes.push((intent, GcObjectOutcome::DeleteAcknowledged));
                }
                Err(error) => {
                    report.failed += 1;
                    // A lost service response can follow a completed delete.
                    // Retain an unknown receipt; do not claim no dispatch.
                    outcomes.push((intent, GcObjectOutcome::Unknown));
                    tracing::warn!(target: "fold_db::sync::backup", chunk_sha256 = %sha,
                        error = %redact_sync_error_text(&error.to_string()),
                        "backup orphan delete did not return a completed receipt");
                }
            }
        }
        drop(known_present);
        self.backup_gc_jobs.outcome_batch(outcomes)?;
        Ok(deleted)
    }
}
