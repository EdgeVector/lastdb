//! Delete partial scoped uploads before any explicit reallocation.

use super::*;

impl SyncEngine {
    /// Best-effort delete of server-allocated seq objects that already landed
    /// before a sibling PUT in the same scoped chunk failed.
    ///
    /// Without this, a re-alloc on the next cycle permanently advances the
    /// scoped counter while the partial objects remain, so org download's
    /// non-contiguous listing check hard-fails forever (manual object cleanup
    /// required). Delete failures are non-fatal: we still surface the original
    /// PUT error and hope a later compact/operator path heals stragglers.
    pub(super) async fn rollback_scoped_partial_puts(&self, target: &SyncTarget, seqs: &[u64]) {
        if seqs.is_empty() {
            return;
        }
        let mut sorted: Vec<u64> = seqs.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        tracing::warn!(
            target: "fold_db::sync",
            target = %target.label,
            count = sorted.len(),
            first = sorted.first().copied().unwrap_or(0),
            last = sorted.last().copied().unwrap_or(0),
            "scoped upload partial PUT: rolling back orphan seq objects before re-alloc retry"
        );
        for chunk in sorted.chunks(MAX_PRESIGN_BATCH) {
            match self.auth.presign_log_delete_target(target, chunk).await {
                Ok(urls) => {
                    for url in urls {
                        if let Err(e) = self.s3.delete(&url).await {
                            tracing::warn!(
                                target: "fold_db::sync",
                                target = %target.label,
                                error = %e,
                                "scoped partial-PUT rollback delete failed (non-fatal)"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync",
                        target = %target.label,
                        error = %e,
                        "scoped partial-PUT rollback presign_log_delete failed (non-fatal)"
                    );
                }
            }
        }
    }
}
