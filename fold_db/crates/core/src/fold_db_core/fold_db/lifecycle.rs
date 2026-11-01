use crate::storage::StorageError;

use super::FoldDB;

impl FoldDB {
    /// Upper bound on how long [`FoldDB::shutdown`] waits for tracked tasks.
    /// The limit matches the cold-cache model start limit.
    const SHUTDOWN_TASK_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    pub async fn shutdown(&self) -> Result<(), StorageError> {
        self.shutdown_with_task_drain_timeout(Self::SHUTDOWN_TASK_DRAIN_TIMEOUT)
            .await
    }

    /// Wait for background tasks. Returns the failure message on timeout.
    async fn drain_pending_tasks_at_shutdown(
        &self,
        task_drain_timeout: std::time::Duration,
    ) -> Option<String> {
        if self
            .pending_tasks
            .wait_for_completion(task_drain_timeout)
            .await
        {
            None
        } else {
            let message =
                format!("shutdown: background tasks did not drain within {task_drain_timeout:?}");
            tracing::error!(
                target: "fold_node::database",
                remaining = self.pending_tasks.count(),
                error = %message,
                "shutdown: continuing to the final durability barrier"
            );
            Some(message)
        }
    }

    async fn shutdown_with_task_drain_timeout(
        &self,
        task_drain_timeout: std::time::Duration,
    ) -> Result<(), StorageError> {
        let mut pre_flush_errors = Vec::new();
        tracing::info!(
            target: "fold_node::database",
            "Shutting down FoldDB: draining background tasks, flushing sync and storage"
        );
        self.background_conflict_janitor.stop();
        if let Err(error) = self.background_field_history.stop().await {
            pre_flush_errors.push(error);
        }
        let pending_task_timeout = self
            .drain_pending_tasks_at_shutdown(task_drain_timeout)
            .await;
        // The keep-small projection is written on a debounce, so the last
        // interval of meter updates is still in memory. Land it here, after
        // the tracked tasks have stopped moving the numbers and before the
        // durability barrier, stamped as a clean stop so the next boot
        // hydrates it as exact. An unclean stop leaves the runtime stamp
        // behind and reopens as `stale_after_unclean_stop` (2026-09-21).
        if let Err(e) = self
            .db_ops
            .records()
            .flush_keep_small_for_clean_stop()
            .await
        {
            tracing::warn!(
                target: "fold_node::database",
                error = %e,
                "shutdown: keep-small flush failed; live-budget meters may reopen stale"
            );
            pre_flush_errors.push(format!("keep-small flush failed: {e}"));
        }
        // Keep the final barrier after accepted work and the debounced
        // keep-small projection have drained.
        let flush_result = self.flush().await;
        let timeout = pending_task_timeout
            .into_iter()
            .chain(pre_flush_errors)
            .collect::<Vec<_>>()
            .join("; ");
        if !timeout.is_empty() {
            return match flush_result {
                Ok(()) => Err(StorageError::BackendError(timeout)),
                Err(error) => Err(StorageError::BackendError(format!(
                    "{timeout}; final durability barrier also failed: {error}"
                ))),
            };
        }
        flush_result
    }

    /// Flushes local storage to ensure all data is persisted
    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db_ops
            .flush()
            .await
            .map_err(|e| StorageError::IoError(std::io::Error::other(e.to_string())))
    }

    /// Wait for all pending background tasks to complete
    pub async fn wait_for_background_tasks(&self, timeout: std::time::Duration) -> bool {
        self.pending_tasks.wait_for_completion(timeout).await
    }
}
mod accepted_flush;
