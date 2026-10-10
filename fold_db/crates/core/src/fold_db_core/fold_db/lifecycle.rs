use crate::storage::StorageError;

use super::FoldDB;

impl FoldDB {
    /// Cancel local sync cadences as soon as the daemon stops accepting calls.
    /// Shutdown joins them before it records a clean flush.
    #[cfg(feature = "cloud-sync")]
    pub fn request_background_stop(&self) {
        self.sync_coordinator.request_background_stop();
    }

    /// Upper bound on how long [`FoldDB::shutdown`] waits for tracked tasks.
    /// The limit matches the cold-cache model start limit.
    const SHUTDOWN_TASK_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Default upper bound on the final sync inside [`FoldDB::shutdown`].
    ///
    /// On 2026-09-24 a primary shutdown sat in the final sync (a cloud list
    /// of 51,407 entries) for 17 minutes with the socket already closed, and
    /// only a SIGKILL ended it. Sync is resumable: the local mutation log and
    /// the cursors are durable, so the next boot continues the upload. A
    /// stop must not wait on the network without a limit.
    /// `LASTDB_SHUTDOWN_SYNC_TIMEOUT_SECS` overrides it.
    #[cfg(feature = "cloud-sync")]
    const SHUTDOWN_SYNC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

    #[cfg(feature = "cloud-sync")]
    fn shutdown_sync_timeout() -> std::time::Duration {
        env_flag::var_parsed::<u64>("LASTDB_SHUTDOWN_SYNC_TIMEOUT_SECS")
            .map_or(Self::SHUTDOWN_SYNC_TIMEOUT, std::time::Duration::from_secs)
    }

    /// Graceful async shutdown: flush sync, stop workers, drain persist lanes,
    /// drain tracked tasks, and then flush storage.
    ///
    /// Search delivery for a request is part of its schema-lane envelope.
    /// Other process tasks can still own storage handles. The task drain
    /// prevents a caller from removing the data directory before they stop.
    /// The final storage flush still runs after either drain times out.
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        self.shutdown_with_task_drain_timeout(Self::SHUTDOWN_TASK_DRAIN_TIMEOUT)
            .await
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
        #[cfg(feature = "cloud-sync")]
        self.request_background_stop();
        // Stop periodic flusher before the final barrier so we do not race
        // two flushes (and so Drop does not try to abort a still-running tick).
        self.background_flush.stop().await;
        // The worker stays disabled in production. Stop remains safe for test
        // instances that construct the explicit repair helper.
        self.background_persist.stop();
        self.background_protein_reaper.stop();
        self.background_tip_history_drain.stop();
        self.background_atom_reclaim_janitor.stop();
        // Drain accepted persist-lane work FIRST, while admission stays open.
        // The capture wait and the final sync below can take many seconds,
        // and a supervisor can SIGKILL the process before they finish (the
        // primary LaunchAgent had a 5 s exit timeout on 2026-09-24, and the
        // kill landed inside the final sync). Acked writes must not wait
        // behind that work to become durable.
        self.drain_persist_lanes_before_sync(task_drain_timeout)
            .await;
        #[cfg(feature = "cloud-sync")]
        pre_flush_errors.extend(self.stop_sync_for_shutdown(task_drain_timeout).await);
        // The final sync can apply remote mutations. Stop lane admission only
        // after that sync finishes, then drain each accepted envelope.
        self.mutation_manager.persist_lanes().stop();
        let persist_lane_timeout = if self
            .mutation_manager
            .persist_lanes()
            .wait_for_drain(task_drain_timeout)
            .await
        {
            None
        } else {
            let message = format!(
                "shutdown: schema persist lanes did not drain within {task_drain_timeout:?}"
            );
            tracing::error!(
                target: "fold_node::database",
                error = %message,
                lanes = %self.busy_persist_lane_summary(),
                "shutdown: continuing to the final durability barrier"
            );
            Some(message)
        };
        let pending_task_timeout = if self
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
        };
        // The keep-small projection is written on a debounce, so the last
        // interval of meter updates is still in memory. Land it here, after
        // the persist-lane drain has stopped moving the numbers and before
        // the durability barrier, stamped as a clean stop so the next boot
        // hydrates it as exact. An unclean stop leaves the runtime stamp
        // behind and reopens as `stale_after_unclean_stop` (2026-09-21).
        if let Err(e) = self.db_ops.atoms().flush_keep_small_for_clean_stop().await {
            tracing::warn!(
                target: "fold_node::database",
                error = %e,
                "shutdown: keep-small flush failed; live-budget meters may reopen stale"
            );
            pre_flush_errors.push(format!("keep-small flush failed: {e}"));
        }
        // Always durability-barrier on graceful shutdown — deferred mutation
        // flush means the last interval of writes may still be in memory.
        let flush_result = self.flush().await;
        let timeout = [persist_lane_timeout, pending_task_timeout]
            .into_iter()
            .flatten()
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

    #[cfg(feature = "cloud-sync")]
    async fn stop_sync_for_shutdown(&self, task_drain_timeout: std::time::Duration) -> Vec<String> {
        let mut errors = Vec::new();
        if !self
            .mutation_manager
            .wait_for_capture_tasks(task_drain_timeout)
            .await
        {
            tracing::warn!(
                target: "fold_node::database",
                "shutdown: post-ack capture queue did not drain before final sync"
            );
            errors.push("post-ack capture queue did not drain".to_string());
        }
        if let Err(error) = self
            .sync_coordinator
            .join_background_tasks(task_drain_timeout)
            .await
        {
            tracing::error!(target: "fold_node::database", %error, "shutdown: local sync task did not stop");
            errors.push(format!("local sync task did not stop: {error}"));
            return errors;
        }
        let sync_timeout = Self::shutdown_sync_timeout();
        match tokio::time::timeout(sync_timeout, self.sync_coordinator.final_sync()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!("sync flush on shutdown failed: {error}");
                errors.push(format!("sync flush failed: {error}"));
            }
            Err(_) => {
                tracing::error!(
                    target: "fold_node::database",
                    timeout_ms = u64::try_from(sync_timeout.as_millis()).unwrap_or(u64::MAX),
                    "shutdown: final sync did not finish in time"
                );
                errors.push("final sync did not finish in time".to_string());
            }
        }
        errors
    }

    /// First shutdown drain: wait for every accepted lane envelope to become
    /// durable without closing admission. The later stop + drain still covers
    /// work that the final sync adds.
    async fn drain_persist_lanes_before_sync(&self, timeout: std::time::Duration) {
        let lanes = self.mutation_manager.persist_lanes();
        let depth = lanes.depth();
        if lanes.wait_for_quiescent(std::time::Duration::ZERO).await {
            return;
        }
        let started = std::time::Instant::now();
        tracing::info!(
            target: "fold_node::database",
            depth,
            lanes = %self.busy_persist_lane_summary(),
            "shutdown: draining persist lanes before sync stop"
        );
        if lanes.wait_for_quiescent(timeout).await {
            tracing::info!(
                target: "fold_node::database",
                depth,
                elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "shutdown: persist lanes drained before sync stop"
            );
        } else {
            tracing::error!(
                target: "fold_node::database",
                timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                remaining = lanes.depth(),
                lanes = %self.busy_persist_lane_summary(),
                "shutdown: persist lanes did NOT drain before sync stop; \
                 queued writes are at risk if the supervisor kills the process"
            );
        }
    }

    /// `schema:queued+reserved[:unhealthy]` for every lane that holds work.
    fn busy_persist_lane_summary(&self) -> String {
        let rows: Vec<String> = self
            .mutation_manager
            .persist_lanes()
            .occupancy()
            .into_iter()
            .filter(|row| {
                row.queued_entries > 0 || row.reserved_entries > 0 || row.queued_bytes > 0
            })
            .map(|row| {
                format!(
                    "{}:{}+{}{}",
                    row.schema_name,
                    row.queued_entries,
                    row.reserved_entries,
                    if row.unhealthy { ":unhealthy" } else { "" }
                )
            })
            .collect();
        if rows.is_empty() {
            "none".to_string()
        } else {
            rows.join(",")
        }
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
