use super::SyncCoordinator;
use crate::sync::SyncError;
use std::time::Duration;
use tokio::task::JoinHandle;

impl SyncCoordinator {
    /// Cancel every cadence before a request handler or local writer drain.
    /// Keep the handles so the later join proves that each task stopped.
    pub fn request_background_stop(&self) {
        for slot in [
            &self.task,
            &self.capture_reexport_task,
            &self.gc_atoms_task,
            &self.plane_compaction_task,
        ] {
            if let Some(handle) = slot.lock().unwrap().as_ref() {
                handle.abort();
            }
        }
    }

    /// Join every cancelled cadence before the final cloud sync.
    /// A synchronous disk operation can delay cancellation; a timed-out join
    /// is an error and must not produce a clean shutdown receipt.
    pub async fn join_background_tasks(&self, timeout: Duration) -> Result<(), String> {
        self.request_background_stop();
        let handles: [(&str, &std::sync::Mutex<Option<JoinHandle<()>>>); 4] = [
            ("background sync", &self.task),
            ("capture re-export", &self.capture_reexport_task),
            ("gc-atoms", &self.gc_atoms_task),
            ("plane compaction", &self.plane_compaction_task),
        ];
        let handles: Vec<_> = handles
            .into_iter()
            .filter_map(|(name, slot)| slot.lock().unwrap().take().map(|handle| (name, handle)))
            .collect();
        let deadline = tokio::time::Instant::now() + timeout;
        for (name, handle) in handles {
            tracing::info!(target: "fold_node::database", task = name, "shutdown: joining local task");
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => return Err(format!("{name} task failed: {error}")),
                Err(_) => return Err(format!("{name} task did not stop within {timeout:?}")),
            }
            tracing::info!(target: "fold_node::database", task = name, "shutdown: local task stopped");
        }
        crate::sync::capture::plane_compactor::cloud_off_restore::drain(deadline).await?;
        Ok(())
    }

    /// Run the final cloud sync only after every local cadence stops.
    pub async fn final_sync(&self) -> Result<(), SyncError> {
        self.force_sync_inner(true).await
    }

    /// Stop the background tasks and run a final cloud sync.
    pub async fn stop(&self) -> Result<(), SyncError> {
        self.join_background_tasks(Duration::from_secs(30))
            .await
            .map_err(SyncError::Storage)?;
        self.final_sync().await
    }
}
