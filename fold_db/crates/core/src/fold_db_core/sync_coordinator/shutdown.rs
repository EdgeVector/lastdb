use super::SyncCoordinator;
use std::time::Duration;

impl SyncCoordinator {
    /// Cancel every cadence before a request handler or local writer drain.
    /// Keep the handles so the later join proves that each task stopped.
    pub fn request_background_stop(&self) {
        if let Some(handle) = self.plane_compaction_task.lock().unwrap().as_ref() {
            handle.abort();
        }
    }

    /// Join local compaction before the final storage flush.
    /// A synchronous disk operation can delay cancellation; a timed-out join
    /// is an error and must not produce a clean shutdown receipt.
    pub async fn join_background_tasks(&self, timeout: Duration) -> Result<(), String> {
        self.request_background_stop();
        let handles = self.plane_compaction_task.lock().unwrap().take();
        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(handle) = handles {
            let name = "plane compaction";
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
}
