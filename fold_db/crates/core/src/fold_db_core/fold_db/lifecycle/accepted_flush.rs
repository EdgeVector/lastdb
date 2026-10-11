//! A local durability barrier before an external metadata authority advances.

use super::*;

impl FoldDB {
    /// Wait for accepted local work and durable write lanes, then flush the
    /// store. This never starts, stops, or waits for a cloud worker.
    pub async fn flush_accepted_mutations(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), StorageError> {
        let deadline = tokio::time::Instant::now() + timeout;
        if !self.pending_tasks.wait_for_completion(timeout).await {
            return Err(StorageError::IoError(std::io::Error::other(
                "accepted local tasks did not finish before the durability barrier",
            )));
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if !self
            .mutation_manager
            .persist_lanes()
            .wait_for_quiescent(remaining)
            .await
        {
            return Err(StorageError::IoError(std::io::Error::other(
                "accepted local write lanes did not finish before the durability barrier",
            )));
        }
        self.flush().await
    }
}
