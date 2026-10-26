//! Persistence helpers for the release registry.

use super::*;

impl SchemaServiceState {
    pub(super) async fn persist_release(&self, record: &ReleaseRecord) -> Result<(), ReleaseError> {
        self.storage
            .backend()
            .save_release(record)
            .await
            .map_err(|e| ReleaseError::Internal(e.to_string()))
    }

    pub(super) async fn persist_channel(&self, record: &ChannelRecord) -> Result<(), ReleaseError> {
        self.storage
            .backend()
            .save_channel(record)
            .await
            .map_err(|e| match e {
                schema_types::FoldDbError::GenerationConflict { expected, supplied } => {
                    ReleaseError::GenerationConflict { expected, supplied }
                }
                other => ReleaseError::Internal(other.to_string()),
            })
    }

    /// Replace the in-memory release + channel stores from a backend load.
    /// Called once at startup, next to the app-registry load.
    ///
    /// # Errors
    /// Returns a lock-poison error.
    pub fn install_loaded_releases(
        &self,
        releases: HashMap<String, ReleaseRecord>,
        channels: HashMap<String, ChannelRecord>,
    ) -> Result<(), schema_types::FoldDbError> {
        {
            let mut store = write_lock(&self.app_releases, "app_releases")?;
            *store = releases;
        }
        let mut store = write_lock(&self.app_channels, "app_channels")?;
        *store = channels;
        Ok(())
    }
}
