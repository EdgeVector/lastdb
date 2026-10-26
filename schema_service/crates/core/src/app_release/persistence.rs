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
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("generation conflict") {
                    if let Some(stored) = msg
                        .split("stored=")
                        .nth(1)
                        .and_then(|s| s.split(',').next())
                    {
                        if let Ok(expected) = stored.parse::<u64>() {
                            return ReleaseError::GenerationConflict {
                                expected,
                                supplied: record.generation,
                            };
                        }
                    }
                }
                ReleaseError::Internal(msg)
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
