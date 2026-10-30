//! Point reads of one release or one channel, from durable storage first.

use super::*;

impl SchemaServiceState {
    /// Point get of one release by its exact key.
    ///
    /// Durable storage answers first. Memory is filled once at start, so a
    /// release published or revoked through another instance (another
    /// Lambda) is absent or stale there. Memory answers only when the backend
    /// keeps no per-record store. A durable record refreshes memory.
    ///
    /// # Errors
    /// Returns the backend read failure or a lock-poison error.
    pub async fn get_release(
        &self,
        release_id: &str,
    ) -> Result<Option<ReleaseRecord>, schema_types::FoldDbError> {
        if !is_sha256_hex(release_id) {
            return Ok(None);
        }
        if let Some(record) = self.storage.backend().load_release(release_id).await? {
            write_lock(&self.app_releases, "app_releases")?
                .insert(release_id.to_string(), record.clone());
            return Ok(Some(record));
        }
        Ok(read_lock(&self.app_releases, "app_releases")?
            .get(release_id)
            .cloned())
    }

    /// Point get of one channel by its exact key.
    ///
    /// Durable storage answers first, as for [`Self::get_release`]: a channel
    /// moved through another instance must not read as unknown or as its old
    /// release here.
    ///
    /// # Errors
    /// Returns the backend read failure or a lock-poison error.
    pub async fn get_channel(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<Option<ChannelRecord>, schema_types::FoldDbError> {
        if !is_valid_app_id(app_id) || !is_valid_channel(channel) {
            return Ok(None);
        }
        let key = channel_key(app_id, channel);
        if let Some(record) = self.storage.backend().load_channel(app_id, channel).await? {
            write_lock(&self.app_channels, "app_channels")?.insert(key, record.clone());
            return Ok(Some(record));
        }
        Ok(read_lock(&self.app_channels, "app_channels")?
            .get(&key)
            .cloned())
    }
}
