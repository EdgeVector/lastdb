//! Point reads of one release or one channel, from durable storage first.

use super::*;

/// The anonymous release read both HTTP routers answer.
#[derive(Debug)]
pub enum ReleaseReadReply {
    /// The release exists. HTTP 200. The value is [`ReleaseRecord::to_public_json`].
    Ready(Value),
    /// No release has that id. HTTP 404. The value is the unknown-release body.
    Missing(Value),
    /// The storage read failed. HTTP 503. The value is the registry-unavailable body.
    Unavailable(Value),
}

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

    /// Classify [`Self::get_release`] for the anonymous HTTP read.
    ///
    /// `Ready` is the public JSON. `Missing` is the unknown-release body,
    /// including an id that is not 64 lowercase hex. `Unavailable` is the
    /// registry-unavailable body.
    #[must_use]
    pub async fn release_read_reply(&self, release_id: &str) -> ReleaseReadReply {
        match self.get_release(release_id).await {
            Ok(Some(record)) => ReleaseReadReply::Ready(record.to_public_json()),
            Ok(None) => ReleaseReadReply::Missing(unknown_release_json(release_id)),
            Err(error) => {
                ReleaseReadReply::Unavailable(registry_unavailable_json(&error.to_string()))
            }
        }
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
