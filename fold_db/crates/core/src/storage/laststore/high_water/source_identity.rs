use super::*;

impl LastStoreHighWaterFile {
    /// Read the existing source identity without creating or changing a marker.
    pub(crate) fn backup_source_identity(&self) -> StorageResult<(String, u64)> {
        let _guard = self.lock_state();
        let bytes = fs::read(&self.path)
            .map_err(|_| self.error(format_args!("backup source identity is unavailable")))?;
        let state: LastStoreHighWater = serde_json::from_slice(&bytes)
            .map_err(|_| self.error(format_args!("backup source identity is invalid")))?;
        if state.version != VERSION || Uuid::parse_str(&state.store_uuid).is_err() {
            return Err(self.error(format_args!("backup source identity is invalid")));
        }
        Ok((state.store_uuid, state.backup_epoch))
    }
}
