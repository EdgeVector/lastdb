use super::*;

impl EncryptingNamespacedStore {
    pub(super) async fn load_reseal_checkpoint(
        &self,
        collection: &str,
    ) -> StorageResult<Option<ResealAtRestCheckpoint>> {
        let meta = self.open_namespace(RESEAL_CHECKPOINT_NAMESPACE).await?;
        let key = checkpoint_key(collection);
        let Some(bytes) = meta.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|e| {
            StorageError::BackendError(format!("reseal-at-rest checkpoint decode: {e}"))
        })
    }

    pub(super) async fn store_reseal_checkpoint(
        &self,
        checkpoint: &ResealAtRestCheckpoint,
    ) -> StorageResult<()> {
        let meta = self.open_namespace(RESEAL_CHECKPOINT_NAMESPACE).await?;
        let key = checkpoint_key(&checkpoint.collection);
        let bytes = serde_json::to_vec(checkpoint).map_err(|e| {
            StorageError::BackendError(format!("reseal-at-rest checkpoint encode: {e}"))
        })?;
        meta.put(key.as_bytes(), bytes).await
    }

    pub(super) async fn clear_reseal_checkpoint(&self, collection: &str) -> StorageResult<()> {
        let meta = self.open_namespace(RESEAL_CHECKPOINT_NAMESPACE).await?;
        let key = checkpoint_key(collection);
        let _ = meta.delete(key.as_bytes()).await?;
        let legacy_key = legacy_checkpoint_key(collection);
        let _ = meta.delete(legacy_key.as_bytes()).await?;
        Ok(())
    }

    pub(super) async fn load_reap_checkpoint(
        &self,
        collection: &str,
    ) -> StorageResult<Option<ReapUnsealedCheckpoint>> {
        let meta = self.open_namespace(REAP_CHECKPOINT_NAMESPACE).await?;
        let key = reap_checkpoint_key(collection);
        let Some(bytes) = meta.get(key.as_bytes()).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes).map(Some).map_err(|e| {
            StorageError::BackendError(format!("reap-unsealed checkpoint decode: {e}"))
        })
    }

    pub(super) async fn store_reap_checkpoint(
        &self,
        checkpoint: &ReapUnsealedCheckpoint,
    ) -> StorageResult<()> {
        let meta = self.open_namespace(REAP_CHECKPOINT_NAMESPACE).await?;
        let key = reap_checkpoint_key(&checkpoint.collection);
        let bytes = serde_json::to_vec(checkpoint).map_err(|e| {
            StorageError::BackendError(format!("reap-unsealed checkpoint encode: {e}"))
        })?;
        meta.put(key.as_bytes(), bytes).await
    }

    pub(super) async fn clear_reap_checkpoint(&self, collection: &str) -> StorageResult<()> {
        let meta = self.open_namespace(REAP_CHECKPOINT_NAMESPACE).await?;
        let key = reap_checkpoint_key(collection);
        let _ = meta.delete(key.as_bytes()).await?;
        Ok(())
    }
}
