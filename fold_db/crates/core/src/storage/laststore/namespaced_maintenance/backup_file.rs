//! Stream a verified immutable backup file into an inactive LastStore.

use super::*;

impl LastStoreNamespacedStore {
    /// The store publishes only after the file SHA, exact length, UUID,
    /// collection, shard, group, and authenticated seal coordinate agree.
    pub fn install_backup_chunk_from_file(
        &self,
        chunk: &BackupChunkRef,
        path: &Path,
    ) -> StorageResult<()> {
        let chunk_uuid = Uuid::parse_str(&chunk.chunk_uuid)
            .map_err(|_| StorageError::BackendError("invalid restored chunk UUID".into()))?;
        let scope = laststore::SealedChunkMeta {
            collection: chunk.collection.clone(),
            shard: chunk.shard,
            group_id: chunk.group_id,
            chunk_uuid,
            path: path.to_path_buf(),
            end_csn: chunk.end_csn,
        };
        let installed = self
            .store
            .install_manifest_chunk_file(&scope, path, &chunk.sha256, chunk.bytes)
            .map_err(LastStoreKvStore::map_error)?;
        if installed.collection != scope.collection
            || installed.shard != scope.shard
            || installed.group_id != scope.group_id
            || installed.chunk_uuid != scope.chunk_uuid
            || installed.end_csn != scope.end_csn
        {
            return Err(StorageError::BackendError(
                "restored file chunk scope differs".into(),
            ));
        }
        Ok(())
    }
}
