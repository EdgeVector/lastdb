//! Field key, tip, liveness and delete-barrier locks.

use super::*;
use futures::{future::BoxFuture, FutureExt};

impl RecordStore {
    /// Run `work` inside one native write request scope. The scope holds the
    /// write gate from its first read to the accept of its one batch, so a
    /// read-modify-write inside it cannot interleave with another writer.
    pub(crate) fn in_write_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::transaction::scope(runtime, work).boxed()
    }

    /// Run `work` inside one native write request scope of a background job.
    /// It holds the write gate like [`Self::in_write_scope`], and it returns
    /// after the batch is durable. Unreserved background work shares the
    /// current collection window instead of sealing one for each batch.
    pub(crate) fn in_background_scope<'a, T: Send + 'a>(
        &'a self,
        work: impl std::future::Future<Output = Result<T, crate::schema::SchemaError>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T, crate::schema::SchemaError>> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        crate::storage::disk_pack::transaction::background_scope(runtime, work).boxed()
    }

    /// Guard the short (read `moc:` → build items → commit) section for one
    /// field key. See [`RecordStore::field_key_commit_locks`].
    pub(crate) async fn lock_field_key_commit(
        &self,
        field_key_id: &str,
        storage_prefix: Option<&str>,
    ) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        self.field_key_commit_lock(field_key_id, storage_prefix)
            .write_owned()
            .await
    }

    /// The map lock ends before the caller waits on the field key lock.
    fn field_key_commit_lock(
        &self,
        field_key_id: &str,
        storage_prefix: Option<&str>,
    ) -> Arc<tokio::sync::RwLock<()>> {
        let lock_key = format!("{}\u{1f}{field_key_id}", storage_prefix.unwrap_or_default());
        let mut map = self
            .field_key_commit_locks
            .lock()
            .expect("field_key_commit_locks poisoned");
        Arc::clone(map.entry(lock_key).or_default())
    }

    /// Serialize source changes and reclaim checks for blob targets.
    pub(crate) async fn lock_liveness_blobs(
        &self,
        blob_refs: &[String],
    ) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
        lock_target_stripes(&self.blob_liveness_locks, blob_refs).await
    }
}
