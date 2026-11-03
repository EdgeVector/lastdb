//! The guarded row commit: one native batch of guarded deletes.
//!
//! The retirement of a time queue bucket writes through
//! [`RecordStore::commit_guarded_deletes`]. It runs inside the native write
//! request scope, so its read sees the committed root, the accepted batches and
//! the pending batch of the request, and the one `batch_mutate` at the end
//! joins that pending batch.

use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

/// A delete that runs only while the key still holds the value the caller read.
/// The commit reads the key in the write gate. A key that holds other bytes, or
/// none, is left alone.
pub(crate) struct GuardedDelete {
    pub key: Vec<u8>,
    pub expected: Vec<u8>,
}

impl RecordStore {
    /// Delete, in one batch, each key of `guarded` that still holds the bytes
    /// the caller read. Returns the number of keys deleted.
    ///
    /// The caller holds the native write gate through a request scope. A call
    /// outside a scope on a native store is a defect and fails here, because the
    /// read of the guarded keys must see the batch it joins.
    pub(crate) async fn commit_guarded_deletes(
        &self,
        guarded: Vec<GuardedDelete>,
    ) -> Result<u64, SchemaError> {
        self.require_write_scope()?;
        let deletes = self.guarded_delete_keys(guarded).await?;
        let deleted = deletes.len() as u64;
        if !deletes.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(deletes.into_iter().map(KvMutation::delete).collect())
                .await
                .map_err(|error| SchemaError::InvalidData(format!("commit rows: {error}")))?;
        }
        Ok(deleted)
    }

    /// A native writer needs a request scope.
    pub(in crate::db_operations::record_store) fn require_write_scope(
        &self,
    ) -> Result<(), SchemaError> {
        if crate::storage::disk_pack::transaction::publication::active(
            &self.namespaced_store.disk_pack_runtime(),
        ) {
            return Ok(());
        }
        Err(SchemaError::InvalidData(
            "row commits require a complete native write scope".into(),
        ))
    }
}
