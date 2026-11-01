//! Guarded deletes: a delete that checks the key in the write gate.

use super::commit::GuardedDelete;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;

impl RecordStore {
    /// The keys of `guarded` that still hold the bytes the caller read. One
    /// batch read, by key, inside the write gate hold of the batch that deletes
    /// them, so no batch can change a key between the read and the delete.
    pub(super) async fn guarded_delete_keys(
        &self,
        guarded: Vec<GuardedDelete>,
    ) -> Result<Vec<Vec<u8>>, SchemaError> {
        if guarded.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<Vec<u8>> = guarded.iter().map(|delete| delete.key.clone()).collect();
        let stored = self
            .main_store
            .inner()
            .get_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read guarded keys: {error}")))?;
        Ok(guarded
            .into_iter()
            .zip(stored)
            .filter(|(delete, current)| current.as_deref() == Some(delete.expected.as_slice()))
            .map(|(delete, _)| delete.key)
            .collect())
    }
}
