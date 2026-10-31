//! Records of a format 7 home: one stored value at each primary key.
//!
//! The mutation manager merges the fields of a record (decision D1). This
//! file reads the records by key and puts the changed ones, with their
//! alternate entry changes and their file blob counter changes, in one batch
//! of the native request. A record write creates no atom, tip, history row or
//! atom counter.

use super::blob_counter::BlobDeltas;
use super::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::StoredRecord;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

/// One record that a native write changes.
pub(crate) struct RecordChange {
    /// The storage key, scope included.
    pub key: String,
    /// The stored value that the write read in the same scope.
    pub before: Option<StoredRecord>,
    /// The merged value that the write puts.
    pub after: StoredRecord,
}

impl AtomStore {
    /// The storage key of the record at one canonical primary key. The record
    /// space codec of the schema blinds the hash and types the range.
    pub(crate) fn record_key(
        &self,
        schema: &Schema,
        key: &KeyValue,
        scope: Option<&str>,
    ) -> Result<String, SchemaError> {
        let space = record_space_uuid(&schema.name);
        let hash = key
            .hash
            .as_deref()
            .ok_or_else(|| SchemaError::InvalidData("a record key requires a hash".into()))?;
        let record = self
            .key_codec_for_molecule(&space)?
            .api_record_key(&space, hash, key.range.as_deref().unwrap_or_default())
            .map_err(|error| SchemaError::InvalidData(format!("invalid record key: {error}")))?;
        Ok(build_storage_key(scope, &record))
    }

    /// The stored records at these storage keys, in the order given. One read
    /// by key for each chunk. Inside a native request the read sees the
    /// request's earlier private writes.
    pub(crate) async fn read_records(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<StoredRecord>>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            let rows = self
                .main_store
                .inner()
                .get_many(chunk.iter().map(|key| key.clone().into_bytes()).collect())
                .await
                .map_err(|error| SchemaError::InvalidData(format!("read records: {error}")))?;
            if rows.len() != chunk.len() {
                return Err(SchemaError::InvalidData(
                    "the record read batch is incomplete".into(),
                ));
            }
            for row in rows {
                out.push(
                    row.map(|bytes| StoredRecord::decode(&bytes))
                        .transpose()
                        .map_err(|error| {
                            SchemaError::InvalidData(format!("a stored record: {error}"))
                        })?,
                );
            }
        }
        Ok(out)
    }

    /// Put the changed records, their alternate entry changes and their file
    /// blob counter changes in one batch of the native request. A blob that a
    /// record stops naming loses one reference, and a blob that it starts
    /// naming gains one. The caller holds the native write gate and read each
    /// `before` in the same scope.
    pub(crate) async fn stage_records(
        &self,
        changes: Vec<RecordChange>,
        alternates: Vec<KvMutation>,
    ) -> Result<(), SchemaError> {
        self.require_write_scope()?;
        let mut blobs = BlobDeltas::default();
        let mut batch = Vec::with_capacity(changes.len() + alternates.len());
        for change in changes {
            if let Some(before) = &change.before {
                blobs.add(&before.blob_refs(), -1);
            }
            blobs.add(&change.after.blob_refs(), 1);
            let value = change.after.encode().map_err(|error| {
                SchemaError::InvalidData(format!("the record write is refused: {error}"))
            })?;
            batch.push(KvMutation::put(change.key.into_bytes(), value));
        }
        batch.extend(alternates);
        batch.extend(self.blob_counter_mutations(blobs).await?);
        self.main_store.inner().batch_mutate(batch).await?;
        Ok(())
    }
}
