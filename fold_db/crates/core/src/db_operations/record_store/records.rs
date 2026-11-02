//! Records of a home: one stored value at each primary key.
//!
//! The mutation manager merges the fields of a record (decision D1). This
//! file reads the records by key and puts the changed ones, with their
//! alternate entry changes and their file blob counter changes, in one batch
//! of the native request.
//! Its field history entries hold copies of old values (`field_history/records.rs`).
//!
//! A read by record key checks the record warm set first (decision D2). A
//! record that the read finds on disk, or that a flushed write puts, enters
//! it.

use super::blob_counter::BlobDeltas;
use super::RecordStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::disk_pack::record_warm::WrittenRecord;
use crate::storage::KvMutation;
use futures::FutureExt;
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

/// One record that a native write changes.
pub(crate) struct RecordChange {
    /// The storage key, scope included.
    pub key: String,
    /// The stored value that the write read in the same scope.
    pub before: Option<Arc<StoredRecord>>,
    /// The merged value that the write puts.
    pub after: StoredRecord,
    /// True when another group of the request writes this key too: the cap
    /// applies to the final record (`record_cap.rs`).
    pub cap_deferred: bool,
}

/// The storage scope of the records of a schema: the prefix that its fields
/// carry in an org database, as the alternate entries of the field key path
/// use. `None` is the personal database.
pub(crate) fn record_scope(schema: &Schema) -> Option<String> {
    schema
        .runtime_fields
        .values()
        .find_map(|field| field.common().storage_prefix())
        .map(str::to_owned)
}

impl RecordStore {
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
            .key_codec_for_field_key(&space)?
            .api_record_key(&space, hash, key.range.as_deref().unwrap_or_default())
            .map_err(|error| SchemaError::InvalidData(format!("invalid record key: {error}")))?;
        Ok(build_storage_key(scope, &record))
    }

    /// The stored records at these storage keys, in the order given. Memory
    /// answers first; one read by key for each chunk of the misses. Inside a
    /// native request the read sees the request's earlier private writes.
    ///
    /// The memory lookup and every disk chunk read one revision: a caller
    /// without a read scope gets one for this call (`record_read_scope`), so a
    /// commit between two chunks cannot mix two revisions in one result. A
    /// caller with a scope keeps its own revision and read limit.
    pub(crate) async fn read_records(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        self.record_read_scope(self.read_records_scoped(keys).boxed())
            .await
    }

    async fn read_records_scoped(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        let runtime = self.namespaced_store.disk_pack_runtime();
        let mut warm = runtime
            .warm_records(keys)
            .map_err(|error| SchemaError::InvalidData(format!("read records: {error}")))?;
        let mut out = match &mut warm {
            Some(read) => read.take_found(),
            None => vec![None; keys.len()],
        };
        let misses: Vec<usize> = (0..keys.len()).filter(|at| out[*at].is_none()).collect();
        let mut admitted = Vec::new();
        for chunk in misses.chunks(READ_CHUNK) {
            let rows = self
                .main_store
                .inner()
                .get_many(
                    chunk
                        .iter()
                        .map(|at| keys[*at].clone().into_bytes())
                        .collect(),
                )
                .await
                .map_err(|error| SchemaError::InvalidData(format!("read records: {error}")))?;
            if rows.len() != chunk.len() {
                return Err(SchemaError::InvalidData(
                    "the record read batch is incomplete".into(),
                ));
            }
            for (at, row) in chunk.iter().zip(rows) {
                let Some(bytes) = row else { continue };
                let record = Arc::new(StoredRecord::decode(&bytes).map_err(|error| {
                    SchemaError::InvalidData(format!("a stored record: {error}"))
                })?);
                if warm.is_some() {
                    admitted.push((*at, Arc::clone(&record), bytes.len()));
                }
                out[*at] = Some(record);
            }
        }
        if let Some(read) = &warm {
            runtime.admit_read_records(read, admitted);
        }
        Ok(out)
    }

    /// Put the changed records, their alternate entry changes and their file
    /// blob counter changes in one batch of the native request. A blob that a
    /// record stops naming loses one reference, and a blob that it starts
    /// naming gains one. `alternates` also carries the field history rows of
    /// the records, and `history_blobs` their blob changes, so each blob
    /// counter changes once for the net delta. The caller holds the native
    /// write gate and read each `before` in the same scope.
    pub(crate) async fn stage_records(
        &self,
        changes: Vec<RecordChange>,
        alternates: Vec<KvMutation>,
        history_blobs: BlobDeltas,
    ) -> Result<(), SchemaError> {
        self.require_write_scope()?;
        let mut blobs = history_blobs;
        let mut batch = Vec::with_capacity(changes.len() + alternates.len());
        let mut written = Vec::with_capacity(changes.len());
        for change in changes {
            if let Some(before) = &change.before {
                blobs.add(&before.blob_refs(), -1);
            }
            blobs.add(&change.after.blob_refs(), 1);
            let value = super::record_cap::record_bytes(&change)?;
            written.push(WrittenRecord::new(
                &change.key,
                Arc::new(change.after),
                &value,
            ));
            batch.push(KvMutation::put(change.key.into_bytes(), value));
        }
        batch.extend(alternates);
        batch.extend(self.blob_counter_mutations(blobs).await?);
        self.main_store.inner().batch_mutate(batch).await?;
        // The values enter the warm set after the flush puts the request on disk.
        self.namespaced_store
            .disk_pack_runtime()
            .stage_written_records(written);
        Ok(())
    }
}
