//! Records of a home: one stored value at each primary key.
//!
//! This file reads whole records by key and supplies each record transition
//! to the format writer in the native request.
//! Its field history entries hold copies of old values (`field_history/records.rs`).
//!
//! A read by record key checks the record warm set first (decision D2). A
//! record that the read finds on disk, or that a flushed write puts, enters
//! it.

use super::RecordStore;
use crate::schema::types::{KeyValue, Schema};
use crate::schema::SchemaError;
use crate::storage::format::history::HistoryChange;
use crate::storage::format::records::{self as format, RecordAddress};
use futures::FutureExt;
use lastdb_model::portable_mutation::{Position, Stamp};
use lastdb_model::record_space::record_space_uuid;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

/// The native get-many API accepts at most this many keys per call.
const READ_CHUNK: usize = 10_000;

pub(crate) use format::RecordChange;

impl RecordStore {
    pub(crate) async fn stage_completed_file_references(
        &self,
        files: Vec<(RecordAddress, Position)>,
        through: u64,
    ) -> Result<(), SchemaError> {
        self.format
            .stage_completed_file_references(files, through)
            .await
    }

    pub(crate) async fn file_retention_complete(&self) -> Result<bool, SchemaError> {
        self.format.file_retention_complete().await
    }

    pub(crate) async fn stage_file_retention_step(&self) -> Result<bool, SchemaError> {
        self.format.stage_file_retention_step().await
    }

    pub(crate) async fn stage_delete_reclamation(
        &self,
        cutoff_ms: u64,
    ) -> Result<format::ReclaimedDeletes, SchemaError> {
        Ok(self.format.stage_delete_reclamation(cutoff_ms).await?)
    }

    pub(crate) async fn current_delete_ages(
        &self,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<format::DeleteAge>, SchemaError> {
        Ok(self.format.current_delete_ages(rows).await?)
    }

    pub(crate) async fn read_applied_deletes(
        &self,
        keys: &[&RecordAddress],
    ) -> Result<Vec<Option<Stamp>>, SchemaError> {
        Ok(self.format.applied_deletes(keys).await?)
    }

    /// The storage key of the record at one canonical primary key. The record
    /// space codec of the schema blinds the hash and types the range.
    pub(crate) fn record_key(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<String, SchemaError> {
        self.record_address(schema, key)
            .map(RecordAddress::into_string)
    }

    /// The write address, minted by the format rather than a raw key string.
    pub(crate) fn record_address(
        &self,
        schema: &Schema,
        key: &KeyValue,
    ) -> Result<RecordAddress, SchemaError> {
        let space = record_space_uuid(&schema.name);
        RecordAddress::new(&self.key_codec_for_field_key(&space)?, &space, key)
    }

    /// The stored records at these storage keys, in the order given. Memory
    /// answers first; one read by key for each chunk of the misses. Inside a
    /// native request the read sees the request's earlier private writes.
    ///
    /// The memory lookup and every disk chunk read one revision: a caller
    /// without a read scope gets one for this call (`record_read_scope`), so a
    /// commit between two chunks cannot mix two revisions in one result. A
    /// caller with a scope keeps its own revision and read limit.
    pub(crate) async fn read_records<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        self.record_read_scope(self.read_records_scoped(keys).boxed())
            .await
    }

    async fn read_records_scoped<K: AsRef<str> + Sync>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Arc<StoredRecord>>>, SchemaError> {
        let runtime = self.runtime.clone();
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
                        .map(|at| keys[*at].as_ref().as_bytes().to_vec())
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

    /// Stage record, pointer, file and history changes in the native request.
    /// The caller read each preimage under the same write gate.
    pub(crate) async fn stage_records(
        &self,
        schema: &Schema,
        changes: Vec<RecordChange>,
        history: Vec<HistoryChange>,
    ) -> Result<(), SchemaError> {
        self.format
            .stage_records(schema, changes, history, |id| {
                self.key_codec_for_field_key(id)
            })
            .await
    }
}
