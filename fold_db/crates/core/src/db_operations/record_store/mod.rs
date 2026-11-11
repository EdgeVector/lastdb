//! Record domain store.
//!
//! Supplies record values and read coordination to the format-owned main/file
//! writer. It has no raw store or namespace factory. External callers use
//! `DbOperations::records()` to reach these operations.

mod bounded_range;
mod delete_replay;
mod erase_meters;
mod locks;
mod read_scope;
pub(crate) mod record_files;
mod record_reads;
mod record_spans;
mod records;
mod schema_release;
mod writer_watch;

pub(crate) use bounded_range::RangeWalk;
pub(crate) use record_reads::KeyedRecord;
pub(crate) use record_spans::{RecordBound, RecordSpan, SpanKey};
pub(crate) use records::RecordChange;
pub use writer_watch::{WriterWatch, DEFAULT_QUIET_SECS, QUIET_SECS_ENV};

use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::SchemaError;
use crate::storage::disk_pack::PackRuntime;
use crate::storage::format::{read::ReadStore, records::RecordFormat};
use std::sync::Arc;

/// Record store for a home. One record at each primary key.
///
/// Backed by the `main` namespace.
#[derive(Clone)]
pub struct RecordStore {
    main_store: ReadStore,
    format: Arc<RecordFormat>,
    runtime: Arc<PackRuntime>,
    /// Validated once before serving. Only stopped-home maintenance can move
    /// this floor; replay cannot write its two metadata records.
    serving_replay_cutoff: Arc<std::sync::OnceLock<u64>>,
    node_key: [u8; 32],
}

impl RecordStore {
    pub(crate) fn new(format: RecordFormat, node_key: [u8; 32]) -> Self {
        Self {
            main_store: format.reader(),
            runtime: format.runtime(),
            format: Arc::new(format),
            serving_replay_cutoff: Arc::default(),
            node_key,
        }
    }

    /// Capture canonical key order for `schema`.
    pub fn key_comparison_context(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<Arc<KeyComparisonContext>, SchemaError> {
        Ok(Arc::new(KeyComparisonContext::for_schema(schema)?))
    }

    pub(crate) fn primary_codec(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<lastdb_model::field_key_codec::FieldKeyCodec, SchemaError> {
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let space = lastdb_model::record_space::record_space_uuid(&schema.name);
        Ok(lastdb_model::field_key_codec::FieldKeyCodec::from_node_key(
            &self.node_key,
            &space,
            profile,
        ))
    }

    pub(crate) fn alternate_codec(
        &self,
        schema: &crate::schema::Schema,
        name: &str,
    ) -> Result<lastdb_model::field_key_codec::FieldKeyCodec, SchemaError> {
        let profile = schema
            .require_record_keys()?
            .alternates
            .get(name)
            .ok_or_else(|| SchemaError::InvalidData("alternate key is not declared".into()))?
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let id = lastdb_model::field_key_id::alternate_field_key_id(schema, name);
        Ok(lastdb_model::field_key_codec::FieldKeyCodec::from_node_key(
            &self.node_key,
            &id,
            profile,
        ))
    }

    /// A read-only view with no raw-handle extraction or write methods.
    pub(crate) fn reader(&self) -> &ReadStore {
        &self.main_store
    }

    /// Flush all pending writes in the record namespace to durable storage.
    pub async fn flush(&self) -> Result<(), crate::storage::StorageError> {
        self.format.flush().await
    }
}
