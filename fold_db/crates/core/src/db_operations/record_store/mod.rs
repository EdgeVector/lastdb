//! Record domain store.
//!
//! Supplies record values and read coordination to the format-owned main/file
//! writer. It has no raw store or namespace factory. External callers use
//! `DbOperations::records()` to reach these operations.
//!
//! Layout:
//! - [`construct`] — `RecordStore` constructors and builder hooks
//! - [`records`], [`record_reads`] — record writes and keyed reads
//! - [`schema_release`] — the release of the rows of a dropped schema
//! - [`record_files`] — the one file of a record, at a path made from its key
//! - [`read_scope`] — one memory and disk boundary for a read
//! - [`locks`] — write scopes and field key commit locks
//! - [`key_format`] — the key format marker of a new empty home
//! - [`erase_meters`] — the erase audit row

mod bounded_range;
mod construct;
mod delete_replay;
mod erase_meters;
mod key_comparison;
mod locks;
mod read_scope;
pub(crate) mod record_files;
mod record_reads;
mod record_spans;
mod records;
mod schema_release;
mod writer_watch;

pub use bounded_range::WalkStats;
pub(crate) use bounded_range::{RangeWalk, WalkTotals};
pub(crate) use record_reads::KeyedRecord;
pub(crate) use record_spans::{RecordBound, RecordSpan, SpanKey};
pub(crate) use records::RecordChange;
pub use writer_watch::{WriterWatch, DEFAULT_QUIET_SECS, QUIET_SECS_ENV};

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
    /// Bounded range reads made by the admin walks since this process started.
    range_walks: Arc<WalkTotals>,
}

impl RecordStore {
    /// Totals of the bounded range reads of the admin walks.
    pub(crate) fn range_walks(&self) -> &WalkTotals {
        &self.range_walks
    }

    pub(crate) fn primary_codec(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<lastdb_model::field_key_codec::FieldKeyCodec, crate::schema::SchemaError> {
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(crate::schema::SchemaError::InvalidData)?;
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
    ) -> Result<lastdb_model::field_key_codec::FieldKeyCodec, crate::schema::SchemaError> {
        let profile = schema
            .require_record_keys()?
            .alternates
            .get(name)
            .ok_or_else(|| {
                crate::schema::SchemaError::InvalidData("alternate key is not declared".into())
            })?
            .range_profile()
            .map_err(crate::schema::SchemaError::InvalidData)?;
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
