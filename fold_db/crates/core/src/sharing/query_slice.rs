//! Query-defined portable `lastdb.slice.v1` materialization.
//!
//! Run one or more owner queries and package schemas, field keys and records
//! into [`LastDbSlicePayload`].
//!
//! A record's file is not a field value, so a slice carries no file. A
//! receiver that needs the bytes asks for them and writes them with `put-blob`
//! under its own record.

use crate::error::FoldDbError;
use crate::fold_db_core::FoldDB;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::{FieldPredicate, Query, QueryOrderBy};
use crate::sharing::delivery_wire::{
    content_addressed_value, ContentAddressedValue, LastDbSlicePayload, SliceFieldKey,
    SliceProvenance, SliceSchema, LASTDB_SLICE_PAYLOAD_VERSION,
};
use std::collections::BTreeMap;

/// One schema leg of a (possibly multi-schema) query-defined slice.
#[derive(Debug, Clone)]
pub struct QuerySliceLeg {
    pub schema_name: String,
    pub fields: Vec<String>,
    pub filter: Option<HashRangeFilter>,
    pub field_predicates: Option<Vec<FieldPredicate>>,
    pub order_by: Option<QueryOrderBy>,
    pub predicate_limit: Option<usize>,
}

impl QuerySliceLeg {
    #[must_use]
    pub fn to_query(&self) -> Query {
        let mut query = Query::new(self.schema_name.clone(), self.fields.clone());
        query.filter = self.filter.clone();
        query.field_predicates = self.field_predicates.clone();
        query.order_by = self.order_by.clone();
        query.predicate_limit = self.predicate_limit;
        query
    }
}

/// Build a portable snapshot payload from one or more owner queries.
///
/// Multi-schema slices are the union of legs: each leg is one `Query` (LastDB
/// queries are per-schema today). Field keys use `KeyValue::to_storage_key()` as
/// `record_key` so hash/range keys round-trip losslessly.
pub async fn materialize_query_slice(
    db: &FoldDB,
    legs: &[QuerySliceLeg],
    provenance_source: impl Into<String>,
    sender_public_key: impl Into<String>,
) -> Result<LastDbSlicePayload, FoldDbError> {
    if legs.is_empty() {
        return Err(FoldDbError::Other(
            "materialize_query_slice requires at least one query leg".into(),
        ));
    }

    let mut builder = SliceBuilder::default();
    for leg in legs {
        builder.add_leg(db, leg).await?;
    }
    let SliceBuilder {
        schemas,
        field_keys,
        records_by_ref,
        ..
    } = builder;

    let created_at = crate::clock::unix_secs();

    Ok(LastDbSlicePayload {
        version: LASTDB_SLICE_PAYLOAD_VERSION.to_string(),
        provenance: SliceProvenance {
            source: provenance_source.into(),
            mode: "snapshot".to_string(),
            created_at,
            sender_public_key: sender_public_key.into(),
        },
        schemas,
        field_keys,
        records: records_by_ref.into_values().collect(),
    })
}

mod builder;
mod pages;
use builder::SliceBuilder;
