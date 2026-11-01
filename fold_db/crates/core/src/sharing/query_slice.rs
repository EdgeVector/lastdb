//! Query-defined portable `lastdb.slice.v1` materialization.
//!
//! Run one or more owner queries and package schemas, field keys, records, and
//! first-class binary blobs into [`LastDbSlicePayload`].
//!
//! File-bearing fields are `$lastdb_file` pointers. The encrypted delivery
//! payload carries raw file bytes and preserves each original CAS address.

use crate::access::AccessContext;
use crate::error::FoldDbError;
use crate::fold_db_core::FoldDB;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::{FieldPredicate, Query, QueryOrderBy};
use crate::sharing::delivery_wire::{
    blob_ref_from_value, content_addressed_blob_with_access, content_addressed_value,
    file_blob_access_from_value, validate_blob_refs, ContentAddressedBlob, ContentAddressedValue,
    FileBlobAccess, LastDbSlicePayload, SignedFieldKey, SliceProvenance, SliceSchema,
    LASTDB_SLICE_PAYLOAD_VERSION,
};
use serde_json::Value;
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

/// Optional resolver for field values that already store only a `$lastdb_file`
/// pointer (bytes live in the local CAS). When materializing from such rows,
/// the resolver supplies the raw bytes so they can be packed into `blobs[]`.
pub type BlobBytesResolver = dyn Fn(&str) -> Result<Option<Vec<u8>>, FoldDbError> + Send + Sync;

/// Build a portable snapshot payload from one or more owner queries.
///
/// Multi-schema slices are the union of legs: each leg is one `Query` (LastDB
/// queries are per-schema today). Field keys use `KeyValue::to_storage_key()` as
/// `record_key` so hash/range keys round-trip losslessly.
///
/// File fields: a `$lastdb_file` pointer has its blob bytes loaded via
/// `blob_resolver`, or from the local CAS with the pointer's access metadata.
pub async fn materialize_query_slice(
    db: &FoldDB,
    legs: &[QuerySliceLeg],
    access: &AccessContext,
    provenance_source: impl Into<String>,
    sender_public_key: impl Into<String>,
) -> Result<LastDbSlicePayload, FoldDbError> {
    materialize_query_slice_with_blob_resolver(
        db,
        legs,
        access,
        provenance_source,
        sender_public_key,
        None,
    )
    .await
}

/// Same as [`materialize_query_slice`] with an explicit blob byte resolver.
pub async fn materialize_query_slice_with_blob_resolver(
    db: &FoldDB,
    legs: &[QuerySliceLeg],
    access: &AccessContext,
    provenance_source: impl Into<String>,
    sender_public_key: impl Into<String>,
    blob_resolver: Option<&BlobBytesResolver>,
) -> Result<LastDbSlicePayload, FoldDbError> {
    if legs.is_empty() {
        return Err(FoldDbError::Other(
            "materialize_query_slice requires at least one query leg".into(),
        ));
    }

    let mut builder = SliceBuilder::default();
    for leg in legs {
        builder.add_leg(db, leg, access, blob_resolver).await?;
    }
    let SliceBuilder {
        schemas,
        field_keys,
        records_by_ref,
        blobs_by_ref,
        ..
    } = builder;

    let created_at = crate::clock::unix_secs();

    let payload = LastDbSlicePayload {
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
        blobs: blobs_by_ref.into_values().collect(),
    };
    validate_blob_refs(&payload)?;
    Ok(payload)
}

mod builder;
mod pages;
use builder::SliceBuilder;

async fn normalize_file_field_value(
    db: &FoldDB,
    value: &Value,
    blob_resolver: Option<&BlobBytesResolver>,
    blobs_by_ref: &mut BTreeMap<String, ContentAddressedBlob>,
) -> Result<Value, FoldDbError> {
    // A pointer — ensure the blob body is packed when we can resolve it.
    if let Some(blob_ref) = blob_ref_from_value(value) {
        let access = file_blob_access_from_value(value)?;
        if !blobs_by_ref.contains_key(blob_ref) {
            let bytes = resolve_blob_bytes(db, blob_ref, access.as_ref(), blob_resolver).await?;
            if let Some(raw) = bytes {
                let name = value
                    .pointer(&format!(
                        "/{}/name",
                        crate::sharing::delivery_wire::LASTDB_FILE_KEY
                    ))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let media_type = value
                    .pointer(&format!(
                        "/{}/media_type",
                        crate::sharing::delivery_wire::LASTDB_FILE_KEY
                    ))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let blob = content_addressed_blob_with_access(&raw, media_type, name, access);
                if blob.blob_ref != blob_ref {
                    return Err(FoldDbError::Other(format!(
                        "resolved bytes hash {} does not match field blob_ref {blob_ref}",
                        blob.blob_ref
                    )));
                }
                blobs_by_ref.insert(blob.blob_ref.clone(), blob);
            } else {
                return Err(FoldDbError::Other(format!(
                    "file field points at blob_ref {blob_ref} but bytes could not be resolved for packaging"
                )));
            }
        }
        return Ok(value.clone());
    }

    Ok(value.clone())
}

/// Resolve the bytes to PACK for a pointer field.
///
/// Every local CAS row is sealed under the file KDK and opens only with the
/// DEK from the pointer's own access metadata. A pointer without that
/// metadata resolves to `None`. Mirrors [`resolve_file_bytes`].
async fn resolve_blob_bytes(
    db: &FoldDB,
    blob_ref: &str,
    access: Option<&FileBlobAccess>,
    blob_resolver: Option<&BlobBytesResolver>,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    if let Some(resolver) = blob_resolver {
        if let Some(bytes) = resolver(blob_ref)? {
            if let Some(access) = access {
                if lastdb_model::hex::sha256_hex(&bytes) != access.file_hash {
                    return Err(FoldDbError::SecurityError(
                        "resolved plaintext differs from the file pointer".into(),
                    ));
                }
            }
            return Ok(Some(bytes));
        }
    }
    match access {
        Some(access) => file_bytes::resolve_local_blob_bytes(db, access).await,
        None => Ok(None),
    }
}

mod file_bytes;
pub use file_bytes::resolve_file_bytes;
