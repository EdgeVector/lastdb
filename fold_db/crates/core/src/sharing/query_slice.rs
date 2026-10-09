//! Query-defined portable `lastdb.slice.v1` materialization + mutation import.
//!
//! Run one or more owner queries, package schemas / molecules / atoms / **first-class
//! binary blobs** into [`LastDbSlicePayload`], seal with delivery wire helpers, then
//! import on another node by replaying Create mutations and writing blobs into the
//! local CAS tree.
//!
//! File-bearing fields are normalized to `$lastdb_file` pointers; raw file bytes
//! live only in `payload.blobs[]` (content-addressed over raw bytes).

use crate::access::AccessContext;
use crate::error::FoldDbError;
use crate::fold_db_core::FoldDB;
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::operations::{FieldPredicate, MutationType, Query, QueryOrderBy};
use crate::schema::types::{KeyValue, Mutation};
use crate::schema::SchemaState;
use crate::sharing::blob_cas;
use crate::sharing::delivery_wire::{
    blob_ref_from_atom_value, content_addressed_atom, content_addressed_blob,
    content_addressed_blob_with_access, extract_embedded_file, file_blob_access_from_atom_value,
    lastdb_file_pointer, validate_blob_refs, ContentAddressedAtom, ContentAddressedBlob,
    FileBlobAccess, LastDbSlicePayload, SignedMolecule, SliceProvenance, SliceSchema,
    LASTDB_SLICE_PAYLOAD_VERSION,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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
/// queries are per-schema today). Molecules use `KeyValue::to_storage_key()` as
/// `record_key` so hash/range keys round-trip losslessly.
///
/// File fields:
/// - legacy `$file` with `content_b64` → extracted into `blobs[]`, atom rewritten
///   to `$lastdb_file` pointer
/// - `$lastdb_file` pointer → blob bytes loaded via `blob_resolver` (or local CAS
///   when `None` and a pool is available)
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

    let mut schemas: Vec<SliceSchema> = Vec::new();
    let mut molecules: Vec<SignedMolecule> = Vec::new();
    let mut atoms_by_ref: BTreeMap<String, ContentAddressedAtom> = BTreeMap::new();
    let mut blobs_by_ref: BTreeMap<String, ContentAddressedBlob> = BTreeMap::new();
    let mut seen_schema: BTreeSet<String> = BTreeSet::new();

    for leg in legs {
        if leg.fields.is_empty() {
            return Err(FoldDbError::Other(format!(
                "query leg for schema '{}' has no fields",
                leg.schema_name
            )));
        }

        let schema = db
            .schema_manager()
            .get_schema_following_supersession(&leg.schema_name)
            .await?
            .ok_or_else(|| {
                FoldDbError::Schema(crate::schema::types::SchemaError::InvalidData(format!(
                    "schema '{}' not found",
                    leg.schema_name
                )))
            })?;

        if seen_schema.insert(leg.schema_name.clone()) {
            let definition = serde_json::to_value(&schema).map_err(FoldDbError::from)?;
            schemas.push(SliceSchema {
                schema_name: leg.schema_name.clone(),
                definition,
                fields: leg.fields.clone(),
            });
        }

        let query = leg.to_query();
        let result = db.query_executor().query_with_access(query, access).await?;

        let mut keys: HashSet<KeyValue> = HashSet::new();
        for field_map in result.values() {
            for key in field_map.keys() {
                keys.insert(key.clone());
            }
        }

        for key in keys {
            let record_key = key.to_storage_key();
            for field_name in &leg.fields {
                let Some(field_map) = result.get(field_name) else {
                    continue;
                };
                let Some(fv) = field_map.get(&key) else {
                    continue;
                };
                let atom_value =
                    normalize_file_field_value(db, &fv.value, blob_resolver, &mut blobs_by_ref)
                        .await?;
                let atom = content_addressed_atom(atom_value)?;
                molecules.push(SignedMolecule {
                    schema_name: leg.schema_name.clone(),
                    record_key: record_key.clone(),
                    field_name: field_name.clone(),
                    atom_ref: atom.atom_ref.clone(),
                    molecule_uuid: fv.molecule_uuid.clone(),
                    molecule_version: fv.molecule_version,
                    writer_pubkey: fv.writer_pubkey.clone(),
                    signature: None,
                });
                atoms_by_ref.entry(atom.atom_ref.clone()).or_insert(atom);
            }
        }
    }

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
        molecules,
        atoms: atoms_by_ref.into_values().collect(),
        blobs: blobs_by_ref.into_values().collect(),
    };
    validate_blob_refs(&payload)?;
    Ok(payload)
}

async fn normalize_file_field_value(
    db: &FoldDB,
    value: &Value,
    blob_resolver: Option<&BlobBytesResolver>,
    blobs_by_ref: &mut BTreeMap<String, ContentAddressedBlob>,
) -> Result<Value, FoldDbError> {
    // Legacy embedded file → first-class blob + pointer atom.
    if let Some(extracted) = extract_embedded_file(value)? {
        let blob = content_addressed_blob(
            &extracted.bytes,
            extracted.media_type.clone(),
            extracted.name.clone(),
        );
        let pointer = lastdb_file_pointer(
            &blob.blob_ref,
            extracted.name.as_deref(),
            extracted.media_type.as_deref(),
        );
        blobs_by_ref.entry(blob.blob_ref.clone()).or_insert(blob);
        return Ok(pointer);
    }

    // Already a pointer — ensure the blob body is packed when we can resolve it.
    if let Some(blob_ref) = blob_ref_from_atom_value(value) {
        let access = file_blob_access_from_atom_value(value)?;
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
/// Operation Trinity: a local CAS row sealed under the file KDK only opens with
/// the DEK from the pointer's own access metadata, so packaging must be handed
/// that metadata. Without it this fell to the plain read, which fails closed on
/// a sealed row — and every Trinity-sealed blob is exactly that. The caller has
/// the `access` in scope already (it stamps it onto the packed blob), so the
/// only thing missing was passing it through. Mirrors [`resolve_file_bytes`].
async fn resolve_blob_bytes(
    db: &FoldDB,
    blob_ref: &str,
    access: Option<&FileBlobAccess>,
    blob_resolver: Option<&BlobBytesResolver>,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    if let Some(resolver) = blob_resolver {
        if let Some(bytes) = resolver(blob_ref)? {
            return Ok(Some(bytes));
        }
    }
    if let Some(access) = access {
        if !access.dek.is_empty() {
            return blob_cas::get_blob_bytes_with_dek_in_ops(db.db_ops(), blob_ref, &access.dek)
                .await;
        }
    }
    blob_cas::get_blob_bytes_in_ops(db.db_ops(), blob_ref).await
}

/// Import a portable slice into `db`: load schemas, write Create mutations for
/// each record, and persist first-class blobs into the local CAS tree.
///
/// File fields remain `$lastdb_file` pointers; apps load bytes via
/// [`blob_cas::get_blob_bytes`] (or [`resolve_file_bytes`]).
pub async fn import_slice_via_mutations(
    db: &FoldDB,
    payload: &LastDbSlicePayload,
    writer_pubkey: impl Into<String>,
) -> Result<usize, FoldDbError> {
    if payload.version != LASTDB_SLICE_PAYLOAD_VERSION {
        return Err(FoldDbError::Other(format!(
            "unsupported slice version '{}'; expected '{}'",
            payload.version, LASTDB_SLICE_PAYLOAD_VERSION
        )));
    }
    validate_blob_refs(payload)?;

    let writer_pubkey = writer_pubkey.into();

    blob_cas::put_blobs_in_ops(db.db_ops(), &payload.blobs).await?;

    let atoms: HashMap<&str, &Value> = payload
        .atoms
        .iter()
        .map(|a| (a.atom_ref.as_str(), &a.value))
        .collect();

    for schema in &payload.schemas {
        let json = serde_json::to_string(&schema.definition).map_err(FoldDbError::from)?;
        let exists = db
            .schema_manager()
            .get_schema_following_supersession(&schema.schema_name)
            .await?
            .is_some();
        if !exists {
            db.load_schema_from_json(&json).await?;
            db.schema_manager()
                .set_schema_state(&schema.schema_name, SchemaState::Available)
                .await?;
        }
    }

    let mut records: BTreeMap<(String, String), BTreeMap<String, Value>> = BTreeMap::new();
    for mol in &payload.molecules {
        let value = atoms.get(mol.atom_ref.as_str()).ok_or_else(|| {
            FoldDbError::Other(format!(
                "molecule {}.{} references missing atom {}",
                mol.schema_name, mol.field_name, mol.atom_ref
            ))
        })?;
        records
            .entry((mol.schema_name.clone(), mol.record_key.clone()))
            .or_default()
            .insert(mol.field_name.clone(), (*value).clone());
    }

    let mut written = 0usize;
    let mut mutations = Vec::new();
    for ((schema_name, record_key), fields) in records {
        let key = KeyValue::from_storage_key(&record_key);
        mutations.push(Mutation::new(
            schema_name,
            fields.into_iter().collect(),
            key,
            writer_pubkey.clone(),
            MutationType::Create,
        ));
        written += 1;
    }

    if !mutations.is_empty() {
        db.mutation_manager()
            .write_mutations_batch_async(mutations, None)
            .await?;
    }

    Ok(written)
}

/// Resolve raw file bytes for a field value that is a `$lastdb_file` pointer.
///
/// Operation Trinity: local CAS rows sealed under the file KDK open with the
/// DEK from pointer access metadata (never stored in plain on the CAS row).
pub async fn resolve_file_bytes(
    db: &FoldDB,
    field_value: &Value,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    let Some(blob_ref) = blob_ref_from_atom_value(field_value) else {
        // Try legacy embedded form.
        return Ok(extract_embedded_file(field_value)?.map(|f| f.bytes));
    };
    if let Some(access) = file_blob_access_from_atom_value(field_value)? {
        if !access.dek.is_empty() {
            return blob_cas::get_blob_bytes_with_dek_in_ops(db.db_ops(), blob_ref, &access.dek)
                .await;
        }
    }
    blob_cas::get_blob_bytes_in_ops(db.db_ops(), blob_ref).await
}

/// Resolve raw file bytes for a `$lastdb_file` pointer, fetching exactly that
/// remote CAS object when the local CAS cache misses and the pointer carries
/// file-blob access metadata.
#[cfg(feature = "cloud-sync")]
pub async fn resolve_file_bytes_on_demand(
    db: &FoldDB,
    sync_engine: &crate::sync::SyncEngine,
    field_value: &Value,
) -> Result<Option<Vec<u8>>, FoldDbError> {
    if let Some(bytes) = resolve_file_bytes(db, field_value).await? {
        return Ok(Some(bytes));
    }

    let Some(access) = file_blob_access_from_atom_value(field_value)? else {
        return Ok(None);
    };
    let Some(blob_ref) = blob_ref_from_atom_value(field_value) else {
        return Ok(None);
    };

    let remote_ref = crate::sync::engine::FileBlobRef {
        blob_ref: access.blob_ref.clone(),
        file_hash: access.file_hash.clone(),
        owner_scope: access.owner_scope.clone(),
        cipher_suite: access.cipher_suite.clone(),
        dek: access.dek.clone(),
        encrypted_size_bytes: access.encrypted_size_bytes.unwrap_or_default(),
    };
    let Some(bytes) = sync_engine
        .download_file_blob(&remote_ref)
        .await
        .map_err(|e| {
            FoldDbError::Other(format!(
                "on-demand file blob fetch failed for {}: {e}",
                access.file_hash
            ))
        })?
    else {
        return Ok(None);
    };

    let name = field_value
        .pointer(&format!(
            "/{}/name",
            crate::sharing::delivery_wire::LASTDB_FILE_KEY
        ))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let media_type = field_value
        .pointer(&format!(
            "/{}/media_type",
            crate::sharing::delivery_wire::LASTDB_FILE_KEY
        ))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let blob = content_addressed_blob_with_access(&bytes, media_type, name, Some(access));
    if blob.blob_ref != blob_ref {
        return Err(FoldDbError::SecurityError(format!(
            "on-demand file blob hash {} does not match pointer {blob_ref}",
            blob.blob_ref
        )));
    }
    blob_cas::put_blob_in_ops(db.db_ops(), &blob).await?;

    let hash_prefix = remote_ref.file_hash.chars().take(12).collect::<String>();
    tracing::info!(
        target: "fold_db::sharing",
        file_hash_prefix = %hash_prefix,
        blob_ref = %remote_ref.blob_ref,
        "fetched file blob on demand"
    );

    Ok(Some(bytes))
}
