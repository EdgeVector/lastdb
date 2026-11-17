//! Portable definition id: the value a listing signs and
//! `GET /api/schemas?include_definition_ids=true` reports.
//!
//! The node computes it in `log_sync::schema_id` (`lastdb_node/src/log_sync/mod.rs`):
//! the catalog `identity_hash`, then `PortableSchemaId::from_catalog` on the
//! stored schema's wire form (`Schema::to_wire`). `from_catalog`
//! (`schema_service/crates/schema_types/src/range_profile.rs`) recomputes the
//! catalog hash and fills any missing field hash before the canonical hash.
//! A normalized manifest entry and a Schema Service definition go through
//! that same function here, without the node.
//!
//! Declare clears field hashes and recomputes them before it stores the schema
//! (`load_owned` in `exec/schema_declare/mod.rs`). `from_catalog` keeps a hash
//! the definition already carries. A manifest that supplies a field hash other
//! than the recomputed one does not match the id the node reports after
//! declare. Published manifests do not supply field hashes. The node fills
//! them, and so does `from_catalog`.

use fold_db::schema::types::DeclarativeSchemaDefinition;

pub(super) fn portable_id(schema: &DeclarativeSchemaDefinition) -> Result<String, String> {
    let catalog = schema
        .identity_hash
        .as_deref()
        .ok_or_else(|| "definition has no catalog identity".to_owned())?;
    let portable = schema_types::PortableSchemaId::from_catalog(catalog, schema.to_wire())?;
    Ok(portable.as_str().to_owned())
}
