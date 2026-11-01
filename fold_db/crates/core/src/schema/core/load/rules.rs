//! Schema refresh rules, independent of storage, locks, and publication.

use crate::schema::types::{Schema, SchemaError};

/// Same-identity loads refresh metadata and retain the schema's owned molecules.
/// No hydrated molecule enters the cache.
pub(super) fn prepare_refresh(schema: &mut Schema, mut stored: Schema) -> Result<(), SchemaError> {
    if schema.alternate_keys != stored.alternate_keys {
        return Err(SchemaError::InvalidData(
            "alternate declarations require the explicit alternate-key change command".into(),
        ));
    }
    let mut molecules = schema.field_molecule_uuids.take().unwrap_or_default();
    if let Some(stored_molecules) = stored.field_molecule_uuids.take() {
        for (field, uuid) in stored_molecules {
            molecules.entry(field).or_insert(uuid);
        }
    }
    schema.field_molecule_uuids = (!molecules.is_empty()).then_some(molecules);
    schema.populate_runtime_fields()?;
    schema.sync_molecule_uuids();
    Ok(())
}

/// A load also sees the stored identity, unlike an update of an incoming schema.
/// Keep a newer stored algorithm even when the incoming copy lacks its identity.
/// The caller reports the preserved version without another storage read.
pub(super) fn preserve_stored_identity(
    schema: &mut Schema,
    stored: Option<&Schema>,
) -> Option<u32> {
    let stored = stored?;
    let version = schema_types::refuses_identity_downgrade(
        stored.identity_hash.as_deref(),
        stored.identity_hash_algo_version,
    )?;
    schema.identity_hash = stored.identity_hash.clone();
    schema.identity_hash_algo_version = stored.identity_hash_algo_version;
    Some(version)
}
