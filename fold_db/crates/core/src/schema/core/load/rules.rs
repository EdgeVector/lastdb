//! Schema refresh rules, independent of storage, locks, and publication.

use crate::schema::types::{Schema, SchemaError};

/// Same-identity loads refresh metadata and retain the schema's owned field keys.
/// No hydrated field key enters the cache.
pub(super) fn prepare_refresh(schema: &mut Schema, mut stored: Schema) -> Result<(), SchemaError> {
    if schema.alternate_keys != stored.alternate_keys {
        return Err(SchemaError::InvalidData(
            "alternate declarations require the explicit alternate-key change command".into(),
        ));
    }
    let mut field_keys = schema.field_key_ids.take().unwrap_or_default();
    if let Some(stored_field_keys) = stored.field_key_ids.take() {
        for (field, uuid) in stored_field_keys {
            field_keys.entry(field).or_insert(uuid);
        }
    }
    schema.field_key_ids = (!field_keys.is_empty()).then_some(field_keys);
    schema.populate_runtime_fields()?;
    schema.sync_field_key_ids();
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
