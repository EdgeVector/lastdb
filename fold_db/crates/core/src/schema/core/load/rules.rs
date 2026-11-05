//! Schema refresh rules, independent of storage, locks, and publication.

use crate::schema::types::{Schema, SchemaError};

/// Same-identity loads refresh metadata. Key declarations stay immutable.
pub(super) fn prepare_refresh(schema: &mut Schema, stored: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys != stored.record_keys || schema.alternate_keys != stored.alternate_keys {
        return Err(SchemaError::InvalidData(
            "explicit primary/alternate key declarations are immutable; declare a new schema and copy explicitly"
                .into(),
        ));
    }
    schema.populate_runtime_fields()?;
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
