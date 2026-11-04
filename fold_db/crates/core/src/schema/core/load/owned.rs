//! Every installed schema owns its values. Field identity stays separate.

use crate::schema::types::{Schema, SchemaError};

/// Check the owned-schema rules on an incoming declaration.
///
/// Startup admitted every stored schema of this home (see
/// [`admit_stored_schema`]), so the stored copy that this declaration replaces
/// needs no second check.
pub(super) fn check_owned_declaration(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "this home requires explicit record_keys; field-derived keys are unsupported".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

/// Admit one stored schema at startup with the owned-schema rules.
///
/// This release opens only its own home format, which never shares values
/// across schemas. So the home format is the layout proof, and startup reads
/// no layout marker.
pub(in crate::schema::core) fn admit_stored_schema(schema: &Schema) -> Result<(), SchemaError> {
    if schema.record_keys.is_none() {
        return Err(SchemaError::InvalidData(
            "stored schema has no explicit record_keys".into(),
        ));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}
