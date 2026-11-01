//! Every installed schema owns its values. Field identity stays separate.

use crate::schema::types::{Schema, SchemaError};

/// Check the owned-schema rules on an incoming declaration.
///
/// Startup admitted every stored schema of this home (see
/// [`admit_stored_schema`]), so the stored copy that this declaration replaces
/// needs no second check.
pub(super) fn check_owned_declaration(schema: &Schema) -> Result<(), SchemaError> {
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

/// Admit one stored schema at startup with the owned-schema rules.
///
/// This release opens only format 7 homes, and every program that writes
/// format 7 refuses cross-schema value sharing. So the home format is the
/// layout proof, and startup reads no layout marker.
pub(in crate::schema::core) fn admit_stored_schema(schema: &Schema) -> Result<(), SchemaError> {
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}
