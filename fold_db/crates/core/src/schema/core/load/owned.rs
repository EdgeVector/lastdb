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

/// Admit one stored schema at startup.
///
/// This release opens only format 7 homes. Every program that writes format 7
/// was built after the removal of cross-schema value sharing, so the format
/// proves the owned layout and no per-schema or home-level marker is read.
/// Each stored schema still gets the owned-schema rules.
pub(in crate::schema::core) fn admit_stored_schema(schema: &Schema) -> Result<(), SchemaError> {
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}
