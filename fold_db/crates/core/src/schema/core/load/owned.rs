//! Every installed schema owns its values. Field identity stays separate.

use crate::schema::types::{Schema, SchemaError};

/// Mark an incoming declaration as owned and check the owned-schema rules.
///
/// Startup admitted every stored schema of this home (see
/// [`admit_stored_schema`]), so the stored copy that this declaration replaces
/// needs no second layout check. A stored copy can lack the per-schema flag
/// when a later release wrote it.
pub(super) fn normalize_owned_declaration(schema: &mut Schema) -> Result<(), SchemaError> {
    schema.owned_values = true;
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)
}

/// Admit one stored schema at startup and mark it owned in memory.
///
/// A home with the home-level owned-layout marker needs no per-schema proof:
/// a serving node wrote the marker only after it admitted every stored schema.
/// A home without the marker still needs the per-schema flag or alternate
/// keys. Such a home without either is from a release with cross-schema value
/// sharing, and startup refuses it.
pub(in crate::schema::core) fn admit_stored_schema(
    schema: &mut Schema,
    home_has_owned_layout_marker: bool,
) -> Result<(), SchemaError> {
    if !home_has_owned_layout_marker && !schema.has_owned_layout() {
        return Err(SchemaError::InvalidData(format!(
            "schema '{}' uses a retired shared-value layout; copy its records into a fresh home",
            schema.name
        )));
    }
    schema
        .validate_owned_schema()
        .map_err(SchemaError::InvalidData)?;
    schema.owned_values = true;
    Ok(())
}
