//! Free helpers shared by MutationManager impl modules.

use crate::schema::types::{Mutation, Schema};
use crate::schema::SchemaError;

/// Stamp every runtime field on `schema` with the request's storage prefix so
/// restore / CAS / query / persist read and write the same org DB keyspace.
///
/// `None` leaves fields untouched (personal home — schemas default to no prefix).
///
/// Request-scoped only: stamp a clone, never the schema that the durable
/// schema registry stores.
pub(crate) fn apply_storage_prefix_to_schema(schema: &mut Schema, storage_prefix: Option<&str>) {
    let Some(prefix) = storage_prefix else {
        return;
    };
    for field in schema.runtime_fields.values_mut() {
        field
            .common_mut()
            .set_storage_prefix(Some(prefix.to_string()));
    }
}

/// Reject any mutation carrying `Provenance::Derived`.
///
/// Derived provenance is a historical wire shape from the deleted computed
/// write path. No live writer can legitimately produce a derived mutation
/// anymore, so any that arrives is forged or stale and is rejected before it
/// pollutes atoms.
/// Previously-stored molecules with derived provenance remain readable —
/// this guards the WRITE path only. Mutations without `Provenance::Derived`
/// (user writes with `Provenance::User { .. }` or `None`) are unchecked.
pub(crate) fn validate_derived_provenance(mutation: &Mutation) -> Result<(), SchemaError> {
    use crate::atom::provenance::Provenance;
    match &mutation.provenance {
        Some(Provenance::Derived { .. }) => Err(SchemaError::InvalidData(format!(
            "Derived mutation targets schema '{}' but derived provenance is \
             no longer accepted on writes",
            mutation.schema_name
        ))),
        _ => Ok(()),
    }
}
