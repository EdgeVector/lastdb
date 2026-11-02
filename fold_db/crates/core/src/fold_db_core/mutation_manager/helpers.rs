//! Free helpers shared by MutationManager impl modules.

use crate::schema::types::Schema;

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
