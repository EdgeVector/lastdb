//! Free helpers shared by MutationManager impl modules.

use std::collections::{HashMap, HashSet};

use crate::db_operations::ChangedKey;
use crate::schema::types::{Mutation, Schema};
use crate::schema::SchemaError;

/// Stamp every runtime field on `schema` with the request's storage prefix so
/// restore / CAS / query / persist read and write the same org DB keyspace.
///
/// `None` leaves fields untouched (personal home — schemas default to no prefix).
///
/// Request-scoped only: call [`clear_storage_prefix_on_schema`] before
/// `store_schema` / `load_schema_internal` so the durable schema registry never
/// permanently inherits an org DB prefix.
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

/// Drop request-scoped storage prefixes before persisting schema metadata.
pub(crate) fn clear_storage_prefix_on_schema(schema: &mut Schema) {
    for field in schema.runtime_fields.values_mut() {
        if field.common().storage_prefix().is_some() {
            field.common_mut().set_storage_prefix(None);
        }
    }
}

/// Per modified field, the set of keys touched in a write batch — the unit the
/// O(changed) persist path uses to write only the changed `mk:` records. A
/// field mapped to an empty set is `Single` (no per-key shape; whole-blob
/// `ref:` rewrite).
pub(crate) type ModifiedFieldKeys = HashMap<String, HashSet<ChangedKey>>;

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
