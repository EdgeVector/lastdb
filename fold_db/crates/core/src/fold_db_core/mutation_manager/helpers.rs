//! Free helpers shared by MutationManager impl modules.

use std::collections::{HashMap, HashSet};

use crate::db_operations::ChangedKey;
use crate::schema::types::field::FieldVariant;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::{SchemaCore, SchemaError};
use tracing::warn;

// `build_delete_tombstone_fields` lived here: the per-field tombstone payload
// that repurposed `MutationType::Delete` wrote through the ordinary atom path.
// Delete now hard-erases via the purge path, so nothing in the product writes a
// tombstone any more and the builder had no live caller. The one remaining
// producer is `test_helpers::seed_legacy_tombstones`, which reconstructs the
// same shape so tests can still stand up the population already on disk.

pub(crate) fn normalize_key_value_for_schema_type(
    schema_name: &str,
    schema_type: &DeclarativeSchemaType,
    mutation_uuid: &str,
    key_value: &mut KeyValue,
) {
    match schema_type {
        DeclarativeSchemaType::Hash => {
            if key_value.range.is_some() {
                warn!(
                    schema = %schema_name,
                    mutation = %mutation_uuid,
                    "Ignoring range key supplied for hash-only schema mutation"
                );
                key_value.range = None;
            }
        }
        DeclarativeSchemaType::Range => {
            if key_value.hash.is_some() {
                warn!(
                    schema = %schema_name,
                    mutation = %mutation_uuid,
                    "Ignoring hash key supplied for range-only schema mutation"
                );
                key_value.hash = None;
            }
        }
        DeclarativeSchemaType::HashRange | DeclarativeSchemaType::Single => {}
    }
}

pub(crate) fn current_atom_uuid(field: &FieldVariant, key_value: &KeyValue) -> Option<String> {
    field.current_atom_uuid(key_value)
}

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

/// Fields eligible for Search-app outbox delivery for this schema.
///
/// Returns `None` when the schema has no field classifications (caller treats
/// that as "no allowlist filter"). Otherwise returns the set of fields tagged
/// `word` that are not excluded by [`is_search_outbox_excluded`].
pub(crate) fn searchable_outbox_fields(schema: &Schema) -> Option<HashSet<String>> {
    if schema.field_classifications.is_empty() {
        return None;
    }

    Some(
        schema
            .field_classifications
            .iter()
            .filter(|(_, classifications)| !is_search_outbox_excluded(classifications))
            .filter(|(_, classifications)| {
                classifications
                    .iter()
                    .any(|classification| classification.eq_ignore_ascii_case("word"))
            })
            .map(|(field_name, _)| field_name.clone())
            .collect(),
    )
}

/// True when field classifications ban Search-app outbox delivery
/// (`secret`, `no_index`, or `no-index`).
pub(crate) fn is_search_outbox_excluded(classifications: &[String]) -> bool {
    classifications.iter().any(|classification| {
        classification.eq_ignore_ascii_case("secret")
            || classification.eq_ignore_ascii_case("no_index")
            || classification.eq_ignore_ascii_case("no-index")
    })
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
pub(crate) fn validate_derived_provenance(
    mutation: &Mutation,
    _schema_manager: &SchemaCore,
) -> Result<(), SchemaError> {
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
