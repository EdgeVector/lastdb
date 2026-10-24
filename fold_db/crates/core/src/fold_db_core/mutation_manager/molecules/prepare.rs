//! Mutation atom preparation and changed-key analysis.

use std::collections::{HashMap, HashSet};

use super::super::MutationManager;
use crate::atom::Atom;
use crate::db_operations::ChangedKey;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use tracing::warn;

mod key_fields;

/// One owned field atom from preparation through physical staging.
/// Keep the original sealing identity even if the runtime schema is rebased.
pub(in crate::fold_db_core::mutation_manager) struct PreparedFieldAtom {
    pub mutation_index: usize,
    pub field_name: String,
    pub atom: Atom,
    pub seal_molecule: String,
}

impl PreparedFieldAtom {
    pub(in crate::fold_db_core::mutation_manager) fn into_located(self) -> (Atom, Option<String>) {
        (self.atom, Some(self.seal_molecule))
    }
}

type PreparedAtomsAndKeys = (Vec<KeyValue>, Vec<PreparedFieldAtom>);

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn resolve_mutation_key_value(
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        crate::schema::require_logical_layout(&schema.schema_type)?;
        // Prefer the pre-computed key_value from the mutation (set by the
        // ingestion service with date normalization and proper field extraction).
        // Fall back to KeyValue::from_mutation() only when both fields are None.
        let mut key_value =
            if mutation.key_value.hash.is_some() || mutation.key_value.range.is_some() {
                mutation.key_value.clone()
            } else {
                let key_config = schema.key.clone();
                KeyValue::from_mutation(
                    &mutation.fields_and_values,
                    key_config.as_ref().ok_or_else(|| {
                        SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' has no key configuration. Cannot execute mutation."
                    ))
                    })?,
                )
            };

        if schema.schema_type == DeclarativeSchemaType::Hash && key_value.range.is_some() {
            warn!(
                schema = %schema_name,
                mutation = %mutation.uuid,
                "Ignoring range key supplied for hash-only schema mutation"
            );
            key_value.range = None;
        }

        // Validate that the key matches the schema type.
        // A mismatch means a bug upstream — fail loudly.
        match &schema.schema_type {
            DeclarativeSchemaType::Hash if key_value.hash.is_none() => Err(
                SchemaError::InvalidData(format!(
                    "Hash schema '{}' mutation {} has no hash key",
                    schema_name, mutation.uuid
                )),
            ),
            DeclarativeSchemaType::HashRange
                if key_value.hash.is_none() || key_value.range.is_none() =>
            {
                Err(SchemaError::InvalidData(format!(
                    "HashRange schema '{}' mutation {} requires both hash and range keys, got hash={:?} range={:?}",
                    schema_name, mutation.uuid, key_value.hash, key_value.range
                )))
            }
            _ => Ok(key_value),
        }
    }

    /// Creates atoms and computes key values for all mutations in a schema group.
    /// Returns key values and one owned record per field atom.
    ///
    /// This phase performs no atom write and no resident apply. The caller
    /// applies personal resident atoms under the slot gates and gives every
    /// canonical atom body to the reserved schema-lane envelope.
    pub(in crate::fold_db_core::mutation_manager) fn prepare_atoms_and_key_values(
        &self,
        schema_name: &str,
        schema: &Schema,
        schema_mutations: &mut [Mutation],
    ) -> Result<PreparedAtomsAndKeys, SchemaError> {
        let mut atom_results: Vec<PreparedFieldAtom> = Vec::new();
        let mut mutation_key_values = Vec::with_capacity(schema_mutations.len());

        for (idx, mutation) in schema_mutations.iter_mut().enumerate() {
            let key_value = Self::resolve_mutation_key_value(schema_name, schema, mutation)?;

            Self::materialize_key_fields(schema_name, schema, mutation, &key_value)?;

            mutation_key_values.push(key_value);

            // Create atoms in memory (no storage yet). The separate Delete
            // route removes current tips and retains shared atom bodies.
            // Defensive: if a Delete ever reaches prepare, do not synthesize
            // per-field tombstones — write only the fields the caller named
            // (usually empty → zero atoms).
            let fields_to_write: &HashMap<String, serde_json::Value> = &mutation.fields_and_values;
            for (field_name, value) in fields_to_write {
                let Some(atom) =
                    Self::create_field_atom(schema_name, schema, mutation, field_name, value)?
                else {
                    continue;
                };
                atom_results.push(PreparedFieldAtom {
                    mutation_index: idx,
                    field_name: field_name.clone(),
                    atom,
                    seal_molecule: seal_molecule_for_atom(schema_name, field_name, schema),
                });
            }
        }

        Ok((mutation_key_values, atom_results))
    }

    /// Build, per field this batch writes, the set of keys it will touch — the
    /// O(changed) unit. Each mutation upserts every field in
    /// `fields_and_values` at its `key_value`, so the touched key for a field is
    /// derived from the supported Hash or HashRange field and the mutation's
    /// key. The returned map drives the
    /// O(changed) write-path molecule restore in
    /// [`Self::restore_missing_molecules`].
    pub(in crate::fold_db_core::mutation_manager) fn changed_keys_by_field(
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
    ) -> HashMap<String, HashSet<ChangedKey>> {
        let mut by_field: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
        for (mutation, key_value) in schema_mutations.iter().zip(mutation_key_values.iter()) {
            // Delete is hard-erased outside this path; only Create/Update
            // fields land here.
            let field_names: Vec<&String> = mutation.fields_and_values.keys().collect();
            for field_name in field_names {
                let Some(field) = schema.runtime_fields.get(field_name) else {
                    continue;
                };
                let Some(changed_key) = field.changed_key_for(key_value) else {
                    continue;
                };
                by_field
                    .entry(field_name.clone())
                    .or_default()
                    .insert(changed_key);
            }
        }
        by_field
    }
}

/// The molecule of the field that an atom came from. It names the key bundle
/// that seals the body in an org scope.
///
/// Uses `deterministic_molecule_uuid(schema, field)` so a first write (no
/// molecule uuid on the field yet) still names the right molecule.
fn seal_molecule_for_atom(schema_name: &str, field_name: &str, schema: &Schema) -> String {
    schema
        .runtime_fields
        .get(field_name)
        .and_then(|field| field.common().molecule_uuid().cloned())
        .unwrap_or_else(|| crate::atom::deterministic_molecule_uuid(schema_name, field_name))
}
