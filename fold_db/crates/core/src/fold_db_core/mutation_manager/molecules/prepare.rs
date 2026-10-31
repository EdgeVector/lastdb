//! Mutation atom preparation and key normalization.

use std::collections::HashMap;

use super::super::MutationManager;
use super::prepared::PreparedFieldAtom;
use crate::atom::Atom;
use crate::db_operations::AtomStore;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use tracing::warn;

mod key_fields;
mod key_rules;

/// Atom preparation outputs retain the mutation position for every field.
pub(in crate::fold_db_core::mutation_manager) struct PreparedAtoms {
    pub mutation_key_values: Vec<KeyValue>,
    pub atom_results: Vec<PreparedFieldAtom>,
    pub deferred_atoms: Option<Vec<Atom>>,
}

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn resolve_mutation_key_value(
        &self,
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
            if schema.uses_independent_fields() {
                return Err(SchemaError::InvalidData(
                    "an independent Hash mutation cannot supply an undeclared range key".into(),
                ));
            }
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
        }.and_then(|key| self.db_ops.molecule_keys().canonical_key(schema, key))
    }

    /// Creates atoms and computes key values for all mutations in a schema group.
    /// Returns key values, atom results, and located atom bodies.
    ///
    /// This phase performs no atom write and no resident apply. The caller
    /// applies personal resident atoms under the slot gates and gives every
    /// canonical atom body to the reserved schema-lane envelope.
    pub(in crate::fold_db_core::mutation_manager) fn prepare_atoms_and_key_values(
        &self,
        schema_name: &str,
        schema: &Schema,
        schema_mutations: &mut [Mutation],
    ) -> Result<PreparedAtoms, SchemaError> {
        let mut atoms_to_store: Vec<Atom> = Vec::new();
        let mut atom_results = Vec::new();
        let mut mutation_key_values = Vec::with_capacity(schema_mutations.len());

        for (idx, mutation) in schema_mutations.iter_mut().enumerate() {
            let key_value = self.resolve_mutation_key_value(schema_name, schema, mutation)?;

            self.materialize_key_fields(schema_name, schema, mutation, &key_value)?;

            mutation_key_values.push(key_value);

            // Create atoms in memory (no storage yet). The separate Delete
            // route removes current tips and retains shared atom bodies.
            // Defensive: if a Delete ever reaches prepare, do not synthesize
            // per-field tombstones — write only the fields the caller named
            // (usually empty → zero atoms).
            let fields_to_write: &HashMap<String, serde_json::Value> = &mutation.fields_and_values;
            for (field_name, value) in fields_to_write {
                if !super::field_rules::validate_field_value(
                    schema_name,
                    schema,
                    field_name,
                    value,
                )? {
                    continue;
                }
                let atom = AtomStore::create_atom(
                    schema_name,
                    value.clone(),
                    mutation.source_file_name.clone(),
                    mutation.metadata.clone(),
                )?;
                atoms_to_store.push(atom.clone());
                atom_results.push(PreparedFieldAtom {
                    mutation_index: idx,
                    field_name: field_name.clone(),
                    atom,
                });
            }
        }

        // The schema lane stores every atom body of the group in one batch.
        Ok(PreparedAtoms {
            mutation_key_values,
            atom_results,
            deferred_atoms: Some(atoms_to_store),
        })
    }
}
