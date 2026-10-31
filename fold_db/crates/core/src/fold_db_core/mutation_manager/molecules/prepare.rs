//! Mutation atom preparation and key normalization.

use std::collections::HashMap;

use super::super::plan::key_fields::{checked_key_shape, requested_key};
use super::super::plan::molecules::{validate_field_value, PreparedFieldAtom};
use super::super::MutationManager;
use crate::atom::Atom;
use crate::db_operations::AtomStore;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use tracing::warn;

mod key_fields;

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
        let requested = requested_key(schema_name, schema, mutation)?;
        if requested.dropped_range {
            warn!(
                schema = %schema_name,
                mutation = %mutation.uuid,
                "Ignoring range key supplied for hash-only schema mutation"
            );
        }
        checked_key_shape(schema_name, schema, mutation, requested.key_value)
            .and_then(|key| self.db_ops.molecule_keys().canonical_key(schema, key))
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
                if !validate_field_value(schema_name, schema, field_name, value)? {
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
