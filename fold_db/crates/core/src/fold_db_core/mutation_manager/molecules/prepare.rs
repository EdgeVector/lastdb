//! Mutation atom preparation and changed-key analysis.

use std::collections::{HashMap, HashSet};

use super::super::MutationManager;
use crate::atom::Atom;
use crate::db_operations::{AtomStore, ChangedKey};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use tracing::warn;

mod key_fields;

type PreparedAtomsAndKeys = (
    Vec<KeyValue>,
    Vec<(usize, String, Atom)>,
    Option<Vec<(Atom, Option<crate::atom::AtomPartition>)>>,
);

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
        storage_prefix: Option<&str>,
    ) -> Result<PreparedAtomsAndKeys, SchemaError> {
        let mut atoms_to_store: Vec<Atom> = Vec::new();
        let mut atom_results: Vec<(usize, String, Atom)> = Vec::new();
        let mut mutation_key_values = Vec::with_capacity(schema_mutations.len());

        for (idx, mutation) in schema_mutations.iter_mut().enumerate() {
            let key_value = Self::resolve_mutation_key_value(schema_name, schema, mutation)?;

            Self::materialize_key_fields(schema_name, schema, mutation, &key_value)?;

            mutation_key_values.push(key_value);

            // Create atoms in memory (no storage yet). `MutationType::Delete`
            // is peeled out of this pipeline in `write_mutations_batch_async`
            // and hard-erased via the purge path (not N tombstone atoms).
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
                atoms_to_store.push(atom.clone());
                atom_results.push((idx, field_name.clone(), atom));
            }
        }

        // Batch store all atoms at once, each with the partition of the slot
        // that owns it. Under `AtomKeyEncoding::PartitionPrefix` that co-locates
        // the body with its tips; under `Flat` the partitions are ignored.
        //
        // Pair each created atom with the mutation key + field it came from
        // (`atom_results` is parallel to the order atoms were pushed).
        let located: Vec<(crate::atom::Atom, Option<crate::atom::AtomPartition>)> = atom_results
            .iter()
            .zip(atoms_to_store)
            .map(|((mut_idx, field_name, _), atom)| {
                let key_value = &mutation_key_values[*mut_idx];
                let partition = partition_for_atom_write(
                    schema_name,
                    field_name,
                    key_value,
                    schema,
                    self.db_ops.atoms(),
                );
                (atom, partition)
            })
            .collect();

        let _ = storage_prefix;
        Ok((mutation_key_values, atom_results, Some(located)))
    }

    /// Build, per field this batch writes, the set of keys it will touch — the
    /// O(changed) unit. Each mutation upserts every field in
    /// `fields_and_values` at its `key_value`, so the touched key for a field is
    /// derived from the field variant's kind (Hash/Range/HashRange) and the
    /// mutation's key. A `Single` field has no per-key shape and is omitted
    /// (its whole-blob molecule is already O(1)). The returned map drives the
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
                    continue; // Single — no per-key shape
                };
                by_field
                    .entry(field_name.clone())
                    .or_default()
                    .insert(changed_key);
            }
        }
        by_field
    }

    // `schema_has_active_share_rule` was removed: it existed only to force
    // `needs_full_load` in `restore_missing_molecules` so share fan-out could
    // rewrite from the in-memory molecule. `store_molecules_split` now re-reads
    // the primary namespace instead (fold #1224), so that full hydration had no
    // remaining consumer. Share-rule detection for fan-out still lives in
    // `persist_modified_molecules` via `list_share_rules_in_ops`.
}

/// Partition for a field write: same bytes the tip walk will scan.
///
/// Uses `deterministic_molecule_uuid(schema, field)` so a first write (no
/// molecule uuid on the field yet) still names the correct slot. Hash segment
/// is storage-form via the atom store's key codec (blinded when enabled).
fn partition_for_atom_write(
    schema_name: &str,
    field_name: &str,
    key_value: &KeyValue,
    schema: &Schema,
    atoms: &AtomStore,
) -> Option<crate::atom::AtomPartition> {
    use crate::atom::{deterministic_molecule_uuid, AtomPartition};
    use crate::schema::types::field::FieldKind;

    let mol_uuid = schema
        .runtime_fields
        .get(field_name)
        .and_then(|f| f.common().molecule_uuid().cloned())
        .unwrap_or_else(|| deterministic_molecule_uuid(schema_name, field_name));

    let kind = schema
        .runtime_fields
        .get(field_name)
        .map_or(FieldKind::Single, |f| f.kind);

    let api_hash = match kind {
        FieldKind::Hash | FieldKind::HashRange => key_value.hash.as_deref().unwrap_or(""),
        FieldKind::Range | FieldKind::Single => "",
    };

    let storage_hash = if api_hash.is_empty() {
        String::new()
    } else {
        match atoms.storage_hash(&mol_uuid, api_hash) {
            Ok(h) => h,
            Err(e) => {
                // Degrade to flat placement rather than fail the whole write:
                // a bad hash encoding is a locality miss, not data loss.
                warn!(
                    schema = %schema_name,
                    field = %field_name,
                    error = %e,
                    "partition_for_atom_write: storage_hash failed; body will be flat"
                );
                return None;
            }
        }
    };

    Some(AtomPartition::for_slot(&mol_uuid, &storage_hash))
}
