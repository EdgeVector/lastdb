//! Apply atom writes to in-memory molecules.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::atom::Atom;
use crate::db_operations::ChangedKey;
use crate::schema::field_write::FieldWrite;
use crate::schema::types::{KeyValue, Mutation, Schema};

use super::super::helpers::ModifiedFieldKeys;
use super::super::MutationManager;
use super::prepared::PreparedFieldAtom;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn apply_mutations_to_molecules(
        &self,
        schema: &mut Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: &[PreparedFieldAtom],
    ) -> ModifiedFieldKeys {
        // field_name -> the keys touched on that field's molecule this batch.
        // An empty changed-key set can still carry a molecule header update.
        let mut modified_fields: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
        let mut slot_heads = super::batch_rebase::BatchSlotHeads::default();

        for PreparedFieldAtom {
            mutation_index: idx,
            field_name,
            atom,
        } in atom_results
        {
            let mutation = &schema_mutations[*idx];
            let key_value = &mutation_key_values[*idx];

            // Only declared fields own atoms.
            let Some(schema_field) = schema.runtime_fields.get_mut(field_name) else {
                continue;
            };

            slot_heads.overlay_field(schema_field, key_value);
            self.write_field_atom(schema_field, key_value, mutation, field_name, atom);
            slot_heads.record_field(schema_field, key_value);
            // Torn-apply check: a plain local write always becomes the tip.
            // An imported write (mutation-log replay or `Provenance::User`
            // data_share) goes through last-writer-wins in the molecule and
            // may legitimately lose to a newer tip, so it is exempt here.
            if mutation.imported_written_at.is_none() && mutation.provenance.is_none() {
                let written = schema_field.current_atom_uuid(key_value);
                debug_assert!(
                    written.as_deref() == Some(atom.uuid()),
                    "in-memory field write must take the written atom"
                );
            }

            // Per-key change unit for this write (Single → empty both components).
            let changed_key: Option<ChangedKey> = schema_field.changed_key_for(key_value);

            // Track for persistence: record the touched key under this field so
            // the persist step writes only the changed `mk:` records. A `Single`
            // field (no `changed_key`) is recorded with an empty set — still
            // present for the per-key store path (header rewrite / empty changed
            // set).
            if schema_field.common().molecule_uuid().is_some() {
                let entry = modified_fields.entry(field_name.clone()).or_default();
                if let Some(ck) = changed_key {
                    entry.insert(ck);
                }
            }
        }

        slot_heads.overlay_changed_schema(schema, &modified_fields);
        modified_fields
    }

    fn write_field_atom(
        &self,
        field: &mut crate::schema::types::field::FieldVariant,
        key: &KeyValue,
        mutation: &Mutation,
        field_name: &str,
        atom: &Atom,
    ) -> bool {
        field.write_mutation(
            key,
            crate::schema::field_write::WriteContext {
                atom: atom.clone(),
                pub_key: mutation.pub_key.clone(),
                source_file_name: mutation.source_file_name.clone(),
                metadata: mutation.metadata.clone(),
                schema_name: mutation.schema_name.clone(),
                field_name: field_name.to_string(),
                signer: Arc::clone(&self.signer),
                writer_override: mutation.provenance.clone(),
                imported_written_at: mutation.imported_written_at,
                logical_counter: mutation.logical_counter,
                author_clock_writer_id: mutation.author_clock_writer_id.clone(),
                mutation_uuid: mutation.uuid.clone(),
                origin_device: mutation.origin_device.clone(),
                imported_version: mutation.imported_version,
            },
        )
    }
}
