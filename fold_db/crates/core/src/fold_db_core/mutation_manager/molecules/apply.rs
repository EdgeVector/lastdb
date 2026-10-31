//! Apply atom writes to in-memory molecules.
//!
//! Execute: write each prepared atom into its field molecule. Decide: the
//! persisted key set comes from [`modified_field_keys`] over the applied
//! schema.

use std::sync::Arc;

use crate::atom::Atom;
use crate::schema::types::{KeyValue, Mutation, Schema};

use super::super::helpers::ModifiedFieldKeys;
use super::super::plan::molecules::{modified_field_keys, PreparedFieldAtom};
use super::super::MutationManager;

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn apply_mutations_to_molecules(
        &self,
        schema: &mut Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: &[PreparedFieldAtom],
    ) -> ModifiedFieldKeys {
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
        }

        // field_name -> the keys touched on that field's molecule this batch.
        // An empty changed-key set can still carry a molecule header update.
        // A field's molecule identity is fixed by its first write above, so
        // deciding after the loop matches a per-write decision.
        let modified_fields = modified_field_keys(schema, mutation_key_values, atom_results);
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
            crate::schema::types::field::WriteContext {
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
