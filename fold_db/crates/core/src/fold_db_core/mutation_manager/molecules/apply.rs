//! Apply atom writes to in-memory molecules.

use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::atom::{Atom, TipAuthor};
use crate::db_operations::ChangedKey;
use crate::protein::LoadedProteinSiblingUpdate;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

use super::super::helpers::ModifiedFieldKeys;
use super::super::MutationManager;
use super::prepared::PreparedFieldAtom;
use super::protein_batch::{load_protein_fold_metadata, schema_storage_prefix};

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn apply_mutations_to_molecules(
        &self,
        schema: &mut Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: &[PreparedFieldAtom],
    ) -> (ModifiedFieldKeys, super::prepared::AcceptedSources) {
        // field_name -> the keys touched on that field's molecule this batch.
        // An empty changed-key set can still carry a molecule header update.
        let mut modified_fields: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
        let mut accepted_sources = HashMap::new();
        let mut slot_heads = super::batch_rebase::BatchSlotHeads::default();

        for PreparedFieldAtom {
            mutation_index: idx,
            field_name,
            atom,
        } in atom_results
        {
            let mutation = &schema_mutations[*idx];
            let key_value = &mutation_key_values[*idx];

            // Extra keys in the mutation payload (e.g. sibling partition keys for
            // field_hash protein fold) are not schema fields — skip atom tip write.
            let Some(schema_field) = schema.runtime_fields.get_mut(field_name) else {
                continue;
            };

            slot_heads.overlay_field(schema_field, key_value);
            let accepted =
                self.write_field_atom(schema_field, key_value, mutation, field_name, atom);
            slot_heads.record_field(schema_field, key_value);
            if accepted {
                if let (Some(uuid), Some(entry)) = (
                    schema_field.common().molecule_uuid(),
                    schema_field.atom_entry_at_key(key_value),
                ) {
                    accepted_sources
                        .insert((*idx, uuid.clone(), atom.uuid().to_string()), entry.clone());
                }
            }
            // Torn-apply check: a plain local write always becomes the tip.
            // An imported write (mutation-log replay or `Provenance::User`
            // data_share) goes through last-writer-wins in the molecule and
            // may legitimately lose to a newer tip, so it is exempt here.
            if mutation.imported_written_at.is_none() && mutation.provenance.is_none() {
                let written = schema_field.current_atom_uuid(key_value);
                debug_assert_eq!(
                    written.as_deref(),
                    Some(atom.uuid()),
                    "in-memory apply must take the written atom for field '{field_name}' of schema '{}'",
                    schema.name
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
        (modified_fields, accepted_sources)
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

    /// After normal molecule tip writes, prepare protein sibling tip updates
    /// when the entry field molecule is bound (field_hash auto-protein or
    /// explicit bind).
    ///
    /// Returns in-memory sibling molecule updates only — **no durable store**.
    /// The native request stages these tips with its entry tips and publishes
    /// every accepted logical effect in one core-model boundary.
    pub(in crate::fold_db_core::mutation_manager) async fn fold_protein_siblings_after_write(
        &self,
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        eligible: &[(usize, &Atom, &str)],
    ) -> Result<super::PreparedSiblingUpdates, SchemaError> {
        if !schema.alternate_keys.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let store = self.db_ops.atoms();
        let mut sibling_updates = Vec::new();
        let mut sibling_origins = Vec::new();

        let member_uuids = super::protein_key_fields::member_ids(schema);
        let metadata = load_protein_fold_metadata(store, &member_uuids).await?;
        if metadata.0.values().all(Option::is_none) {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let key_maps = self
            .protein_key_fields(
                schema,
                schema_mutations,
                mutation_key_values,
                eligible,
                &metadata,
            )
            .boxed()
            .await?;
        let (owners_by_molecule, proteins_by_uuid) = &metadata;

        // Keep the atom order. Two fields can target the same sibling slot,
        // and the later tip must still observe the earlier update.
        for &(idx, atom, mol_uuid) in eligible {
            let mutation = &schema_mutations[idx];
            let Some(protein_uuid) = owners_by_molecule.get(mol_uuid).and_then(Option::as_ref)
            else {
                continue;
            };
            let protein = proteins_by_uuid
                .get(protein_uuid)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "protein {protein_uuid} not found for sibling fold"
                    ))
                })?;

            let entry_layout = schema.key.as_ref().and_then(|key| {
                key.hash_field
                    .as_deref()
                    .map(|hash_field| (hash_field, key.range_field.as_deref()))
            });
            let author_clock = mutation_author_clock(mutation);
            let updates = store
                .protein_sibling_tip_updates_for_loaded_protein(LoadedProteinSiblingUpdate {
                    protein,
                    entry_molecule_uuid: mol_uuid,
                    entry_layout,
                    fields: &key_maps.after[&idx],
                    atom_uuid: atom.uuid(),
                    keypair: self.signer.as_ref(),
                    author_clock,
                    storage_prefix: schema_storage_prefix(schema, Some(mol_uuid)),
                })
                .await?;
            for (sibling_molecule, data, changed, base) in updates {
                if !changed.is_empty() {
                    sibling_updates.push((sibling_molecule, data, changed));
                    sibling_origins.push(super::prepared::SiblingOrigin {
                        source: (idx, mol_uuid.to_string(), atom.uuid().to_string()),
                        base,
                        replacement: None,
                    });
                }
            }
        }
        let moves = self
            .prepare_protein_moves(
                &super::protein_moves::ProteinMoveInput {
                    schema,
                    keys: mutation_key_values,
                    eligible,
                    fields: &key_maps,
                    metadata: &metadata,
                },
                &mut sibling_updates,
                &mut sibling_origins,
            )
            .boxed()
            .await?;
        Ok((sibling_updates, sibling_origins, moves))
    }
}

/// The imported author clock of a mutation, with the sync device that wrote it.
pub(super) fn mutation_author_clock(mutation: &Mutation) -> Option<TipAuthor> {
    mutation.imported_written_at.map(|written_at| {
        let device_id = if mutation.author_clock_writer_id.is_empty() {
            match &mutation.provenance {
                Some(crate::atom::provenance::Provenance::User { pubkey, .. }) => pubkey.clone(),
                _ => mutation.pub_key.clone(),
            }
        } else {
            mutation.author_clock_writer_id.clone()
        };
        TipAuthor {
            written_at,
            logical_counter: mutation.logical_counter,
            device_id,
            mutation_uuid: mutation.uuid.clone(),
            origin_device: mutation.origin_device.clone(),
        }
    })
}
