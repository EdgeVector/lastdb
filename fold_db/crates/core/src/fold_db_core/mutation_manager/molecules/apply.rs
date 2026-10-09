//! Apply atom writes to in-memory molecules.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::atom::{Atom, MutationEvent};
use crate::db_operations::ChangedKey;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

use super::super::helpers::ModifiedFieldKeys;
use super::super::MutationManager;
use super::protein_batch::{eligible_protein_atoms, load_protein_fold_metadata};

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn apply_mutations_to_molecules(
        &self,
        schema: &mut Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: Vec<(usize, String, Atom)>,
    ) -> (Vec<MutationEvent>, ModifiedFieldKeys) {
        // field_name -> the keys touched on that field's molecule this batch.
        // A field present with an empty ChangedKey set still persists (e.g. Single empty slot).
        let mut modified_fields: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
        // History-off: always empty. Previous version is tip `prev_atom_uuid`.
        let mutation_events: Vec<MutationEvent> = Vec::new();

        for (idx, field_name, atom) in atom_results {
            let mutation = &schema_mutations[idx];
            let key_value = &mutation_key_values[idx];

            // Extra keys in the mutation payload (e.g. sibling partition keys for
            // field_hash protein fold) are not schema fields — skip atom tip write.
            let Some(schema_field) = schema.runtime_fields.get_mut(&field_name) else {
                continue;
            };

            if self.tip_history_enabled_for_writes() {
                if let Some(molecule) = schema_field.molecule.as_mut() {
                    molecule.set_tip_history_enabled(true);
                }
            }

            // Write mutation to memory. When the mutation carries a
            // `Provenance::User` (e.g. inbound `data_share` replay), pass it
            // through as `writer_override` so the per-key signing layer
            // preserves the original author's `writer_pubkey` on the
            // `AtomEntry` instead of overwriting via the local signer.
            // Molecule set_* records previous content as tip `prev_atom_uuid`.
            schema_field.write_mutation(
                key_value,
                crate::schema::types::field::WriteContext {
                    atom: atom.clone(),
                    pub_key: mutation.pub_key.clone(),
                    source_file_name: mutation.source_file_name.clone(),
                    metadata: mutation.metadata.clone(),
                    schema_name: mutation.schema_name.clone(),
                    field_name: field_name.clone(),
                    signer: Arc::clone(&self.signer),
                    writer_override: mutation.provenance.clone(),
                    imported_written_at: mutation.imported_written_at,
                    logical_counter: mutation.logical_counter,
                    author_clock_writer_id: mutation.author_clock_writer_id.clone(),
                    mutation_uuid: mutation.uuid.clone(),
                    imported_version: mutation.imported_version,
                },
            );
            // Torn-apply check: a plain local write always becomes the tip.
            // An imported write (mutation-log replay or `Provenance::User`
            // data_share) goes through last-writer-wins in the molecule and
            // may legitimately lose to a newer tip, so it is exempt here.
            if mutation.imported_written_at.is_none() && mutation.provenance.is_none() {
                let written = super::super::helpers::current_atom_uuid(schema_field, key_value);
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
            // set). Product create/update never writes `ref:{M}` whole-molecule
            // blobs; that collection is legacy residual only (inventory + purge).
            if schema_field.common().molecule_uuid().is_some() {
                let entry = modified_fields.entry(field_name.clone()).or_default();
                if let Some(ck) = changed_key {
                    entry.insert(ck);
                }
            }
        }

        (mutation_events, modified_fields)
    }

    /// Rebase prepared protein sibling updates on the current resident tips.
    ///
    /// Protein preparation reads durable molecule rows before apply-gate
    /// acquisition. A prior acknowledged sibling update can still be dirty at
    /// that point. Use that resident tip as the LWW base, while the sibling
    /// revision snapshot detects a change after this preparation starts.
    pub(in crate::fold_db_core::mutation_manager) fn rebase_sibling_updates_on_resident(
        &self,
        sibling_updates: Vec<(
            String,
            crate::db_operations::MoleculeData,
            HashSet<ChangedKey>,
        )>,
    ) -> Vec<(
        String,
        crate::db_operations::MoleculeData,
        HashSet<ChangedKey>,
    )> {
        use crate::atom::{AtomEntry, MoleculeHashRange};

        let resident = self.resident_graph();
        let mut slot_heads: HashMap<(String, String, String), Option<AtomEntry>> = HashMap::new();
        let mut rebased_updates = Vec::with_capacity(sibling_updates.len());

        for (molecule_uuid, data, changed) in sibling_updates {
            // The protein planner emits one tuple for one member slot. Keep a
            // defensive fallback so a future multi-slot planner stays correct
            // until it gets an equivalent per-slot rebase.
            let Some(changed_key) = (changed.len() == 1)
                .then(|| changed.iter().next())
                .flatten()
            else {
                rebased_updates.push((molecule_uuid, data, changed));
                continue;
            };
            let hash = changed_key.disk_hash().to_string();
            let range = changed_key.disk_range().to_string();
            let Some(proposed) = data.get_atom_entry(&hash, &range).cloned() else {
                rebased_updates.push((molecule_uuid, data, changed));
                continue;
            };

            let slot = (molecule_uuid.clone(), hash.clone(), range.clone());
            let current = slot_heads.entry(slot).or_insert_with(|| {
                resident
                    .resolve_tip(&molecule_uuid, &hash, &range)
                    .map(|tip| {
                        let tip = tip.value;
                        AtomEntry::thin_with_author(
                            tip.atom_uuid,
                            tip.written_at,
                            tip.logical_counter,
                            tip.device_id,
                            tip.mutation_uuid,
                            String::new(),
                        )
                    })
            });

            let rebased = if let Some(base) = current.as_ref() {
                let metadata = data.get_key_metadata(&hash, &range).cloned();
                let mut rebased = MoleculeHashRange::from_write_records(
                    molecule_uuid.clone(),
                    data.version(),
                    chrono::Utc::now(),
                    vec![(hash.clone(), range.clone(), base.clone(), metadata)],
                );
                rebased.set_atom_uuid_from_values_imported_with_author(
                    hash.clone(),
                    range.clone(),
                    proposed.atom_uuid.clone(),
                    proposed.lww_device().to_string(),
                    proposed.writer_pubkey.clone(),
                    proposed.signature.clone(),
                    proposed.signature_version,
                    Some(proposed.written_at),
                    proposed.logical_counter,
                    proposed.mutation_uuid.clone(),
                );
                rebased
            } else {
                data
            };
            *current = rebased.get_atom_entry(&hash, &range).cloned();
            rebased_updates.push((molecule_uuid, rebased, changed));
        }

        rebased_updates
    }

    /// After normal molecule tip writes, prepare protein sibling tip updates
    /// when the entry field molecule is bound (field_hash auto-protein or
    /// explicit bind).
    ///
    /// Returns in-memory sibling molecule updates only — **no durable store**.
    /// The write path publishes them to resident under write-mode and defers
    /// (or inline-stores) durable put with the entry batch so sibling flush is
    /// not on the mutation hot path.
    pub(in crate::fold_db_core::mutation_manager) async fn fold_protein_siblings_after_write(
        &self,
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: &[(usize, String, crate::atom::Atom)],
    ) -> Result<
        Vec<(
            String,
            crate::db_operations::MoleculeData,
            std::collections::HashSet<crate::db_operations::ChangedKey>,
        )>,
        SchemaError,
    > {
        let store = self.db_ops.atoms();
        let keypair = self.signer.as_ref();
        let mut sibling_updates = Vec::new();
        // A schema can declare one key layout while the API supplies another
        // compatible KeyValue. A shared field molecule can carry both layouts
        // in one protein. The layout-based planner cannot identify the actual
        // entry slot from field names alone, so exclude every slot this batch
        // writes directly. Publishing an entry twice lets a stale sibling
        // plan replace the fresh resident tip even when durable LWW rejects it.
        let entry_slots: HashSet<(String, String, String)> = atom_results
            .iter()
            .filter_map(|(idx, field_name, _)| {
                let field = schema.runtime_fields.get(field_name)?;
                let molecule_uuid = field.common().molecule_uuid()?.clone();
                let (hash, range) = field.disk_slot_for_key(&mutation_key_values[*idx])?;
                Some((molecule_uuid, hash, range))
            })
            .collect();

        if atom_results
            .iter()
            .any(|(_, name, _)| crate::record_molecule::is_record_molecule_field(name))
        {
            if let Ok(peers) = self.schema_manager.get_schemas() {
                for (peer_name, peer) in peers {
                    if peer_name == schema.name {
                        continue;
                    }
                    crate::schema::field_hash_coherence::bind_cross_key_record_protein(
                        store, schema, &peer,
                    )
                    .await?;
                }
            }
        }

        let eligible =
            eligible_protein_atoms(schema, schema_mutations, mutation_key_values, atom_results);
        let member_uuids: Vec<&str> = eligible.iter().map(|(_, _, mol_uuid)| *mol_uuid).collect();
        let (owners_by_molecule, proteins_by_uuid) =
            load_protein_fold_metadata(store, &member_uuids).await?;

        // Keep the atom order. Two fields can target the same sibling slot,
        // and the later tip must still observe the earlier update.
        for (idx, atom, mol_uuid) in eligible {
            let key_value = &mutation_key_values[idx];
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

            let mut fields: HashMap<String, String> = HashMap::new();
            if let Some(ref key) = schema.key {
                if let (Some(hf), Some(h)) = (key.hash_field.as_ref(), key_value.hash.as_ref()) {
                    fields.insert(hf.clone(), h.clone());
                }
                if let (Some(rf), Some(r)) = (key.range_field.as_ref(), key_value.range.as_ref()) {
                    fields.insert(rf.clone(), r.clone());
                }
            }
            // Include sibling key fields if present on the mutation payload
            // (multi-key fold needs both board and milestone in the field map).
            for (k, v) in &mutation.fields_and_values {
                if let Some(s) = v.as_str() {
                    fields.entry(k.clone()).or_insert_with(|| s.to_string());
                } else if !v.is_null() {
                    fields
                        .entry(k.clone())
                        .or_insert_with(|| v.to_string().trim_matches('"').to_string());
                }
            }

            let entry_layout = schema.key.as_ref().and_then(|key| {
                key.hash_field
                    .as_deref()
                    .map(|hash_field| (hash_field, key.range_field.as_deref()))
            });
            let author_clock = mutation.imported_written_at.map(|written_at| {
                let device_id = if mutation.author_clock_writer_id.is_empty() {
                    match &mutation.provenance {
                        Some(crate::atom::provenance::Provenance::User { pubkey, .. }) => {
                            pubkey.clone()
                        }
                        _ => mutation.pub_key.clone(),
                    }
                } else {
                    mutation.author_clock_writer_id.clone()
                };
                (
                    written_at,
                    mutation.logical_counter,
                    device_id,
                    mutation.uuid.clone(),
                )
            });
            let updates = store
                .protein_sibling_tip_updates_for_loaded_protein(
                    protein,
                    mol_uuid,
                    entry_layout,
                    &fields,
                    atom.uuid(),
                    keypair,
                    author_clock,
                )
                .await?;
            for (sibling_molecule, data, mut changed) in updates {
                changed.retain(|key| {
                    !entry_slots.contains(&(
                        sibling_molecule.clone(),
                        key.disk_hash().to_string(),
                        key.disk_range().to_string(),
                    ))
                });
                if !changed.is_empty() {
                    sibling_updates.push((sibling_molecule, data, changed));
                }
            }
        }
        Ok(sibling_updates)
    }
}
