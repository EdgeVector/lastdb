//! Resident-graph tip publishing and dirty-key bookkeeping for molecule persists.

use super::*;

impl MutationManager {
    /// Publish API-form tips into the resident graph so the next HashKey
    /// resolve hits fresh content (not a stale rehydrate).
    ///
    /// `dirty` selects the install mode: `false` after a successful durable
    /// tip store (the store already has it — clean, evictable); `true` when
    /// the durable put is still DEFERRED (`LASTDB_RESIDENT_MODE=write`
    /// pre-ack install) so eviction is refused and a failed deferred put
    /// leaves the persist worker a retry.
    pub(super) fn publish_changed_tips_to_resident(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &HashSet<ChangedKey>,
        dirty: bool,
    ) {
        let resident = self.db_ops.resident();
        for ck in changed {
            let api_hash = ck.disk_hash();
            let api_range = ck.disk_range();
            let Some(entry) = data.get_atom_entry(api_hash, api_range) else {
                continue;
            };
            let tip = crate::resident::ResidentTip {
                molecule_uuid: molecule_uuid.to_string(),
                hash: api_hash.to_string(),
                range: api_range.to_string(),
                atom_uuid: entry.atom_uuid.clone(),
                written_at: entry.written_at,
                logical_counter: entry.logical_counter,
                device_id: entry.lww_device().to_string(),
                mutation_uuid: entry.mutation_uuid.clone(),
                key_metadata: data.get_key_metadata(api_hash, api_range).cloned(),
                writer_pubkey: entry.writer_pubkey.clone(),
            };
            if dirty {
                resident.apply_tip(tip);
            } else {
                resident.publish_tip_after_store(tip);
            }
        }
    }

    pub(super) fn changed_resident_tips(
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &HashSet<ChangedKey>,
    ) -> Vec<crate::resident::ResidentTip> {
        changed
            .iter()
            .filter_map(|ck| {
                let api_hash = ck.disk_hash();
                let api_range = ck.disk_range();
                let entry = data.get_atom_entry(api_hash, api_range)?;
                Some(crate::resident::ResidentTip {
                    molecule_uuid: molecule_uuid.to_string(),
                    hash: api_hash.to_string(),
                    range: api_range.to_string(),
                    atom_uuid: entry.atom_uuid.clone(),
                    written_at: entry.written_at,
                    logical_counter: entry.logical_counter,
                    device_id: entry.lww_device().to_string(),
                    mutation_uuid: entry.mutation_uuid.clone(),
                    key_metadata: data.get_key_metadata(api_hash, api_range).cloned(),
                    writer_pubkey: entry.writer_pubkey.clone(),
                })
            })
            .collect()
    }

    pub(super) fn clear_changed_tip_dirty_for_revisions(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &HashSet<ChangedKey>,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) {
        let resident = self.db_ops.resident();
        for tip in Self::changed_resident_tips(molecule_uuid, data, changed) {
            let Some(ticket) = slot_revisions.iter().find(|ticket| {
                ticket.molecule_uuid == tip.molecule_uuid
                    && ticket.disk_hash == tip.hash
                    && ticket.disk_range == tip.range
            }) else {
                continue;
            };
            resident.clear_tip_dirty_for_revision_if_current(&tip, ticket.resident_revision);
        }
    }

    /// Install changed tips into resident **before** durable molecule put
    /// (`LASTDB_RESIDENT_MODE=write`). Uses the same tip shape as
    /// [`Self::publish_changed_tips_to_resident`] so HashKey resolve hits
    /// immediately on mutation ack. `dirty` is forwarded per tip: pass
    /// `acks_on_resident()` so a pre-durable-put install pins the entries
    /// until the deferred (or inline-degraded) put succeeds.
    pub(in crate::fold_db_core::mutation_manager) fn publish_modified_tips_to_resident(
        &self,
        schema: &Schema,
        modified_fields: &ModifiedFieldKeys,
        dirty: bool,
    ) {
        for (field_name, changed) in modified_fields {
            let Some(schema_field) = schema.runtime_fields.get(field_name) else {
                continue;
            };
            let Some(molecule_uuid) = schema_field.common().molecule_uuid() else {
                continue;
            };
            let Some(data) = schema_field.molecule_data() else {
                continue;
            };
            self.publish_changed_tips_to_resident(molecule_uuid, data, changed, dirty);
        }
    }

    /// The [`crate::resident::DirtyKey`]s a mode=write batch installed
    /// pre-ack: deferred atom bodies and their resident file-blob closure
    /// (keyed by uuid/ref, exactly as
    /// [`crate::resident::ResidentAtom::from_atom`]`.dirty_key()` derives
    /// them) plus the changed tips — derived with the same walk and the same
    /// skips as [`Self::publish_modified_tips_to_resident`]. The deferred
    /// task (or the inline-degrade path) marks these persisted after its
    /// durable put succeeds.
    pub(in crate::fold_db_core::mutation_manager) fn resident_batch_dirty_keys(
        schema: &Schema,
        modified_fields: &ModifiedFieldKeys,
        deferred_atoms: Option<&[(crate::atom::Atom, Option<crate::atom::AtomPartition>)]>,
    ) -> Vec<crate::resident::DirtyKey> {
        use crate::resident::DirtyKey;

        let mut keys = Vec::new();
        if let Some(located) = deferred_atoms {
            for (atom, _partition) in located {
                keys.push(DirtyKey::Atom(atom.uuid().to_string()));
            }
        }
        for (field_name, changed) in modified_fields {
            let Some(schema_field) = schema.runtime_fields.get(field_name) else {
                continue;
            };
            let Some(molecule_uuid) = schema_field.common().molecule_uuid() else {
                continue;
            };
            let Some(data) = schema_field.molecule_data() else {
                continue;
            };
            for ck in changed {
                let api_hash = ck.disk_hash();
                let api_range = ck.disk_range();
                if data.get_atom_entry(api_hash, api_range).is_none() {
                    continue;
                }
                keys.push(DirtyKey::MoleculeTip {
                    molecule_uuid: molecule_uuid.clone(),
                    hash: api_hash.to_string(),
                    range: api_range.to_string(),
                });
            }
        }
        keys
    }

    /// Dirty keys for protein-sibling tips installed pre-ack (write-mode).
    pub(in crate::fold_db_core::mutation_manager) fn sibling_tip_dirty_keys(
        sibling_updates: &[(String, MoleculeData, HashSet<ChangedKey>)],
    ) -> Vec<crate::resident::DirtyKey> {
        use crate::resident::DirtyKey;

        let mut keys = Vec::new();
        for (molecule_uuid, data, changed) in sibling_updates {
            for ck in changed {
                let api_hash = ck.disk_hash();
                let api_range = ck.disk_range();
                if data.get_atom_entry(api_hash, api_range).is_none() {
                    continue;
                }
                keys.push(DirtyKey::MoleculeTip {
                    molecule_uuid: molecule_uuid.clone(),
                    hash: api_hash.to_string(),
                    range: api_range.to_string(),
                });
            }
        }
        keys
    }

    /// Publish protein-sibling tips to resident (dirty when durable put is deferred).
    pub(in crate::fold_db_core::mutation_manager) fn publish_sibling_tips_to_resident(
        &self,
        sibling_updates: &[(String, MoleculeData, HashSet<ChangedKey>)],
        dirty: bool,
    ) {
        for (molecule_uuid, data, changed) in sibling_updates {
            self.publish_changed_tips_to_resident(molecule_uuid, data, changed, dirty);
        }
    }

    /// Durable-store protein sibling tip updates prepared on the hot path.
    pub(in crate::fold_db_core::mutation_manager) async fn persist_sibling_tip_updates(
        &self,
        sibling_updates: &[(String, MoleculeData, HashSet<ChangedKey>)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if sibling_updates.is_empty() {
            return Ok(());
        }
        self.db_ops
            .atoms()
            .store_molecules_changed_keys_batch(sibling_updates, storage_prefix)
            .await
    }

    pub(super) fn clear_deferred_job_dirty_after_success(
        &self,
        job: &mut DeferredPersistJob,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) {
        for (field_name, changed) in &job.modified_fields {
            let Some(field) = job.schema.runtime_fields.get(field_name) else {
                continue;
            };
            let (Some(molecule_uuid), Some(data)) =
                (field.common().molecule_uuid(), field.molecule_data())
            else {
                continue;
            };
            self.clear_changed_tip_dirty_for_revisions(
                molecule_uuid,
                data,
                changed,
                slot_revisions,
            );
        }
        for (molecule_uuid, data, changed) in &job.sibling_updates {
            self.clear_changed_tip_dirty_for_revisions(
                molecule_uuid,
                data,
                changed,
                slot_revisions,
            );
        }
        let batch_dirty_keys = std::mem::take(&mut job.batch_dirty_keys);
        self.db_ops
            .resident()
            .mark_keys_persisted(Self::non_tip_dirty_keys(batch_dirty_keys));
    }

    pub(super) fn complete_resident_persist_turn(
        &self,
        slot_revisions: &[crate::resident::PersistSlotRevision],
    ) -> Result<(), SchemaError> {
        self.db_ops
            .resident()
            .complete_persist_turn(slot_revisions)
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "resident persist turn completion failed: {error}"
                ))
            })
    }

    pub(in crate::fold_db_core::mutation_manager) fn non_tip_dirty_keys(
        keys: Vec<crate::resident::DirtyKey>,
    ) -> Vec<crate::resident::DirtyKey> {
        keys.into_iter()
            .filter(|key| !matches!(key, crate::resident::DirtyKey::MoleculeTip { .. }))
            .collect()
    }
}
