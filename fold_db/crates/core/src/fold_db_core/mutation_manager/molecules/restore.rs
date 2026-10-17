//! Molecule restoration before applying a mutation batch.

use std::collections::{HashMap, HashSet};

use chrono::Utc;

use crate::atom::{AtomEntry, MoleculeHashRange};
use crate::db_operations::{ChangedKey, MoleculeData};
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::MutationManager;
use super::scoped_gate::schema_is_scoped;

/// Field name, molecule uuid, storage prefix and the changed keys to hydrate.
type FieldLoadPlan = (String, String, Option<String>, HashSet<ChangedKey>);

impl MutationManager {
    /// Capture revisions before graph/warm/disk reads. A retry discards its
    /// prior molecule so a graph miss cannot retain an obsolete write base.
    pub(in crate::fold_db_core::mutation_manager) async fn reload_changed_for_prepare(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Result<Vec<super::PreparedSlotRevision>, SchemaError> {
        let prepared = self.prepared_slot_revisions(schema, changed_keys);
        for name in changed_keys.keys() {
            if let Some(field) = schema.runtime_fields.get_mut(name) {
                field.molecule = None;
            }
        }
        self.restore_missing_molecules(schema, changed_keys).await?;
        Ok(prepared)
    }

    pub(in crate::fold_db_core::mutation_manager) async fn restore_missing_molecules(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Result<(), SchemaError> {
        // Always O(changed): only hydrate fields this batch will touch.
        //
        // Share-prefix fan-out used to force a full molecule load here via
        // `needs_full_load` / `schema_has_active_share_rule`, on the premise that
        // `store_molecules_split` rewrote from the in-memory molecule. Since
        // fold #1224 that path re-reads the molecule from the **primary**
        // namespace (`load_molecule_per_key(uuid, None)`), so full hydration on
        // the write path has no remaining consumer. (Org pre-tag migration is
        // still detected per field inside `load_molecule_for_write`, which
        // returns `None` so we fall back to `refresh_from_db`.)
        //
        // Pre-fix this loop walked *every* empty runtime field and fell
        // through to `refresh_from_db` for fields not in `changed_keys` —
        // multi-second cold thrash on multi-field schemas (brain put) for
        // molecules never applied in this batch. Share-rule schemas paid that
        // cost on every write regardless of how few keys changed.
        // Plan every field load first, then issue independent cold loads
        // concurrently. BoardCards touches ~24 field molecules per mutation;
        // sequential restore under the molecule gate was a multi-second hold
        // (status: mean molecule_gate hold ~3.1 s on primary 2026-08-17).
        self.overlay_resident_changed_keys(schema, changed_keys);
        let plans = Self::plan_cold_field_loads(schema, changed_keys);
        let need_store = self.seed_fields_from_resident(schema, plans)?;
        if need_store.is_empty() {
            return Ok(());
        }

        let need_full = self.load_fields_for_write(schema, need_store).await?;
        if !need_full.is_empty() {
            self.load_fields_full(schema, need_full).await?;
        }
        // A batch can mix dirty graph hits with cold keys. Overlay after the
        // cold load as well so one miss cannot discard the other dirty heads.
        self.overlay_resident_changed_keys(schema, changed_keys);
        Ok(())
    }

    /// Copy acknowledged resident tips onto fields that already hold a molecule.
    pub(super) fn overlay_resident_changed_keys(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) {
        if crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops)
            && !schema_is_scoped(schema)
        {
            let overlay: Vec<(String, String, HashSet<ChangedKey>)> = schema
                .runtime_fields
                .iter()
                .filter_map(|(name, field)| {
                    if !field.has_molecule() {
                        return None;
                    }
                    let uuid = field.common().molecule_uuid()?.clone();
                    let keys = changed_keys.get(name.as_str())?.clone();
                    if keys.is_empty() {
                        return None;
                    }
                    Some((name.clone(), uuid, keys))
                })
                .collect();
            for (field_name, uuid, keys) in overlay {
                if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                    self.overlay_changed_keys_from_resident(field, &uuid, &keys);
                }
            }
        }
    }

    /// Plan a cold load for each field that has a molecule uuid but no loaded
    /// molecule and is touched by this batch.
    fn plan_cold_field_loads(
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<FieldLoadPlan> {
        let mut plans: Vec<FieldLoadPlan> = Vec::new();
        for (name, field) in &schema.runtime_fields {
            if field.common().molecule_uuid().is_none()
                || field.has_molecule()
                || !changed_keys.contains_key(name.as_str())
            {
                continue;
            }
            let Some(uuid) = field.common().molecule_uuid().cloned() else {
                continue;
            };
            let storage_prefix = field.common().storage_prefix().map(ToString::to_string);
            let Some(keys) = changed_keys.get(name.as_str()) else {
                continue;
            };
            if keys.is_empty() {
                continue;
            }
            plans.push((name.clone(), uuid, storage_prefix, keys.clone()));
        }
        plans
    }

    /// Seed fields from the resident set without IO; returns the plans that
    /// still need a store load.
    fn seed_fields_from_resident(
        &self,
        schema: &mut Schema,
        plans: Vec<FieldLoadPlan>,
    ) -> Result<Vec<FieldLoadPlan>, SchemaError> {
        if !crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops) {
            return Ok(plans);
        }
        let mut need_store: Vec<FieldLoadPlan> = Vec::new();
        for (field_name, uuid, storage_prefix, keys) in plans {
            if storage_prefix.is_none() {
                if let Some(data) = self.try_seed_molecule_from_resident(&uuid, &keys) {
                    if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                        field.set_molecule_data(data)?;
                    }
                    continue;
                }
            }
            need_store.push((field_name, uuid, storage_prefix, keys));
        }
        Ok(need_store)
    }

    /// Load the per-key molecule headers for write concurrently; returns the
    /// fields with no per-key header, which need the full load.
    async fn load_fields_for_write(
        &self,
        schema: &mut Schema,
        need_store: Vec<FieldLoadPlan>,
    ) -> Result<Vec<String>, SchemaError> {
        let store_loads = futures::future::try_join_all(need_store.iter().map(
            |(field_name, uuid, storage_prefix, keys)| {
                let db_ops = self.db_ops.clone();
                let field_name = field_name.clone();
                let uuid = uuid.clone();
                let storage_prefix = storage_prefix.clone();
                let keys = keys.clone();
                async move {
                    let data = db_ops
                        .atoms()
                        .load_molecule_for_write(&uuid, storage_prefix.as_deref(), &keys)
                        .await?;
                    Ok::<_, SchemaError>((field_name, uuid, storage_prefix, data))
                }
            },
        ))
        .await?;

        let mut need_full: Vec<String> = Vec::new();
        for (field_name, _uuid, _prefix, data) in store_loads {
            if let Some(data) = data {
                if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                    field.set_molecule_data(data)?;
                }
            } else {
                // No per-key header — fall through to the full load path below.
                need_full.push(field_name);
            }
        }

        Ok(need_full)
    }

    /// Full-load the fields that have no per-key header and set their molecules.
    async fn load_fields_full(
        &self,
        schema: &mut Schema,
        need_full: Vec<String>,
    ) -> Result<(), SchemaError> {
        let full_loads = futures::future::try_join_all(need_full.iter().map(|field_name| {
            let db_ops = self.db_ops.clone();
            let field_name = field_name.clone();
            let plan = schema.runtime_fields.get(&field_name).and_then(|field| {
                let uuid = field.common().molecule_uuid()?.clone();
                let prefix = field.common().storage_prefix().map(str::to_string);
                let kind = field.kind;
                Some((uuid, prefix, kind))
            });
            async move {
                let Some((uuid, prefix, kind)) = plan else {
                    return Ok((field_name, None));
                };
                let data = db_ops
                    .atoms()
                    .load_molecule_per_key(&uuid, prefix.as_deref())
                    .await?;
                let data = data.map(|d| d.retyped_to_slot(kind.retype_slot()));
                Ok::<_, SchemaError>((field_name, data))
            }
        }))
        .await?;

        for (field_name, data) in full_loads {
            let Some(data) = data else {
                continue;
            };
            if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                field.set_molecule_data(data)?;
            }
        }
        Ok(())
    }

    /// Copy acknowledged resident tips onto an already-hydrated catalog
    /// molecule, including LWW clocks. `has_molecule()` used to skip restore
    /// entirely, so CAS and apply could keep a stale catalog head after a
    /// later catalog reload. Uuid-only overlay stamped `now_nanos()` and
    /// dropped origin clocks.
    fn overlay_changed_keys_from_resident(
        &self,
        field: &mut crate::schema::types::field::FieldVariant,
        molecule_uuid: &str,
        keys: &HashSet<ChangedKey>,
    ) {
        if !crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops) {
            return;
        }

        let resident = self.db_ops.resident();
        for ck in keys {
            let hash = ck.disk_hash();
            let range = ck.disk_range();
            let Some(tip) = resident.resolve_tip(molecule_uuid, hash, range) else {
                continue;
            };
            let mol = field.molecule.get_or_insert_with(|| {
                MoleculeHashRange::from_write_records(
                    molecule_uuid.to_string(),
                    0,
                    Utc::now(),
                    Vec::new(),
                )
            });
            mol.overlay_resident_tip(
                hash.to_string(),
                range.to_string(),
                atom_entry_from_resident_tip(&tip.value),
                tip.value.key_metadata.clone(),
            );
        }
    }

    /// Build a write-only [`MoleculeData`] from resident tips when every
    /// touched key is already in T0. Returns `None` if any tip is missing so
    /// the caller can fall through to LastStore.
    ///
    /// Uses `version=0` shell semantics: the subsequent `write_mutation`
    /// appends the new tip and the deferred persist path rewrites changed keys.
    /// Prior tip history on disk is preserved by the durable put (LWW / thin
    /// tips), not by this in-memory shell.
    ///
    /// The shell **must** copy the resident tip's LWW identity (`written_at`,
    /// `logical_counter`, `device_id`, `mutation_uuid`). A zero-clock seed
    /// makes an older imported intent look newer than the acknowledged T0
    /// tip when `LASTDB_RESIDENT_MODE=write` (including when the deferred
    /// budget is 0 and persist is inline). Catalog reload starts every
    /// batch with `molecule: None`, so this seed is the ordinary LWW
    /// operand under write mode.
    ///
    /// The molecule's `update_order` is a TAIL (`from_write_records`). Product
    /// persist does not write that tail.
    pub(in crate::fold_db_core::mutation_manager) fn try_seed_molecule_from_resident(
        &self,
        molecule_uuid: &str,
        keys: &HashSet<ChangedKey>,
    ) -> Option<MoleculeData> {
        if !crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops) {
            return None;
        }
        let resident = self.db_ops.resident();
        let mut records = Vec::with_capacity(keys.len());
        for ck in keys {
            let api_hash = ck.disk_hash();
            let api_range = ck.disk_range();
            let tip = resident.resolve_tip(molecule_uuid, api_hash, api_range)?;
            // Tip present is enough to seed; atom body may arrive via prepare
            // publish before apply, or stay on the durable path. Copy the
            // resident LWW clocks so imported apply can lose to T0.
            let entry = atom_entry_from_resident_tip(&tip.value);
            records.push((
                api_hash.to_string(),
                api_range.to_string(),
                entry,
                tip.value.key_metadata.clone(),
            ));
        }
        Some(MoleculeHashRange::from_write_records(
            molecule_uuid.to_string(),
            0,
            Utc::now(),
            records,
        ))
    }
}

pub(super) fn atom_entry_from_resident_tip(tip: &crate::resident::ResidentTip) -> AtomEntry {
    let mut entry = AtomEntry::thin_with_author(
        tip.atom_uuid.clone(),
        tip.written_at,
        tip.logical_counter,
        tip.device_id.clone(),
        tip.mutation_uuid.clone(),
        String::new(),
    )
    .with_origin_device(tip.origin_device.clone());
    entry.writer_pubkey = tip.writer_pubkey.clone();
    entry
}
