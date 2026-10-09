//! Molecule restoration before applying a mutation batch.

use std::collections::{HashMap, HashSet};

use chrono::Utc;

use crate::atom::{AtomEntry, MoleculeHashRange};
use crate::db_operations::{ChangedKey, MoleculeData};
use crate::schema::types::Schema;
use crate::schema::SchemaError;

use super::super::MutationManager;

impl MutationManager {
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
        if self.acks_on_resident() {
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

        let mut plans: Vec<(String, String, Option<String>, HashSet<ChangedKey>)> = Vec::new();
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

        // Resident-seed can short-circuit without IO; do it first and drop
        // those fields from the concurrent store load.
        let mut need_store: Vec<(String, String, Option<String>, HashSet<ChangedKey>)> = Vec::new();
        for (field_name, uuid, storage_prefix, keys) in plans {
            if self.acks_on_resident() {
                if let Some(data) = self.try_seed_molecule_from_resident(&uuid, &keys) {
                    if let Some(field) = schema.runtime_fields.get_mut(&field_name) {
                        field.set_molecule_data(data)?;
                    }
                    continue;
                }
            }
            need_store.push((field_name, uuid, storage_prefix, keys));
        }

        if need_store.is_empty() {
            return Ok(());
        }

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

        if need_full.is_empty() {
            return Ok(());
        }

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
        let Some(mol) = field.molecule.as_mut() else {
            return;
        };
        let resident = self.db_ops.resident();
        for ck in keys {
            let hash = ck.disk_hash();
            let range = ck.disk_range();
            let Some(tip) = resident.resolve_tip(molecule_uuid, hash, range) else {
                continue;
            };
            mol.overlay_resident_atom_entry(
                hash.to_string(),
                range.to_string(),
                atom_entry_from_resident_tip(&tip.value),
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
            records.push((api_hash.to_string(), api_range.to_string(), entry, None));
        }
        Some(MoleculeHashRange::from_write_records(
            molecule_uuid.to_string(),
            0,
            Utc::now(),
            records,
        ))
    }
}

fn atom_entry_from_resident_tip(tip: &crate::resident::ResidentTip) -> AtomEntry {
    AtomEntry::thin_with_author(
        tip.atom_uuid.clone(),
        tip.written_at,
        tip.logical_counter,
        tip.device_id.clone(),
        tip.mutation_uuid.clone(),
        String::new(),
    )
}
