//! Short memory-only apply-gate transaction (PR 3).
//!
//! This module is the apply-gate *scope*: it must contain no async wait and no
//! storage-ops handle. Cold restore and persist-lane IO live in the caller,
//! outside the held gates.

use std::collections::{HashMap, HashSet};

use crate::atom::{Atom, MutationEvent};
use crate::db_operations::ChangedKey;
use crate::resident::PersistSlotRevision;
use crate::schema::types::{KeyValue, Mutation, Schema};
use crate::schema::SchemaError;

use super::super::helpers::ModifiedFieldKeys;
use super::super::MutationManager;
use super::LanePersistJob;

/// Prepared resident revision for one slot, recorded after restore and
/// checked again under the gates.
#[derive(Debug, Clone)]
pub(in crate::fold_db_core::mutation_manager) struct PreparedSlotRevision {
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
    pub resident_revision: u64,
}

/// One schema group's prepared resident delta. Built for every schema in the
/// batch before any slot gate is acquired or any tip is published.
pub(in crate::fold_db_core::mutation_manager) struct PreparedSchemaDelta {
    /// Registered before atom creation so an atom-GC cut cannot miss this job.
    pub pending_persist_task: Option<crate::fold_db_core::pending_task_tracker::PendingTask>,
    pub schema_name: String,
    pub schema: Schema,
    pub schema_mutations: Vec<Mutation>,
    pub mutation_key_values: Vec<KeyValue>,
    pub atom_results: Vec<(usize, String, Atom)>,
    pub deferred_atoms: Option<Vec<(Atom, Option<crate::atom::AtomPartition>)>>,
    pub search_batch: Option<crate::db_operations::search_index::IndexChangeBatch>,
    pub share_prefixes: Vec<String>,
    pub sibling_updates: Vec<(
        String,
        crate::db_operations::MoleculeData,
        HashSet<ChangedKey>,
    )>,
    /// Resident revisions used to prepare the protein sibling updates.
    ///
    /// Sibling molecules can belong to another schema lane. Their revisions
    /// therefore need the same optimistic validation as the entry molecules.
    pub sibling_prepared: Vec<PreparedSlotRevision>,
    pub idempotency_entries: Vec<(String, String)>,
    pub changed_keys: HashMap<String, HashSet<ChangedKey>>,
    pub prepared: Vec<PreparedSlotRevision>,
    pub tracks_retention_age: bool,
    pub tracks_retention_hash_partitions: bool,
    pub defer_reservation: Option<crate::memory_budget::DeferReservation>,
    pub lane_reservation: Option<crate::resident::PersistReservation<LanePersistJob>>,
    /// Wait for this envelope after all schema groups enter their lanes.
    pub wait_for_persist_lane: bool,
}

/// Memory-only result of one apply-gate transaction.
pub(in crate::fold_db_core::mutation_manager) struct ApplyGateOutcome {
    pub mutation_events: Vec<MutationEvent>,
    pub modified_fields: ModifiedFieldKeys,
    pub slot_revisions: Vec<PersistSlotRevision>,
}

pub(in crate::fold_db_core::mutation_manager) struct PurgeApplyOutcome {
    pub schema_tombstones: Vec<(String, String, String)>,
    pub slots: Vec<super::ResidentPurgeSlot>,
    pub slot_revisions: Vec<PersistSlotRevision>,
}

fn unique_prepared_slot_revisions(
    mut slots: Vec<PreparedSlotRevision>,
) -> Vec<PreparedSlotRevision> {
    slots.sort_unstable_by(|left, right| {
        (
            left.molecule_uuid.as_str(),
            left.disk_hash.as_str(),
            left.disk_range.as_str(),
        )
            .cmp(&(
                right.molecule_uuid.as_str(),
                right.disk_hash.as_str(),
                right.disk_range.as_str(),
            ))
    });
    slots.dedup_by(|left, right| {
        let same_slot = left.molecule_uuid == right.molecule_uuid
            && left.disk_hash == right.disk_hash
            && left.disk_range == right.disk_range;
        if same_slot {
            debug_assert_eq!(
                left.resident_revision, right.resident_revision,
                "one apply-gate snapshot must give a slot one predecessor revision"
            );
        }
        same_slot
    });
    slots
}

fn advanced_persist_slot_revisions(
    resident: &crate::resident::ResidentGraph,
    predecessors: Vec<PreparedSlotRevision>,
) -> Vec<PersistSlotRevision> {
    predecessors
        .into_iter()
        .filter_map(|predecessor| {
            let target = resident.slot_resident_revision(
                &predecessor.molecule_uuid,
                &predecessor.disk_hash,
                &predecessor.disk_range,
            );
            debug_assert!(
                target >= predecessor.resident_revision,
                "resident slot revision cannot decrease under an apply gate"
            );
            (target > predecessor.resident_revision).then_some(PersistSlotRevision {
                molecule_uuid: predecessor.molecule_uuid,
                disk_hash: predecessor.disk_hash,
                disk_range: predecessor.disk_range,
                resident_revision: target,
                durable_revision: predecessor.resident_revision,
            })
        })
        .collect()
}

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn prepared_slot_revisions(
        &self,
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<PreparedSlotRevision> {
        let graph = self.resident_graph();
        let mut out = Vec::new();
        for (field_name, keys) in changed_keys {
            let Some(field) = schema.runtime_fields.get(field_name) else {
                continue;
            };
            let Some(uuid) = field.common().molecule_uuid() else {
                continue;
            };
            if keys.is_empty() {
                out.push(PreparedSlotRevision {
                    molecule_uuid: uuid.clone(),
                    disk_hash: String::new(),
                    disk_range: String::new(),
                    resident_revision: graph.slot_resident_revision(uuid, "", ""),
                });
                continue;
            }
            for ck in keys {
                let hash = ck.disk_hash();
                let range = ck.disk_range();
                out.push(PreparedSlotRevision {
                    molecule_uuid: uuid.clone(),
                    disk_hash: hash.to_string(),
                    disk_range: range.to_string(),
                    resident_revision: graph.slot_resident_revision(uuid, hash, range),
                });
            }
        }
        out
    }

    pub(in crate::fold_db_core::mutation_manager) fn prepared_revisions_match(
        &self,
        prepared: &[PreparedSlotRevision],
    ) -> bool {
        let graph = self.resident_graph();
        prepared.iter().all(|slot| {
            graph.slot_resident_revision(&slot.molecule_uuid, &slot.disk_hash, &slot.disk_range)
                == slot.resident_revision
        })
    }

    /// Record the resident revisions used by prepared protein sibling tips.
    pub(in crate::fold_db_core::mutation_manager) fn prepared_sibling_slot_revisions(
        &self,
        sibling_updates: &[(
            String,
            crate::db_operations::MoleculeData,
            HashSet<ChangedKey>,
        )],
    ) -> Vec<PreparedSlotRevision> {
        let graph = self.resident_graph();
        let mut out = Vec::new();
        for (molecule_uuid, _data, changed) in sibling_updates {
            if changed.is_empty() {
                out.push(PreparedSlotRevision {
                    molecule_uuid: molecule_uuid.clone(),
                    disk_hash: String::new(),
                    disk_range: String::new(),
                    resident_revision: graph.slot_resident_revision(molecule_uuid, "", ""),
                });
                continue;
            }
            for key in changed {
                let hash = key.disk_hash();
                let range = key.disk_range();
                out.push(PreparedSlotRevision {
                    molecule_uuid: molecule_uuid.clone(),
                    disk_hash: hash.to_string(),
                    disk_range: range.to_string(),
                    resident_revision: graph.slot_resident_revision(molecule_uuid, hash, range),
                });
            }
        }
        out.sort_unstable_by(|left, right| {
            (
                left.molecule_uuid.as_str(),
                left.disk_hash.as_str(),
                left.disk_range.as_str(),
            )
                .cmp(&(
                    right.molecule_uuid.as_str(),
                    right.disk_hash.as_str(),
                    right.disk_range.as_str(),
                ))
        });
        out.dedup_by(|left, right| {
            left.molecule_uuid == right.molecule_uuid
                && left.disk_hash == right.disk_hash
                && left.disk_range == right.disk_range
        });
        out
    }

    /// Rebuild in-memory molecules from resident tips after a revision
    /// mismatch. No storage read.
    pub(in crate::fold_db_core::mutation_manager) fn reseed_changed_from_resident(
        &self,
        schema: &mut Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Result<(), SchemaError> {
        for (field_name, keys) in changed_keys {
            let Some(field) = schema.runtime_fields.get_mut(field_name) else {
                continue;
            };
            let Some(uuid) = field.common().molecule_uuid().cloned() else {
                continue;
            };
            if keys.is_empty() {
                continue;
            }
            if let Some(data) = self.try_seed_molecule_from_resident(&uuid, keys) {
                field.set_molecule_data(data)?;
            }
        }
        Ok(())
    }

    /// Apply mutations, publish resident tips, bump slot revisions.
    ///
    /// Callers hold sorted apply gates around this function. Do not add
    /// storage IO or an async wait here.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::fold_db_core::mutation_manager) fn apply_gate_transaction(
        &self,
        schema: &mut Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
        atom_results: Vec<(usize, String, Atom)>,
        located_atoms: Option<&[(Atom, Option<crate::atom::AtomPartition>)]>,
        sibling_updates: &[(
            String,
            crate::db_operations::MoleculeData,
            HashSet<ChangedKey>,
        )],
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> ApplyGateOutcome {
        // Overlay, catalog-head strip, overlay forget, and tip publish apply
        // for personal and org-prefix writes. `purge_apply_gate_transaction`
        // already stamps org overlays; a personal-only consult left org
        // Create after Delete as a catalog-head no-op.
        if let Some(located_atoms) = located_atoms {
            let resident = self.resident_graph();
            for (atom, partition) in located_atoms {
                resident.apply_atom(
                    crate::resident::ResidentAtom::from_atom(atom)
                        .with_partition(partition.as_ref()),
                );
            }
        }
        // Overlay is serving truth: a restored catalog head on a tombstoned
        // key must not make a byte-identical Create a same-uuid no-op.
        // Drop that catalog entry so apply writes a real tip again.
        {
            let resident = self.resident_graph();
            for key_value in mutation_key_values {
                for field in schema.runtime_fields.values_mut() {
                    let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
                        continue;
                    };
                    let Some((slot_hash, slot_range)) = field.disk_slot_for_key(key_value) else {
                        continue;
                    };
                    if resident.is_key_tombstoned(&molecule_uuid, &slot_hash, &slot_range) {
                        if let Some(molecule) = field.molecule.as_mut() {
                            molecule.remove_atom_uuid(&slot_hash, &slot_range);
                        }
                    }
                }
            }
        }
        let (mutation_events, modified_fields) = self.apply_mutations_to_molecules(
            schema,
            schema_mutations,
            mutation_key_values,
            atom_results,
        );
        {
            let resident = self.resident_graph();
            for (mutation, key_value) in schema_mutations.iter().zip(mutation_key_values) {
                let hash = key_value.hash.as_deref().unwrap_or("");
                let range = key_value.range.as_deref().unwrap_or("");
                resident.forget_schema_key_tombstone(&mutation.schema_name, hash, range);
                if schema.name != mutation.schema_name {
                    resident.forget_schema_key_tombstone(&schema.name, hash, range);
                }
                if let Some(identity) = schema.get_identity_hash() {
                    resident.forget_schema_key_tombstone(identity, hash, range);
                }
                for field in schema.runtime_fields.values() {
                    let Some(molecule_uuid) = field.common().molecule_uuid() else {
                        continue;
                    };
                    let Some((slot_hash, slot_range)) = field.disk_slot_for_key(key_value) else {
                        continue;
                    };
                    resident.forget_key_tombstone(molecule_uuid, &slot_hash, &slot_range);
                }
            }
        }
        let mut predecessor_revisions = self.prepared_slot_revisions(schema, changed_keys);
        predecessor_revisions.extend(self.prepared_sibling_slot_revisions(sibling_updates));
        let predecessor_revisions = unique_prepared_slot_revisions(predecessor_revisions);
        self.publish_modified_tips_to_resident(schema, &modified_fields, true);
        self.publish_sibling_tips_to_resident(sibling_updates, true);
        let slot_revisions =
            advanced_persist_slot_revisions(self.resident_graph(), predecessor_revisions);
        ApplyGateOutcome {
            mutation_events,
            modified_fields,
            slot_revisions,
        }
    }

    /// Publish every prepared schema delta while the caller holds every
    /// affected slot gate. Memory-only: no storage await.
    pub(in crate::fold_db_core::mutation_manager) fn publish_prepared_resident_deltas(
        &self,
        prepared: &mut [PreparedSchemaDelta],
    ) -> Vec<ApplyGateOutcome> {
        let mut outcomes = Vec::with_capacity(prepared.len());
        for delta in prepared.iter_mut() {
            outcomes.push(self.apply_gate_transaction(
                &mut delta.schema,
                &delta.schema_mutations,
                &delta.mutation_key_values,
                delta.atom_results.clone(),
                delta.deferred_atoms.as_deref(),
                &delta.sibling_updates,
                &delta.changed_keys,
            ));
        }
        outcomes
    }

    /// Install a purge's serving tombstones while the sorted slot gates are held.
    /// This function is memory-only. Durable deletion runs later on the lane.
    ///
    /// Overlays and slot tickets apply for personal and org-prefix erasures.
    /// Org T0 can still hold a cold-read tip; skipping the overlay left that
    /// tip serving after persist forgot nothing and never called `purge_tip`.
    pub(in crate::fold_db_core::mutation_manager) fn purge_apply_gate_transaction(
        &self,
        schema: &Schema,
        erasures: &[Mutation],
        tombstone_id: u64,
        delete_winning_slots: Option<&HashSet<(String, String, String)>>,
    ) -> PurgeApplyOutcome {
        let mut schema_tombstones = Vec::new();
        let mut slots = Vec::new();
        let mut slot_predecessors = HashMap::new();
        let resident = self.resident_graph();
        for mutation in erasures {
            let hash = mutation.key_value.hash.as_deref().unwrap_or("");
            let range = mutation.key_value.range.as_deref().unwrap_or("");
            let mut all_slots_won = true;
            for field in schema.runtime_fields.values() {
                let Some(molecule_uuid) = field.common().molecule_uuid() else {
                    continue;
                };
                let Some((slot_hash, slot_range)) = field.disk_slot_for_key(&mutation.key_value)
                else {
                    continue;
                };
                let slot_key = (molecule_uuid.clone(), slot_hash.clone(), slot_range.clone());
                if delete_winning_slots.is_some_and(|winning| !winning.contains(&slot_key)) {
                    all_slots_won = false;
                    continue;
                }
                if !slot_predecessors.contains_key(&slot_key) {
                    slot_predecessors.insert(
                        slot_key.clone(),
                        resident.slot_resident_revision(molecule_uuid, &slot_hash, &slot_range),
                    );
                    slots.push(super::ResidentPurgeSlot {
                        molecule_uuid: molecule_uuid.clone(),
                        hash: slot_hash.clone(),
                        range: slot_range.clone(),
                    });
                }
                resident.apply_key_tombstone_with_id(
                    molecule_uuid,
                    &slot_hash,
                    &slot_range,
                    tombstone_id,
                );
            }
            // A schema tombstone hides every field. Keep it absent when one
            // newer field tip survives this older Delete.
            if all_slots_won {
                let mut aliases = vec![mutation.schema_name.clone(), schema.name.clone()];
                if let Some(identity) = schema.get_identity_hash() {
                    aliases.push(identity.clone());
                }
                aliases.sort_unstable();
                aliases.dedup();
                for alias in aliases {
                    resident.apply_schema_key_tombstone_with_id(&alias, hash, range, tombstone_id);
                    schema_tombstones.push((alias, hash.to_string(), range.to_string()));
                }
            }
        }
        schema_tombstones.sort_unstable();
        schema_tombstones.dedup();
        let predecessor_revisions = slot_predecessors
            .into_iter()
            .map(
                |((molecule_uuid, disk_hash, disk_range), resident_revision)| {
                    PreparedSlotRevision {
                        molecule_uuid,
                        disk_hash,
                        disk_range,
                        resident_revision,
                    }
                },
            )
            .collect();
        let predecessor_revisions = unique_prepared_slot_revisions(predecessor_revisions);
        let slot_revisions =
            advanced_persist_slot_revisions(self.resident_graph(), predecessor_revisions);
        PurgeApplyOutcome {
            schema_tombstones,
            slots,
            slot_revisions,
        }
    }
}
