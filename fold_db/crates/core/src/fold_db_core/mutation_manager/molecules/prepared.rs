//! Slot observations retained from before a read through apply-gate validation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::{ChangedKey, MoleculeData};
use crate::resident::{OwnedSlotRead, ResidentGraph};
use crate::schema::types::Schema;

use super::super::MutationManager;
use super::scoped_gate::schema_is_scoped;

pub(in crate::fold_db_core::mutation_manager) type SiblingUpdate =
    (String, MoleculeData, HashSet<ChangedKey>);
pub(in crate::fold_db_core::mutation_manager) type SourceId = (usize, String, String);
pub(in crate::fold_db_core::mutation_manager) type AcceptedSources =
    HashMap<SourceId, crate::atom::AtomEntry>;

pub(in crate::fold_db_core::mutation_manager) struct SiblingOrigin {
    pub source: SourceId,
    pub base: Option<(crate::atom::AtomEntry, Option<crate::atom::KeyMetadata>)>,
}
pub(in crate::fold_db_core::mutation_manager) type PreparedSiblingUpdates = (
    Vec<SiblingUpdate>,
    Vec<PreparedSlotRevision>,
    Vec<SiblingOrigin>,
);

/// The lease prevents a completed slot from losing its revision identity.
#[derive(Debug, Clone)]
pub(in crate::fold_db_core::mutation_manager) struct PreparedSlotRevision {
    pub molecule_uuid: String,
    pub disk_hash: String,
    pub disk_range: String,
    pub resident_revision: u64,
    pub durable_revision: u64,
    _observation: OwnedSlotRead,
}

impl PreparedSlotRevision {
    pub(super) fn capture(
        graph: &Arc<ResidentGraph>,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Self {
        let observation = graph.observe_slot_owned(molecule_uuid, hash, range);
        Self {
            molecule_uuid: molecule_uuid.to_string(),
            disk_hash: hash.to_string(),
            disk_range: range.to_string(),
            resident_revision: observation.resident_revision,
            durable_revision: observation.durable_revision,
            _observation: observation,
        }
    }
}

impl MutationManager {
    pub(in crate::fold_db_core::mutation_manager) fn prepared_slot_revisions(
        &self,
        schema: &Schema,
        changed_keys: &HashMap<String, HashSet<ChangedKey>>,
    ) -> Vec<PreparedSlotRevision> {
        if schema_is_scoped(schema) {
            return Vec::new();
        }
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
                out.push(PreparedSlotRevision::capture(graph, uuid, "", ""));
                continue;
            }
            for ck in keys {
                let hash = ck.disk_hash();
                let range = ck.disk_range();
                out.push(PreparedSlotRevision::capture(graph, uuid, hash, range));
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
            let current =
                graph.slot_control(&slot.molecule_uuid, &slot.disk_hash, &slot.disk_range);
            current.resident_revision == slot.resident_revision
                && current.durable_revision == slot.durable_revision
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
                out.push(PreparedSlotRevision::capture(graph, molecule_uuid, "", ""));
                continue;
            }
            for key in changed {
                let hash = key.disk_hash();
                let range = key.disk_range();
                out.push(PreparedSlotRevision::capture(
                    graph,
                    molecule_uuid,
                    hash,
                    range,
                ));
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
}
