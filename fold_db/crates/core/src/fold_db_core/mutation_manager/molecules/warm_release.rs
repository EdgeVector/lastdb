//! Drop warm-set copies before the graph holds the dirty tips.

use std::collections::HashSet;

use crate::atom::molecule_uuid::parse_molecule_uuid_bytes;
use crate::db_operations::ChangedKey;
use crate::resident::{LogicalResidentSet, MoleculeId};
use crate::schema::types::{KeyValue, Schema};

use super::super::MutationManager;

impl MutationManager {
    /// Drop warm-set copies of the tips this write is about to make dirty.
    pub(super) fn release_warm_tips(
        &self,
        schema: &Schema,
        mutation_key_values: &[KeyValue],
        sibling_updates: &[(
            String,
            crate::db_operations::MoleculeData,
            HashSet<ChangedKey>,
        )],
    ) {
        let Some(set) = self.db_ops.logical_resident_set() else {
            return;
        };
        let mut guard = set.lock().expect("logical resident set");
        for key_value in mutation_key_values {
            for field in schema.runtime_fields.values() {
                let Some(molecule_uuid) = field.common().molecule_uuid() else {
                    continue;
                };
                let Some((hash, range)) = field.disk_slot_for_key(key_value) else {
                    continue;
                };
                release_one_warm_tip(&mut guard, molecule_uuid, &hash, &range);
            }
        }
        for (molecule_uuid, _data, changed) in sibling_updates {
            for key in changed {
                release_one_warm_tip(&mut guard, molecule_uuid, key.disk_hash(), key.disk_range());
            }
        }
    }
}

fn release_one_warm_tip(
    guard: &mut LogicalResidentSet,
    molecule_uuid: &str,
    hash: &str,
    range: &str,
) {
    let Some(bytes) = parse_molecule_uuid_bytes(molecule_uuid) else {
        return;
    };
    guard.take_tip_for_graph(MoleculeId::from_bytes(bytes), hash, range);
}
