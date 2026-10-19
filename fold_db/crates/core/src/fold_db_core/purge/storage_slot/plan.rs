//! Planning helpers for the exact storage-slot purge.

use super::*;

/// What `purge_storage_slots_bulk` hands to `execute_guarded_purge`.
pub(super) struct StorageSlotPlan {
    pub(super) planned: Vec<PlannedStorageSlot>,
    pub(super) candidate_atoms: HashSet<String>,
    pub(super) tip_version_keys: Vec<String>,
    pub(super) atom_ref_v2_edge_keys: Vec<String>,
}

/// Check that a target names a real slot of its field and return its disk key.
pub(super) fn validate_slot_target(
    schema: &crate::schema::Schema,
    schema_name: &str,
    target: &StorageSlotPurgeTarget,
) -> Result<(String, String), SchemaError> {
    let field = schema
        .runtime_fields
        .get(&target.field_name)
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Storage-slot purge field '{}' is not present in schema '{schema_name}'",
                target.field_name
            ))
        })?;
    let actual_molecule = field.common().molecule_uuid().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "Storage-slot purge field '{}' has no molecule in schema '{schema_name}'",
            target.field_name
        ))
    })?;
    if actual_molecule != &target.molecule_uuid {
        return Err(SchemaError::InvalidData(format!(
            "Storage-slot purge molecule does not belong to field '{}' in schema '{schema_name}'",
            target.field_name
        )));
    }
    field
        .slot_coordinates(&target.storage_key)
        .ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "Storage-slot purge target has the wrong key shape for field '{}' in schema '{schema_name}'",
                target.field_name
            ))
        })
}

/// Validate every target, drop duplicates and (under `Skip`) absent slots, and
/// collect the atoms and chain keys the guarded purge needs.
pub(super) async fn plan_storage_slot_targets(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::Schema,
    schema_name: &str,
    targets: &[StorageSlotPurgeTarget],
    missing: PurgeMissingPolicy,
) -> Result<StorageSlotPlan, SchemaError> {
    let mut plan = StorageSlotPlan {
        planned: Vec::new(),
        candidate_atoms: HashSet::new(),
        tip_version_keys: Vec::new(),
        atom_ref_v2_edge_keys: Vec::new(),
    };
    let mut seen: HashSet<(String, String, String)> = HashSet::new();

    let current = super::current::current_storage_slot_targets(db_ops, schema, targets).await?;
    for target in current {
        let (disk_hash, disk_range) = validate_slot_target(schema, schema_name, target)?;
        let identity = (
            target.molecule_uuid.clone(),
            disk_hash.clone(),
            disk_range.clone(),
        );
        if !seen.insert(identity) {
            continue;
        }
        // `validate_slot_target` proved the field exists.
        let field = &schema.runtime_fields[&target.field_name];
        let Some(atom_uuid) = current_atom_for_key(field, &target.storage_key) else {
            match missing {
                PurgeMissingPolicy::Refuse => {
                    return Err(SchemaError::InvalidData(format!(
                        "Storage-slot purge target not found in field '{}' of schema '{schema_name}'",
                        target.field_name
                    )));
                }
                // Already gone is the expected steady state for a scan-driven
                // drain: an earlier chunk in this same run can legitimately
                // have erased this slot's shared tombstone atom.
                PurgeMissingPolicy::Skip => continue,
            }
        };
        plan.candidate_atoms.insert(atom_uuid);
        let chain = collect_target_chain(db_ops, field, &target.storage_key).await?;
        plan.tip_version_keys.extend(chain.tip_version_keys);
        plan.atom_ref_v2_edge_keys
            .extend(chain.atom_ref_v2_edge_keys);
        plan.candidate_atoms.extend(chain.atom_uuids);
        plan.planned.push(PlannedStorageSlot {
            field_name: target.field_name.clone(),
            molecule_uuid: target.molecule_uuid.clone(),
            storage_key: target.storage_key.clone(),
            disk_hash,
            disk_range,
        });
    }
    Ok(plan)
}

/// Keep the invalidation intent before the first destructive operation.
/// A flush can fail after the core removes the slot and reloads the
/// schema. The next idempotent attempt then finds no slot, so only this
/// retained checkpoint can still produce the Search tombstone.
pub(super) fn checkpoint_slot_evidence(
    planned: &[PlannedStorageSlot],
    evidence_checkpoint: &mut Vec<StorageSlotPurgeEvidence>,
) {
    for slot in planned {
        let duplicate = evidence_checkpoint.iter().any(|saved| {
            saved.field_name == slot.field_name
                && saved.molecule_uuid == slot.molecule_uuid
                && saved.storage_key == slot.storage_key
        });
        if !duplicate {
            evidence_checkpoint.push(StorageSlotPurgeEvidence {
                field_name: slot.field_name.clone(),
                molecule_uuid: slot.molecule_uuid.clone(),
                storage_key: slot.storage_key.clone(),
            });
        }
    }
}

/// The descriptor is consumed only by the delete-ledger fingerprint helper;
/// it is never persisted or logged. That preserves per-slot audit identity
/// without exposing opaque storage segments in operator-facing material.
pub(super) fn slot_ledger_descriptor(planned: &[PlannedStorageSlot]) -> String {
    if planned.len() == 1 {
        format!(
            "storage-slot:{}:{}",
            planned[0].molecule_uuid,
            describe_key(&planned[0].storage_key)
        )
    } else {
        format!("<bulk-storage-slots:{}>", planned.len())
    }
}
