//! Exact current-tip checks for maintenance targets under their tip locks.

use super::*;

pub(super) async fn current_storage_slot_targets<'a>(
    db_ops: &Arc<DbOperations>,
    schema: &crate::schema::Schema,
    targets: &'a [StorageSlotPurgeTarget],
) -> Result<Vec<&'a StorageSlotPurgeTarget>, SchemaError> {
    let keys: Vec<String> = targets
        .iter()
        .map(StorageSlotPurgeTarget::storage_record_key)
        .collect();
    let current = db_ops
        .atoms()
        .delete_target_tips_with_generations(&keys, None)
        .await?;
    let candidates: Vec<_> = targets
        .iter()
        .zip(current)
        .filter_map(|(target, current)| {
            let current = current?;
            let field = schema.runtime_fields.get(&target.field_name)?;
            if current_atom_for_key(field, &target.storage_key).as_deref()
                != Some(current.entry.atom_uuid.as_str())
            {
                return None;
            }
            if target
                .expected_tombstone
                .as_ref()
                .is_some_and(|expected| expected != &current.entry.atom_uuid)
            {
                return None;
            }
            Some((target, current.entry.atom_uuid))
        })
        .collect();
    let uuids: Vec<&str> = candidates.iter().map(|(_, uuid)| uuid.as_str()).collect();
    let atoms = db_ops.atoms().get_atoms(&uuids, None).await?;
    Ok(candidates
        .into_iter()
        .zip(atoms)
        .filter_map(|((target, _), atom)| {
            if target.expected_tombstone.is_some()
                && !atom.is_some_and(|atom| crate::atom::is_tombstone_value(atom.content()))
            {
                return None;
            }
            Some(target)
        })
        .collect())
}
