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
    Ok(targets
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
            Some(target)
        })
        .collect())
}
