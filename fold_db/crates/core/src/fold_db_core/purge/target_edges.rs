//! Exact current-tip edge keys for purge; no archived-node traversal.
use super::*;

pub(crate) fn collect_target_edges(
    db_ops: &Arc<DbOperations>,
    field: &crate::schema::types::field::FieldVariant,
    key: &KeyValue,
) -> Result<Vec<String>, SchemaError> {
    let Some((hash, range)) = field.slot_coordinates(key) else {
        return Ok(Vec::new());
    };
    let Some(molecule) = field.common().molecule_uuid() else {
        return Ok(Vec::new());
    };
    let Some(entry) = field
        .molecule_data()
        .and_then(|data| data.get_atom_entry(&hash, &range))
    else {
        return Ok(Vec::new());
    };
    Ok(vec![db_ops.atoms().atom_ref_tip_edge_v2_key(
        molecule,
        &hash,
        &range,
        entry,
        field.common().storage_prefix(),
    )?])
}
