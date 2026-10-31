//! Exact current reads use the shared core model before the disk resolver.

use super::*;
use crate::schema::types::KeyValuePageOrder;

/// A missing filter cannot certify an exact key selection.
pub(super) async fn resolve_core_model(
    field: &FieldVariant,
    db_ops: &Arc<DbOperations>,
    filter: Option<&HashRangeFilter>,
    window: Option<KeyWindow>,
    include_tombstones: bool,
) -> Result<Option<HashMap<KeyValue, FieldValue>>, SchemaError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let Some(mut matches) = crate::storage::disk_pack::logical_paths::core_filter::matches(
        db_ops.namespaced_store().disk_pack_runtime(),
        field.common().storage_prefix(),
        field.common().molecule_uuid().map(String::as_str),
        field.kind,
        filter,
        include_tombstones,
    )?
    else {
        return Ok(None);
    };
    if let Some(window) = window {
        matches.sort_by(|(a, ..), (b, ..)| a.cmp_page_order(b));
        matches.dedup_by(|(a, ..), (b, ..)| a == b);
        matches = match window {
            KeyWindow::Offset { offset, limit } => {
                matches.into_iter().skip(offset).take(limit).collect()
            }
            KeyWindow::After { after, limit } => matches
                .into_iter()
                .filter(|(key, ..)| key.cmp_page_order(&after).is_gt())
                .take(limit)
                .collect(),
        };
    }
    let mut values = fetch_atoms_with_key_metadata_async_with_prefix(
        db_ops,
        matches,
        field.common().storage_prefix(),
        field.common().molecule_uuid().map(String::as_str),
    )
    .await?;
    retain_visible_values(&mut values, include_tombstones);
    field.stamp_molecule_meta(&mut values);
    record_resolved_tip_keys(field, &values);
    Ok(Some(values))
}
