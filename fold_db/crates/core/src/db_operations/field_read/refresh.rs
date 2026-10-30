use super::*;
use crate::db_operations::atom_store::MoleculeRangeForm;

pub(super) fn record_resolved_tip_keys(
    field: &FieldVariant,
    values: &HashMap<KeyValue, FieldValue>,
) {
    let Some(molecule_uuid) = field.common().molecule_uuid() else {
        return;
    };
    for key in values.keys() {
        crate::request_phases::record_tip_key(
            molecule_uuid,
            key.hash.as_deref().unwrap_or(""),
            key.range.as_deref().unwrap_or(""),
        );
    }
}

/// Refreshes the field's data from the database.
pub async fn refresh_from_db(
    field: &mut FieldVariant,
    db_ops: &DbOperations,
) -> Result<(), SchemaError> {
    field.require_supported_read(None)?;
    let profile = read_range_profile(field, db_ops).await?;
    crate::schema::types::key_comparison::KeyComparisonContext::run_for_molecule(
        profile,
        captured_field_read(db_ops, refresh_from_db_captured(field, db_ops).boxed()),
    )
    .await
}

async fn refresh_from_db_captured(
    field: &mut FieldVariant,
    db_ops: &DbOperations,
) -> Result<(), SchemaError> {
    let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
        return Ok(());
    };
    if let Some(data) = db_ops
        .atoms()
        .load_molecule_per_key(&molecule_uuid, field.common().storage_prefix())
        .await?
    {
        field.molecule = Some(data);
    }
    Ok(())
}

/// Load the selected key set through the typed core model and disk root.
pub(super) async fn refresh_for_read(
    field: &mut FieldVariant,
    db_ops: &DbOperations,
    filter: &HashRangeFilter,
    include_tombstones: bool,
) -> Result<MoleculeRangeForm, SchemaError> {
    let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
        refresh_from_db(field, db_ops).await?;
        return Ok(MoleculeRangeForm::Storage);
    };
    let narrowed = db_ops
        .atoms()
        .load_filtered_molecule_for_read(
            &molecule_uuid,
            field.kind.filter_layout(),
            field.common().storage_prefix(),
            filter,
            include_tombstones,
        )
        .await?;
    if let Some((data, ranges)) = narrowed {
        field.molecule = Some(data);
        Ok(ranges)
    } else {
        refresh_from_db(field, db_ops).await?;
        Ok(MoleculeRangeForm::Storage)
    }
}
