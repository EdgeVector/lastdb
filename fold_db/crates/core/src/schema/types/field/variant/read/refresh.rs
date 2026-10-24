use super::*;
use crate::db_operations::atom_store::MoleculeRangeForm;

impl FieldVariant {
    pub(super) fn record_resolved_tip_keys(&self, values: &HashMap<KeyValue, FieldValue>) {
        let Some(molecule_uuid) = self.common().molecule_uuid() else {
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
    pub async fn refresh_from_db(&mut self, db_ops: &DbOperations) -> Result<(), SchemaError> {
        self.require_supported_read(None)?;
        captured_field_read(db_ops, self.refresh_from_db_captured(db_ops).boxed()).await
    }

    async fn refresh_from_db_captured(&mut self, db_ops: &DbOperations) -> Result<(), SchemaError> {
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            return Ok(());
        };
        if let Some(data) = db_ops
            .atoms()
            .load_molecule_per_key(&molecule_uuid, self.common().storage_prefix())
            .await?
        {
            self.molecule = Some(data);
        }
        Ok(())
    }

    /// Load the selected key set through the typed core model and disk root.
    pub(super) async fn refresh_for_read(
        &mut self,
        db_ops: &DbOperations,
        filter: &HashRangeFilter,
        include_tombstones: bool,
    ) -> Result<MoleculeRangeForm, SchemaError> {
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            self.refresh_from_db(db_ops).await?;
            return Ok(MoleculeRangeForm::Storage);
        };
        let narrowed = db_ops
            .atoms()
            .load_filtered_molecule_for_read(
                &molecule_uuid,
                self.kind.filter_layout(),
                self.common().storage_prefix(),
                filter,
                include_tombstones,
            )
            .await?;
        if let Some((data, ranges)) = narrowed {
            self.molecule = Some(data);
            Ok(ranges)
        } else {
            self.refresh_from_db(db_ops).await?;
            Ok(MoleculeRangeForm::Storage)
        }
    }
}
