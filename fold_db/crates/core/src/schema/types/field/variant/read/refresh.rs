use super::*;

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
        self.require_supported_read(None, false)?;
        captured_field_read(db_ops, self.refresh_from_db_captured(db_ops).boxed()).await
    }

    async fn refresh_from_db_captured(&mut self, db_ops: &DbOperations) -> Result<(), SchemaError> {
        super::super::super::base::refresh_field_molecule_from_db(
            &mut self.inner,
            &mut self.molecule,
            self.kind,
            db_ops,
        )
        .await
    }

    /// Load the selected key set through the typed core model and disk root.
    pub(super) async fn refresh_for_read(
        &mut self,
        db_ops: &DbOperations,
        filter: &HashRangeFilter,
        include_tombstones: bool,
    ) -> Result<(), SchemaError> {
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            return self.refresh_from_db(db_ops).await;
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
        match narrowed {
            Some(data) if self.kind.matches_data(&data) => self.molecule = Some(data),
            _ => self.refresh_from_db(db_ops).await?,
        }
        Ok(())
    }
}
