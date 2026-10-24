//! Preserve the full core slot before a query projects it to a field value.

use super::*;

impl FieldVariant {
    pub(super) fn note_loaded_tip(
        &self,
        db_ops: &Arc<DbOperations>,
        storage_key: &KeyValue,
        api_key: &KeyValue,
    ) -> Result<(), SchemaError> {
        let runtime = db_ops.namespaced_store().disk_pack_runtime();
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(());
        }
        let Some(molecule) = self.common().molecule_uuid() else {
            return Ok(());
        };
        let Some(entry) = self.atom_entry_at_key(storage_key) else {
            return Ok(());
        };
        let scope = self.common().storage_prefix();
        let metadata = self.key_metadata_at_key(storage_key);
        let selected = crate::storage::disk_pack::logical_paths::core_read::selected_tip(
            runtime.clone(),
            scope,
            molecule,
            api_key,
            &entry.atom_uuid,
        )?;
        // A narrowed reader already recorded the actual winner before API-key
        // projection. A reconstructed primary key must not replace that receipt.
        let matching = selected.as_ref().is_some_and(|selected| {
            selected.entry == *entry && selected.metadata.as_ref() == metadata
        });
        if matching
            && selected
                .as_ref()
                .is_some_and(|selected| selected.sources.is_some() || selected.tip_source.is_none())
        {
            // Complete sources already exist. A location-only receipt cannot
            // gain direct authority from a reconstructed key either.
            return Ok(());
        }
        if !matching {
            let source = self.slot_coordinates_ref(storage_key).map(|(hash, range)| {
                crate::schema::types::field::build_storage_key(
                    scope,
                    &crate::atom::molecule_key_codec::hash_range_record_key(molecule, hash, range),
                )
            });
            crate::storage::disk_pack::logical_paths::core_read::note_tip(
                runtime,
                scope,
                molecule,
                api_key,
                entry,
                metadata,
                source.as_deref(),
            )?;
        }
        Self::note_query_tip_candidates(db_ops, scope, molecule, api_key, &entry.atom_uuid)
    }

    pub(super) fn note_query_tip_candidates(
        db_ops: &Arc<DbOperations>,
        scope: Option<&str>,
        molecule: &str,
        api_key: &KeyValue,
        atom_uuid: &str,
    ) -> Result<(), SchemaError> {
        let runtime = db_ops.namespaced_store().disk_pack_runtime();
        if !crate::storage::disk_pack::read_snapshot_active(runtime.clone()) {
            return Ok(());
        }
        let selected = crate::storage::disk_pack::logical_paths::core_read::selected_tip(
            runtime.clone(),
            scope,
            molecule,
            api_key,
            atom_uuid,
        )?;
        if selected.is_none_or(|selected| selected.tip_source.is_none()) {
            return Ok(());
        }
        let base = db_ops
            .atoms()
            .key_codec_for_molecule(molecule)?
            .api_hash_range_record_key(
                molecule,
                api_key.hash.as_deref().unwrap_or_default(),
                api_key.range.as_deref().unwrap_or_default(),
            )
            .map_err(|error| SchemaError::InvalidData(error.to_string()))?;
        crate::storage::disk_pack::logical_paths::core_read::note_candidates(
            runtime,
            scope,
            molecule,
            api_key,
            atom_uuid,
            std::slice::from_ref(&base),
        )?;
        Ok(())
    }
}
