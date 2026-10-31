use super::*;

impl AtomStore {
    pub(crate) async fn active_molecule_generation(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationPointer>, SchemaError> {
        let key = build_storage_key(
            storage_prefix,
            &molecule_key_codec::molecule_generation_pointer_key(molecule_uuid),
        );
        self.molecule_control_from_memory(&key)
            .await
            .map_err(|error| match error {
                SchemaError::ReadResourceLimit { .. } | SchemaError::WriteResourceLimit { .. } => {
                    error
                }
                error => SchemaError::InvalidData(format!(
                    "read generation pointer for molecule {molecule_uuid}: {error}"
                )),
            })
    }

    /// Select one logical slot from the immutable base and the live change row.
    pub(crate) fn merge_generation_slot(
        live: Option<PerKeyRecord>,
        base: Option<MoleculeGenerationSlot>,
    ) -> Option<PerKeyRecord> {
        super::super::super::helpers::merge_generation_slot(live, base)
    }

    pub(crate) fn apply_generation_delete(
        record: Option<PerKeyRecord>,
        deletion: Option<MoleculeGenerationDelete>,
    ) -> Option<PerKeyRecord> {
        super::super::super::helpers::apply_generation_delete(record, deletion)
    }
}
