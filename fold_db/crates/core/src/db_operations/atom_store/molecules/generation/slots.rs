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
}
