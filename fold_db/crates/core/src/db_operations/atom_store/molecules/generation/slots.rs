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
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read generation pointer for molecule {molecule_uuid}: {error}"
                ))
            })
    }

    /// Select one logical slot from the immutable base and the live change row.
    pub(crate) fn merge_generation_slot(
        live: Option<PerKeyRecord>,
        base: Option<MoleculeGenerationSlot>,
    ) -> Option<PerKeyRecord> {
        match base {
            None
            | Some(MoleculeGenerationSlot {
                record: None,
                shadow: None,
            }) => live,
            Some(MoleculeGenerationSlot {
                record: Some(base), ..
            }) => match live {
                Some(live) if incoming_record_wins(&live, &base) => Some(live),
                _ => Some(base),
            },
            Some(MoleculeGenerationSlot {
                record: None,
                shadow: Some(shadow),
            }) => live
                .filter(|candidate| incoming_wins_lww(candidate.entry.lww_key(), shadow.lww_key())),
        }
    }

    pub(crate) async fn generation_slot_for_record_key(
        &self,
        base_key: &str,
        generation: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationSlot>, SchemaError> {
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_key)
        else {
            return Ok(None);
        };
        let Some(generation_key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
            molecule_uuid,
            generation,
            base_key,
        ) else {
            return Ok(None);
        };
        let storage_key = build_storage_key(storage_prefix, &generation_key);
        self.main_store
            .get_item(&storage_key)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read molecule generation slot {storage_key}: {error}"
                ))
            })
    }

    pub(crate) async fn generation_delete_for_record_key(
        &self,
        base_key: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Option<MoleculeGenerationDelete>, SchemaError> {
        let Some(molecule_uuid) = molecule_key_codec::molecule_uuid_from_storage_key(base_key)
        else {
            return Ok(None);
        };
        let Some(key) = molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
            molecule_uuid,
            base_key,
        ) else {
            return Ok(None);
        };
        self.main_store
            .get_item(&build_storage_key(storage_prefix, &key))
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!(
                    "read generation delete for molecule {molecule_uuid}: {error}"
                ))
            })
    }

    pub(crate) fn apply_generation_delete(
        record: Option<PerKeyRecord>,
        deletion: Option<MoleculeGenerationDelete>,
    ) -> Option<PerKeyRecord> {
        match (record, deletion) {
            (Some(record), Some(deletion))
                if !incoming_wins_lww(record.entry.lww_key(), deletion.shadow.lww_key()) =>
            {
                None
            }
            (record, _) => record,
        }
    }
}
