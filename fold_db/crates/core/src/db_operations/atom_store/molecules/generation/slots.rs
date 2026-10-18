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
