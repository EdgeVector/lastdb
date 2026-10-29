use super::*;

impl AtomStore {
    /// A full rewrite requires the molecule's complete key set. A write tail
    /// or a partial warm molecule cannot replace that complete set.
    fn refuse_partial_full_rewrite(
        molecule_uuid: &str,
        data: &MoleculeData,
    ) -> Result<(), SchemaError> {
        if data.is_partial() {
            return Err(SchemaError::InvalidData(format!(
                "refusing full rewrite of molecule {molecule_uuid} from a partial \
                 molecule: it holds {} key(s), not the molecule's complete state",
                data.atom_count()
            )));
        }
        Ok(())
    }

    /// Serialized generation tip records keyed for storage.
    fn tip_record_items(
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        records
            .iter()
            .map(|(base_key, record)| {
                serde_json::to_value(record)
                    .map(|value| (build_storage_key(storage_prefix, base_key), value))
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "serialize generation tip record: {error}"
                        ))
                    })
            })
            .collect()
    }

    /// The molecule header for one activation batch.
    fn activation_structural_items(
        molecule_uuid: &str,
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        // The complete tips land in an immutable generation. Ordinary writers
        // keep appending live `mk:` changes while that body is built; the final
        // pointer and the structural rows below share one small activation
        // batch.
        let header_value = serde_json::to_value(header)
            .map_err(|e| SchemaError::InvalidData(format!("serialize molecule header: {e}")))?;
        Ok(vec![(
            build_storage_key(
                storage_prefix,
                &molecule_key_codec::header_key(molecule_uuid),
            ),
            header_value,
        )])
    }

    /// Append the tip records to `items`.
    fn append_activation_tip_items(
        items: &mut Vec<(String, Value)>,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let tip_items = Self::tip_record_items(records, storage_prefix)?;
        // Transition mirror for existing bounded maintenance readers.
        // LEGACY-SUNSET: class=residue remove after every `mk:` maintenance
        // reader resolves the selected generation and one release dwell shows
        // zero direct-base fallbacks.
        items.extend(tip_items);
        Ok(())
    }

    pub(in super::super) fn prepare_molecule_generation_activation(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
        generation_cut: super::super::PreparedMoleculeGeneration,
    ) -> Result<super::super::PreparedMoleculeGenerationActivation, SchemaError> {
        Self::refuse_partial_full_rewrite(molecule_uuid, data)?;
        let records = self.per_key_storage_records(molecule_uuid, data, domain)?;
        let header = MoleculeHeader::new(data.version(), data.updated_at());

        let mut items = Self::activation_structural_items(molecule_uuid, &header, storage_prefix)?;
        Self::append_activation_tip_items(&mut items, &records, storage_prefix)?;

        // No accounting here: these items reach the activation commit as
        // `activation_items`, and the meters count them there, against the old
        // tips that commit reads.
        Ok(super::super::PreparedMoleculeGenerationActivation {
            molecule_uuid: molecule_uuid.to_string(),
            prepared: generation_cut,
            base_records: records,
            activation_items: items,
        })
    }

    /// Build every immutable molecule body, then select all bodies in one batch.
    pub(crate) async fn store_molecule_generations_batch(
        &self,
        molecules: Vec<(
            String,
            MoleculeData,
            super::super::PreparedMoleculeGeneration,
        )>,
        bodies: &[crate::atom::Atom],
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
    ) -> Result<(), SchemaError> {
        let mut activations = Vec::with_capacity(molecules.len());
        for (molecule_uuid, data, generation_cut) in molecules {
            activations.push(self.prepare_molecule_generation_activation(
                &molecule_uuid,
                &data,
                storage_prefix,
                domain,
                generation_cut,
            )?);
        }
        self.activate_prepared_molecule_generations(activations, bodies, storage_prefix)
            .await
            .map(|_| ())
    }
}
