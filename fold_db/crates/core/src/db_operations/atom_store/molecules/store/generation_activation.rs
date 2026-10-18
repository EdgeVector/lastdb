use super::*;

impl AtomStore {
    /// Archived tip versions for the tip-version chain (`tv:{id}`).
    fn tip_version_items(
        data: &MoleculeData,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut items = Vec::new();
        for (version_id, entry) in data.pending_tip_versions() {
            let value = serde_json::to_value(entry).map_err(|e| {
                SchemaError::InvalidData(format!("serialize tip version {version_id}: {e}"))
            })?;
            items.push((
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::tip_version_key(version_id),
                ),
                value,
            ));
        }
        Ok(items)
    }

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

    /// Tip versions, backrefs, atom-ref edges, and the molecule header row
    /// for one activation batch.
    fn activation_structural_items(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        records: &[(String, PerKeyRecord)],
        header: &MoleculeHeader,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut items: Vec<(String, Value)> = Vec::with_capacity(records.len() + 2);
        // The complete tips land in an immutable generation. Ordinary writers
        // keep appending live `mk:` changes while that body is built; the final
        // pointer and the structural rows below share one small activation
        // batch.
        items.extend(Self::tip_version_items(data, storage_prefix)?);
        // Derived atom -> archived-tip access pattern. These rows land in the
        // same durable batch as their authoritative `tv:` nodes, including
        // replay/full-rewrite paths.
        items.extend(self.pending_tip_version_backref_items(
            molecule_uuid,
            data,
            records,
            storage_prefix,
        )?);
        // Shadow atom-reference edges share this durable batch with the `mk:`
        // tips and optional `tv:` history rows they describe.
        items.extend(self.pending_atom_ref_edge_items(
            molecule_uuid,
            data,
            records,
            storage_prefix,
        )?);
        let header_value = serde_json::to_value(header)
            .map_err(|e| SchemaError::InvalidData(format!("serialize molecule header: {e}")))?;
        items.push((
            build_storage_key(
                storage_prefix,
                &molecule_key_codec::header_key(molecule_uuid),
            ),
            header_value,
        ));
        Ok(items)
    }

    /// Append tip records, automatic-GC reference markers and the transition
    /// mirror to `items`; returns the tip record items.
    fn append_activation_tip_items(
        &self,
        items: &mut Vec<(String, Value)>,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let tip_items = Self::tip_record_items(records, storage_prefix)?;
        let automatic_gc_uuids = self.automatic_gc_tip_reference_uuids(&tip_items, storage_prefix);
        items
            .extend(self.automatic_gc_reference_marker_items(&automatic_gc_uuids, storage_prefix)?);
        // Transition mirror for existing bounded maintenance readers.
        // LEGACY-SUNSET: class=residue remove after every `mk:` maintenance
        // reader resolves the selected generation and one release dwell shows
        // zero direct-base fallbacks.
        items.extend(tip_items.clone());
        Ok(tip_items)
    }

    pub(in super::super) async fn prepare_molecule_generation_activation(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
        generation_cut: super::super::PreparedMoleculeGeneration,
    ) -> Result<super::super::PreparedMoleculeGenerationActivation, SchemaError> {
        Self::refuse_partial_full_rewrite(molecule_uuid, data)?;
        let records = self.per_key_storage_records(molecule_uuid, data, domain)?;
        let header = MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        };

        let mut items = self.activation_structural_items(
            molecule_uuid,
            data,
            &records,
            &header,
            storage_prefix,
        )?;
        let tip_items = self.append_activation_tip_items(&mut items, &records, storage_prefix)?;

        refuse_legacy_ref_blob_store_items(&items)?;
        self.account_keep_small_items(&tip_items);
        self.account_keep_small_items(&items);
        // Debounced persist only — never `flush_keep_small` on a write path.
        // The 2026-09-21 primary restart loop came from a per-write flush
        // here: every activation re-appended the whole per-molecule counter
        // map (~7 MB) into one `metadata` hash group, ~16 GB/hour.
        let _ = self.persist_keep_small().await;
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
        storage_prefix: Option<&str>,
        domain: MoleculeKeyDomain,
    ) -> Result<(), SchemaError> {
        let mut activations = Vec::with_capacity(molecules.len());
        for (molecule_uuid, data, generation_cut) in molecules {
            activations.push(
                self.prepare_molecule_generation_activation(
                    &molecule_uuid,
                    &data,
                    storage_prefix,
                    domain,
                    generation_cut,
                )
                .await?,
            );
        }
        self.activate_prepared_molecule_generations(activations, storage_prefix)
            .await
            .map(|_| ())
    }
}
