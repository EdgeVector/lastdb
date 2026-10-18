use super::*;

impl AtomStore {
    /// Batch-read exact `mk:` keys for one page.
    ///
    /// `base_keys` are already encoded
    /// ([`molecule_key_codec::hash_range_record_key`]). This does not re-encode
    /// API hash/range candidates: a storage-form page key passed through
    /// [`Self::load_per_key_records_for_slots`] would be blinded again, and
    /// that helper also reads deletes when no generation pointer exists.
    ///
    /// One `get_items` reads every base key. Each molecule pays one generation
    /// pointer read. Generation slots and sparse deletes are read only when
    /// that pointer exists, matching [`Self::get_per_key`]. A molecule with no
    /// pointer does not read deletes, so a delete without an active generation
    /// cannot hide a record. Output slot `i` is the merged record for
    /// `base_keys[i]`, or `None` when the key is absent.
    pub(crate) async fn load_exact_per_key_records(
        &self,
        storage_prefix: Option<&str>,
        base_keys: &[String],
    ) -> Result<Vec<Option<PerKeyRecord>>, SchemaError> {
        if base_keys.is_empty() {
            return Ok(Vec::new());
        }
        let storage_keys: Vec<String> = base_keys
            .iter()
            .map(|base| build_storage_key(storage_prefix, base))
            .collect();
        let mut hit = self
            .main_store
            .get_items::<PerKeyRecord>(&storage_keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("batch read per-key records: {e}")))?;

        let mut slots_by_molecule: std::collections::BTreeMap<&str, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (slot, base_key) in base_keys.iter().enumerate() {
            if let Some(molecule_uuid) =
                molecule_key_codec::molecule_uuid_from_storage_key(base_key)
            {
                slots_by_molecule
                    .entry(molecule_uuid)
                    .or_default()
                    .push(slot);
            }
        }

        for (molecule_uuid, slots) in &slots_by_molecule {
            let Some(pointer) = self
                .active_molecule_generation(molecule_uuid, storage_prefix)
                .await?
            else {
                continue;
            };
            let mut generation_keys = Vec::new();
            let mut generation_slots = Vec::new();
            for &slot in slots {
                if let Some(key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
                    molecule_uuid,
                    &pointer.generation,
                    &base_keys[slot],
                ) {
                    generation_keys.push(build_storage_key(storage_prefix, &key));
                    generation_slots.push(slot);
                }
            }
            let generation = self
                .main_store
                .get_items::<super::super::super::MoleculeGenerationSlot>(&generation_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "batch read generation records for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for (slot, base) in generation_slots.into_iter().zip(generation) {
                if base.is_some() {
                    hit[slot] = Self::merge_generation_slot(hit[slot].take(), base);
                }
            }

            let mut delete_keys = Vec::new();
            let mut delete_slots = Vec::new();
            for &slot in slots {
                if let Some(key) =
                    molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                        molecule_uuid,
                        &base_keys[slot],
                    )
                {
                    delete_keys.push(build_storage_key(storage_prefix, &key));
                    delete_slots.push(slot);
                }
            }
            let deletes = self
                .main_store
                .get_items::<super::super::super::MoleculeGenerationDelete>(&delete_keys)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "batch read generation deletes for molecule {molecule_uuid}: {error}"
                    ))
                })?;
            for (slot, deletion) in delete_slots.into_iter().zip(deletes) {
                if deletion.is_some() {
                    hit[slot] = Self::apply_generation_delete(hit[slot].take(), deletion);
                }
            }
        }
        self.merge_captured_exact_tips(storage_prefix, base_keys, slots_by_molecule, &mut hit)?;
        Ok(hit)
    }

    /// Batch-fetch per-key molecule records for a known set of `(hash, range)`
    /// slots — the co-key multi-field list path's secondary-field fan-out.
    ///
    /// One `get_many` over the storage layer (single `spawn_blocking` hop)
    /// instead of N sequential `get_per_key` calls. Missing keys yield no
    /// entry (sparse fields). Each output position matches its input slot and
    /// retains the actual winning storage key, including fallback spellings.
    pub(crate) async fn load_per_key_records_for_slots(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<Vec<Option<(String, PerKeyRecord)>>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let api_keys: Vec<_> = keys
            .iter()
            .map(|(hash, range)| {
                crate::schema::types::key_value::KeyValue::new(
                    Some(hash.clone()),
                    Some(range.clone()),
                )
            })
            .collect();
        self.load_query_tip_records(storage_prefix, molecule_uuid, &api_keys)
            .await
    }
}
