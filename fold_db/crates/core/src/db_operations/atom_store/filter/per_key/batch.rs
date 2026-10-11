use super::*;

/// Per-slot record and the storage key it was read from.
struct SlotHits {
    record: Vec<Option<PerKeyRecord>>,
    key: Vec<Option<String>>,
}

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

        for (molecule_uuid, slots) in slots_by_molecule {
            let Some(pointer) = self
                .active_molecule_generation(molecule_uuid, storage_prefix)
                .await?
            else {
                continue;
            };
            let mut generation_keys = Vec::new();
            let mut generation_slots = Vec::new();
            for &slot in &slots {
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
            for &slot in &slots {
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
        Ok(hit)
    }

    /// Batch-fetch per-key molecule records for a known set of `(hash, range)`
    /// slots — the co-key multi-field list path's secondary-field fan-out.
    ///
    /// One `get_many` over the storage layer (single `spawn_blocking` hop)
    /// instead of N sequential `get_per_key` calls. Missing keys yield no
    /// entry (sparse fields). Order of `keys` is preserved for present hits
    /// only by walking the input list.
    pub(crate) async fn load_per_key_records_for_slots(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<Vec<(String, String, PerKeyRecord)>, SchemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let (storage_keys, slot_of) =
            self.slot_storage_keys(molecule_uuid, storage_prefix, keys)?;
        let mut hits = self
            .read_slot_base_hits(molecule_uuid, &storage_keys, &slot_of, keys.len())
            .await?;
        self.overlay_slot_generation_records(
            molecule_uuid,
            storage_prefix,
            &storage_keys,
            &slot_of,
            &mut hits,
        )
        .await?;
        self.overlay_slot_generation_deletes(
            molecule_uuid,
            storage_prefix,
            &storage_keys,
            &slot_of,
            &mut hits,
        )
        .await?;
        self.retain_unblocked_slot_hits(keys, hits).await
    }

    /// Storage-form candidate keys for every API `(hash, range)` slot, plus
    /// the originating slot of each candidate. Dual-read across molecule-UUID
    /// spellings (write-form base64url and legacy hex) and across hash/range
    /// HMAC keys bound to each spelling.
    fn slot_storage_keys(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        keys: &[(String, String)],
    ) -> Result<(Vec<String>, Vec<usize>), SchemaError> {
        let codec = self.key_codec_for_molecule(molecule_uuid);
        let mut storage_keys: Vec<String> = Vec::new();
        let mut slot_of: Vec<usize> = Vec::new();
        for (i, (api_hash, api_range)) in keys.iter().enumerate() {
            let candidates = codec
                .api_hash_range_record_keys_for_read(molecule_uuid, api_hash, api_range)
                .map_err(|e| SchemaError::InvalidData(e.to_string()))?;
            for base in candidates {
                storage_keys.push(build_storage_key(storage_prefix, &base));
                slot_of.push(i);
            }
        }
        Ok((storage_keys, slot_of))
    }

    /// One `get_items` over every candidate key; the first present candidate
    /// of each slot wins.
    async fn read_slot_base_hits(
        &self,
        molecule_uuid: &str,
        storage_keys: &[String],
        slot_of: &[usize],
        slot_count: usize,
    ) -> Result<SlotHits, SchemaError> {
        let fetched = self
            .main_store
            .get_items::<PerKeyRecord>(storage_keys)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!(
                    "batch read per-key records for molecule {molecule_uuid}: {e}"
                ))
            })?;
        let mut hits = SlotHits {
            record: vec![None; slot_count],
            key: vec![None; slot_count],
        };
        for ((slot, storage_key), rec) in slot_of.iter().copied().zip(storage_keys).zip(fetched) {
            if hits.record[slot].is_none() {
                if let Some(rec) = rec {
                    hits.record[slot] = Some(rec);
                    hits.key[slot] = Some(storage_key.clone());
                }
            }
        }
        Ok(hits)
    }

    /// Merge the active generation's slot rows over the base hits.
    async fn overlay_slot_generation_records(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        storage_keys: &[String],
        slot_of: &[usize],
        hits: &mut SlotHits,
    ) -> Result<(), SchemaError> {
        let Some(pointer) = self
            .active_molecule_generation(molecule_uuid, storage_prefix)
            .await?
        else {
            return Ok(());
        };
        let strip = build_storage_key(storage_prefix, "");
        let mut generation_keys = Vec::new();
        let mut generation_slot_of = Vec::new();
        for (slot, storage_key) in storage_keys.iter().enumerate() {
            let bare = storage_key.strip_prefix(&strip).unwrap_or(storage_key);
            if let Some(key) = molecule_key_codec::molecule_generation_bound_for_record_bound(
                molecule_uuid,
                &pointer.generation,
                bare,
            ) {
                generation_keys.push(build_storage_key(storage_prefix, &key));
                generation_slot_of.push((slot_of[slot], storage_key.clone()));
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
        for ((slot, storage_key), base) in generation_slot_of.into_iter().zip(generation) {
            if base.is_some() {
                hits.record[slot] = Self::merge_generation_slot(hits.record[slot].take(), base);
                hits.key[slot] = Some(storage_key);
            }
        }
        Ok(())
    }

    /// Apply generation delete markers to the hits.
    async fn overlay_slot_generation_deletes(
        &self,
        molecule_uuid: &str,
        storage_prefix: Option<&str>,
        storage_keys: &[String],
        slot_of: &[usize],
        hits: &mut SlotHits,
    ) -> Result<(), SchemaError> {
        let strip = build_storage_key(storage_prefix, "");
        let mut delete_keys = Vec::new();
        let mut delete_slot_of = Vec::new();
        for (candidate, slot) in storage_keys.iter().zip(slot_of.iter().copied()) {
            let bare = candidate.strip_prefix(&strip).unwrap_or(candidate);
            if let Some(key) = molecule_key_codec::molecule_generation_delete_bound_for_record_bound(
                molecule_uuid,
                bare,
            ) {
                delete_keys.push(build_storage_key(storage_prefix, &key));
                delete_slot_of.push(slot);
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
        for (slot, deletion) in delete_slot_of.into_iter().zip(deletes) {
            if deletion.is_some() {
                hits.record[slot] =
                    Self::apply_generation_delete(hits.record[slot].take(), deletion);
            }
        }
        Ok(())
    }

    /// Drop hits that a winning Delete barrier blocks; keep input order.
    async fn retain_unblocked_slot_hits(
        &self,
        keys: &[(String, String)],
        hits: SlotHits,
    ) -> Result<Vec<(String, String, PerKeyRecord)>, SchemaError> {
        let present_keys: Vec<String> = hits
            .record
            .iter()
            .enumerate()
            .filter_map(|(slot, rec)| rec.as_ref().and(hits.key[slot].as_ref()).cloned())
            .collect();
        let barriers = self.winning_delete_barriers(&present_keys).await?;
        let mut out = Vec::with_capacity(keys.len());
        for (slot, ((hash, range), rec)) in keys.iter().zip(hits.record).enumerate() {
            if let Some(rec) = rec {
                let final_key = hits.key[slot].as_deref().ok_or_else(|| {
                    SchemaError::InvalidData("per-key record has no storage key".into())
                })?;
                if barriers
                    .get(final_key)
                    .is_none_or(|barrier| !barrier.blocks_tip(&rec.entry))
                {
                    out.push((hash.clone(), range.clone(), rec));
                }
            }
        }
        Ok(out)
    }
}
