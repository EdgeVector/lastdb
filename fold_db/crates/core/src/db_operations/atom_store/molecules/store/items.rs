use super::*;

impl AtomStore {
    /// Keep one maximum-version value per exact `mk:` key, then compare it
    /// with the durable value while the caller holds that key's tip lock.
    pub(in super::super) async fn retain_durable_tip_winners(
        &self,
        items: &mut Vec<(String, Value)>,
    ) -> Result<(), SchemaError> {
        // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
        let mut ordinary = Vec::with_capacity(items.len());
        let mut tips: HashMap<String, PerKeyRecord> = HashMap::new();
        for (key, value) in items.drain(..) {
            if !is_tip_item_key(&key) {
                ordinary.push((key, value));
                continue;
            }
            let incoming: PerKeyRecord = serde_json::from_value(value).map_err(|error| {
                SchemaError::InvalidData(format!("parse pending durable tip {key}: {error}"))
            })?;
            match tips.entry(key) {
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(incoming);
                }
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    let legacy_last_write = is_legacy_zero_clock(&incoming.entry)
                        && is_legacy_zero_clock(&slot.get().entry);
                    if legacy_last_write
                        || crate::atom::incoming_wins_lww(
                            incoming.entry.lww_key(),
                            slot.get().entry.lww_key(),
                        )
                    {
                        slot.insert(incoming);
                    }
                }
            }
        }
        let tip_keys: Vec<String> = tips.keys().cloned().collect();
        let barriers = self.winning_delete_barriers(&tip_keys).await?;
        let eligible_keys: Vec<String> = tip_keys
            .into_iter()
            .filter(|key| {
                !barriers
                    .get(key)
                    .is_some_and(|barrier| barrier.blocks_tip(&tips[key].entry))
            })
            .collect();
        // Keep this key order for the batch result. A blocked tip must not
        // read its durable value, even when that value is malformed.
        let durable_tips = self.delete_target_tips(&eligible_keys).await?;
        let mut accepted_slots = std::collections::HashSet::new();
        let mut accepted_tip_edges = std::collections::HashSet::new();
        for (key, durable) in eligible_keys.into_iter().zip(durable_tips) {
            let incoming = tips.remove(&key).expect("eligible tip must exist");
            if durable.as_ref().is_some_and(|current| {
                let legacy_last_write =
                    is_legacy_zero_clock(&incoming.entry) && is_legacy_zero_clock(&current.entry);
                !legacy_last_write
                    && !crate::atom::incoming_wins_lww(
                        incoming.entry.lww_key(),
                        current.entry.lww_key(),
                    )
            }) {
                continue;
            }
            let value = serde_json::to_value(&incoming).map_err(|error| {
                SchemaError::InvalidData(format!("serialize durable tip winner {key}: {error}"))
            })?;
            let base_key = key.find("mk:").map_or(key.as_str(), |at| &key[at..]);
            if let (Some(molecule), Some((hash, range))) = (
                molecule_key_codec::molecule_uuid_from_storage_key(base_key),
                molecule_key_codec::decode_hash_range_any(base_key),
            ) {
                let slot = (molecule.to_string(), hash, range);
                accepted_tip_edges.insert((
                    slot.0.clone(),
                    slot.1.clone(),
                    slot.2.clone(),
                    incoming.entry.atom_uuid.clone(),
                    format!(
                        "{:020}:{}:{}",
                        incoming.entry.written_at,
                        incoming.entry.lww_device(),
                        incoming.entry.atom_uuid
                    ),
                ));
                accepted_slots.insert(slot);
            }
            ordinary.push((key, value));
        }
        // Tip-edge transitions are part of the same winner decision. A delayed
        // losing flush must not publish an active loser edge or deactivate the
        // durable winner's edge. Tip-version edges remain valid history.
        ordinary.retain(|(_, value)| {
            if value.get("edge_type").and_then(Value::as_str) != Some("tip") {
                return true;
            }
            let Some(molecule) = value.get("molecule_uuid").and_then(Value::as_str) else {
                return true;
            };
            let Some(hash) = value.get("disk_hash").and_then(Value::as_str) else {
                return true;
            };
            let Some(range) = value.get("disk_range").and_then(Value::as_str) else {
                return true;
            };
            let slot = (molecule.to_string(), hash.to_string(), range.to_string());
            if !accepted_slots.contains(&slot) {
                return false;
            }
            if !value
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return true;
            }
            let Some(atom) = value.get("atom_uuid").and_then(Value::as_str) else {
                return false;
            };
            let Some(version) = value.get("version_id").and_then(Value::as_str) else {
                return false;
            };
            accepted_tip_edges.contains(&(
                slot.0,
                slot.1,
                slot.2,
                atom.to_string(),
                version.to_string(),
            ))
        });
        *items = ordinary;
        Ok(())
    }

    /// Build one molecule's items for a changed-key write.
    pub(in super::super) async fn changed_key_store_items(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &std::collections::HashSet<ChangedKey>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut records = Vec::with_capacity(changed.len());
        for ck in changed {
            let api_hash = ck.disk_hash();
            let api_range = ck.disk_range();
            if let Some(entry) = data.get_atom_entry(api_hash, api_range) {
                let storage_hash = self.storage_hash(molecule_uuid, api_hash)?;
                let storage_range = self.storage_range(molecule_uuid, api_range)?;
                records.push((
                    molecule_key_codec::hash_range_record_key(
                        molecule_uuid,
                        &storage_hash,
                        &storage_range,
                    ),
                    PerKeyRecord {
                        entry: entry.clone(),
                        meta: data.get_key_metadata(api_hash, api_range).cloned(),
                    },
                ));
            }
        }
        let header = MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        };

        // Page index (`mhr:`) is retired — see HASH_RANGE_PAGE_INDEX_ENABLED.
        let include_hash_range_page_index =
            super::super::super::helpers::HASH_RANGE_PAGE_INDEX_ENABLED;
        let mut items: Vec<(String, Value)> = Vec::with_capacity(
            records.len()
                + if include_hash_range_page_index {
                    records.len() * 2 + 2
                } else {
                    2
                },
        );
        for (base_key, rec) in &records {
            let value = serde_json::to_value(rec)
                .map_err(|e| SchemaError::InvalidData(format!("serialize per-key record: {e}")))?;
            items.push((build_storage_key(storage_prefix, base_key), value));
        }
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
        items.extend(self.pending_tip_version_backref_items(
            molecule_uuid,
            data,
            &records,
            storage_prefix,
        )?);
        items.extend(self.pending_atom_ref_edge_items(
            molecule_uuid,
            data,
            &records,
            storage_prefix,
        )?);
        if include_hash_range_page_index {
            items.extend(records.iter().filter_map(|(base_key, _)| {
                hash_range_page_index_item_for_record_key(molecule_uuid, base_key, storage_prefix)
            }));
        }
        // HashKey lookup markers (`mhk:`) — retired co-write; see
        // HASH_RANGE_HASH_KEY_LOOKUP_ENABLED.
        if super::super::super::helpers::HASH_RANGE_HASH_KEY_LOOKUP_ENABLED {
            items.extend(
                self.hash_key_lookup_items_for_changed_records(
                    molecule_uuid,
                    &records,
                    storage_prefix,
                )
                .await?,
            );
        }
        let header_value = serde_json::to_value(&header)
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

    #[cfg(any(feature = "sharing", test))]
    pub(in super::super) fn hash_key_lookup_items_for_records(
        molecule_uuid: &str,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut by_hash: HashMap<String, Option<(String, PerKeyRecord)>> = HashMap::new();
        for (base_key, rec) in records {
            let Some((hash, range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                continue;
            };
            by_hash
                .entry(hash)
                .and_modify(|slot| {
                    if slot
                        .as_ref()
                        .is_none_or(|(seen_range, _)| seen_range != &range)
                    {
                        *slot = None;
                    }
                })
                .or_insert_with(|| Some((range, rec.clone())));
        }

        by_hash
            .into_iter()
            .map(|(hash, unique)| {
                let unique_ref = unique.as_ref().map(|(range, rec)| (range.as_str(), rec));
                hash_key_lookup_item(molecule_uuid, &hash, unique_ref, storage_prefix)
            })
            .collect()
    }

    pub(in super::super) async fn hash_key_lookup_items_for_changed_records(
        &self,
        molecule_uuid: &str,
        records: &[(String, PerKeyRecord)],
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let mut changed_by_hash: HashMap<String, Vec<(String, PerKeyRecord)>> = HashMap::new();
        for (base_key, rec) in records {
            let Some((hash, range)) =
                molecule_key_codec::decode_hash_range(base_key, molecule_uuid)
            else {
                continue;
            };
            changed_by_hash
                .entry(hash)
                .or_default()
                .push((range, rec.clone()));
        }

        let mut items = Vec::with_capacity(changed_by_hash.len());
        for (hash, changed) in changed_by_hash {
            let mut by_range: HashMap<String, PerKeyRecord> = HashMap::new();
            let existing = self
                .scan_per_key_prefix_paged(
                    &molecule_key_codec::hash_range_scan_prefix_for_hash(molecule_uuid, &hash),
                    storage_prefix,
                    2,
                )
                .await?;
            for (base_key, rec) in existing {
                if let Some((_, range)) = molecule_key_codec::decode_hash_range_any(&base_key)
                    .or_else(|| molecule_key_codec::decode_hash_range(&base_key, molecule_uuid))
                {
                    by_range.insert(range, rec);
                }
            }
            for (range, rec) in changed {
                by_range.insert(range, rec);
            }

            let unique = if by_range.len() == 1 {
                by_range
                    .iter()
                    .next()
                    .map(|(range, rec)| (range.as_str(), rec))
            } else {
                None
            };
            items.push(hash_key_lookup_item(
                molecule_uuid,
                &hash,
                unique,
                storage_prefix,
            )?);
        }
        Ok(items)
    }
}
