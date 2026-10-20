use super::*;

/// Ordinary rows plus the winning pending value for each `mk:` tip key.
type CollapsedTips = (Vec<(String, Value)>, HashMap<String, PerKeyRecord>);

impl AtomStore {
    /// Keep one maximum-version value per exact `mk:` key, then compare it
    /// with the durable value while the caller holds that key's tip lock.
    ///
    /// Returns each decision between the pending tip and a durable tip that
    /// holds a different atom. The caller records them before it writes the tips.
    pub(in super::super) async fn retain_durable_tip_winners(
        &self,
        items: &mut Vec<(String, Value)>,
    ) -> Result<Vec<crate::db_operations::atom_store::DurableTipDecision>, SchemaError> {
        let (mut ordinary, mut tips) = Self::collapse_pending_tips(items)?;
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
        let mut decisions = Vec::new();
        for (key, durable) in eligible_keys.into_iter().zip(durable_tips) {
            let incoming = tips.remove(&key).expect("eligible tip must exist");
            let mut incoming_loses = false;
            if let Some(current) = durable.as_ref() {
                incoming_loses = !crate::atom::incoming_wins_lww(
                    incoming.entry.lww_key(),
                    current.entry.lww_key(),
                );
                decisions.extend(self.durable_tip_decision(
                    &key,
                    &incoming.entry,
                    &current.entry,
                    !incoming_loses,
                ));
            }
            if incoming_loses {
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
        Self::retain_accepted_tip_edges(&mut ordinary, &accepted_slots, &accepted_tip_edges);
        *items = ordinary;
        Ok(decisions)
    }

    /// Split pending items into ordinary rows and one LWW winner per `mk:` tip key.
    fn collapse_pending_tips(
        items: &mut Vec<(String, Value)>,
    ) -> Result<CollapsedTips, SchemaError> {
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
                    if crate::atom::incoming_wins_lww(
                        incoming.entry.lww_key(),
                        slot.get().entry.lww_key(),
                    ) {
                        slot.insert(incoming);
                    }
                }
            }
        }
        Ok((ordinary, tips))
    }

    /// Tip-edge transitions are part of the same winner decision.
    /// Keep only edges of accepted slots; an active edge must match an accepted winner.
    fn retain_accepted_tip_edges(
        ordinary: &mut Vec<(String, Value)>,
        accepted_slots: &std::collections::HashSet<(String, String, String)>,
        accepted_tip_edges: &std::collections::HashSet<(String, String, String, String, String)>,
    ) {
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
    }

    /// The per-key records for the changed keys that still have an atom entry.
    fn changed_key_records(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &std::collections::HashSet<ChangedKey>,
    ) -> Result<Vec<(String, PerKeyRecord)>, SchemaError> {
        let mut records = Vec::with_capacity(changed.len());
        for ck in changed {
            let api_hash = ck.hash_or_empty();
            let api_range = ck.range_or_empty();
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
        Ok(records)
    }

    /// Build one molecule's items for a changed-key write.
    pub(in super::super) fn changed_key_store_items(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        changed: &std::collections::HashSet<ChangedKey>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let records = self.changed_key_records(molecule_uuid, data, changed)?;
        let header = MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        };

        let mut items: Vec<(String, Value)> = Vec::with_capacity(records.len() + 2);
        for (base_key, rec) in &records {
            let value = serde_json::to_value(rec)
                .map_err(|e| SchemaError::InvalidData(format!("serialize per-key record: {e}")))?;
            items.push((build_storage_key(storage_prefix, base_key), value));
        }
        items.extend(self.pending_atom_ref_edge_items(
            molecule_uuid,
            data,
            &records,
            storage_prefix,
        )?);
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
}
