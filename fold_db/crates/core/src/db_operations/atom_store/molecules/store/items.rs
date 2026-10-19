use super::*;
use crate::db_operations::atom_store::counter::TipChange;

/// Ordinary rows plus the winning pending value for each `mk:` tip key.
type CollapsedTips = (Vec<(String, Value)>, HashMap<String, PerKeyRecord>);

/// The outcome of the last-writer-wins comparison of one tip store.
pub(in super::super) struct TipWinners {
    /// The winning tip records. The planner writes them and counts their atoms.
    pub(in super::super) tips: Vec<TipChange>,
    /// Every other row of the store (headers and similar).
    pub(in super::super) rows: Vec<(String, Value)>,
    /// Decisions between a pending tip and a durable tip that holds another atom.
    pub(in super::super) decisions: Vec<crate::db_operations::atom_store::DurableTipDecision>,
}

impl AtomStore {
    /// Keep one maximum-version value per exact `mk:` key, then compare it
    /// with the durable value while the caller holds that key's tip lock.
    ///
    /// Returns the winning tips, the other rows, and each decision between the
    /// pending tip and a durable tip that holds a different atom. The caller
    /// records the decisions and then commits the winners through the planner.
    pub(in super::super) async fn retain_durable_tip_winners(
        &self,
        items: Vec<(String, Value)>,
    ) -> Result<TipWinners, SchemaError> {
        let (rows, mut tips) = Self::collapse_pending_tips(items)?;
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
        let mut winners = Vec::new();
        let mut decisions = Vec::new();
        for (key, durable) in eligible_keys.into_iter().zip(durable_tips) {
            let incoming = tips.remove(&key).expect("eligible tip must exist");
            let mut incoming_loses = false;
            if let Some(current) = durable.as_ref() {
                let legacy_last_write =
                    is_legacy_zero_clock(&incoming.entry) && is_legacy_zero_clock(&current.entry);
                incoming_loses = !legacy_last_write
                    && !crate::atom::incoming_wins_lww(
                        incoming.entry.lww_key(),
                        current.entry.lww_key(),
                    );
                if !legacy_last_write {
                    decisions.extend(self.durable_tip_decision(
                        &key,
                        &incoming.entry,
                        &current.entry,
                        !incoming_loses,
                    ));
                }
            }
            if incoming_loses {
                continue;
            }
            winners.push(TipChange {
                key,
                new: Some(incoming),
            });
        }
        Ok(TipWinners {
            tips: winners,
            rows,
            decisions,
        })
    }

    /// Split pending items into ordinary rows and one LWW winner per `mk:` tip key.
    fn collapse_pending_tips(items: Vec<(String, Value)>) -> Result<CollapsedTips, SchemaError> {
        let mut ordinary = Vec::with_capacity(items.len());
        let mut tips: HashMap<String, PerKeyRecord> = HashMap::new();
        for (key, value) in items {
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
        Ok((ordinary, tips))
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
