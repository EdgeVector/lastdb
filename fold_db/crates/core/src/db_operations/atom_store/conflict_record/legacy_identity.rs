//! Retained legacy IDs survive coordinate conversion without a second key copy.

use super::{AtomStore, SchemaError, TipConflict};
use crate::atom::{molecule_key_codec::ConflictKeyIdentity, MutationEvent};
use crate::db_operations::atom_store::QueueValue;
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet};

const ALIAS_RECORD_CAP: usize = 10_000;

impl AtomStore {
    pub(super) fn historical_conflict_coordinate(
        &self,
        molecule: &str,
        api: Option<(&str, &str)>,
    ) -> Result<Option<(String, String)>, SchemaError> {
        let codec = self.key_codec_for_molecule(molecule)?;
        if codec.conflict_identity() == ConflictKeyIdentity::StoredV1 {
            return Ok(None);
        }
        let (hash, range) =
            api.ok_or_else(|| invalid("a legacy conflict identity requires API coordinates"))?;
        codec
            .historical_conflict_coordinate(molecule, hash, range)
            .map_err(|_| invalid("cannot reproduce the legacy conflict coordinate"))
    }

    pub(super) async fn without_historical_repeats(
        &self,
        conflicts: Vec<TipConflict>,
        scope: Option<&str>,
    ) -> Result<Vec<TipConflict>, SchemaError> {
        let mut legacy = Vec::new();
        let mut current = Vec::new();
        for conflict in conflicts {
            if conflict.digest_coordinate.is_some() {
                legacy.push(conflict);
            } else {
                current.push(conflict);
            }
        }
        if legacy.is_empty() {
            return Ok(current);
        }
        let keys: Vec<_> = legacy
            .iter()
            .map(|conflict| conflict.history_key(scope))
            .collect();
        let rows = self.main_store.get_items::<MutationEvent>(&keys).await?;
        if rows.len() != legacy.len() {
            return Err(invalid("incomplete historical conflict read"));
        }
        let mut missing = Vec::new();
        for (conflict, row) in legacy.into_iter().zip(rows) {
            match row {
                Some(event) => self.validate_historical_conflict(&conflict, &event)?,
                None => missing.push(conflict),
            }
        }
        if missing.is_empty() {
            return Ok(current);
        }
        let aliases = self.historical_aliases(&missing, scope).await?;
        for conflict in missing {
            let codec = self.key_codec_for_molecule(&conflict.molecule_uuid)?;
            let slot = slot_key(&conflict, scope);
            let mut repeated = false;
            if let Some(events) = aliases.get(&slot) {
                for (history_key, event) in events {
                    if event.molecule_uuid != conflict.molecule_uuid {
                        return Err(invalid("historical alias belongs to another molecule"));
                    }
                    let hash = event.field_key.hash.as_deref().unwrap_or("");
                    let range = event.field_key.range.as_deref().unwrap_or("");
                    if hash != conflict.field_key.hash.as_deref().unwrap_or("")
                        || codec
                            .canonical_api_range(range)
                            .map_err(|_| invalid("invalid historical range alias"))?
                            != conflict.field_key.range.as_deref().unwrap_or("")
                    {
                        return Err(invalid("historical alias belongs to another slot"));
                    }
                    let mut candidate = conflict.clone();
                    candidate.digest_coordinate = codec
                        .historical_conflict_coordinate(&conflict.molecule_uuid, hash, range)
                        .map_err(|_| invalid("invalid historical conflict domain"))?;
                    if candidate.history_key(scope) == *history_key {
                        self.validate_historical_conflict(&conflict, event)?;
                        repeated = true;
                        break;
                    }
                }
            }
            if !repeated {
                current.push(conflict);
            }
        }
        Ok(current)
    }

    fn validate_historical_conflict(
        &self,
        conflict: &TipConflict,
        event: &MutationEvent,
    ) -> Result<(), SchemaError> {
        let codec = self.key_codec_for_molecule(&conflict.molecule_uuid)?;
        let range = codec
            .canonical_api_range(event.field_key.range.as_deref().unwrap_or(""))
            .map_err(|_| invalid("invalid retained conflict range"))?;
        let expected = conflict.event();
        if event.molecule_uuid != expected.molecule_uuid
            || event.field_key.hash != expected.field_key.hash
            || range != expected.field_key.range.as_deref().unwrap_or("")
            || event.timestamp != expected.timestamp
            || event.old_atom_uuid != expected.old_atom_uuid
            || event.new_atom_uuid != expected.new_atom_uuid
            || event.kind != expected.kind
            || event.is_conflict != expected.is_conflict
            || event.conflict_loser_atom != expected.conflict_loser_atom
            || event.source_order != expected.source_order
            || event.suppressed_by_tip != expected.suppressed_by_tip
            || event.suppressed_by_delete.is_some()
        {
            return Err(invalid("a retained conflict differs from its identity"));
        }
        Ok(())
    }

    async fn historical_aliases(
        &self,
        conflicts: &[TipConflict],
        scope: Option<&str>,
    ) -> Result<BTreeMap<String, Vec<(String, MutationEvent)>>, SchemaError> {
        let slots: BTreeMap<_, _> = conflicts
            .iter()
            .map(|conflict| {
                (
                    slot_key(conflict, scope),
                    crate::db_operations::atom_store::retention::slot_dir_prefix(
                        scope,
                        &conflict.molecule_uuid,
                        &conflict.storage_hash,
                        &conflict.storage_range,
                    ),
                )
            })
            .collect();
        let pages = stream::iter(slots.into_iter().map(|(slot, prefix)| async move {
            let rows = self
                .main_store
                .scan_items_with_prefix_paged::<serde_json::Value>(&prefix, ALIAS_RECORD_CAP + 1)
                .await?;
            if rows.len() > ALIAS_RECORD_CAP {
                return Err(invalid("historical conflict alias allowance exceeded"));
            }
            let mut histories = BTreeSet::new();
            for (key, value) in rows {
                let bytes = serde_json::to_vec(&value)
                    .map_err(|_| invalid("invalid historical directory entry"))?;
                if let QueueValue::Record(entry) = QueueValue::decode(&bytes)
                    .map_err(|_| invalid("invalid historical directory entry"))?
                {
                    if entry.slot == slot {
                        if entry.dir != key {
                            return Err(invalid("historical directory identity differs"));
                        }
                        if entry.version != 1 {
                            return Err(invalid("historical directory version differs"));
                        }
                        histories.insert(entry.history);
                    }
                }
            }
            Ok((slot, histories))
        }))
        .buffer_unordered(8)
        .try_fold(
            (Vec::new(), 0usize),
            |(mut pages, total), page| async move {
                let total = total.saturating_add(page.1.len());
                if total > ALIAS_RECORD_CAP {
                    return Err(invalid("historical conflict alias allowance exceeded"));
                }
                pages.push(page);
                Ok((pages, total))
            },
        )
        .await?;
        let pages = pages.0;
        let histories: BTreeSet<_> = pages
            .iter()
            .flat_map(|(_, histories)| histories.iter().cloned())
            .collect();
        if histories.len() > ALIAS_RECORD_CAP {
            return Err(invalid("historical conflict alias allowance exceeded"));
        }
        let keys: Vec<_> = histories.into_iter().collect();
        let rows = self.main_store.get_items::<MutationEvent>(&keys).await?;
        if rows.len() != keys.len() {
            return Err(invalid("incomplete historical alias read"));
        }
        let rows: BTreeMap<_, _> = keys
            .into_iter()
            .zip(rows)
            .filter_map(|(key, row)| row.map(|row| (key, row)))
            .collect();
        Ok(pages
            .into_iter()
            .map(|(slot, keys)| {
                let events = keys
                    .into_iter()
                    .filter_map(|key| rows.get(&key).map(|event| (key, event.clone())))
                    .collect();
                (slot, events)
            })
            .collect())
    }
}

fn slot_key(conflict: &TipConflict, scope: Option<&str>) -> String {
    crate::schema::types::field::build_storage_key(
        scope,
        &crate::atom::molecule_key_codec::hash_range_record_key(
            &conflict.molecule_uuid,
            &conflict.storage_hash,
            &conflict.storage_range,
        ),
    )
}

fn invalid(message: &str) -> SchemaError {
    SchemaError::InvalidData(message.to_owned())
}
