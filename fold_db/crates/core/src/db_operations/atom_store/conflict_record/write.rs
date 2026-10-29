//! The ordered write of recorded conflicts.

use super::TipConflict;
use crate::db_operations::atom_store::counter::{event_atoms, RecordRefs};
use crate::db_operations::atom_store::RecordSlot;
use crate::db_operations::AtomStore;
use crate::db_operations::{
    home_conflict_event_key, next_home_conflict_event_seq, HomeConflictEvent,
};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::BTreeMap;

/// SHA-256 identity of one conflict pair. See [`TipConflict::digest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TipConflictDigest(pub(super) [u8; 32]);

impl TipConflictDigest {
    pub(super) fn hex(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// The first 64 bits, as the number of a 20-digit conflict id stamp.
    pub(super) fn stamp(&self) -> u64 {
        let mut head = [0u8; 8];
        head.copy_from_slice(&self.0[..8]);
        u64::from_be_bytes(head)
    }
}

/// The rows and the atom references of the conflicts that one tip store keeps.
/// The caller hands both to the planner, which writes them in the batch of the
/// tips.
#[derive(Default)]
pub(crate) struct Recorded {
    pub(crate) rows: Vec<(String, Value)>,
    pub(crate) refs: Vec<RecordRefs>,
}

impl AtomStore {
    /// Build the rows of every pair that two different sync devices wrote.
    ///
    /// The rows are, for each pair, the history row, the conflict row, the
    /// `mcc:` entry, and then one `hcu:` event for each molecule. The history
    /// row names the atoms of the pair; each distinct atom holds one reference
    /// while the record exists, so the loser of a decision keeps its body. The
    /// planner writes the rows, the references and the tips in one batch, so a
    /// crash keeps all of them or none, and a repeat of the decision rewrites
    /// the same keys and adds no reference.
    pub(crate) async fn record_tip_conflicts(
        &self,
        conflicts: Vec<TipConflict>,
        storage_prefix: Option<&str>,
    ) -> Result<Recorded, SchemaError> {
        let mut unique: BTreeMap<super::TipConflictDigest, TipConflict> = BTreeMap::new();
        for conflict in conflicts {
            if !conflict.same_device() {
                unique.entry(conflict.digest()).or_insert(conflict);
            }
        }
        if unique.is_empty() {
            return Ok(Recorded::default());
        }
        let count = unique.len();
        let recorded = self.conflict_items(&unique, storage_prefix).await?;
        // A reader that scanned the conflict rows before this point sees a new
        // epoch and does not stamp its stale list.
        self.conflict_record_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        for conflict in unique.values() {
            crate::atom::history_event_memo::invalidate_for_event_key(
                &conflict.history_key(storage_prefix),
            );
        }
        tracing::info!(count, "recorded tip conflicts");
        Ok(recorded)
    }

    /// The rows and references of one record call.
    async fn conflict_items(
        &self,
        unique: &BTreeMap<TipConflictDigest, TipConflict>,
        storage_prefix: Option<&str>,
    ) -> Result<Recorded, SchemaError> {
        let detected_at = chrono::Utc::now();
        let mut recorded = Recorded::default();
        let mut new_ids: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for conflict in unique.values() {
            let event = conflict.event();
            let history_key = conflict.history_key(storage_prefix);
            let conflict_key = conflict.conflict_key(storage_prefix);
            // The queue entry and the slot directory entry go with a new record.
            let new_rows = self.retention_rows(
                storage_prefix,
                &history_key,
                &RecordSlot {
                    molecule: &conflict.molecule_uuid,
                    hash: &conflict.storage_hash,
                    range: &conflict.storage_range,
                },
                Some(conflict_key.clone()),
            )?;
            recorded.refs.push(RecordRefs {
                key: history_key.clone(),
                atoms: event_atoms(&event),
                files: Vec::new(),
                history_slot: None,
                new_rows,
                molecule: event.molecule_uuid.clone(),
            });
            recorded.rows.push((history_key, to_value(&event)?));
            recorded.rows.push((
                conflict_key,
                to_value(&conflict.sync_conflict(detected_at))?,
            ));
            new_ids
                .entry(conflict.molecule_uuid.as_str())
                .or_default()
                .push(conflict.conflict_id());
        }
        recorded
            .rows
            .extend(self.mcc_rows(&new_ids, storage_prefix).await?);
        for molecule_uuid in new_ids.keys() {
            recorded.rows.push(hcu_row(molecule_uuid, storage_prefix)?);
        }
        Ok(recorded)
    }

    /// Add each new conflict id to the `mcc:{molecule}` list that already
    /// exists. A molecule with no list keeps none: the reader scans
    /// `conflict\0{molecule}:` when the list is absent, and then it finds the
    /// conflict row.
    async fn mcc_rows(
        &self,
        new_ids: &BTreeMap<&str, Vec<String>>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let keys: Vec<String> = new_ids
            .keys()
            .map(|molecule_uuid| build_storage_key(storage_prefix, &format!("mcc:{molecule_uuid}")))
            .collect();
        let existing: Vec<Option<Value>> = self
            .raw()
            .get_items(&keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read mcc lists: {error}")))?;
        let mut rows = Vec::new();
        for ((key, ids), current) in keys.into_iter().zip(new_ids.values()).zip(existing) {
            // An absent list, or a legacy stamp that is not a list, stays as it
            // is: the reader re-derives it from the conflict rows.
            let Some(mut list) =
                current.and_then(|value| serde_json::from_value::<Vec<String>>(value).ok())
            else {
                continue;
            };
            let before = list.len();
            for id in ids {
                if !list.contains(id) {
                    list.push(id.clone());
                }
            }
            if list.len() != before {
                rows.push((key, to_value(&list)?));
            }
        }
        Ok(rows)
    }
}

/// One `hcu:evt` event names the molecule for the next conflict reader.
fn hcu_row(
    molecule_uuid: &str,
    storage_prefix: Option<&str>,
) -> Result<(String, Value), SchemaError> {
    let key = build_storage_key(
        storage_prefix,
        &home_conflict_event_key(&next_home_conflict_event_seq()),
    );
    Ok((key, to_value(&HomeConflictEvent::named(molecule_uuid))?))
}

fn to_value<T: serde::Serialize>(row: &T) -> Result<Value, SchemaError> {
    serde_json::to_value(row)
        .map_err(|error| SchemaError::InvalidData(format!("serialize conflict row: {error}")))
}
