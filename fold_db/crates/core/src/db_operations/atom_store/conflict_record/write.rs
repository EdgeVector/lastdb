//! The ordered write of recorded conflicts.

use super::TipConflict;
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

impl AtomStore {
    /// Write the rows of every pair that two different sync devices wrote, and
    /// flush them. Return how many pairs it wrote.
    ///
    /// One batch holds, in this order: the reverse edges (compact v2 edges, so
    /// the delete gate sees them), then for each pair the history row, the
    /// conflict row, the `mcc:` entry, and then one `hcu:` event for each
    /// molecule. The flush returns before the caller writes the tip. A crash
    /// at any step leaves the tip as it was, and a repeat of the decision
    /// rewrites the same keys.
    pub(crate) async fn record_tip_conflicts(
        &self,
        conflicts: Vec<TipConflict>,
        storage_prefix: Option<&str>,
    ) -> Result<usize, SchemaError> {
        let mut unique: BTreeMap<super::TipConflictDigest, TipConflict> = BTreeMap::new();
        for conflict in conflicts {
            if !conflict.same_device() {
                unique.entry(conflict.digest()).or_insert(conflict);
            }
        }
        if unique.is_empty() {
            return Ok(0);
        }
        let count = unique.len();
        {
            // The gate covers the `mcc:` read, extend and write.
            let _gate = self.conflict_record_gate.lock().await;
            let items = self.conflict_items(&unique, storage_prefix).await?;
            self.batch_put_items_with_atom_ref_v2(items, storage_prefix)
                .await?;
        }
        self.flush()
            .await
            .map_err(|error| SchemaError::InvalidData(format!("flush tip conflicts: {error}")))?;
        for conflict in unique.values() {
            crate::atom::legacy_history_memo::invalidate_for_event_key(
                &conflict.history_key(storage_prefix),
            );
        }
        tracing::info!(count, "recorded tip conflicts");
        Ok(count)
    }

    /// The batch of one record call: edges first, then the rows.
    async fn conflict_items(
        &self,
        unique: &BTreeMap<TipConflictDigest, TipConflict>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<(String, Value)>, SchemaError> {
        let detected_at = chrono::Utc::now();
        let mut edges = Vec::new();
        let mut rows = Vec::new();
        let mut new_ids: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for conflict in unique.values() {
            let event = conflict.event();
            let history_key = conflict.history_key(storage_prefix);
            edges.extend(self.mutation_history_atom_ref_edge_items(
                &history_key,
                &event,
                storage_prefix,
            )?);
            rows.push((history_key, to_value(&event)?));
            rows.push((
                conflict.conflict_key(storage_prefix),
                to_value(&conflict.sync_conflict(detected_at))?,
            ));
            new_ids
                .entry(conflict.molecule_uuid.as_str())
                .or_default()
                .push(conflict.conflict_id());
        }
        rows.extend(self.mcc_rows(&new_ids, storage_prefix).await?);
        for molecule_uuid in new_ids.keys() {
            rows.push(hcu_row(molecule_uuid, storage_prefix)?);
        }
        edges.extend(rows);
        Ok(edges)
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
