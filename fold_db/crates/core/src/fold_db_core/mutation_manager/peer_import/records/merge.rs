//! The merge of the record outcomes of one page into the stored records.
//!
//! Each event merges in order into the records that the earlier events of the
//! page left: the delete floor first, then each field by stamp, then the
//! metadata (decision D1). An equal stamp keeps the stored part, so a replay
//! changes nothing. A losing value from another device becomes a conflict.
//! The stamps carry no causal order, so a later edit of a value that another
//! device wrote also keeps that value as a conflict, as on the molecule path.

use crate::db_operations::atom_store::{LostWrite, RecordChange};
use crate::schema::types::KeyValue;
use crate::storage::disk_pack::logical_capture::{AcceptedRecordOutcome, RecordFieldOutcome};
use lastdb_model::record_value::{FieldMerge, StoredRecord};
use std::collections::HashMap;
use std::sync::Arc;

/// One record of the page.
pub(super) struct PeerRecord {
    pub(super) record: String,
    pub(super) schema: String,
    pub(super) key: KeyValue,
    pub(super) before: Option<Arc<StoredRecord>>,
    pub(super) after: StoredRecord,
    /// The mutation of the last part that changed the record.
    pub(super) mutation: Option<String>,
}

/// The records of one page, in the order of their first outcome.
pub(super) struct PageMerge {
    records: Vec<PeerRecord>,
    at: HashMap<String, usize>,
    lost: Vec<LostWrite>,
}

impl PageMerge {
    /// `keys` holds the storage key, the schema and the key of each distinct
    /// record, and `stored` the stored value of each, in the same order.
    pub(super) fn new(
        keys: Vec<(String, String, KeyValue)>,
        stored: Vec<Option<Arc<StoredRecord>>>,
    ) -> Self {
        let mut at = HashMap::with_capacity(keys.len());
        let records = keys
            .into_iter()
            .zip(stored)
            .enumerate()
            .map(|(index, ((record, schema, key), before))| {
                at.insert(record.clone(), index);
                PeerRecord {
                    after: before.as_deref().cloned().unwrap_or_default(),
                    record,
                    schema,
                    key,
                    before,
                    mutation: None,
                }
            })
            .collect();
        Self {
            records,
            at,
            lost: Vec::new(),
        }
    }

    /// Merge one outcome into the record at the storage key `record`.
    pub(super) fn apply(&mut self, record: &str, outcome: &AcceptedRecordOutcome) {
        let Some(&index) = self.at.get(record) else {
            return;
        };
        let target = &mut self.records[index];
        if let Some(floor) = &outcome.deleted {
            let prior = target.after.deleted.clone();
            let removed = target.after.delete(floor.clone());
            if removed || target.after.deleted != prior {
                target.mutation = Some(floor.mutation_id.clone());
            }
        }
        for (field, part) in &outcome.fields {
            if let Some(lost) = merge_part(target, field, part) {
                self.lost.push(lost);
            }
        }
        if let Some(meta) = &outcome.meta {
            if target.after.merge_meta(
                meta.source_file_name.clone(),
                meta.metadata.clone(),
                meta.stamp.clone(),
            ) {
                target.mutation = Some(meta.stamp.mutation_id.clone());
            }
        }
    }

    /// The changed records, and the losing values found.
    pub(super) fn finish(self) -> (Vec<PeerRecord>, Vec<LostWrite>) {
        let changed = self
            .records
            .into_iter()
            .filter(|record| record.mutation.is_some())
            .collect();
        (changed, self.lost)
    }
}

impl PeerRecord {
    pub(super) fn change(&self) -> RecordChange {
        RecordChange {
            key: self.record.clone(),
            before: self.before.clone(),
            after: self.after.clone(),
        }
    }
}

/// Merge one field. A newer stamp from another device makes the replaced
/// value lose; an older stamp from another device loses to the stored value.
/// An equal value does not conflict: each update writes its key fields again.
fn merge_part(
    target: &mut PeerRecord,
    field: &str,
    part: &RecordFieldOutcome,
) -> Option<LostWrite> {
    let stored = target.after.stamps.get(field).cloned();
    let merged = target
        .after
        .merge_field(field, part.value.clone(), part.stamp.clone());
    let (value, loser, winner) = match merged {
        FieldMerge::Written { old, old_stamp } => {
            target.mutation = Some(part.stamp.mutation_id.clone());
            match (old, old_stamp) {
                (Some(old), Some(stamp))
                    if stamp.device_id != part.stamp.device_id
                        && part.value.as_ref() != Some(&old) =>
                {
                    (old, stamp, part.stamp.clone())
                }
                _ => return None,
            }
        }
        FieldMerge::Kept => match (stored, &part.value) {
            (Some(stamp), Some(value))
                if stamp != part.stamp
                    && stamp.device_id != part.stamp.device_id
                    && target.after.fields.get(field) != Some(value) =>
            {
                (value.clone(), part.stamp.clone(), stamp)
            }
            _ => return None,
        },
        FieldMerge::Deleted => return None,
    };
    Some(LostWrite {
        record: target.record.clone(),
        schema: target.schema.clone(),
        key: target.key.clone(),
        field: field.to_owned(),
        value,
        loser,
        winner,
    })
}
