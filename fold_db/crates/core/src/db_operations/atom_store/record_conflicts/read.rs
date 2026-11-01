//! Reads by key of the markers and the rows. An entry at or past its expiry
//! does not count: the janitor releases it.

use super::{marker_key, record_conflict_row_key, ConflictMarker, RecordConflictRow};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use lastdb_model::record_value::FieldStamp;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// One kept losing value of one field.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecordConflictEntry {
    /// The losing mutation.
    pub mutation_id: String,
    pub value: Value,
    pub loser: FieldStamp,
    pub winner: FieldStamp,
    pub detected_at: u64,
    pub expires_at: u64,
}

/// The kept losing values of one selected field.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecordFieldConflicts {
    pub field: String,
    pub conflicts: Vec<RecordConflictEntry>,
}

impl AtomStore {
    /// The markers of the records at `records`, in order, from one read by
    /// key. The entries that expired at the retention clock are gone.
    async fn read_markers(
        &self,
        records: &[String],
    ) -> Result<Vec<Option<ConflictMarker>>, SchemaError> {
        let keys = records
            .iter()
            .map(|record| marker_key(record).ok_or_else(|| invalid("a record key")))
            .collect::<Result<Vec<_>, _>>()?;
        let stored = self
            .read_conflict_keys(&keys.iter().collect::<Vec<_>>())
            .await?;
        let now = self.retention_state().clock_nanos();
        stored
            .into_iter()
            .map(|bytes| {
                bytes
                    .map(|bytes| {
                        let mut marker: ConflictMarker = serde_json::from_slice(&bytes)
                            .map_err(|error| invalid(&format!("a stored marker: {error}")))?;
                        marker.prune(now);
                        Ok(marker)
                    })
                    .transpose()
            })
            .collect()
    }

    /// The conflicted fields of each record at `records`, in order.
    pub(crate) async fn record_conflict_fields(
        &self,
        records: &[String],
    ) -> Result<Vec<BTreeSet<String>>, SchemaError> {
        Ok(self
            .read_markers(records)
            .await?
            .into_iter()
            .map(|marker| marker.map(|marker| marker.fields.into_keys().collect()))
            .map(Option::unwrap_or_default)
            .collect())
    }

    /// The kept losing values of the selected fields of each record: one read
    /// by key of the markers, then one read by key of the rows they name.
    pub(crate) async fn selected_record_conflicts(
        &self,
        selections: &[(String, Vec<String>)],
    ) -> Result<Vec<Vec<RecordFieldConflicts>>, SchemaError> {
        let records: Vec<String> = selections
            .iter()
            .map(|(record, _)| record.clone())
            .collect();
        let markers = self.read_markers(&records).await?;
        let mut out: Vec<Vec<RecordFieldConflicts>> = selections
            .iter()
            .map(|(_, fields)| {
                fields
                    .iter()
                    .map(|field| RecordFieldConflicts {
                        field: field.clone(),
                        conflicts: Vec::new(),
                    })
                    .collect()
            })
            .collect();
        let mut wanted = Vec::new();
        for (at, ((record, fields), marker)) in selections.iter().zip(&markers).enumerate() {
            for (slot, field) in fields.iter().enumerate() {
                let entries = marker.as_ref().and_then(|marker| marker.fields.get(field));
                for mutation in entries.into_iter().flat_map(|entries| entries.keys()) {
                    let row = record_conflict_row_key(record, field, mutation)
                        .ok_or_else(|| invalid("a record key"))?;
                    wanted.push((row, at, slot, mutation.as_str()));
                }
            }
        }
        let stored = self
            .read_conflict_keys(&wanted.iter().map(|(row, ..)| row).collect::<Vec<_>>())
            .await?;
        for ((_, at, slot, mutation), bytes) in wanted.iter().zip(stored) {
            // A row that the janitor released since the marker read is gone.
            let Some(bytes) = bytes else { continue };
            let row: RecordConflictRow = serde_json::from_slice(&bytes)
                .map_err(|error| invalid(&format!("a stored conflict row: {error}")))?;
            let field = &mut out[*at][*slot];
            if row.record != selections[*at].0
                || row.field != field.field
                || row.loser.mutation_id != *mutation
            {
                return Err(invalid("a conflict row names another record"));
            }
            field.conflicts.push(entry(&row));
        }
        Ok(out)
    }
}

fn entry(row: &RecordConflictRow) -> RecordConflictEntry {
    RecordConflictEntry {
        mutation_id: row.loser.mutation_id.clone(),
        value: row.value.clone(),
        loser: row.loser.clone(),
        winner: row.winner.clone(),
        detected_at: row.detected_at,
        expires_at: row.expires_at,
    }
}

fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("record conflict read: {reason}"))
}
