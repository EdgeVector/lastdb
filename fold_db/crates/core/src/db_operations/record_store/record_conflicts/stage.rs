//! The rows of the losing values of one peer import, for its one batch.

use super::{
    marker_key, record_conflict_row_key, ConflictMarker, LostWrite, RecordConflictRow, ROW_VERSION,
};
use crate::atom::file_pointer::blob_refs_of_atom;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use std::collections::{BTreeMap, BTreeSet};

const READ_CHUNK: usize = 10_000;

impl RecordStore {
    /// The puts of the new conflict rows, their queue rows and the changed
    /// markers. One read by key for the rows and the markers. A row that is
    /// there already (the same losing write) changes nothing. The caller adds
    /// the puts to the batch of the records.
    pub(crate) async fn record_conflict_mutations(
        &self,
        lost: Vec<LostWrite>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        if lost.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = BTreeMap::new();
        for write in lost {
            if !blob_refs_of_atom(&write.value, None).is_empty() {
                return Err(invalid("a losing value with a file blob reference"));
            }
            let row =
                record_conflict_row_key(&write.record, &write.field, &write.loser.mutation_id)
                    .ok_or_else(|| invalid("a record key without a record family"))?;
            rows.insert(row, write);
        }
        let mut markers: BTreeMap<String, Option<ConflictMarker>> = BTreeMap::new();
        for write in rows.values() {
            let marker = marker_key(&write.record)
                .ok_or_else(|| invalid("a record key without a record family"))?;
            markers.insert(marker, None);
        }
        let keys: Vec<&String> = rows.keys().chain(markers.keys()).collect();
        let stored = self.read_conflict_keys(&keys).await?;
        let (row_bytes, marker_bytes) = stored.split_at(rows.len());
        for ((_, slot), bytes) in markers.iter_mut().zip(marker_bytes) {
            if let Some(bytes) = bytes {
                *slot = Some(
                    serde_json::from_slice(bytes)
                        .map_err(|error| invalid(&format!("a stored marker: {error}")))?,
                );
            }
        }
        let fresh: Vec<(String, LostWrite)> = rows
            .into_iter()
            .zip(row_bytes)
            .filter(|(_, bytes)| bytes.is_none())
            .map(|(row, _)| row)
            .collect();
        self.conflict_puts(fresh, markers)
    }

    pub(super) async fn read_conflict_keys(
        &self,
        keys: &[&String],
    ) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            let rows = self
                .main_store
                .inner()
                .get_many(chunk.iter().map(|key| key.as_bytes().to_vec()).collect())
                .await
                .map_err(|error| invalid(&format!("read conflict rows: {error}")))?;
            if rows.len() != chunk.len() {
                return Err(invalid("the conflict read batch is incomplete"));
            }
            out.extend(rows);
        }
        Ok(out)
    }

    /// The rows of each new losing write, and each marker with its expired
    /// entries dropped and the new entries added.
    fn conflict_puts(
        &self,
        fresh: Vec<(String, LostWrite)>,
        mut markers: BTreeMap<String, Option<ConflictMarker>>,
    ) -> Result<Vec<KvMutation>, SchemaError> {
        if fresh.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.retention_state();
        let detected_at = state.clock_nanos();
        let expires_at = detected_at.saturating_add(state.window_nanos());
        let mut puts: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut touched = BTreeSet::new();
        for (row, write) in fresh {
            for (key, value) in
                self.record_conflict_retention_rows(&row, &write.record, detected_at, expires_at)?
            {
                puts.insert(key, encode(&value)?);
            }
            let marker_at = marker_key(&write.record)
                .ok_or_else(|| invalid("a record key without a record family"))?;
            let marker = markers
                .get_mut(&marker_at)
                .ok_or_else(|| invalid("a conflict without its marker"))?
                .get_or_insert_with(|| ConflictMarker {
                    v: ROW_VERSION,
                    fields: BTreeMap::new(),
                });
            marker.prune(detected_at);
            marker
                .fields
                .entry(write.field.clone())
                .or_default()
                .insert(write.loser.mutation_id.clone(), expires_at);
            touched.insert(marker_at);
            let value = RecordConflictRow {
                v: ROW_VERSION,
                schema: write.schema,
                key: write.key,
                field: write.field,
                record: write.record,
                value: write.value,
                loser: write.loser,
                winner: write.winner,
                detected_at,
                expires_at,
            };
            puts.insert(row, encode(&value)?);
        }
        for key in touched {
            if let Some(Some(marker)) = markers.get(&key) {
                puts.insert(key, encode(marker)?);
            }
        }
        Ok(puts
            .into_iter()
            .map(|(key, value)| KvMutation::put(key.into_bytes(), value))
            .collect())
    }
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, SchemaError> {
    serde_json::to_vec(value).map_err(|error| invalid(&format!("encode a conflict row: {error}")))
}

pub(super) fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("record conflict: {reason}"))
}
