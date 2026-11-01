//! The conflict rows of a purged record leave in the batch of the purge. A
//! purge is a hard erase, and a conflict row holds a losing value inline.
//!
//! The stored marker names the losing mutation and the expiry of each row of
//! the record, so the purge deletes by key: each row, the queue entry of each
//! row, and the marker. The directory row of a queue bucket is shared by the
//! other entries of the bucket; the janitor retires an empty bucket.

use super::stage::invalid;
use super::{marker_key, record_conflict_row_key, ConflictMarker};
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;

impl RecordStore {
    /// The deletes of the conflict rows of `records`, their queue entries and
    /// their markers. One read by key of the markers. A marker that does not
    /// decode refuses the purge: its rows cannot be named.
    pub(crate) async fn purged_record_conflicts(
        &self,
        records: &[&str],
    ) -> Result<Vec<KvMutation>, SchemaError> {
        let mut markers = Vec::with_capacity(records.len());
        for record in records {
            let marker = marker_key(record)
                .ok_or_else(|| invalid("a record key without a record family"))?;
            markers.push((*record, marker));
        }
        if markers.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&String> = markers.iter().map(|(_, marker)| marker).collect();
        let stored = self.read_conflict_keys(&keys).await?;
        let mut deletes = Vec::new();
        for ((record, key), bytes) in markers.iter().zip(stored) {
            let Some(bytes) = bytes else {
                continue;
            };
            let marker: ConflictMarker = serde_json::from_slice(&bytes)
                .map_err(|error| invalid(&format!("a purge cannot read a marker: {error}")))?;
            for (field, entries) in &marker.fields {
                for (mutation, expires) in entries {
                    let row = record_conflict_row_key(record, field, mutation)
                        .ok_or_else(|| invalid("a record key without a record family"))?;
                    let queue = self.record_conflict_queue_key(&row, *expires);
                    deletes.push(KvMutation::delete(queue.into_bytes()));
                    deletes.push(KvMutation::delete(row.into_bytes()));
                }
            }
            deletes.push(KvMutation::delete(key.as_bytes().to_vec()));
        }
        Ok(deletes)
    }
}
