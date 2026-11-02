//! The chunk of the dwell pass: delete the blobs of a page of due entries.
//!
//! A chunk enters the native write gate, reads each counter again, and in one
//! batch deletes the blob row, the counter and the queue entry of every blob that
//! is still at exactly zero with the `zero_since` of its entry. A reference that
//! arrived during the dwell raised the counter, so the chunk finds a count above
//! zero and deletes only the stale queue entry.

use super::dwell::DwellStep;
use super::plan::BlobEvent;
use super::queue::BlobQueue;
use super::{blob_counter_key_of_id, blob_id, BlobCounter, BlobRead};
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::Deserialize;
use std::collections::HashMap;

/// One queue entry whose dwell is over, as a step read it.
#[derive(Debug, Clone)]
pub(super) struct DwellEntry {
    pub(super) key: Vec<u8>,
    pub(super) zero_since: u64,
    pub(super) id: String,
    /// The blob reference. `None` for an entry whose key or value is malformed.
    pub(super) blob: Option<String>,
}

#[derive(Deserialize)]
struct QueueValue {
    blob: String,
}

enum Fate {
    Delete(BlobCounter),
    Stale,
    Unreadable,
}

impl RecordStore {
    /// Delete the blobs of one chunk of due entries. Takes the native write gate
    /// for this chunk only. Reads each counter again inside the gate.
    pub(super) async fn blob_dwell_delete(
        &self,
        entries: &[DwellEntry],
    ) -> Result<DwellStep, SchemaError> {
        if entries.is_empty() {
            return Ok(DwellStep::default());
        }
        let refs: Vec<String> = entries
            .iter()
            .filter_map(|entry| entry.blob.clone())
            .collect();
        let step = self
            .in_write_scope(async {
                // The native gate first, then the blob gates: the upload path
                // uses the same order.
                let _blobs = self.lock_liveness_blobs(&refs).await;
                self.dwell_chunk_in_gate(entries).await
            })
            .await?;
        self.blob_events.note_deleted(step.deleted, step.stale);
        tracing::info!(
            target: "fold_db::blob_dwell",
            deleted = step.deleted,
            stale = step.stale,
            unreadable = step.unreadable,
            "blob dwell chunk"
        );
        Ok(step)
    }

    async fn dwell_chunk_in_gate(&self, entries: &[DwellEntry]) -> Result<DwellStep, SchemaError> {
        let named: Vec<&DwellEntry> = entries.iter().filter(|e| e.blob.is_some()).collect();
        let keys: Vec<String> = named
            .iter()
            .map(|entry| blob_counter_key_of_id(&entry.id))
            .collect();
        let counters = self.read_blob_counters_at(&keys).await?;
        let mut state: HashMap<&str, BlobRead> = HashMap::new();
        for (entry, counter) in named.iter().zip(counters) {
            state.insert(entry.id.as_str(), counter);
        }
        let absent = BlobRead::Absent;
        let mut main = Vec::new();
        let mut rows = Vec::new();
        let mut step = DwellStep::default();
        for entry in entries {
            let read = state.get(entry.id.as_str()).unwrap_or(&absent);
            match fate(entry, read) {
                Fate::Delete(counter) => {
                    main.extend(delete_mutations(entry));
                    rows.push(counter.blob.into_bytes());
                    step.deleted += 1;
                }
                Fate::Stale => {
                    main.push(KvMutation::delete(entry.key.clone()));
                    step.stale += 1;
                }
                Fate::Unreadable => {
                    // Keep the blob and its counter. Drop the entry, so the same
                    // entry does not come back at every pass.
                    self.blob_events.note(&entry.id, BlobEvent::Unreadable);
                    main.push(KvMutation::delete(entry.key.clone()));
                    step.unreadable += 1;
                }
            }
        }
        self.stage_dwell_writes(BlobQueue::keep_last_directory_rows(main), rows)
            .await?;
        Ok(step)
    }

    async fn stage_dwell_writes(
        &self,
        main: Vec<KvMutation>,
        rows: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        if !rows.is_empty() {
            let store = self.namespaced_store.as_ref().ok_or_else(|| {
                SchemaError::InvalidData("blob dwell requires a native store".into())
            })?;
            store
                .open_namespace("cas_blobs")
                .await
                .map_err(|error| SchemaError::InvalidData(format!("open cas_blobs: {error}")))?
                .batch_delete(rows)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("delete blob rows: {error}")))?;
        }
        if !main.is_empty() {
            self.main_store
                .inner()
                .batch_mutate(main)
                .await
                .map_err(|error| SchemaError::InvalidData(format!("blob dwell batch: {error}")))?;
        }
        Ok(())
    }
}

/// The entry of a due row. A key or a value that does not read gives an entry
/// with no blob: the chunk deletes it as stale.
pub(super) fn decode_entry(queue: &BlobQueue, key: &str, value: &[u8]) -> DwellEntry {
    let Some((zero_since, id)) = queue.parse_entry(key.as_bytes()) else {
        return DwellEntry {
            key: key.as_bytes().to_vec(),
            zero_since: 0,
            id: String::new(),
            blob: None,
        };
    };
    let blob = serde_json::from_slice::<QueueValue>(value)
        .ok()
        .map(|value| value.blob);
    DwellEntry {
        key: key.as_bytes().to_vec(),
        zero_since,
        id,
        blob,
    }
}

/// The decision for one due entry, from the counter read inside the gate.
fn fate(entry: &DwellEntry, read: &BlobRead) -> Fate {
    let counter = match read {
        BlobRead::Absent => return Fate::Stale,
        BlobRead::Unreadable => return Fate::Unreadable,
        BlobRead::Present(counter) => counter,
    };
    // A counter that names another blob than its key does would make the pass
    // delete the wrong row. Treat it as stale and leave the verifier to report it.
    if counter.refs != 0
        || counter.suspect
        || counter.zero_since != Some(entry.zero_since)
        || blob_id(&counter.blob) != entry.id
    {
        return Fate::Stale;
    }
    Fate::Delete(counter.clone())
}

/// Counter and queue entry. The caller deletes the `cas_blobs` row.
fn delete_mutations(entry: &DwellEntry) -> [KvMutation; 2] {
    [
        KvMutation::delete(blob_counter_key_of_id(&entry.id).into_bytes()),
        KvMutation::delete(entry.key.clone()),
    ]
}
