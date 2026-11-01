//! The janitor side: the release of the records that a page of due queue keys
//! names.

use super::entry::RetentionEntry;
use crate::db_operations::record_store::counter::RowChanges;
use crate::db_operations::record_store::time_queue::DuePage;
use crate::db_operations::record_store::RecordStore;
use crate::schema::SchemaError;

/// The records one release batch releases, at most. A page of the queue has
/// as many keys.
pub(crate) const RELEASE_BATCH_RECORDS: usize = 32;

/// What one release batch did.
#[derive(Debug, Default)]
pub(crate) struct ReleaseOutcome {
    /// Conflict rows released: rows deleted.
    pub(crate) released: u64,
    /// Entries left alone: an entry whose row key has no valid scope.
    pub(crate) skipped: u64,
}

impl RecordStore {
    /// The keys of the record entries of a page. An entry that does not decode,
    /// or whose key has no readable expiry, is counted and left alone: the cursor
    /// moves past it, and the entry stays.
    pub(super) fn due_record_keys(&self, page: &DuePage) -> Vec<String> {
        let queue = self.retention_state().queue();
        let mut records = Vec::new();
        for (key, value) in &page.rows {
            if queue.entry_expiry(key).is_none() {
                tracing::error!(%key, "retention queue key has no readable expiry");
                self.retention_state().note_undecodable();
                continue;
            }
            match serde_json::from_slice::<RetentionEntry>(value) {
                Ok(_) => records.push(key.clone()),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                }
            }
        }
        records
    }

    /// Release the records of the queue keys `due`: one native write scope, one
    /// batch. The entries are read again inside the gate, and an entry that is
    /// gone, or is not due at `now_nanos`, is left alone. The scope holds the
    /// write gate for this batch only, never across pages.
    pub(crate) async fn release_due_records(
        &self,
        due: &[String],
        now_nanos: u64,
    ) -> Result<ReleaseOutcome, SchemaError> {
        if due.is_empty() {
            return Ok(ReleaseOutcome::default());
        }
        self.in_background_scope(async {
            let mut outcome = ReleaseOutcome::default();
            let mut releases = Vec::new();
            for entry in self.read_due_entries(due, now_nanos).await? {
                match entry.scope() {
                    Ok(_) => releases.push(entry.request()),
                    Err(error) => {
                        tracing::error!(%error, "retention entry has no valid scope");
                        outcome.skipped += 1;
                    }
                }
            }
            if !releases.is_empty() {
                let commit = self
                    .commit_rows(RowChanges {
                        releases,
                        ..RowChanges::default()
                    })
                    .await?;
                outcome.released = commit.records_released;
            }
            Ok(outcome)
        })
        .await
    }

    /// The due queue keys, read again in the gate and decoded, in key order. A
    /// key that is gone, not due, or not readable gives nothing.
    async fn read_due_entries(
        &self,
        due: &[String],
        now_nanos: u64,
    ) -> Result<Vec<RetentionEntry>, SchemaError> {
        let queue = self.retention_state().queue();
        let keys: Vec<Vec<u8>> = due.iter().map(|key| key.as_bytes().to_vec()).collect();
        let stored = self
            .main_store
            .inner()
            .get_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read queue entries: {error}")))?;
        let mut entries = Vec::with_capacity(due.len());
        for (key, bytes) in due.iter().zip(stored) {
            let Some(bytes) = bytes else {
                continue;
            };
            match queue.entry_expiry(key) {
                Some(expiry) if expiry <= now_nanos => {}
                Some(_) => continue,
                None => {
                    tracing::error!(%key, "retention queue key has no readable expiry");
                    self.retention_state().note_undecodable();
                    continue;
                }
            }
            match serde_json::from_slice::<RetentionEntry>(&bytes) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                }
            }
        }
        Ok(entries)
    }
}
