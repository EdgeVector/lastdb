//! The janitor side: one page of the queue, one release batch, the depth.

use super::entry::RetentionEntry;
use super::keys;
use crate::db_operations::atom_store::counter::{ReleaseRequest, TipChangeSet};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// One page of queue keys, oldest expiry first.
#[derive(Debug, Default)]
pub(crate) struct QueuePage {
    pub(crate) keys: Vec<String>,
    /// The page holds as many keys as the limit, so more may follow.
    pub(crate) full: bool,
}

impl QueuePage {
    /// The keys whose expiry is at or before `now_nanos`. The queue is sorted
    /// by expiry, so the due keys come first.
    pub(crate) fn due(&self, now_nanos: u64) -> Vec<String> {
        self.keys
            .iter()
            .take_while(|key| keys::queue_expiry(key).is_some_and(|expiry| expiry <= now_nanos))
            .cloned()
            .collect()
    }
}

/// What one release batch did.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReleaseOutcome {
    /// History rows released: references lowered, rows deleted.
    pub(crate) released: u64,
    /// Entries left alone: an entry that did not decode.
    pub(crate) skipped: u64,
}

impl AtomStore {
    /// Read at most `limit` queue keys that sort after `after`. This is an
    /// ordinary read. It holds no write gate, and the release batch reads the
    /// entries again inside the gate.
    pub(crate) async fn read_queue_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<QueuePage, SchemaError> {
        let (start, end) = keys::queue_range(after);
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), limit)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read retention queue: {error}")))?;
        let keys: Vec<String> = rows
            .into_iter()
            .filter_map(|(key, _)| String::from_utf8(key).ok())
            .collect();
        let full = keys.len() >= limit;
        Ok(QueuePage { keys, full })
    }

    /// The number of queue entries, counted up to `cap`. The second value is
    /// true when the queue holds more.
    pub(crate) async fn queue_depth(&self, cap: usize) -> Result<(usize, bool), SchemaError> {
        let (start, end) = keys::queue_range(None);
        let rows = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), cap + 1)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("count retention queue: {error}")))?;
        Ok((rows.len().min(cap), rows.len() > cap))
    }

    /// Release the records of the queue entries `due`: one native write scope,
    /// one batch. The entries are read again inside the gate, and an entry that
    /// is gone, or is not due at `now_nanos`, is left alone. The scope holds
    /// the write gate for this batch only, never across pages.
    pub(crate) async fn release_due_entries(
        &self,
        due: Vec<String>,
        now_nanos: u64,
    ) -> Result<ReleaseOutcome, SchemaError> {
        if due.is_empty() {
            return Ok(ReleaseOutcome::default());
        }
        self.in_background_scope(async {
            let (entries, mut skipped) = self.read_due_entries(&due, now_nanos).await?;
            let mut by_scope: BTreeMap<Option<String>, Vec<ReleaseRequest>> = BTreeMap::new();
            for entry in &entries {
                match entry.scope() {
                    Ok(scope) => by_scope
                        .entry(scope.map(str::to_string))
                        .or_default()
                        .push(entry.request()),
                    Err(error) => {
                        tracing::error!(%error, "retention entry has no valid scope");
                        skipped += 1;
                    }
                }
            }
            let mut released = 0;
            for (scope, releases) in by_scope {
                let commit = self
                    .commit_tip_changes(TipChangeSet {
                        scope: scope.as_deref(),
                        releases,
                        ..TipChangeSet::default()
                    })
                    .await?;
                released += commit.records_released;
            }
            Ok(ReleaseOutcome { released, skipped })
        })
        .await
    }

    /// The decoded entries of the queue keys that still exist and are due.
    async fn read_due_entries(
        &self,
        due: &[String],
        now_nanos: u64,
    ) -> Result<(Vec<RetentionEntry>, u64), SchemaError> {
        let keys: Vec<Vec<u8>> = due.iter().map(|key| key.as_bytes().to_vec()).collect();
        let stored = self
            .main_store
            .inner()
            .get_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read queue entries: {error}")))?;
        let mut entries = Vec::new();
        let mut skipped = 0;
        for (key, bytes) in due.iter().zip(stored) {
            let Some(bytes) = bytes else {
                continue;
            };
            if !keys::queue_expiry(key).is_some_and(|expiry| expiry <= now_nanos) {
                continue;
            }
            match serde_json::from_slice::<RetentionEntry>(&bytes) {
                Ok(entry) => entries.push(entry),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                    skipped += 1;
                }
            }
        }
        Ok((entries, skipped))
    }
}
