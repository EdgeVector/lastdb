//! The janitor side: the release of the records that a page of due queue keys
//! names.

use super::entry::{QueueValue, RetentionEntry};
use crate::db_operations::atom_store::counter::{ReleaseRequest, TipChangeSet};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;
use std::collections::BTreeMap;

/// The records one release batch releases, at most. A page of the queue has
/// as many keys, and a drain of a slot takes as many entries.
pub(crate) const RELEASE_BATCH_RECORDS: usize = 32;

/// What one release batch did.
#[derive(Debug, Default)]
pub(crate) struct ReleaseOutcome {
    /// History rows released: references lowered, rows deleted.
    pub(crate) released: u64,
    /// Entries left alone: an entry that did not decode.
    pub(crate) skipped: u64,
}

/// One due queue key after the read in the gate.
enum Due {
    /// Gone, not due, or not readable: nothing to release.
    Nothing,
    Record(RetentionEntry),
}

impl AtomStore {
    /// Release the records of the queue keys `due`: one native write scope, one
    /// batch. The entries are read again inside the gate, and an entry that is
    /// gone, or is not due at `now_nanos`, is left alone. The scope holds the
    /// write gate for this batch only, never across pages.
    ///
    /// A record entry releases one record. The caller passes record entries
    /// only: a drain marker needs no write gate, and the janitor drains it in
    /// its own batches (`steps.rs`). A marker that comes here is left alone.
    /// `expired` names the markers that are too old to drain; the batch deletes
    /// them (`steps.rs`, `split_due_rows`).
    pub(crate) async fn release_due_records(
        &self,
        due: &[String],
        expired: &[String],
        now_nanos: u64,
    ) -> Result<ReleaseOutcome, SchemaError> {
        if due.is_empty() && expired.is_empty() {
            return Ok(ReleaseOutcome::default());
        }
        self.in_background_scope(async {
            let items = if due.is_empty() {
                Vec::new()
            } else {
                self.read_due_items(due, now_nanos).await?
            };
            let mut outcome = ReleaseOutcome::default();
            let mut entries: Vec<RetentionEntry> = Vec::new();
            for item in items {
                if let Due::Record(entry) = item {
                    entries.push(entry);
                }
            }
            let dead = expired.iter().map(|key| key.as_bytes().to_vec()).collect();
            outcome.skipped += self
                .commit_releases(entries, dead, &mut outcome.released)
                .await?;
            Ok(outcome)
        })
        .await
    }

    /// Release `entries` in one batch for each storage scope, and delete the
    /// keys `extra` in the last of them (or alone, when no entry is left). The
    /// caller holds a write scope. Returns the number of entries that have no
    /// valid scope, which the batch leaves alone.
    pub(super) async fn commit_releases(
        &self,
        entries: Vec<RetentionEntry>,
        extra: Vec<Vec<u8>>,
        released: &mut u64,
    ) -> Result<u64, SchemaError> {
        let mut skipped = 0;
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
        // The extra deletes go with the last batch. A crash before it leaves
        // the entries that they name, so the next pass finds them again.
        let last = by_scope.len().saturating_sub(1);
        let mut extra = Some(extra);
        for (at, (scope, releases)) in by_scope.into_iter().enumerate() {
            let commit = self
                .commit_tip_changes(TipChangeSet {
                    scope: scope.as_deref(),
                    releases,
                    deletes: if at == last {
                        extra.take().unwrap_or_default()
                    } else {
                        Vec::new()
                    },
                    ..TipChangeSet::default()
                })
                .await?;
            *released += commit.records_released;
        }
        if let Some(deletes) = extra.filter(|deletes| !deletes.is_empty()) {
            self.commit_tip_changes(TipChangeSet {
                deletes,
                ..TipChangeSet::default()
            })
            .await?;
        }
        Ok(skipped)
    }

    /// The due queue keys, read again in the gate and decoded, in key order.
    async fn read_due_items(
        &self,
        due: &[String],
        now_nanos: u64,
    ) -> Result<Vec<Due>, SchemaError> {
        let queue = self.retention_state().queue();
        let keys: Vec<Vec<u8>> = due.iter().map(|key| key.as_bytes().to_vec()).collect();
        let stored = self
            .main_store
            .inner()
            .get_many(keys)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read queue entries: {error}")))?;
        let mut items = Vec::with_capacity(due.len());
        for (key, bytes) in due.iter().zip(stored) {
            let Some(bytes) = bytes else {
                items.push(Due::Nothing);
                continue;
            };
            match queue.entry_expiry(key) {
                Some(expiry) if expiry <= now_nanos => {}
                Some(_) => {
                    items.push(Due::Nothing);
                    continue;
                }
                None => {
                    tracing::error!(%key, "retention queue key has no readable expiry");
                    self.retention_state().note_undecodable();
                    items.push(Due::Nothing);
                    continue;
                }
            }
            match QueueValue::decode(&bytes) {
                Ok(QueueValue::Record(entry)) => items.push(Due::Record(entry)),
                Ok(QueueValue::Drain(_)) => items.push(Due::Nothing),
                Err(error) => {
                    tracing::error!(%key, %error, "retention queue entry does not decode");
                    self.retention_state().note_undecodable();
                    items.push(Due::Nothing);
                }
            }
        }
        Ok(items)
    }
}
