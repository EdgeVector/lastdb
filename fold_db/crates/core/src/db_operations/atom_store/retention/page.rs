//! The janitor side: one page of the queue, one release batch, the depth.

use super::entry::{DrainMarker, QueueValue, RetentionEntry};
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
    /// by expiry, so the due keys come first. A key with no readable expiry
    /// counts as due: the release batch reports it and the wake moves past it,
    /// so one bad key cannot hold back every later entry.
    pub(crate) fn due(&self, now_nanos: u64) -> Vec<String> {
        self.keys
            .iter()
            .take_while(|key| keys::queue_expiry(key).is_none_or(|expiry| expiry <= now_nanos))
            .cloned()
            .collect()
    }
}

/// The records one release batch releases, at most. A page of the queue has
/// as many keys, and a drain marker takes what is left of the number.
pub(crate) const RELEASE_BATCH_RECORDS: usize = 32;

/// What one release batch did.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReleaseOutcome {
    /// History rows released: references lowered, rows deleted.
    pub(crate) released: u64,
    /// Entries left alone: an entry that did not decode.
    pub(crate) skipped: u64,
    /// The number of due keys, counted from the first, that the batch finished.
    /// A drain marker that still holds records is not finished, and the next
    /// page read starts at it.
    pub(crate) completed: usize,
}

/// One due queue key after the read in the gate.
enum Due {
    /// Gone, not due, or not readable: nothing to release.
    Nothing,
    Record(RetentionEntry),
    Drain(DrainMarker),
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
    ///
    /// A record entry releases one record. A drain marker releases the records
    /// of its erased slot, as many as the batch has room for, and the marker
    /// goes with the last of them.
    pub(crate) async fn release_due_entries(
        &self,
        due: Vec<String>,
        now_nanos: u64,
    ) -> Result<ReleaseOutcome, SchemaError> {
        if due.is_empty() {
            return Ok(ReleaseOutcome::default());
        }
        self.in_background_scope(async {
            let items = self.read_due_items(&due, now_nanos).await?;
            let mut outcome = ReleaseOutcome::default();
            let mut by_scope: BTreeMap<Option<String>, Vec<ReleaseRequest>> = BTreeMap::new();
            let mut marker_deletes: Vec<Vec<u8>> = Vec::new();
            let mut retained: Option<(usize, String)> = None;
            let mut room = RELEASE_BATCH_RECORDS;
            for (index, item) in items.into_iter().enumerate() {
                match item {
                    Due::Nothing => {}
                    Due::Record(entry) => {
                        room = room.saturating_sub(1);
                        Self::add_requests(&[entry], &mut by_scope, &mut outcome);
                    }
                    Due::Drain(marker) => {
                        if room == 0 {
                            break;
                        }
                        let (found, more) = self
                            .matching_entries(
                                &marker.dir_prefix,
                                &marker.slot,
                                room,
                                marker.erased_at,
                            )
                            .await?;
                        room -= found.len();
                        Self::add_requests(&found, &mut by_scope, &mut outcome);
                        if more {
                            // Records remain. The next batch continues here.
                            retained = Some((index, marker.queue.clone()));
                            break;
                        }
                        marker_deletes.push(marker.queue.as_bytes().to_vec());
                    }
                }
                outcome.completed = index + 1;
            }
            let mut extra_deletes = Some(marker_deletes);
            for (scope, releases) in by_scope {
                let commit = self
                    .commit_tip_changes(TipChangeSet {
                        scope: scope.as_deref(),
                        releases,
                        deletes: extra_deletes.take().unwrap_or_default(),
                        ..TipChangeSet::default()
                    })
                    .await?;
                outcome.released += commit.records_released;
            }
            if let Some(deletes) = extra_deletes.filter(|deletes| !deletes.is_empty()) {
                self.commit_tip_changes(TipChangeSet {
                    deletes,
                    ..TipChangeSet::default()
                })
                .await?;
            }
            if let Some((index, queue)) = retained.filter(|_| outcome.released == 0) {
                // The marker still holds records and this batch ended none (a
                // row that does not decode, or rows of other slots in the way).
                // Drop the marker, so it cannot hold back the queue behind it.
                // The records wait for their own windows.
                tracing::error!(%queue, "a slot drain made no progress; the marker is dropped");
                self.retention_state().note_undecodable();
                self.commit_tip_changes(TipChangeSet {
                    deletes: vec![queue.into_bytes()],
                    ..TipChangeSet::default()
                })
                .await?;
                outcome.completed = index + 1;
            }
            Ok(outcome)
        })
        .await
    }

    fn add_requests(
        entries: &[RetentionEntry],
        by_scope: &mut BTreeMap<Option<String>, Vec<ReleaseRequest>>,
        outcome: &mut ReleaseOutcome,
    ) {
        for entry in entries {
            match entry.scope() {
                Ok(scope) => by_scope
                    .entry(scope.map(str::to_string))
                    .or_default()
                    .push(entry.request()),
                Err(error) => {
                    tracing::error!(%error, "retention entry has no valid scope");
                    outcome.skipped += 1;
                }
            }
        }
    }

    /// The due queue keys, read again in the gate and decoded, in key order.
    async fn read_due_items(
        &self,
        due: &[String],
        now_nanos: u64,
    ) -> Result<Vec<Due>, SchemaError> {
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
            match keys::queue_expiry(key) {
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
                Ok(QueueValue::Drain(marker)) => items.push(Due::Drain(marker)),
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
