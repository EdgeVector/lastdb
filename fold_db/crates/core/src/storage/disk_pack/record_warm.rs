//! The record warm set of a format 7 home (decision D2).
//!
//! A read by record key checks memory first. A record that the read finds on
//! disk enters memory only while the view still has the revision of the
//! memory lookup, so a write that committed in between cannot leave an old
//! value. A record that a write puts enters memory after the flush puts the
//! request on disk, under the same view lock that publishes the request.
//!
//! A native write request reads disk only: it must see its own private
//! writes. A call whose captured view is older than the current view also
//! reads disk only, so one call never mixes two revisions.

use super::*;
use crate::resident::RecordCoordinate;
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

/// The memory answer of one read by record key.
pub(crate) struct WarmRecordRead {
    /// The view revision of the memory answer.
    revision: u64,
    coordinates: Vec<RecordCoordinate>,
    found: Vec<Option<Arc<StoredRecord>>>,
}

impl WarmRecordRead {
    /// The memory value at each key position, in the order given. `None`
    /// reads disk.
    pub(crate) fn take_found(&mut self) -> Vec<Option<Arc<StoredRecord>>> {
        std::mem::take(&mut self.found)
    }
}

/// One record value that a native write request puts.
pub(crate) struct WrittenRecord {
    coordinate: RecordCoordinate,
    record: Arc<StoredRecord>,
    bytes: usize,
    digest: u64,
}

impl WrittenRecord {
    /// `value` is the encoded record that the request puts at `key`.
    pub(crate) fn new(key: &str, record: Arc<StoredRecord>, value: &[u8]) -> Self {
        Self {
            coordinate: kv::coordinate("main", key.as_bytes()),
            record,
            bytes: value.len(),
            digest: digest(value),
        }
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl PackRuntime {
    /// Look up records by storage key in memory. `None` when this read must
    /// read disk only.
    pub(crate) fn warm_records(&self, keys: &[String]) -> StorageResult<Option<WarmRecordRead>> {
        if transaction::PendingBatch::current(self).is_some() {
            return Ok(None);
        }
        let coordinates: Vec<_> = keys
            .iter()
            .map(|key| kv::coordinate("main", key.as_bytes()))
            .collect();
        let (revision, found) = match read::ReadContext::captured(self) {
            Some(context) => {
                let Some(mut held) = logical_paths::lock_current_receipt(
                    self,
                    &context,
                    "native record warm view",
                    "native record warm set",
                ) else {
                    return Ok(None);
                };
                let found = held.core.record_lookup(&coordinates);
                // A memory hit costs the call the same read allowance as disk.
                for (key, hit) in keys.iter().zip(&found) {
                    if let Some((_, bytes)) = hit {
                        context.account_memory_record(&mut held.call, key.len(), *bytes)?;
                    }
                }
                (context.revision, found)
            }
            None => {
                let view = self.view.read().expect("native record warm view");
                let found = self
                    .core
                    .lock()
                    .expect("native record warm set")
                    .record_lookup(&coordinates);
                (view.revision, found)
            }
        };
        let hits = found.iter().flatten().count();
        for _ in 0..hits {
            crate::read_receipts::warm_hit();
        }
        laststore::record_logical_warm_misses((found.len() - hits) as u64);
        Ok(Some(WarmRecordRead {
            revision,
            coordinates,
            found: found
                .into_iter()
                .map(|hit| hit.map(|(record, _)| record))
                .collect(),
        }))
    }

    /// Records that a disk read found after `read` missed them: the key
    /// position, the decoded record and its encoded length.
    pub(crate) fn admit_read_records(
        &self,
        read: &WarmRecordRead,
        records: Vec<(usize, Arc<StoredRecord>, usize)>,
    ) {
        if records.is_empty() {
            return;
        }
        let records = records
            .into_iter()
            .map(|(at, record, bytes)| (read.coordinates[at].clone(), record, bytes))
            .collect();
        let view = self.view.read().expect("native record warm view");
        if view.revision != read.revision {
            return;
        }
        self.core
            .lock()
            .expect("native record warm set")
            .admit_records(records);
    }

    /// Stage the records that the current native write request puts. They
    /// enter memory after the flush puts the request on disk. Staged bytes
    /// above a quarter of the warm byte limit enter on a later read instead.
    pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
        if let Some(pending) = transaction::PendingBatch::current(self) {
            pending.stage_warm_records(records, crate::resident::core_warm_bytes() / 4);
        }
    }
}

/// The records that the committed requests put, in commit order. A record
/// enters only when its request's final body at the key is the staged value
/// and no later request changed the key. The caller publishes them under the
/// view lock of the same commit.
pub(super) fn committed_records(
    requests: &[Arc<transaction::PendingBatch>],
) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
    let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
    for request in requests {
        let staged = request.take_warm_records();
        if latest.is_empty() && staged.is_empty() {
            continue;
        }
        if !latest.is_empty() {
            for coordinate in request.changed_coordinates() {
                latest.remove(&coordinate);
            }
        }
        for written in staged {
            let (collection, key) = &written.coordinate;
            let current = request.lookup(collection, key).is_some_and(|pending| {
                pending.body.as_ref().is_some_and(|body| {
                    body.len() == written.bytes && digest(body) == written.digest
                })
            });
            if current {
                latest.insert(written.coordinate, (written.record, written.bytes));
            }
        }
    }
    latest
        .into_iter()
        .map(|(coordinate, (record, bytes))| (coordinate, record, bytes))
        .collect()
}

fn digest(value: &[u8]) -> u64 {
    BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
}
