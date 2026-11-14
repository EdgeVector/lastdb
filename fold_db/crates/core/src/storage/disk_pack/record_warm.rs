//! Warm-record lookup, staged write tracking, and post-commit warm admission.

use super::read::{unpoison, ReadContext};
use super::transaction::PendingBatch;
use super::{kv, PackRuntime, StorageResult};
use crate::resident::{core_warm_bytes, RecordCoordinate};
use crate::storage::format::MAIN_NAMESPACE;
use lastdb_model::record_value::StoredRecord;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
use std::sync::Arc;

pub(crate) struct WarmRecordRead {
    pub(super) revision: u64,
    pub(super) coordinates: Vec<RecordCoordinate>,
    pub(super) found: Vec<Option<Arc<StoredRecord>>>,
}

impl WarmRecordRead {
    pub(crate) fn take_found(&mut self) -> Vec<Option<Arc<StoredRecord>>> {
        std::mem::take(&mut self.found)
    }
}

#[derive(Default)]
pub(in crate::storage::disk_pack) struct StagedWarmRecords {
    pub(super) records: Vec<WrittenRecord>,
    pub(super) total_bytes: usize,
}

pub(crate) struct WrittenRecord {
    coordinate: RecordCoordinate,
    record: Arc<StoredRecord>,
    encoded_bytes: usize,
    digest: u64,
}

impl WrittenRecord {
    pub(crate) fn new(key: &str, record: Arc<StoredRecord>, encoded: &[u8]) -> Self {
        Self {
            coordinate: kv::coordinate(MAIN_NAMESPACE, key.as_bytes()),
            record,
            encoded_bytes: encoded.len(),
            digest: digest(encoded),
        }
    }
}

impl PackRuntime {
    pub(crate) fn warm_records<K: AsRef<str>>(
        &self,
        keys: &[K],
    ) -> StorageResult<Option<WarmRecordRead>> {
        if PendingBatch::current(self).is_some() {
            return Ok(None);
        }
        let view = self.view.read().expect("native record warm view");
        let ctx = ReadContext::captured(self);
        if ctx.as_ref().is_some_and(|c| c.revision != view.revision) {
            return Ok(None);
        }
        let coordinates: Vec<_> = keys
            .iter()
            .map(|key| kv::coordinate(MAIN_NAMESPACE, key.as_ref().as_bytes()))
            .collect();
        let mut reader = ctx.as_ref().map(|c| unpoison(&c.reader));
        let hits = self
            .core
            .lock()
            .expect("native record warm set")
            .record_lookup(&coordinates);
        if let (Some(ctx), Some(reader)) = (&ctx, &mut reader) {
            for (key, hit) in keys.iter().zip(&hits) {
                if let Some((_, bytes)) = hit {
                    ctx.account_memory_record(reader, key.as_ref().len(), *bytes)?;
                }
            }
        }
        let found = hits
            .into_iter()
            .map(|hit| hit.map(|(rec, _)| rec))
            .collect();
        Ok(Some(WarmRecordRead {
            revision: view.revision,
            coordinates,
            found,
        }))
    }

    pub(crate) fn admit_read_records(
        &self,
        read: &WarmRecordRead,
        records: Vec<(usize, Arc<StoredRecord>, usize)>,
    ) {
        if !records.is_empty()
            && self.view.read().expect("native record warm view").revision == read.revision
        {
            let items = records
                .into_iter()
                .map(|(at, rec, bytes)| (read.coordinates[at].clone(), rec, bytes))
                .collect();
            unpoison(&self.core).admit_records(items);
        }
    }

    pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
        if let Some(pending) = PendingBatch::current(self) {
            let limit = core_warm_bytes() / 4;
            let mut staged = pending.warm_records.lock().expect("native warm records");
            for item in records {
                let Some(next_bytes) = staged
                    .total_bytes
                    .checked_add(item.encoded_bytes)
                    .filter(|&bytes| bytes <= limit)
                else {
                    break;
                };
                staged.total_bytes = next_bytes;
                staged.records.push(item);
            }
        }
    }
}

pub(in crate::storage::disk_pack) fn committed_records(
    requests: &[Arc<PendingBatch>],
) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
    let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
    for request in requests {
        let staged =
            std::mem::take(&mut *request.warm_records.lock().expect("native warm records")).records;
        if latest.is_empty() && staged.is_empty() {
            continue;
        }
        for changed in request.changed_coordinates() {
            latest.remove(&changed);
        }
        for item in staged {
            let matches = request
                .lookup(&item.coordinate.0, &item.coordinate.1)
                .is_some_and(|pending| {
                    pending
                        .body
                        .as_deref()
                        .is_some_and(|b| b.len() == item.encoded_bytes && digest(b) == item.digest)
                });
            if matches {
                latest.insert(item.coordinate, (item.record, item.encoded_bytes));
            }
        }
    }
    latest
        .into_iter()
        .map(|(coord, (rec, bytes))| (coord, rec, bytes))
        .collect()
}

fn digest(value: &[u8]) -> u64 {
    BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
}
