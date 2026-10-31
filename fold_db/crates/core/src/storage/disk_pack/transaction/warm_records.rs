//! Record values that a native write request puts, kept for the record warm
//! set until the flush puts the request on disk.

use super::super::record_warm::WrittenRecord;
use super::PendingBatch;

#[derive(Default)]
pub(super) struct WarmRecords {
    records: Vec<WrittenRecord>,
    bytes: usize,
}

impl PendingBatch {
    /// Keep record values for the warm set until the flush, up to `limit`
    /// encoded bytes for this request; the rest enter on a later read.
    pub(in crate::storage::disk_pack) fn stage_warm_records(
        &self,
        records: Vec<WrittenRecord>,
        limit: usize,
    ) {
        let mut staged = self.warm_records.lock().expect("native warm records");
        for record in records {
            let Some(bytes) = staged
                .bytes
                .checked_add(record.bytes())
                .filter(|bytes| *bytes <= limit)
            else {
                break;
            };
            staged.bytes = bytes;
            staged.records.push(record);
        }
    }

    pub(in crate::storage::disk_pack) fn take_warm_records(&self) -> Vec<WrittenRecord> {
        let mut staged = self.warm_records.lock().expect("native warm records");
        staged.bytes = 0;
        std::mem::take(&mut staged.records)
    }
}
