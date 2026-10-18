//! Private preparation reads its existing immutable flush-log prefix.

use super::super::transaction::{PendingBatch, PendingRecord};
use super::ReadContext;
use std::collections::BTreeMap;
use std::sync::Arc;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn overlay_record(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        key: &str,
    ) -> Option<Arc<PendingRecord>> {
        pending
            .and_then(|batch| batch.lookup(collection, key))
            .or_else(|| {
                self.accepted
                    .iter()
                    .rev()
                    .find_map(|batch| batch.lookup(collection, key))
            })
    }

    pub(in crate::storage::disk_pack) fn overlay_rows(
        &self,
        pending: Option<&Arc<PendingBatch>>,
        collection: &str,
        start: &[u8],
        end: &[u8],
    ) -> Vec<Arc<PendingRecord>> {
        let mut rows = BTreeMap::new();
        for batch in self.accepted.iter() {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        if let Some(batch) = pending {
            for record in batch.matching(collection, start, end) {
                rows.insert(record.key.clone(), record);
            }
        }
        rows.into_values().collect()
    }
}
